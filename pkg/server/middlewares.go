package server

import (
	"fmt"
	"net/http"
	"net/netip"
	"strings"

	json "github.com/bytedance/sonic"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func (s *Server) clientNetworkMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.accessPolicyErr != nil {
			http.Error(w, "Client network policy is invalid", http.StatusServiceUnavailable)
			return
		}
		if !clientAddressAllowed(r.RemoteAddr, s.allowedClients) {
			// Keep this at debug level: an Internet scan must not be able to
			// turn a correctly-blocked request into unbounded warning logs.
			s.logger.Debug().
				Str("remote_address", r.RemoteAddr).
				Msg("Rejected request outside allowed client networks")
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func clientAddressAllowed(remoteAddress string, allowed []netip.Prefix) bool {
	if len(allowed) == 0 {
		return true
	}

	remoteAddress = strings.TrimSpace(remoteAddress)
	addressPort, err := netip.ParseAddrPort(remoteAddress)
	var address netip.Addr
	if err == nil {
		address = addressPort.Addr()
	} else {
		address, err = netip.ParseAddr(remoteAddress)
	}
	if err != nil {
		return false
	}
	address = address.Unmap().WithZone("")
	for _, prefix := range allowed {
		if prefix.Contains(address) {
			return true
		}
	}
	return false
}

func (s *Server) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if setup is needed
		cfg := config.Get()
		if !cfg.UseAuth {
			// Preserve credentialless local API clients, which normally omit
			// browser origin headers, while rejecting a browser page trying to
			// mutate the loopback control plane from another origin.
			if !isSafeHTTPMethod(r.Method) && browserOriginPresent(r) &&
				!requestOriginAllowed(r, cfg.AppURL) {
				if s.isAPIRequest(r) {
					s.sendJSONError(w, "Cross-site request rejected", http.StatusForbidden)
				} else {
					http.Error(w, "Cross-site request rejected", http.StatusForbidden)
				}
				return
			}
			next.ServeHTTP(w, r)
			return
		}

		isAPI := s.isAPIRequest(r)

		if cfg.NeedsAuth() {
			if isAPI {
				s.sendJSONError(w, "Authentication setup required", http.StatusUnauthorized)
			} else {
				redirectLocal(w, cfg.URLBase, "register", http.StatusSeeOther)
			}
			return
		}

		// Check for API token first
		if s.isValidAPIToken(r) {
			next.ServeHTTP(w, r)
			return
		}

		// Fall back to a versioned browser session. Credential changes bump the
		// version in auth.json, invalidating every previously issued cookie.
		if !s.sessionAuthenticated(r) {
			if isAPI {
				s.sendJSONError(w, "Authentication required. Please provide a valid API token in the Authorization header (Bearer <token>) or authenticate via session cookies.", http.StatusUnauthorized)
			} else {
				redirectLocal(w, cfg.URLBase, "login", http.StatusSeeOther)
			}
			return
		}
		if !s.browserMutationAllowed(r) {
			if isAPI {
				s.sendJSONError(w, "Cross-site request rejected", http.StatusForbidden)
			} else {
				http.Error(w, "Cross-site request rejected", http.StatusForbidden)
			}
			return
		}

		next.ServeHTTP(w, r)
	})
}

// isAPIRequest checks if the request is for an API endpoint
func (s *Server) isAPIRequest(r *http.Request) bool {
	path := pathWithoutURLBase(r.URL.Path, s.urlBase)
	return strings.HasPrefix(path, "/api/") ||
		strings.HasPrefix(path, "/webhooks/")
}

// sendJSONError sends a JSON error response
func (s *Server) sendJSONError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	err := json.ConfigDefault.NewEncoder(w).Encode(map[string]any{
		"error":  message,
		"status": statusCode,
	})
	if err != nil {
		return
	}
}

// setupRedirectMiddleware redirects to /setup if setup is not completed
func (s *Server) setupRedirectMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cfg := config.Get()

		path := pathWithoutURLBase(r.URL.Path, s.urlBase)
		// Skip setup check for setup-related routes
		if strings.HasPrefix(path, "/setup") ||
			strings.HasPrefix(path, "/api/setup") ||
			strings.HasPrefix(path, "/api/login") ||
			strings.HasPrefix(path, "/api/logout") ||
			strings.HasPrefix(path, "/api/config") ||
			strings.HasPrefix(path, "/assets") ||
			strings.HasPrefix(path, "/images") ||
			path == "/live" ||
			path == "/ready" ||
			path == "/version" {
			next.ServeHTTP(w, r)
			return
		}

		// Check if setup is completed
		if err := cfg.SetupComplete(); err != nil {
			isAPI := s.isAPIRequest(r)
			if isAPI {
				s.sendJSONError(w, fmt.Sprintf("[error] %s Setup wizard must be completed first. Please visit /setup", err), http.StatusServiceUnavailable)
			} else {
				redirectLocal(w, cfg.URLBase, "setup", http.StatusSeeOther)
			}
			return
		}

		next.ServeHTTP(w, r)
	})
}
