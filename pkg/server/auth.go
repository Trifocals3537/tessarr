package server

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/config"
	"golang.org/x/crypto/bcrypt"
)

func (s *Server) verifyAuth(username, password string) bool {
	// If you're storing hashed password, use bcrypt to compare
	if username == "" {
		return false
	}
	auth := config.Get().GetAuth()
	if auth == nil {
		return false
	}
	if username != auth.Username {
		return false
	}
	err := bcrypt.CompareHashAndPassword([]byte(auth.Password), []byte(password))
	return err == nil
}

func (s *Server) skipAuthHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	// Only allow skipping auth during initial setup (before setup is complete)
	if err := cfg.SetupComplete(); err == nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if !isLoopbackBindAddress(cfg.BindAddress) {
		http.Error(
			w,
			"Authentication cannot be disabled on a non-loopback listener",
			http.StatusForbidden,
		)
		return
	}
	if !s.requireBrowserMutation(w, r) {
		return
	}
	result, err := config.Update(func(draft *config.Config) error {
		draft.UseAuth = false
		return nil
	})
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to save config")
		http.Error(w, "failed to save config", http.StatusInternalServerError)
		return
	}
	redirectLocal(w, result.Active.URLBase, "", http.StatusSeeOther)
}

// isValidAPIToken checks if the request contains a valid API token
func (s *Server) isValidAPIToken(r *http.Request) bool {
	// Check Authorization header for Bearer token
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return false
	}

	// Support both "Bearer <token>" and "Token <token>" formats
	var token string
	if after, ok := strings.CutPrefix(authHeader, "Bearer "); ok {
		token = after
	} else if after, ok := strings.CutPrefix(authHeader, "Token "); ok {
		token = after
	} else {
		return false
	}

	if token == "" {
		return false
	}

	// GetReader auth config and check if token exists
	auth := config.Get().GetAuth()
	if auth == nil || auth.APIToken == "" {
		return false
	}

	// Check if the provided token matches the configured token
	return subtle.ConstantTimeCompare([]byte(token), []byte(auth.APIToken)) == 1
}

// generateAPIToken creates a new random API token
func (s *Server) generateAPIToken() (string, error) {
	bytes := make([]byte, 32) // 256-bit token
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

// refreshAPIToken generates a new API token and saves it
func (s *Server) refreshAPIToken() (string, error) {
	if config.Get().GetAuth() == nil {
		return "", fmt.Errorf("authentication not configured")
	}

	// Generate new token
	token, err := s.generateAPIToken()
	if err != nil {
		return "", err
	}

	_, err = config.Update(func(draft *config.Config) error {
		if draft.Auth == nil {
			return fmt.Errorf("authentication not configured")
		}
		draft.Auth.APIToken = token
		return nil
	})
	if err != nil {
		return "", err
	}

	return token, nil
}
