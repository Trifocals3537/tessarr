// Package httpsecurity shares browser-origin checks across the control plane
// and download-client compatibility interfaces.
package httpsecurity

import (
	"net/http"
	"net/url"
	"strings"
)

func BrowserOriginPresent(r *http.Request) bool {
	return r != nil && (strings.TrimSpace(r.Header.Get("Origin")) != "" ||
		strings.TrimSpace(r.Header.Get("Referer")) != "")
}

// ProtocolRequestAllowed preserves native clients without browser origin
// headers. Browser requests must match the listener or configured public URL,
// including GET requests: compatibility APIs also mutate state through GET.
func ProtocolRequestAllowed(r *http.Request, appURL string) bool {
	if r == nil {
		return false
	}
	if BrowserOriginPresent(r) {
		return RequestOriginAllowed(r, appURL)
	}
	// A browser can suppress Referer on a GET mutation. Fetch Metadata still
	// distinguishes that cross-origin request from a native Arr client.
	switch strings.ToLower(strings.TrimSpace(r.Header.Get("Sec-Fetch-Site"))) {
	case "", "none", "same-origin":
		return true
	default:
		return false
	}
}

func RequestOriginAllowed(r *http.Request, appURL string) bool {
	if r == nil {
		return false
	}
	rawOrigin := strings.TrimSpace(r.Header.Get("Origin"))
	if rawOrigin == "" {
		rawOrigin = strings.TrimSpace(r.Header.Get("Referer"))
	}
	if rawOrigin == "" || strings.EqualFold(rawOrigin, "null") {
		return false
	}
	origin, err := url.Parse(rawOrigin)
	if err != nil || origin.Host == "" ||
		(!strings.EqualFold(origin.Scheme, "http") && !strings.EqualFold(origin.Scheme, "https")) {
		return false
	}
	listenerScheme := "http"
	if r.TLS != nil {
		listenerScheme = "https"
	}
	// The connection determines the listener transport. Proxy headers cannot
	// authorize another scheme; reverse proxies use the explicit appURL below.
	if strings.EqualFold(origin.Scheme, listenerScheme) && sameOriginHost(origin.Host, r.Host, listenerScheme) {
		return true
	}
	configured, err := url.Parse(strings.TrimSpace(appURL))
	return err == nil && configured.Host != "" &&
		strings.EqualFold(origin.Scheme, configured.Scheme) &&
		sameOriginHost(origin.Host, configured.Host, origin.Scheme)
}

func sameOriginHost(left, right, scheme string) bool {
	leftURL, leftErr := url.Parse("//" + strings.TrimSpace(left))
	rightURL, rightErr := url.Parse("//" + strings.TrimSpace(right))
	if leftErr != nil || rightErr != nil || leftURL.Hostname() == "" || rightURL.Hostname() == "" {
		return false
	}
	defaultPort := ""
	switch strings.ToLower(scheme) {
	case "http":
		defaultPort = "80"
	case "https":
		defaultPort = "443"
	}
	leftPort, rightPort := leftURL.Port(), rightURL.Port()
	if leftPort == "" {
		leftPort = defaultPort
	}
	if rightPort == "" {
		rightPort = defaultPort
	}
	return strings.EqualFold(leftURL.Hostname(), rightURL.Hostname()) && leftPort == rightPort
}
