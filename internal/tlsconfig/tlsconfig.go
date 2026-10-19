// Package tlsconfig centralizes the minimum transport-security policy used by
// outbound connections.
package tlsconfig

import "crypto/tls"

// Verified returns a TLS configuration that verifies the peer with the
// platform trust store and requires TLS 1.2 or newer. An empty serverName is
// appropriate for net/http, which supplies the request host during dialing.
func Verified(serverName string) *tls.Config {
	return Harden(&tls.Config{ServerName: serverName})
}

// Harden clones a caller-provided TLS configuration and applies Tessarr's
// minimum verification policy without mutating the caller's configuration.
// Existing trust roots, client certificates, and stricter TLS versions are
// preserved.
func Harden(base *tls.Config) *tls.Config {
	var secured *tls.Config
	if base == nil {
		secured = &tls.Config{}
	} else {
		secured = base.Clone()
	}

	secured.InsecureSkipVerify = false
	if secured.MinVersion < tls.VersionTLS12 {
		secured.MinVersion = tls.VersionTLS12
	}
	return secured
}
