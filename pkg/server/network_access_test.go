package server

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
)

func TestClientAddressAllowed(t *testing.T) {
	allowed := []netip.Prefix{
		netip.MustParsePrefix("127.0.0.0/8"),
		netip.MustParsePrefix("192.0.2.10/32"),
		netip.MustParsePrefix("fd7a:115c:a1e0::/48"),
	}

	tests := []struct {
		name       string
		remoteAddr string
		allowed    []netip.Prefix
		want       bool
	}{
		{
			name:       "empty policy preserves allow all",
			remoteAddr: "203.0.113.5:43210",
			want:       true,
		},
		{
			name:       "loopback proxy",
			remoteAddr: "127.0.0.1:43210",
			allowed:    allowed,
			want:       true,
		},
		{
			name:       "IPv4 mapped loopback proxy",
			remoteAddr: "[::ffff:127.0.0.1]:43210",
			allowed:    allowed,
			want:       true,
		},
		{
			name:       "private listener address",
			remoteAddr: "192.0.2.10:43210",
			allowed:    allowed,
			want:       true,
		},
		{
			name:       "tailnet IPv6",
			remoteAddr: "[fd7a:115c:a1e0::1234]:43210",
			allowed:    allowed,
			want:       true,
		},
		{
			name:       "direct Internet client",
			remoteAddr: "203.0.113.5:43210",
			allowed:    allowed,
		},
		{
			name:       "malformed address fails closed",
			remoteAddr: "not-an-address",
			allowed:    allowed,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := clientAddressAllowed(
				test.remoteAddr,
				test.allowed,
			); got != test.want {
				t.Fatalf(
					"clientAddressAllowed(%q) = %t, want %t",
					test.remoteAddr,
					got,
					test.want,
				)
			}
		})
	}
}

func TestClientNetworkMiddlewareIgnoresForwardedAddress(t *testing.T) {
	server := &Server{
		allowedClients: []netip.Prefix{
			netip.MustParsePrefix("127.0.0.0/8"),
		},
	}
	handler := server.clientNetworkMiddleware(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		},
	))

	request := httptest.NewRequest(http.MethodGet, "/version", nil)
	request.RemoteAddr = "203.0.113.5:43210"
	request.Header.Set("X-Forwarded-For", "127.0.0.1")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf(
			"status = %d, want %d",
			response.Code,
			http.StatusForbidden,
		)
	}
}
