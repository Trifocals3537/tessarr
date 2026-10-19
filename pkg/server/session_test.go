package server

import (
	"net/http"
	"testing"
)

func TestSessionCookieDefaultsAreBrowserSafe(t *testing.T) {
	store := newSessionCookieStore("test-secret", false)
	options := store.Options

	if !options.HttpOnly {
		t.Fatal("session cookie must be HttpOnly")
	}
	if options.SameSite != http.SameSiteLaxMode {
		t.Fatalf("SameSite = %v, want Lax", options.SameSite)
	}
	if options.MaxAge != 7*24*60*60 {
		t.Fatalf("MaxAge = %d, want seven days", options.MaxAge)
	}
}

func TestSessionCookieCanRequireHTTPS(t *testing.T) {
	store := newSessionCookieStore("test-secret", true)
	if !store.Options.Secure {
		t.Fatal("session cookie must be Secure when configured behind HTTPS")
	}
}
