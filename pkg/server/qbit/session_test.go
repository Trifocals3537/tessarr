package qbit

import (
	"strings"
	"testing"
	"time"
)

func TestQbitSessionTokenIsOpaqueAndExpires(t *testing.T) {
	store := newQbitSessionStore()
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }

	token, err := store.create("http://sonarr:8989", "super-secret-token")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(token, "sonarr") || strings.Contains(token, "secret") {
		t.Fatalf("session token contains credentials: %q", token)
	}
	username, password, ok := store.credentials(token)
	if !ok || username != "http://sonarr:8989" || password != "super-secret-token" {
		t.Fatalf("session lookup = %q, %q, %v", username, password, ok)
	}

	now = now.Add(qbitSessionTTL)
	if _, _, ok := store.credentials(token); ok {
		t.Fatal("expired session remained valid")
	}
}
