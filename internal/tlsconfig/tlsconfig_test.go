package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"testing"
)

func TestHardenEnforcesVerificationWithoutMutatingInput(t *testing.T) {
	roots := x509.NewCertPool()
	input := &tls.Config{
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS10,
		RootCAs:            roots,
		ServerName:         "news.example.test",
	}

	got := Harden(input)

	if got == input {
		t.Fatal("Harden returned the caller-owned TLS config")
	}
	if got.InsecureSkipVerify {
		t.Fatal("Harden left certificate verification disabled")
	}
	if got.MinVersion != tls.VersionTLS12 {
		t.Fatalf("minimum TLS version = %x, want TLS 1.2", got.MinVersion)
	}
	if got.RootCAs != roots {
		t.Fatal("Harden did not preserve custom trust roots")
	}
	if got.ServerName != input.ServerName {
		t.Fatalf("server name = %q, want %q", got.ServerName, input.ServerName)
	}
	if !input.InsecureSkipVerify || input.MinVersion != tls.VersionTLS10 {
		t.Fatal("Harden mutated the caller-owned TLS config")
	}
}

func TestHardenPreservesStricterMinimumVersion(t *testing.T) {
	got := Harden(&tls.Config{MinVersion: tls.VersionTLS13})
	if got.MinVersion != tls.VersionTLS13 {
		t.Fatalf("minimum TLS version = %x, want TLS 1.3", got.MinVersion)
	}
}
