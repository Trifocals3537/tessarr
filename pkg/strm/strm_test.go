package strm

import (
	"net/url"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

const testSecret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestFileURLRoundTripAndSignature(t *testing.T) {
	base, err := url.Parse("https://media.example/tessarr")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := FileURL(base, testSecret, "entry-id", "file-id", "Movie 100% Ready.mkv")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(raw, "%2525") {
		t.Fatalf("display name was double escaped: %s", raw)
	}
	entryID, fileID, ok := ParseOwnedURL(raw, testSecret)
	if !ok || entryID != "entry-id" || fileID != "file-id" {
		t.Fatalf("ParseOwnedURL(%q) = %q, %q, %t", raw, entryID, fileID, ok)
	}

	tampered := strings.Replace(raw, "/file-id/", "/other-id/", 1)
	if _, _, ok := ParseOwnedURL(tampered, testSecret); ok {
		t.Fatal("tampered identity passed signature verification")
	}
}

func TestBaseURLUsesAppURLAndURLBase(t *testing.T) {
	cfg := &config.Config{AppURL: "https://media.example", URLBase: "/tessarr"}
	base, err := BaseURL(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := base.String(); got != "https://media.example/tessarr" {
		t.Fatalf("BaseURL = %q", got)
	}
}

func TestSignRejectsInvalidSecret(t *testing.T) {
	if _, err := Sign("not-a-key", "stream", "entry", "file"); err == nil {
		t.Fatal("expected invalid secret error")
	}
}
