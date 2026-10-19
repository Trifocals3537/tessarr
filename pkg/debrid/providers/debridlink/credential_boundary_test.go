package debridlink

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestCheckFileDoesNotSendAPICredentialToSignedURL(t *testing.T) {
	provider := newDebridLinkTestProvider(t, config.Debrid{
		Name:      "debridlink",
		APIKey:    "main-token",
		UserAgent: "Tessarr-Test",
	}, func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("Authorization = %q, want no API credential on signed URL", got)
		}
		if got := r.Header.Get("User-Agent"); got != "Tessarr-Test" {
			t.Errorf("User-Agent = %q, want configured non-secret header", got)
		}
		if got := r.Header.Get("Range"); got != "bytes=0-0" {
			t.Errorf("Range = %q, want one-byte probe", got)
		}
		_, _ = io.WriteString(w, "x")
	})

	if err := provider.CheckFile(context.Background(), "", provider.Host+"/signed"); err != nil {
		t.Fatal(err)
	}
}

func TestSpeedTestKeepsCredentialsOnAPIAndOffSignedURL(t *testing.T) {
	provider := newDebridLinkTestProvider(t, config.Debrid{
		Name:            "debridlink",
		APIKey:          "main-token",
		DownloadAPIKeys: []string{"download-token"},
		UserAgent:       "Tessarr-Test",
	}, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/account/infos":
			if got := r.Header.Get("Authorization"); got != "Bearer main-token" {
				t.Errorf("API Authorization = %q, want main API credential", got)
			}
			w.WriteHeader(http.StatusOK)
		case "/signed":
			if got := r.Header.Get("Authorization"); got != "" {
				t.Errorf("signed URL Authorization = %q, want no provider credential", got)
			}
			if got := r.Header.Get("User-Agent"); got != "Tessarr-Test" {
				t.Errorf("User-Agent = %q, want configured non-secret header", got)
			}
			if got := r.Header.Get("Range"); got != "bytes=0-1048575" {
				t.Errorf("Range = %q, want one-megabyte speed probe", got)
			}
			// Keep the mock transfer longer than the coarsest supported clock
			// tick so Windows cannot report a zero-duration successful probe.
			time.Sleep(10 * time.Millisecond)
			_, _ = io.WriteString(w, strings.Repeat("x", 1024))
		default:
			t.Errorf("unexpected request path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})

	provider.accountsManager.StoreDownloadLink(types.DownloadLink{
		Debrid:       "debridlink",
		Token:        "download-token",
		Link:         "source-link",
		DownloadLink: provider.Host + "/signed",
	})
	result := provider.SpeedTest(context.Background())
	if result.Error != "" || result.BytesRead != 1024 || result.SpeedMBps <= 0 {
		t.Fatalf("SpeedTest() = %#v, want successful credential-free CDN probe", result)
	}
}
