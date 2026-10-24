package realdebrid

import (
	"context"
	"github.com/Trifocals3537/tessarr/internal/request"
	"github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCleanupPresenceRequiresAuthoritativeEvidence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   common.TorrentPresence
	}{
		{"present", 200, `{"id":"transfer-1"}`, common.TorrentPresent},
		{"typed-absence", 404, `{"error":"Resource not found","error_code":7}`, common.TorrentAbsent},
		{"proxy-404", 404, `<html>not found</html>`, common.TorrentPresenceUnknown},
		{"generic-json-404", 404, `{"error":"not found"}`, common.TorrentPresenceUnknown},
		{"auth-error", 401, `{"error":"Bad token","error_code":8}`, common.TorrentPresenceUnknown},
		{"wrong-error-code", 404, `{"error":"Permission denied","error_code":9}`, common.TorrentPresenceUnknown},
		{"missing-error-message", 404, `{"error_code":7}`, common.TorrentPresenceUnknown},
		{"wrong-id", 200, `{"id":"other"}`, common.TorrentPresenceUnknown},
		{"error-on-success", 200, `{"id":"transfer-1","error_code":7}`, common.TorrentPresenceUnknown},
		{"server-fault", 500, `{"error":"Resource not found","error_code":7}`, common.TorrentPresenceUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/torrents/info/transfer-1" {
					t.Errorf("unexpected mutation or endpoint: %s %s", r.Method, r.URL)
				}

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client := &RealDebrid{Host: server.URL, client: request.New(request.WithMaxRetries(0))}
			presence, err := client.TorrentPresenceContext(context.Background(), "transfer-1")
			if presence != tc.want {
				t.Fatalf("presence=%v error=%v want=%v", presence, err, tc.want)
			}
			if tc.want == common.TorrentPresenceUnknown && err == nil {
				t.Fatal("unverifiable response was not surfaced")
			}
			if tc.want != common.TorrentPresenceUnknown && err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if presence, err := client.TorrentPresenceContext(ctx, "transfer-1"); presence != common.TorrentPresenceUnknown || err == nil {
				t.Fatal("canceled lookup supplied presence evidence")
			}
		})
	}
}
