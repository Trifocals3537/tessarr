package premiumize

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
		{"present", 200, `{"status":"success","transfers":[{"id":"transfer-1","status":"running"}]}`, common.TorrentPresent},
		{"list-miss", 200, `{"status":"success","transfers":[]}`, common.TorrentPresenceUnknown},
		{"missing-list", 200, `{"status":"success"}`, common.TorrentPresenceUnknown},
		{"auth-error", 401, `{"status":"error","code":"bad_token"}`, common.TorrentPresenceUnknown},
		{"proxy-404", 404, `<html>missing</html>`, common.TorrentPresenceUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/api/transfer/list" {
					t.Errorf("unexpected mutation or endpoint: %s %s", r.Method, r.URL)
				}

				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client := &Premiumize{Host: server.URL, client: request.New(request.WithMaxRetries(0))}
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
