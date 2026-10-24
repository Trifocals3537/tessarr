package debridlink

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
		{"present", 200, `{"success":true,"value":[{"id":"transfer-1"}]}`, common.TorrentPresent},
		{"exact-empty", 200, `{"success":true,"value":[]}`, common.TorrentAbsent},
		{"complete-empty", 200, `{"success":true,"value":[],"pagination":{"page":0,"pages":1,"next":-1}}`, common.TorrentAbsent},
		{"partial-empty", 200, `{"success":true,"value":[],"pagination":{"page":0,"pages":2,"next":1}}`, common.TorrentPresenceUnknown},
		{"wrong-page", 200, `{"success":true,"value":[],"pagination":{"page":1,"pages":2,"next":-1}}`, common.TorrentPresenceUnknown},
		{"wrong-id", 200, `{"success":true,"value":[{"id":"other"}]}`, common.TorrentPresenceUnknown},
		{"null-value", 200, `{"success":true,"value":null}`, common.TorrentPresenceUnknown},
		{"missing-value", 200, `{"success":true}`, common.TorrentPresenceUnknown},
		{"api-error", 200, `{"success":false,"error":"badToken","value":[]}`, common.TorrentPresenceUnknown},
		{"auth-error", 401, `{"success":false,"error":"badToken"}`, common.TorrentPresenceUnknown},
		{"proxy-404", 404, `<html>missing</html>`, common.TorrentPresenceUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/seedbox/list" {
					t.Errorf("unexpected mutation or endpoint: %s %s", r.Method, r.URL)
				}
				if r.URL.Query().Get("ids") != "transfer-1" || r.URL.Query().Get("page") != "0" || r.URL.Query().Get("perPage") != "100" {
					t.Errorf("lookup was not an exact bounded query: %s", r.URL)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client := &DebridLink{Host: server.URL, client: request.New(request.WithMaxRetries(0))}
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
