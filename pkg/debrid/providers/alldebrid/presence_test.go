package alldebrid

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
		{"present", 200, `{"status":"success","data":{"magnets":[{"id":123}]}}`, common.TorrentPresent},
		{"typed-absence", 200, `{"status":"error","error":{"code":"MAGNET_INVALID_ID"}}`, common.TorrentAbsent},
		{"typed-404", 404, `{"status":"error","error":{"code":"MAGNET_INVALID_ID"}}`, common.TorrentAbsent},
		{"empty-match", 200, `{"status":"success","data":{"magnets":[]}}`, common.TorrentPresenceUnknown},
		{"wrong-id", 200, `{"status":"success","data":{"magnets":[{"id":456}]}}`, common.TorrentPresenceUnknown},
		{"auth-error", 401, `{"status":"error","error":{"code":"AUTH_BAD_APIKEY"}}`, common.TorrentPresenceUnknown},
		{"proxy-404", 404, `<html>missing</html>`, common.TorrentPresenceUnknown},
		{"server-fault", 500, `{"status":"error","error":{"code":"MAGNET_INVALID_ID"}}`, common.TorrentPresenceUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/magnet/status" {
					t.Errorf("unexpected mutation or endpoint: %s %s", r.Method, r.URL)
				}
				if r.URL.Query().Get("id") != "123" {
					t.Errorf("lookup was not exact: %s", r.URL)
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			}))
			defer server.Close()
			client := &AllDebrid{Host: server.URL, client: request.New(request.WithMaxRetries(0))}
			presence, err := client.TorrentPresenceContext(context.Background(), "123")
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
			if presence, err := client.TorrentPresenceContext(ctx, "123"); presence != common.TorrentPresenceUnknown || err == nil {
				t.Fatal("canceled lookup supplied presence evidence")
			}
		})
	}
}
