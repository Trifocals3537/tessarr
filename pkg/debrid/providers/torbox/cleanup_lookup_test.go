package torbox

import (
	"context"
	"errors"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"net/http"
	"testing"
)

func TestCleanupLookupRequiresTypedExactProviderAbsence(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		absent     bool
	}{
		{"typed", `{"success":true,"error":"ITEM_NOT_FOUND","data":{}}`, 404, true},
		{"proxy-page", `<html>Not found</html>`, 404, false},
		{"different-error", `{"success":false,"error":"AUTH_ERROR"}`, 404, false},
		{"provider-fault", `{"success":false,"error":"ITEM_NOT_FOUND"}`, 500, false},
		{"empty-list", `{"success":true,"data":[]}`, 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			provider := newCachedCheckTorbox(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" || r.URL.Path != "/api/torrents/mylist" || r.URL.Query().Get("id") != "123" || r.URL.Query().Get("bypass_cache") != "true" {
					t.Errorf("lookup was not authoritative exact-ID GET: %s", r.URL)
				}
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			})
			_, err := provider.GetTorrentContext(context.Background(), "123")
			if errors.Is(err, customerror.TorrentNotFoundError) != tc.absent {
				t.Fatalf("absence classification: %v", err)
			}
			if err == nil {
				t.Fatal("empty/malformed provider response accepted")
			}
			presence, err := provider.TorrentPresenceContext(context.Background(), "123")
			if tc.absent {
				if presence != common.TorrentAbsent || err != nil {
					t.Fatal("typed absence was not accepted by presence capability", presence, err)
				}
			} else if presence != common.TorrentPresenceUnknown || err == nil {
				t.Fatal("unverifiable response became presence evidence", presence, err)
			}
		})
	}
}
