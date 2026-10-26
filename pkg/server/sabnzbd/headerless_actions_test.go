package sabnzbd

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestHeaderlessGETDeleteRequiresExplicitCredentials(t *testing.T) {
	sab, router := newSABRouteTestServer(t)
	config.Get().UseAuth = false
	for _, tt := range []struct {
		name, id, password string
		want               int
	}{
		{name: "no credentials", id: "missing", want: http.StatusUnauthorized},
		{name: "invalid credentials", id: "invalid", password: "wrong", want: http.StatusUnauthorized},
		{name: "configured native client", id: "native", password: "arr-token", want: http.StatusOK},
	} {
		t.Run(tt.name, func(t *testing.T) {
			entry := &storage.Entry{InfoHash: "headerless-delete-" + tt.id, Name: "headerless-delete-" + tt.id, Category: "tv", Protocol: config.ProtocolNZB}
			entry.SavePath = filepath.Join(config.Get().DownloadFolder, entry.Category)
			if err := os.MkdirAll(entry.SavePath, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := sab.manager.Queue().Add(entry); err != nil {
				t.Fatal(err)
			}
			values := url.Values{"mode": {ModeQueue}, "name": {"delete"}, "value": {entry.InfoHash}, "cat": {"tv"}}
			if tt.password != "" {
				values.Set("ma_username", "https://sonarr.example.test")
				values.Set("ma_password", tt.password)
			}
			r := httptest.NewRequest(http.MethodGet, "http://192.0.2.20/api/?"+values.Encode(), nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			_, err := sab.manager.Queue().GetTorrent(entry.InfoHash)
			deleted := storage.IsQueuedEntryNotFound(err)
			if w.Code != tt.want || deleted != (tt.want == http.StatusOK) {
				t.Fatalf("status=%d, want %d; deleted=%v: %s", w.Code, tt.want, deleted, w.Body.String())
			}
		})
	}
}
