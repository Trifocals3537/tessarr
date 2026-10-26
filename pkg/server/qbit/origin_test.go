package qbit

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	"github.com/Trifocals3537/tessarr/pkg/manager"
)

func TestQBitBrowserOriginChecksPreventTagMutation(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	cfg := config.Get()
	cfg.BindAddress = "127.0.0.1"
	cfg.DownloadFolder = t.TempDir()
	mgr := manager.New()
	t.Cleanup(func() { _ = mgr.Stop(); _ = logger.Close() })
	mgr.Arr().AddOrUpdate(arr.New("tv", "https://sonarr.test", "arr-token", false, nil, "", "config"))
	q := New(mgr)
	router := q.Routes()
	for _, useAuth := range []bool{false, true} {
		cfg.UseAuth = useAuth
		for _, scheme := range []string{"http", "https"} {
			for _, origin := range []string{"", "http://127.0.0.1", "https://127.0.0.1", "https://untrusted.test", "null"} {
				q.Tags = nil
				r := httptest.NewRequest(http.MethodPost, scheme+"://127.0.0.1/torrents/createTags", strings.NewReader("tags=origin-fixture"))
				r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				r.Header.Set("Origin", origin)
				if useAuth {
					r.SetBasicAuth("https://sonarr.test", "arr-token")
				}
				w := httptest.NewRecorder()
				router.ServeHTTP(w, r)
				allowed := origin == "" || origin == scheme+"://127.0.0.1"
				want := http.StatusForbidden
				if allowed {
					want = http.StatusOK
				}
				if w.Code != want || (len(q.Tags) > 0) != allowed {
					t.Fatalf("auth=%v scheme=%s origin=%q: status=%d tags=%v", useAuth, scheme, origin, w.Code, q.Tags)
				}
			}
		}
	}
	// Browser checks also guard GET compatibility actions and login before
	// their handlers or form parsing, independently of supplied credentials.
	for _, path := range []string{"/torrents/pause", "/auth/login"} {
		r := httptest.NewRequest(http.MethodGet, "http://127.0.0.1"+path, nil)
		r.Header.Set("Referer", "https://untrusted.test/")
		w := httptest.NewRecorder()
		q.categoryContext(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("cross-site request reached handler") })).ServeHTTP(w, r)
		if w.Code != http.StatusForbidden {
			t.Fatalf("%s status=%d", path, w.Code)
		}
	}
}
