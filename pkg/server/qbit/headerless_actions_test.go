package qbit

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	"github.com/Trifocals3537/tessarr/pkg/manager"
)

func TestHeaderlessGETActionsRequireExplicitCredentials(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	cfg := config.Get()
	cfg.UseAuth = true
	cfg.DownloadFolder = t.TempDir()
	mgr := manager.New()
	t.Cleanup(func() { _ = mgr.Stop(); _ = logger.Close() })
	mgr.Arr().AddOrUpdate(arr.New("tv", "https://sonarr.test", "arr-token", false, nil, "", "config"))
	q := New(mgr)
	router := q.Routes()
	const listener = "http://192.0.2.20"
	form := url.Values{"username": {"https://sonarr.test"}, "password": {"arr-token"}}
	login := httptest.NewRequest(http.MethodPost, listener+"/auth/login", strings.NewReader(form.Encode()))
	login.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	login.Header.Set("Origin", listener)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, login)
	if w.Code != http.StatusOK || len(w.Result().Cookies()) != 1 {
		t.Fatalf("native login failed: status=%d", w.Code)
	}
	sid := w.Result().Cookies()[0]
	for _, useAuth := range []bool{true, false} {
		cfg.UseAuth = useAuth
		for _, path := range []string{"/torrents/pause", "/torrents/resume", "/torrents/recheck", "/app/shutdown"} {
			for _, tt := range []struct {
				name, origin, site, password string
				omitCookie                   bool
				want                         int
			}{
				{name: "HTTP no-referrer navigation with SID", want: http.StatusForbidden},
				{name: "headerless request without SID", omitCookie: true, want: http.StatusForbidden},
				{name: "same-origin browser", origin: listener, want: http.StatusOK},
				{name: "same-origin metadata", site: "same-origin", want: http.StatusOK},
				{name: "native explicit credentials", password: "arr-token", omitCookie: true, want: http.StatusOK},
				{name: "invalid credentials cannot fall back to SID", password: "wrong", want: http.StatusUnauthorized},
			} {
				t.Run(tt.name+"/"+path+"/auth="+fmt.Sprint(useAuth), func(t *testing.T) {
					r := httptest.NewRequest(http.MethodGet, listener+path, nil)
					if !tt.omitCookie {
						r.AddCookie(sid)
					}
					r.Header.Set("Origin", tt.origin)
					r.Header.Set("Sec-Fetch-Site", tt.site)
					if tt.password != "" {
						r.SetBasicAuth("https://sonarr.test", tt.password)
					}
					w := httptest.NewRecorder()
					router.ServeHTTP(w, r)
					if w.Code != tt.want {
						t.Fatalf("status=%d, want %d: %s", w.Code, tt.want, w.Body.String())
					}
				})
			}
		}
		for _, path := range []string{"/torrents/info", "/torrents/categories", "/app/version"} {
			r := httptest.NewRequest(http.MethodGet, listener+path, nil)
			r.AddCookie(sid)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				t.Fatalf("native session read %s failed: %d", path, w.Code)
			}
		}
		for _, path := range []string{"/torrents/pause", "/torrents/resume", "/torrents/recheck"} {
			r := httptest.NewRequest(http.MethodPost, listener+path, nil)
			r.AddCookie(sid)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			if w.Code != http.StatusOK {
				t.Fatalf("native session POST %s failed: %d", path, w.Code)
			}
		}
		q.Tags = nil
		r := httptest.NewRequest(http.MethodPost, listener+"/torrents/createTags", strings.NewReader("tags=native-session"))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.AddCookie(sid)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, r)
		if w.Code != http.StatusOK || len(q.Tags) != 1 || q.Tags[0] != "native-session" {
			t.Fatalf("native session POST mutation failed: status=%d tags=%v", w.Code, q.Tags)
		}
	}
}
