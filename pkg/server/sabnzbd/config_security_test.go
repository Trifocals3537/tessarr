package sabnzbd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func TestSABConfigNeverPublishesProviderPasswords(t *testing.T) {
	sab, router := newSABRouteTestServer(t)
	cfg := config.Get()
	cfg.UseAuth = true
	cfg.Usenet.Providers = []config.UsenetProvider{
		{Host: "news.test", Port: 563, Username: "first-user", Password: "first-private-password", SSL: true, MaxConnections: 8},
		{Host: "backup.test", Port: 119, Username: "backup-user", Password: "backup-private-password", MaxConnections: 2},
	}
	sab.SetConfig(cfg)
	for _, mode := range []string{ModeConfig, ModeGetConfig} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			t.Run(method+"/"+mode, func(t *testing.T) {
				values := url.Values{"mode": {mode}, "cat": {"tv"}, "ma_username": {"https://sonarr.example.test"}, "ma_password": {"arr-token"}}
				r := newSABFormRequest(values)
				if method == http.MethodGet {
					r = httptest.NewRequest(method, "/api/?"+values.Encode(), nil)
				}
				w := httptest.NewRecorder()
				router.ServeHTTP(w, r)
				if w.Code != http.StatusOK {
					t.Fatalf("status=%d", w.Code)
				}
				for _, provider := range cfg.Usenet.Providers {
					if strings.Contains(w.Body.String(), provider.Password) {
						t.Fatal("response contains a provider password")
					}
				}
				var got ConfigResponse
				if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
				if got.Config == nil || len(got.Config.Servers) != 2 || len(got.Config.Categories) == 0 {
					t.Fatal("compatibility configuration lost server/category data")
				}
				for i, server := range got.Config.Servers {
					provider := cfg.Usenet.Providers[i]
					if server.Password != "" || server.Host != provider.Host || server.Port != provider.Port || server.SSL != provider.SSL || server.Connections != provider.MaxConnections {
						t.Fatal("response lost non-secret server settings or retained a password")
					}
				}
			})
		}
	}
	if cfg.Usenet.Providers[0].Password != "first-private-password" || cfg.Usenet.Providers[1].Password != "backup-private-password" {
		t.Fatal("building the response changed runtime credentials")
	}
	for _, server := range sab.config.Servers {
		if server.Password != "" {
			t.Fatal("response snapshot retains a provider password")
		}
	}
}

func TestSABBrowserOriginChecksCoverGETAndPOST(t *testing.T) {
	_, router := newSABRouteTestServer(t)
	for _, useAuth := range []bool{false, true} {
		config.Get().UseAuth = useAuth
		for _, scheme := range []string{"http", "https"} {
			for _, method := range []string{http.MethodGet, http.MethodPost} {
				for _, origin := range []string{"", "http://example.com", "https://example.com", "https://untrusted.test", "null"} {
					values := url.Values{"mode": {ModeVersion}, "cat": {"tv"}, "ma_username": {"https://sonarr.example.test"}, "ma_password": {"arr-token"}}
					target, body := scheme+"://example.com/api/", values.Encode()
					if method == http.MethodGet {
						target += "?" + values.Encode()
						body = ""
					}
					r := httptest.NewRequest(method, target, strings.NewReader(body))
					r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
					r.Header.Set("Origin", origin)
					w := httptest.NewRecorder()
					router.ServeHTTP(w, r)
					want := http.StatusOK
					if origin != "" && origin != scheme+"://example.com" {
						want = http.StatusForbidden
					}
					if w.Code != want {
						t.Fatalf("auth=%v scheme=%s %s origin=%q: status=%d, want %d", useAuth, scheme, method, origin, w.Code, want)
					}
				}
			}
		}
	}
}
