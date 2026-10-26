package httpsecurity

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProtocolRequestOriginPolicy(t *testing.T) {
	for _, tt := range []struct {
		name, origin, referer, appURL string
		want                          bool
	}{
		{name: "native client", want: true},
		{name: "matching listener", origin: "http://tessarr.test", want: true},
		{name: "default port", origin: "http://tessarr.test:80", want: true},
		{name: "referer", referer: "http://tessarr.test/repair", want: true},
		{name: "proxy URL", origin: "https://public.test", appURL: "https://public.test/tessarr/", want: true},
		{name: "cross origin", origin: "https://untrusted.test"},
		{name: "cross origin referer", referer: "https://untrusted.test/page"},
		{name: "origin takes precedence", origin: "null", referer: "http://tessarr.test/"},
		{name: "null", origin: "null"},
		{name: "malformed", origin: "://bad"},
		{name: "different port", origin: "http://tessarr.test:8080"},
		{name: "proxy wrong scheme", origin: "http://public.test", appURL: "https://public.test"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
				r := httptest.NewRequest(method, "http://tessarr.test/api", nil)
				r.Header.Set("Origin", tt.origin)
				r.Header.Set("Referer", tt.referer)
				if got := ProtocolRequestAllowed(r, tt.appURL); got != tt.want {
					t.Errorf("%s allowed=%v, want %v", method, got, tt.want)
				}
			}
		})
	}
	if ProtocolRequestAllowed(nil, "") {
		t.Fatal("nil request accepted")
	}
	for _, site := range []string{"cross-site", "same-site", "same-origin", "none"} {
		r := httptest.NewRequest(http.MethodGet, "http://tessarr.test/api", nil)
		r.Header.Set("Sec-Fetch-Site", site)
		want := site == "same-origin" || site == "none"
		if ProtocolRequestAllowed(r, "") != want {
			t.Errorf("GET without Referer, Sec-Fetch-Site=%q: want allowed=%v", site, want)
		}
	}
}
