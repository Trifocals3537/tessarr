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

func TestRequestOriginMatchesListenerTransport(t *testing.T) {
	for _, tt := range []struct {
		name, target, origin, referer, appURL string
		want                                  bool
	}{
		{name: "HTTP listener", target: "http://tessarr.test/api", origin: "http://tessarr.test", want: true},
		{name: "HTTPS listener", target: "https://tessarr.test/api", origin: "https://tessarr.test", want: true},
		{name: "HTTPS default port", target: "https://tessarr.test:443/api", origin: "https://tessarr.test", want: true},
		{name: "HTTP origin to HTTPS listener", target: "https://tessarr.test/api", origin: "http://tessarr.test"},
		{name: "HTTPS origin to HTTP listener", target: "http://tessarr.test/api", origin: "https://tessarr.test"},
		{name: "same explicit port different scheme", target: "https://tessarr.test:8443/api", origin: "http://tessarr.test:8443"},
		{name: "HTTP referrer to HTTPS listener", target: "https://tessarr.test/api", referer: "http://tessarr.test/page"},
		{name: "HTTPS referrer to HTTP listener", target: "http://tessarr.test/api", referer: "https://tessarr.test/page"},
		{name: "same-host TLS proxy", target: "http://tessarr.test/api", origin: "https://tessarr.test", appURL: "https://tessarr.test/tessarr/", want: true},
		{name: "different-host TLS proxy", target: "http://backend.test:8282/api", origin: "https://public.test", appURL: "https://public.test/tessarr/", want: true},
		{name: "proxy referrer", target: "http://backend.test:8282/api", referer: "https://public.test/tessarr/", appURL: "https://public.test/tessarr/", want: true},
		{name: "wrong proxy scheme", target: "http://backend.test:8282/api", origin: "http://public.test", appURL: "https://public.test/tessarr/"},
		{name: "HTTP origin cannot use HTTPS app URL", target: "https://tessarr.test/api", origin: "http://tessarr.test", appURL: "https://tessarr.test"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
				r := httptest.NewRequest(method, tt.target, nil)
				// Incoming server requests usually have no URL scheme. Transport
				// comes from TLS, not absolute URLs or untrusted proxy headers.
				r.URL.Scheme = ""
				r.Header.Set("Origin", tt.origin)
				r.Header.Set("Referer", tt.referer)
				r.Header.Set("Sec-Fetch-Site", "cross-site")
				r.Header.Set("X-Forwarded-Proto", "https")
				r.Header.Set("Forwarded", "proto=https")
				if got := RequestOriginAllowed(r, tt.appURL); got != tt.want {
					t.Errorf("%s request allowed=%v, want %v", method, got, tt.want)
				}
				if got := ProtocolRequestAllowed(r, tt.appURL); got != tt.want {
					t.Errorf("%s protocol allowed=%v, want %v", method, got, tt.want)
				}
			}
		})
	}
}

func TestGETMutationNeedsCredentials(t *testing.T) {
	for _, tt := range []struct {
		method, origin, referer, site string
		want                          bool
	}{
		{method: http.MethodGet, want: true},
		{method: http.MethodGet, origin: "http://tessarr.test"},
		{method: http.MethodGet, referer: "http://tessarr.test/page"},
		{method: http.MethodGet, site: "same-origin"},
		{method: http.MethodGet, site: "none"},
		{method: http.MethodPost},
	} {
		r := httptest.NewRequest(tt.method, "http://192.0.2.20/api", nil)
		r.Header.Set("Origin", tt.origin)
		r.Header.Set("Referer", tt.referer)
		r.Header.Set("Sec-Fetch-Site", tt.site)
		if got := GETMutationNeedsCredentials(r); got != tt.want {
			t.Errorf("%+v: needs credentials=%v", tt, got)
		}
	}
}
