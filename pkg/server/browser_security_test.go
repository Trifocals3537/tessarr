package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func TestRequestOriginAllowed(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "http://tessarr.example/api/config", nil)
	request.Header.Set("Origin", "http://tessarr.example")
	if !requestOriginAllowed(request, "") {
		t.Fatal("matching request origin was rejected")
	}

	request.Header.Set("Origin", "https://attacker.example")
	if requestOriginAllowed(request, "") {
		t.Fatal("cross-site request origin was accepted")
	}

	request.Header.Del("Origin")
	if requestOriginAllowed(request, "") {
		t.Fatal("mutation without Origin or Referer was accepted")
	}

	request = httptest.NewRequest(http.MethodPost, "http://tessarr.example:80/api/config", nil)
	request.Header.Set("Origin", "http://tessarr.example")
	if !requestOriginAllowed(request, "") {
		t.Fatal("equivalent default ports were treated as different origins")
	}
}

func TestURLBaseRoutingHelpers(t *testing.T) {
	tests := []struct {
		base   string
		target string
		want   string
	}{
		{base: "/", target: "login", want: "/login"},
		{base: "/tessarr/", target: "login", want: "/tessarr/login"},
		{base: "tessarr", target: "/settings", want: "/tessarr/settings"},
		{base: "/tessarr/", target: "", want: "/tessarr/"},
	}
	for _, test := range tests {
		if got := urlBasePath(test.base, test.target); got != test.want {
			t.Fatalf("urlBasePath(%q, %q) = %q, want %q", test.base, test.target, got, test.want)
		}
	}

	if got := pathWithoutURLBase("/tessarr/api/config", "/tessarr/"); got != "/api/config" {
		t.Fatalf("pathWithoutURLBase() = %q, want /api/config", got)
	}
	if got := pathWithoutURLBase("/tessarr", "/tessarr/"); got != "/" {
		t.Fatalf("base root path = %q, want /", got)
	}
}

func TestSafeLocalRedirectPathRejectsExternalAndHeaderValues(t *testing.T) {
	tests := []struct {
		name   string
		base   string
		target string
	}{
		{name: "header injection", base: "/tessarr\r\nLocation: https://attacker.example"},
		{name: "backslash", base: `/\\attacker.example`},
		{name: "network path backslash", base: `/\attacker.example`},
		{name: "query", base: "/tessarr?next=https://attacker.example"},
		{name: "fragment", base: "/tessarr#attacker"},
		{name: "target query", base: "/tessarr", target: "login?next=https://attacker.example"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got, err := safeLocalRedirectPath(test.base, test.target); err == nil {
				t.Fatalf("safeLocalRedirectPath(%q, %q) = %q, want rejection", test.base, test.target, got)
			}
		})
	}
}

func TestRedirectLocalWritesOnlyValidatedPath(t *testing.T) {
	response := httptest.NewRecorder()
	redirectLocal(response, "/tessarr/", "login", http.StatusSeeOther)
	if response.Code != http.StatusSeeOther || response.Header().Get("Location") != "/tessarr/login" {
		t.Fatalf("redirect response = (%d, %q)", response.Code, response.Header().Get("Location"))
	}

	response = httptest.NewRecorder()
	redirectLocal(response, "/bad\r\nX-Test: injected", "login", http.StatusSeeOther)
	if response.Code != http.StatusInternalServerError || response.Header().Get("Location") != "" {
		t.Fatalf("unsafe redirect response = (%d, %q)", response.Code, response.Header().Get("Location"))
	}
}

func TestAPIRequestDetectionHonorsURLBase(t *testing.T) {
	server := &Server{urlBase: "/tessarr/"}
	for _, path := range []string{
		"http://example.test/tessarr/api/config",
		"http://example.test/tessarr/webhooks/tautulli",
	} {
		if !server.isAPIRequest(httptest.NewRequest(http.MethodGet, path, nil)) {
			t.Fatalf("API path %q was not detected under URL base", path)
		}
	}
	if server.isAPIRequest(httptest.NewRequest(http.MethodGet, "http://example.test/tessarr/settings", nil)) {
		t.Fatal("settings page was detected as an API request")
	}
}

func TestSessionVersionInvalidatesExistingCookie(t *testing.T) {
	cfg := useServerTestConfig(t, "127.0.0.1", true, &config.Auth{
		Username:       "admin",
		Password:       "hash",
		SessionVersion: 7,
	})
	server := &Server{cookie: newSessionCookieStore("test-secret", false)}
	cookie := authenticatedTestCookie(t, server, cfg.Auth.SessionVersion)

	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.AddCookie(cookie)
	if !server.sessionAuthenticated(request) {
		t.Fatal("current session version was rejected")
	}

	cfg.Auth.SessionVersion++
	if server.sessionAuthenticated(request) {
		t.Fatal("stale session remained valid after credential version changed")
	}
}

func TestSessionMutationRequiresSameOrigin(t *testing.T) {
	cfg := useServerTestConfig(t, "127.0.0.1", true, &config.Auth{
		Username:       "admin",
		Password:       "hash",
		SessionVersion: 3,
	})
	server := &Server{cookie: newSessionCookieStore("test-secret", false)}
	cookie := authenticatedTestCookie(t, server, cfg.Auth.SessionVersion)
	called := false
	handler := server.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodPost, "http://tessarr.example/api/config", nil)
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || called {
		t.Fatalf("missing-origin mutation status = %d, called = %v", response.Code, called)
	}

	request = httptest.NewRequest(http.MethodPost, "http://tessarr.example/api/config", nil)
	request.Header.Set("Origin", "http://tessarr.example")
	request.AddCookie(cookie)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || !called {
		t.Fatalf("same-origin mutation status = %d, called = %v", response.Code, called)
	}
}

func TestDisabledAuthStillRejectsCrossSiteBrowserMutation(t *testing.T) {
	useServerTestConfig(t, "127.0.0.1", false, &config.Auth{})
	server := &Server{}
	called := false
	handler := server.authMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(http.MethodPost, "http://tessarr.example/api/config", nil)
	request.Header.Set("Origin", "https://attacker.example")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || called {
		t.Fatalf("cross-site status = %d, called = %v", response.Code, called)
	}

	// Non-browser local automation remains compatible when authentication is
	// deliberately disabled: it sends no Origin or Referer header.
	request = httptest.NewRequest(http.MethodPost, "http://tessarr.example/api/config", nil)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || !called {
		t.Fatalf("originless client status = %d, called = %v", response.Code, called)
	}
}

func authenticatedTestCookie(t *testing.T, server *Server, version uint64) *http.Cookie {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/login", nil)
	response := httptest.NewRecorder()
	session, err := server.cookie.Get(request, "auth-session")
	if err != nil {
		t.Fatal(err)
	}
	session.Values["authenticated"] = true
	session.Values["username"] = "admin"
	session.Values["session_version"] = version
	if err := session.Save(request, response); err != nil {
		t.Fatal(err)
	}
	cookies := response.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("saved session cookies = %d, want 1", len(cookies))
	}
	return cookies[0]
}
