package server

import (
	"html/template"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/go-chi/chi/v5"
	"golang.org/x/crypto/bcrypt"
)

func TestRemoteIncompleteSetupCanSignInThroughWebRoutes(t *testing.T) {
	for _, base := range []string{"/", "/tessarr/"} {
		t.Run(base, func(t *testing.T) {
			password, err := bcrypt.GenerateFromPassword([]byte("setup-password"), bcrypt.MinCost)
			if err != nil {
				t.Fatal(err)
			}
			cfg := useServerTestConfig(t, "0.0.0.0", true, &config.Auth{Username: "admin", Password: string(password), SessionVersion: 1})
			cfg.URLBase = base
			if cfg.SetupComplete() == nil {
				t.Fatal("fixture must require setup")
			}
			templates := template.Must(template.ParseFS(content, "templates/*.html"))
			s := &Server{urlBase: base, cookie: newSessionCookieStore("setup-test-secret", false), templates: templates, loginLimiter: newLoginAttemptLimiter()}
			router := chi.NewRouter()
			router.Mount(strings.TrimSuffix(base, "/")+"/", s.WebRoutes())
			request := func(method, target, body string, cookie *http.Cookie) *httptest.ResponseRecorder {
				r := httptest.NewRequest(method, "http://tessarr.test"+target, strings.NewReader(body))
				if method == http.MethodPost {
					r.Header.Set("Content-Type", "application/json")
					r.Header.Set("Origin", "http://tessarr.test")
				}
				if cookie != nil {
					r.AddCookie(cookie)
				}
				w := httptest.NewRecorder()
				router.ServeHTTP(w, r)
				return w
			}
			setup := request(http.MethodGet, base+"setup", "", nil)
			if setup.Code != http.StatusSeeOther || setup.Header().Get("Location") != base+"login" {
				t.Fatalf("unauthenticated setup=%d location=%q", setup.Code, setup.Header().Get("Location"))
			}
			login := request(http.MethodGet, base+"login", "", nil)
			if login.Code != http.StatusOK || !strings.Contains(login.Body.String(), "loginForm") {
				t.Fatalf("login page=%d location=%q", login.Code, login.Header().Get("Location"))
			}
			bad := request(http.MethodPost, base+"login", `{"username":"admin","password":"wrong"}`, nil)
			if bad.Code != http.StatusUnauthorized || len(bad.Result().Cookies()) != 0 {
				t.Fatal("bad credentials were accepted")
			}
			signedIn := request(http.MethodPost, base+"login", `{"username":"admin","password":"setup-password"}`, nil)
			if signedIn.Code != http.StatusSeeOther || len(signedIn.Result().Cookies()) != 1 {
				t.Fatalf("login=%d", signedIn.Code)
			}
			cookie := signedIn.Result().Cookies()[0]
			root := request(http.MethodGet, base, "", cookie)
			if root.Code != http.StatusSeeOther || root.Header().Get("Location") != base+"setup" {
				t.Fatal("incomplete config bypassed setup")
			}
			setup = request(http.MethodGet, base+"setup", "", cookie)
			if setup.Code != http.StatusOK || !strings.Contains(setup.Body.String(), "Setup") {
				t.Fatalf("signed-in setup=%d", setup.Code)
			}
		})
	}
}
