package server

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	json "github.com/bytedance/sonic"
)

func TestRegistrationAllowedOnlyForUnconfiguredLoopbackAuth(t *testing.T) {
	tests := []struct {
		name string
		cfg  *config.Config
		want bool
	}{
		{
			name: "unconfigured loopback auth",
			cfg: &config.Config{
				BindAddress: "127.0.0.1",
				UseAuth:     true,
				Auth:        &config.Auth{},
			},
			want: true,
		},
		{
			name: "configured auth",
			cfg: &config.Config{
				BindAddress: "127.0.0.1",
				UseAuth:     true,
				Auth: &config.Auth{
					Username: "admin",
					Password: "hash",
				},
			},
		},
		{
			name: "authentication disabled",
			cfg: &config.Config{
				BindAddress: "127.0.0.1",
				Auth:        &config.Auth{},
			},
		},
		{
			name: "remote listener",
			cfg: &config.Config{
				BindAddress: "0.0.0.0",
				UseAuth:     true,
				Auth:        &config.Auth{},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := registrationAllowed(test.cfg); got != test.want {
				t.Fatalf("registrationAllowed() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestRegisterCannotOverwriteExistingCredentials(t *testing.T) {
	cfg := useServerTestConfig(
		t,
		"127.0.0.1",
		true,
		&config.Auth{Username: "original", Password: "original-hash"},
	)
	form := url.Values{
		"username":        {"attacker"},
		"password":        {"changed-password"},
		"confirmPassword": {"changed-password"},
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/register",
		strings.NewReader(form.Encode()),
	)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()

	(&Server{}).RegisterHandler(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
	if cfg.Auth.Username != "original" || cfg.Auth.Password != "original-hash" {
		t.Fatalf("credentials were overwritten: %#v", cfg.Auth)
	}
}

func TestRemoteRegistrationCannotClaimMissingCredentials(t *testing.T) {
	cfg := useServerTestConfig(t, "0.0.0.0", true, &config.Auth{})
	request := httptest.NewRequest(http.MethodPost, "/register", nil)
	response := httptest.NewRecorder()

	(&Server{}).RegisterHandler(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
	if cfg.Auth.Username != "" || cfg.Auth.Password != "" {
		t.Fatalf("remote request created credentials: %#v", cfg.Auth)
	}
}

func TestRemoteUnauthenticatedAuthUpdateIsRejected(t *testing.T) {
	cfg := useServerTestConfig(t, "0.0.0.0", false, &config.Auth{})
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/update-auth",
		strings.NewReader(
			`{"username":"attacker","password":"changed-password","confirm_password":"changed-password"}`,
		),
	)
	response := httptest.NewRecorder()

	(&Server{}).handleUpdateAuth(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
	if cfg.UseAuth || cfg.Auth.Username != "" || cfg.Auth.Password != "" {
		t.Fatalf("remote request enabled authentication: %#v", cfg)
	}
}

func TestAuthUpdateRejectedWhileRestartPending(t *testing.T) {
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/update-auth",
		strings.NewReader(`{"username":"admin","password":"new-password","confirm_password":"new-password"}`),
	)
	response := httptest.NewRecorder()

	(&Server{restartPending: true}).handleUpdateAuth(response, request)

	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusConflict)
	}
	if response.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q, want 1", response.Header().Get("Retry-After"))
	}
}

func TestAuthDisableUsesPendingDesiredListener(t *testing.T) {
	useServerTestConfig(
		t,
		"127.0.0.1",
		true,
		&config.Auth{Username: "admin", Password: "hash"},
	)
	if _, err := config.Update(func(draft *config.Config) error {
		draft.BindAddress = "192.0.2.10"
		draft.EnableWebdavAuth = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(
		http.MethodPost,
		"/api/update-auth",
		strings.NewReader(`{"username":"","password":""}`),
	)
	response := httptest.NewRecorder()
	(&Server{}).handleUpdateAuth(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
	persisted, err := config.LoadForValidation(config.GetMainPath())
	if err != nil {
		t.Fatal(err)
	}
	if !persisted.UseAuth {
		t.Fatal("authentication was disabled in the pending remote configuration")
	}
	if persisted.BindAddress != "192.0.2.10" {
		t.Fatalf("pending bind address = %q, want 192.0.2.10", persisted.BindAddress)
	}
}

func TestRemoteInitialSetupCannotSkipAuthentication(t *testing.T) {
	cfg := useServerTestConfig(t, "0.0.0.0", true, &config.Auth{})
	request := httptest.NewRequest(http.MethodPost, "/skip-auth", nil)
	response := httptest.NewRecorder()

	(&Server{}).skipAuthHandler(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
	if !cfg.UseAuth {
		t.Fatal("remote request disabled authentication")
	}

	request = httptest.NewRequest(
		http.MethodPost,
		"/api/setup/complete",
		strings.NewReader(`{
			"auth":{"skip_auth":true},
			"debrid":{"provider":"realdebrid","api_key":"test-key"},
			"download":{"download_folder":"/tmp/tessarr-test"},
			"mount":{"mount_type":"none"}
		}`),
	)
	response = httptest.NewRecorder()
	(&Server{}).setupCompleteHandler(response, request)

	if response.Code != http.StatusForbidden {
		t.Fatalf(
			"setup status = %d, want %d",
			response.Code,
			http.StatusForbidden,
		)
	}
	if !cfg.UseAuth {
		t.Fatal("remote setup disabled authentication")
	}
}

func TestWebhookUsesAPIAuthentication(t *testing.T) {
	useServerTestConfig(
		t,
		"10.0.0.2",
		true,
		&config.Auth{
			Username: "admin",
			Password: "hash",
			APIToken: "webhook-token",
		},
	)
	s := &Server{cookie: newSessionCookieStore("test-session-secret", false)}
	called := false
	handler := s.authMiddleware(http.HandlerFunc(func(
		w http.ResponseWriter,
		_ *http.Request,
	) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	}))

	request := httptest.NewRequest(
		http.MethodPost,
		"/webhooks/tautulli",
		nil,
	)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf(
			"unauthenticated status = %d, want %d",
			response.Code,
			http.StatusUnauthorized,
		)
	}
	if called {
		t.Fatal("unauthenticated webhook reached handler")
	}

	request = httptest.NewRequest(
		http.MethodPost,
		"/webhooks/tautulli",
		nil,
	)
	request.Header.Set("Authorization", "Bearer webhook-token")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf(
			"authenticated status = %d, want %d",
			response.Code,
			http.StatusNoContent,
		)
	}
	if !called {
		t.Fatal("authenticated webhook did not reach handler")
	}
}

func useServerTestConfig(
	t *testing.T,
	bindAddress string,
	useAuth bool,
	auth *config.Auth,
) *config.Config {
	t.Helper()

	previousPath := config.GetMainPath()
	config.Reset()
	configDir := t.TempDir()
	config.SetConfigPath(configDir)
	t.Cleanup(func() {
		config.Reset()
		config.SetConfigPath(previousPath)
	})

	data, err := json.Marshal(map[string]any{
		"bind_address": bindAddress,
		"use_auth":     useAuth,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(configDir, "config.json"),
		data,
		0600,
	); err != nil {
		t.Fatal(err)
	}
	authData, err := json.Marshal(auth)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(configDir, "auth.json"),
		authData,
		0600,
	); err != nil {
		t.Fatal(err)
	}

	return config.Get()
}
