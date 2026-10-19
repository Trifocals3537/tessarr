package rclone

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/rs/zerolog"
)

func TestDoRejectsTrailingJSONAndDoesNotExposeErrorBody(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	t.Run("trailing JSON", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{} {}`))
		}))
		defer server.Close()

		client := NewClient(server.URL, "", "", zerolog.Nop())
		var result map[string]any
		err := client.Do(context.Background(), Request{Command: "core/version"}, &result)
		if err == nil || !strings.Contains(err.Error(), "multiple values") {
			t.Fatalf("Do error = %v, want trailing JSON rejection", err)
		}
	})

	t.Run("redacted error body", func(t *testing.T) {
		const secret = "https://remote.invalid/object?token=do-not-log"
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, secret, http.StatusTeapot)
		}))
		defer server.Close()

		client := NewClient(server.URL, "", "", zerolog.Nop())
		err := client.Do(context.Background(), Request{Command: "core/version"}, nil)
		if err == nil || strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "do-not-log") {
			t.Fatalf("Do error exposed rclone response body: %v", err)
		}
	})
}
