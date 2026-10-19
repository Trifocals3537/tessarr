package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	json "github.com/bytedance/sonic"
)

func TestSetupRejectsSymlinkedDownloadRoot(t *testing.T) {
	cfg := useServerTestConfig(t, "127.0.0.1", true, &config.Auth{})
	previousDownloadRoot := cfg.DownloadFolder
	parent := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(parent, "downloads")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}

	payload, err := json.Marshal(map[string]any{
		"auth": map[string]any{
			"username": "admin",
			"password": "a-valid-test-password",
		},
		"debrid": map[string]any{
			"provider": "realdebrid",
			"api_key":  "test-key",
		},
		"download": map[string]any{
			"download_folder": filepath.Join(link, "nested"),
		},
		"mount": map[string]any{
			"mount_type": "none",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/setup/complete", strings.NewReader(string(payload)))
	request.Header.Set("Origin", "http://"+request.Host)
	response := httptest.NewRecorder()

	(&Server{}).setupCompleteHandler(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body = %q", response.Code, http.StatusBadRequest, response.Body.String())
	}
	if cfg.DownloadFolder != previousDownloadRoot {
		t.Fatalf("unsafe download root was published: %q", cfg.DownloadFolder)
	}
	if _, err := os.Lstat(filepath.Join(outside, "nested")); !os.IsNotExist(err) {
		t.Fatalf("setup created content through symlinked root: %v", err)
	}
}
