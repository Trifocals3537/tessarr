package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func representativeConfig() *config.Config {
	return &config.Config{
		BindAddress: "127.0.0.1",
		Port:        "8282",
		LogLevel:    "info",
		Debrids: []config.Debrid{{
			Name:     "primary",
			Provider: "realdebrid",
			APIKey:   "secret",
		}},
		Mount: config.Mount{
			Type:      config.MountTypeDFS,
			MountPath: "/mnt/media",
			DFS: config.DFS{
				ChunkSize: "32MB",
			},
		},
		Usenet: config.Usenet{
			MaxConnections: 12,
			ReadAhead:      "16MB",
		},
		DownloadFolder: "/downloads",
	}
}

func TestDecodeConfigPatchPreservesOmittedSections(t *testing.T) {
	current := representativeConfig()
	updated, err := decodeConfigUpdate(
		http.MethodPatch,
		strings.NewReader(`{"log_level":"debug","mount":{"dfs":{"chunk_size":"64MB"}}}`),
		current,
	)
	if err != nil {
		t.Fatalf("decode PATCH: %v", err)
	}

	if updated.LogLevel != "debug" {
		t.Fatalf("log level = %q, want debug", updated.LogLevel)
	}
	if len(updated.Debrids) != 1 || updated.Debrids[0].APIKey != "secret" {
		t.Fatalf("debrids were not preserved: %#v", updated.Debrids)
	}
	if updated.Mount.Type != config.MountTypeDFS || updated.Mount.MountPath != "/mnt/media" {
		t.Fatalf("mount identity was not preserved: %#v", updated.Mount)
	}
	if updated.Mount.DFS.ChunkSize != "64MB" {
		t.Fatalf("chunk size = %q, want 64MB", updated.Mount.DFS.ChunkSize)
	}
	if updated.Usenet.MaxConnections != 12 {
		t.Fatalf("usenet config was not preserved: %#v", updated.Usenet)
	}
}

func TestDecodeConfigPatchPreservesOmittedNestedUsenetFields(t *testing.T) {
	current := representativeConfig()
	current.Usenet.ConnIdleTimeout = "5m"

	updated, err := decodeConfigUpdate(
		http.MethodPatch,
		strings.NewReader(`{"usenet":{"max_connections":6,"read_ahead":"32MB"}}`),
		current,
	)
	if err != nil {
		t.Fatalf("decode nested Usenet PATCH: %v", err)
	}
	if updated.Usenet.MaxConnections != 6 || updated.Usenet.ReadAhead != "32MB" {
		t.Fatalf("updated Usenet fields were not applied: %#v", updated.Usenet)
	}
	if updated.Usenet.ConnIdleTimeout != "5m" {
		t.Fatalf("connection idle timeout = %q, want preserved 5m", updated.Usenet.ConnIdleTimeout)
	}
}

func TestDecodeConfigPatchNullRemovesField(t *testing.T) {
	current := representativeConfig()
	current.AppURL = "https://example.test"

	updated, err := decodeConfigUpdate(
		http.MethodPatch,
		strings.NewReader(`{"app_url":null}`),
		current,
	)
	if err != nil {
		t.Fatalf("decode PATCH: %v", err)
	}
	if updated.AppURL != "" {
		t.Fatalf("app URL = %q, want empty after null merge patch", updated.AppURL)
	}
}

func TestDecodeLegacyPostRejectsPartialDocument(t *testing.T) {
	_, err := decodeConfigUpdate(
		http.MethodPost,
		strings.NewReader(`{"log_level":"debug"}`),
		representativeConfig(),
	)
	if err == nil {
		t.Fatal("partial legacy POST succeeded")
	}
	for _, field := range legacyPostRequiredFields {
		if !strings.Contains(err.Error(), field) {
			t.Fatalf("error %q does not identify missing field %q", err, field)
		}
	}
}

func TestDecodeLegacyPostAcceptsCompleteDocument(t *testing.T) {
	updated, err := decodeConfigUpdate(
		http.MethodPost,
		strings.NewReader(`{
			"log_level":"debug",
			"debrids":[],
			"mount":{"type":"none"},
			"usenet":{"providers":[]}
		}`),
		representativeConfig(),
	)
	if err != nil {
		t.Fatalf("decode complete legacy POST: %v", err)
	}
	if updated.LogLevel != "debug" {
		t.Fatalf("log level = %q, want debug", updated.LogLevel)
	}
	if updated.Debrids == nil {
		t.Fatal("explicit empty debrids array was not retained")
	}
	if updated.BindAddress != "127.0.0.1" {
		t.Fatalf("legacy POST erased an omitted field: bind address = %q", updated.BindAddress)
	}
}

func TestDecodeConfigPutReplacesOmittedSections(t *testing.T) {
	updated, err := decodeConfigUpdate(
		http.MethodPut,
		strings.NewReader(`{"log_level":"debug"}`),
		representativeConfig(),
	)
	if err != nil {
		t.Fatalf("decode PUT: %v", err)
	}
	if len(updated.Debrids) != 0 || updated.Mount.Type != "" {
		t.Fatalf("PUT did not replace the document: %#v", updated)
	}
}

func TestDecodeConfigUpdateRejectsNonObjectAndOversizedBodies(t *testing.T) {
	if _, err := decodeConfigUpdate(http.MethodPatch, strings.NewReader(`[]`), representativeConfig()); err == nil {
		t.Fatal("array PATCH succeeded")
	}

	oversized := strings.NewReader(strings.Repeat(" ", maxConfigRequestBytes+1))
	if _, err := decodeConfigUpdate(http.MethodPatch, oversized, representativeConfig()); err == nil {
		t.Fatal("oversized PATCH succeeded")
	}
}

func TestHandleUpdateConfigRejectsWhileRestartPending(t *testing.T) {
	s := &Server{restartPending: true}
	request := httptest.NewRequest(http.MethodPatch, "/api/config", strings.NewReader(`{"log_level":"debug"}`))
	response := httptest.NewRecorder()

	s.handleUpdateConfig(response, request)

	if response.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusConflict)
	}
	if response.Header().Get("Retry-After") != "1" {
		t.Fatalf("Retry-After = %q, want 1", response.Header().Get("Retry-After"))
	}
}

func TestValidateConfigUpdateFailsClosed(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*config.Config)
		wantErr bool
	}{
		{
			name: "private subnet with authentication and WebDAV disabled",
			mutate: func(cfg *config.Config) {
				cfg.BindAddress = "192.0.2.10"
				cfg.UseAuth = true
				cfg.DisableWebDav = true
				cfg.AllowedClientCIDRs = []string{"192.0.2.10/32"}
			},
		},
		{
			name: "private subnet without authentication",
			mutate: func(cfg *config.Config) {
				cfg.BindAddress = "192.0.2.10"
				cfg.DisableWebDav = true
			},
			wantErr: true,
		},
		{
			name: "private subnet with unprotected WebDAV",
			mutate: func(cfg *config.Config) {
				cfg.BindAddress = "192.0.2.10"
				cfg.UseAuth = true
			},
			wantErr: true,
		},
		{
			name: "invalid client network",
			mutate: func(cfg *config.Config) {
				cfg.AllowedClientCIDRs = []string{"not-a-network"}
			},
			wantErr: true,
		},
		{
			name: "missing providers",
			mutate: func(cfg *config.Config) {
				cfg.Debrids = nil
				cfg.Usenet = config.Usenet{}
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := representativeConfig()
			test.mutate(cfg)
			err := validateConfigUpdate(cfg)
			if (err != nil) != test.wantErr {
				t.Fatalf(
					"validateConfigUpdate() error = %v, wantErr %t",
					err,
					test.wantErr,
				)
			}
		})
	}
}

func TestHandleUpdateConfigRejectsInvalidPolicyBeforePersistence(t *testing.T) {
	previousPath := config.GetMainPath()
	configDir := t.TempDir()
	config.Reset()
	config.SetConfigPath(configDir)
	t.Cleanup(func() {
		config.Reset()
		config.SetConfigPath(previousPath)
	})

	configPath := filepath.Join(configDir, "config.json")
	original := []byte(`{
		"bind_address":"192.0.2.10",
		"port":"8282",
		"use_auth":true,
		"disable_webdav":true,
		"allowed_client_cidrs":["192.0.2.10/32"],
		"debrids":[{"name":"primary","provider":"realdebrid","api_key":"secret"}],
		"download_folder":"/downloads"
	}`)
	if err := os.WriteFile(configPath, original, 0600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	s := &Server{}
	request := httptest.NewRequest(
		http.MethodPatch,
		"/api/config",
		strings.NewReader(`{"allowed_client_cidrs":["not-a-network"]}`),
	)
	response := httptest.NewRecorder()

	s.handleUpdateConfig(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf(
			"status = %d, want %d; body = %q",
			response.Code,
			http.StatusBadRequest,
			response.Body.String(),
		)
	}
	persisted, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if strings.Contains(string(persisted), "not-a-network") {
		t.Fatalf("invalid network policy was persisted: %s", persisted)
	}
}
