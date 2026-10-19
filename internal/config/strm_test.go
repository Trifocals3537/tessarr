package config

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestStrmDefaultsGenerateStrongPersistentShape(t *testing.T) {
	cfg := &Config{Strm: Strm{Enabled: true, Path: filepath.Join(t.TempDir(), "strm")}}
	if err := cfg.setStrmDefaults(true); err != nil {
		t.Fatal(err)
	}
	if cfg.Strm.DeliveryMode != StrmDeliveryProxy {
		t.Fatalf("delivery mode = %q", cfg.Strm.DeliveryMode)
	}
	key, err := hex.DecodeString(cfg.Strm.Secret)
	if err != nil || len(key) != 32 {
		t.Fatalf("generated secret = %q, err = %v", cfg.Strm.Secret, err)
	}
}

func TestStrmValidation(t *testing.T) {
	valid := Strm{
		Enabled: true, Path: filepath.Join(t.TempDir(), "strm"),
		Secret: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}
	if err := valid.Validate("https://media.example/tessarr"); err != nil {
		t.Fatalf("valid STRM config rejected: %v", err)
	}

	tests := []struct {
		name   string
		strm   Strm
		appURL string
	}{
		{name: "missing path", strm: Strm{Enabled: true, Secret: valid.Secret}},
		{name: "invalid key", strm: Strm{Enabled: true, Path: valid.Path, Secret: "short"}},
		{name: "invalid delivery", strm: Strm{Enabled: true, Path: valid.Path, Secret: valid.Secret, DeliveryMode: "copy"}},
		{name: "URL credentials", strm: valid, appURL: "https://user:pass@media.example"},
		{name: "URL query", strm: valid, appURL: "https://media.example?token=secret"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.strm.Validate(test.appURL); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestLoadConfigPersistsGeneratedStrmSecret(t *testing.T) {
	configDir := useTemporaryConfigPath(t)
	configPath := filepath.Join(configDir, "config.json")
	if err := os.WriteFile(configPath, []byte(`{"strm":{"enabled":true,"path":"/tmp/tessarr-strm-test"}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	first := &Config{}
	if err := first.loadConfig(); err != nil {
		t.Fatal(err)
	}
	if len(first.Strm.Secret) != 64 {
		t.Fatalf("generated secret length = %d, want 64", len(first.Strm.Secret))
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	var persisted Config
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Strm.Secret != first.Strm.Secret {
		t.Fatal("generated STRM secret was not persisted")
	}

	second := &Config{}
	if err := second.loadConfig(); err != nil {
		t.Fatal(err)
	}
	if second.Strm.Secret != first.Strm.Secret {
		t.Fatal("STRM secret changed after reload")
	}
}
