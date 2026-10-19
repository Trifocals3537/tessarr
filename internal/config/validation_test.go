package config

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestLoadForValidationIsReadOnly(t *testing.T) {
	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.json")
	original := []byte(`{
  "use_auth": true,
  "debrids": [
    {
      "name": "realdebrid",
      "api_key": "test-key"
    }
  ]
}`)
	if err := os.WriteFile(configPath, original, 0600); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadForValidation(configDir)
	if err != nil {
		t.Fatalf("LoadForValidation() error = %v", err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if want := filepath.Join(configDir, "downloads"); cfg.DownloadFolder != want {
		t.Fatalf("DownloadFolder = %q, want %q", cfg.DownloadFolder, want)
	}
	if want := filepath.Join(configDir, "usenet", "streams"); cfg.Usenet.DiskBufferPath != want {
		t.Fatalf("Usenet.DiskBufferPath = %q, want %q", cfg.Usenet.DiskBufferPath, want)
	}

	after, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, original) {
		t.Fatal("LoadForValidation changed config.json")
	}
	if _, err := os.Stat(filepath.Join(configDir, "auth.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("auth.json was created during validation: %v", err)
	}
	if entries, err := os.ReadDir(configDir); err != nil {
		t.Fatal(err)
	} else if len(entries) != 1 || entries[0].Name() != "config.json" {
		t.Fatalf("validation created unexpected files: %v", entries)
	}
}

func TestLoadForValidationRejectsInvalidJSON(t *testing.T) {
	configDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(`{"debrids":`), 0600); err != nil {
		t.Fatal(err)
	}

	if _, err := LoadForValidation(configDir); err == nil {
		t.Fatal("LoadForValidation() error = nil, want invalid JSON error")
	}
}

func TestLoadForValidationRejectsMalformedExistingAuth(t *testing.T) {
	for _, contents := range []string{`{"username":`, `[]`} {
		t.Run(contents, func(t *testing.T) {
			configDir := t.TempDir()
			if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(`{}`), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(configDir, "auth.json"), []byte(contents), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadForValidation(configDir); err == nil {
				t.Fatal("LoadForValidation() accepted malformed auth.json")
			}
		})
	}
}

func TestLoadForValidationReadsExistingAuthWithoutChangingIt(t *testing.T) {
	configDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(configDir, "config.json"), []byte(`{"use_auth":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	authData := []byte(`{"username":"operator","password":"hash","session_secret":"secret"}`)
	authPath := filepath.Join(configDir, "auth.json")
	if err := os.WriteFile(authPath, authData, 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadForValidation(configDir)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Auth == nil || cfg.Auth.Username != "operator" || cfg.Auth.SessionSecret != "secret" {
		t.Fatalf("validated auth = %#v", cfg.Auth)
	}
	after, err := os.ReadFile(authPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, authData) {
		t.Fatal("validation changed auth.json")
	}
	if runtime.GOOS != "windows" {
		info, err := os.Stat(authPath)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o644 {
			t.Fatalf("validation changed auth.json mode to %o", info.Mode().Perm())
		}
	}
}

func TestLoadForValidationRequiresExistingConfig(t *testing.T) {
	configDir := t.TempDir()

	_, err := LoadForValidation(configDir)
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("LoadForValidation() error = %v, want os.ErrNotExist", err)
	}
}

func TestLiveLoadSecuresExistingConfigAndAuthFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not expose Unix permission bits")
	}

	previousPath := GetMainPath()
	configDir := t.TempDir()
	SetConfigPath(configDir)
	t.Cleanup(func() {
		SetConfigPath(previousPath)
	})

	configPath := filepath.Join(configDir, "config.json")
	authPath := filepath.Join(configDir, "auth.json")
	if err := os.WriteFile(configPath, []byte(`{}`), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		authPath,
		[]byte(`{"session_secret":"existing-secret"}`),
		0644,
	); err != nil {
		t.Fatal(err)
	}

	cfg := &Config{}
	if err := cfg.loadConfig(); err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}

	for _, path := range []string{configPath, authPath} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != privateFileMode {
			t.Fatalf("%s mode = %o, want %o", path, got, privateFileMode)
		}
	}
}

func TestValidationDoesNotChangeExistingConfigMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows does not expose Unix permission bits")
	}

	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.json")
	if err := os.WriteFile(configPath, []byte(`{}`), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadForValidation(configDir); err != nil {
		t.Fatalf("LoadForValidation() error = %v", err)
	}

	info, err := os.Stat(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0644 {
		t.Fatalf("validation changed config mode to %o, want 644", got)
	}
}

func TestSetDefaultsUsesLoopbackBindAddress(t *testing.T) {
	cfg := &Config{}
	if err := cfg.setDefaultsForPath(t.TempDir(), false); err != nil {
		t.Fatalf("setDefaultsForPath() error = %v", err)
	}
	if cfg.BindAddress != DefaultBindAddress {
		t.Fatalf("BindAddress = %q, want %q", cfg.BindAddress, DefaultBindAddress)
	}
}

func TestSetDefaultsPreservesExplicitBindAddress(t *testing.T) {
	cfg := &Config{BindAddress: "0.0.0.0"}
	if err := cfg.setDefaultsForPath(t.TempDir(), false); err != nil {
		t.Fatalf("setDefaultsForPath() error = %v", err)
	}
	if cfg.BindAddress != "0.0.0.0" {
		t.Fatalf("BindAddress = %q, want explicit override", cfg.BindAddress)
	}
}

func TestContainerRemoteDefaultsProtectWebDAV(t *testing.T) {
	t.Setenv("TESSARR_BIND_ADDRESS", "0.0.0.0")
	t.Setenv("TESSARR_ENABLE_WEBDAV_AUTH", "true")

	cfg := &Config{UseAuth: true}
	cfg.applyEnvOverrides()
	if cfg.BindAddress != "0.0.0.0" {
		t.Fatalf("BindAddress = %q, want container listener", cfg.BindAddress)
	}
	if !cfg.EnableWebdavAuth {
		t.Fatal("container environment did not protect WebDAV")
	}
	if err := cfg.ValidateDeployment(); err != nil {
		t.Fatalf("container defaults failed deployment validation: %v", err)
	}
}

func TestValidateLogLevel(t *testing.T) {
	base := Config{
		Debrids:        []Debrid{{Name: "primary", Provider: "realdebrid", APIKey: "test-key"}},
		DownloadFolder: "/downloads",
	}
	for _, level := range []string{"", "trace", "DEBUG", "info", "warn", "warning", "error"} {
		cfg := base
		cfg.LogLevel = level
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() rejected log level %q: %v", level, err)
		}
	}

	base.LogLevel = "verbose"
	if err := base.Validate(); err == nil || !strings.Contains(err.Error(), "unsupported log level") {
		t.Fatalf("Validate() error = %v, want unsupported log level", err)
	}
}

func TestValidateURLBase(t *testing.T) {
	for _, value := range []string{"", "/", "/tessarr/", "/media/ui/"} {
		if err := validateURLBase(value); err != nil {
			t.Fatalf("validateURLBase(%q) error = %v", value, err)
		}
	}

	for _, value := range []string{
		"https://example.com/",
		"//example.com/",
		"/tessarr?next=evil",
		"/tessarr#fragment",
		"/tessarr\\admin/",
		"/tessarr\r\nX-Test: injected/",
		"/tessarr/%0d%0aX-Test:%20injected/",
		"/tessarr/../admin/",
	} {
		if err := validateURLBase(value); err == nil {
			t.Fatalf("validateURLBase(%q) accepted unsafe value", value)
		}
	}
}

func TestValidateDeploymentRejectsUnprotectedRemoteServices(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "loopback without auth",
			cfg: Config{
				BindAddress: "127.0.0.1",
			},
		},
		{
			name: "remote without auth",
			cfg: Config{
				BindAddress:   "0.0.0.0",
				DisableWebDav: true,
			},
			wantErr: true,
		},
		{
			name: "remote auth with open WebDAV",
			cfg: Config{
				BindAddress: "10.0.0.2",
				UseAuth:     true,
			},
			wantErr: true,
		},
		{
			name: "remote auth with WebDAV disabled",
			cfg: Config{
				BindAddress:   "10.0.0.2",
				UseAuth:       true,
				DisableWebDav: true,
			},
		},
		{
			name: "remote auth with WebDAV auth",
			cfg: Config{
				BindAddress:      "10.0.0.2",
				UseAuth:          true,
				EnableWebdavAuth: true,
			},
		},
		{
			name: "remote auth with valid client networks",
			cfg: Config{
				BindAddress:        "0.0.0.0",
				UseAuth:            true,
				DisableWebDav:      true,
				AllowedClientCIDRs: []string{"127.0.0.0/8", "192.0.2.10"},
			},
		},
		{
			name: "invalid client network",
			cfg: Config{
				BindAddress:        "127.0.0.1",
				AllowedClientCIDRs: []string{"not-a-network"},
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.cfg.ValidateDeployment()
			if (err != nil) != test.wantErr {
				t.Fatalf(
					"ValidateDeployment() error = %v, wantErr %t",
					err,
					test.wantErr,
				)
			}
		})
	}
}

func TestParseAllowedClientCIDRs(t *testing.T) {
	prefixes, err := ParseAllowedClientCIDRs([]string{
		"127.0.0.1",
		"192.0.2.99/24",
		"::1",
	})
	if err != nil {
		t.Fatalf("ParseAllowedClientCIDRs() error = %v", err)
	}

	got := make([]string, len(prefixes))
	for index, prefix := range prefixes {
		got[index] = prefix.String()
	}
	want := []string{"127.0.0.1/32", "192.0.2.0/24", "::1/128"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("prefixes = %v, want %v", got, want)
	}
}

func TestSetDefaultsBoundsJobQueueCapacity(t *testing.T) {
	tests := []struct {
		name string
		in   int
		want int
	}{
		{name: "missing uses backward compatible default", want: DefaultJobQueueCapacity},
		{name: "explicit value", in: 64, want: 64},
		{name: "upper bound", in: MaxJobQueueCapacity + 1, want: MaxJobQueueCapacity},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := &Config{JobQueueCapacity: test.in}
			if err := cfg.setDefaultsForPath(t.TempDir(), false); err != nil {
				t.Fatalf("setDefaultsForPath() error = %v", err)
			}
			if cfg.JobQueueCapacity != test.want {
				t.Fatalf("JobQueueCapacity = %d, want %d", cfg.JobQueueCapacity, test.want)
			}
		})
	}
}

func TestJobQueueCapacityEnvironmentOverride(t *testing.T) {
	t.Setenv("TESSARR_JOB_QUEUE_CAPACITY", "73")
	cfg := &Config{}
	cfg.applyEnvOverrides()
	if cfg.JobQueueCapacity != 73 {
		t.Fatalf("JobQueueCapacity = %d, want 73", cfg.JobQueueCapacity)
	}
}

func TestRelativeSymlinksEnvironmentOverride(t *testing.T) {
	t.Setenv("TESSARR_RELATIVE_SYMLINKS", "true")
	cfg := &Config{}
	cfg.applyEnvOverrides()
	if !cfg.RelativeSymlinks {
		t.Fatal("RelativeSymlinks = false, want environment override enabled")
	}
}

func TestFirstLoadAppliesBindAddressEnvironmentOverride(t *testing.T) {
	previousPath := GetMainPath()
	configDir := t.TempDir()
	SetConfigPath(configDir)
	t.Cleanup(func() {
		SetConfigPath(previousPath)
	})
	t.Setenv("TESSARR_BIND_ADDRESS", "0.0.0.0")

	cfg := &Config{}
	if err := cfg.loadConfig(); err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if cfg.BindAddress != "0.0.0.0" {
		t.Fatalf("runtime BindAddress = %q, want environment override", cfg.BindAddress)
	}

	data, err := os.ReadFile(filepath.Join(configDir, "config.json"))
	if err != nil {
		t.Fatalf("read generated config: %v", err)
	}
	if !bytes.Contains(data, []byte(`"bind_address": "127.0.0.1"`)) {
		t.Fatalf("generated config did not retain the safe native bind default: %s", data)
	}
}
