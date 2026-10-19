package config

import (
	"strings"
	"testing"
)

func TestDebridDefaultsSeparateProviderTypeAndInstanceName(t *testing.T) {
	cfg := &Config{
		Debrids: []Debrid{{
			Provider: " TORBOX ",
			Name:     " Primary ",
			APIKey:   "test-key",
		}},
		Arrs: []Arr{{Name: "Sonarr", SelectedDebrid: "primary"}},
	}
	if err := cfg.setDefaultsForPath(t.TempDir(), false); err != nil {
		t.Fatalf("setDefaultsForPath: %v", err)
	}
	if got := cfg.Debrids[0].Provider; got != "torbox" {
		t.Fatalf("Provider = %q, want torbox", got)
	}
	if got := cfg.Debrids[0].Name; got != "Primary" {
		t.Fatalf("Name = %q, want Primary", got)
	}
	if got := cfg.Arrs[0].SelectedDebrid; got != "Primary" {
		t.Fatalf("SelectedDebrid = %q, want Primary", got)
	}
}

func TestDebridDefaultsPreserveLegacyNameAsProviderType(t *testing.T) {
	cfg := &Config{Debrids: []Debrid{{Name: "RealDebrid", APIKey: "test-key"}}}
	if err := cfg.setDefaultsForPath(t.TempDir(), false); err != nil {
		t.Fatalf("setDefaultsForPath: %v", err)
	}
	if got := cfg.Debrids[0].Provider; got != "realdebrid" {
		t.Fatalf("Provider = %q, want realdebrid", got)
	}
	if got := cfg.Debrids[0].Name; got != "RealDebrid" {
		t.Fatalf("Name = %q, want RealDebrid", got)
	}
}

func TestValidateDebridsRejectsUnsupportedProviderType(t *testing.T) {
	err := validateDebrids([]Debrid{{Provider: "unknown", Name: "Primary", APIKey: "test-key"}})
	if err == nil || !strings.Contains(err.Error(), "unsupported debrid provider") {
		t.Fatalf("validateDebrids error = %v, want unsupported-provider detail", err)
	}
}

func TestValidateMountNamespaceRejectsCollisionsAndUnsafeNames(t *testing.T) {
	tests := []struct {
		name          string
		debrids       []Debrid
		customFolders map[string]CustomFolders
		want          string
	}{
		{
			name: "duplicate providers are case insensitive",
			debrids: []Debrid{
				{Name: "Primary"},
				{Name: "primary"},
			},
			want: "collides",
		},
		{
			name:    "provider cannot claim usenet key",
			debrids: []Debrid{{Name: "Usenet"}},
			want:    "built-in name",
		},
		{
			name:    "provider cannot hide built-in folder",
			debrids: []Debrid{{Name: "__ALL__"}},
			want:    "built-in name",
		},
		{
			name:    "provider must be a portable component",
			debrids: []Debrid{{Name: "primary/backup"}},
			want:    "not portable",
		},
		{
			name:          "custom folder cannot hide provider",
			debrids:       []Debrid{{Name: "Primary"}},
			customFolders: map[string]CustomFolders{"primary": {}},
			want:          "collides",
		},
		{
			name: "custom folders are case insensitive",
			customFolders: map[string]CustomFolders{
				"Movies": {},
				"movies": {},
			},
			want: "collides",
		},
		{
			name:          "custom folder cannot hide version file",
			customFolders: map[string]CustomFolders{"VERSION.TXT": {}},
			want:          "built-in name",
		},
		{
			name:          "custom folder cannot have leading whitespace",
			customFolders: map[string]CustomFolders{" Movies": {}},
			want:          "whitespace",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateMountNamespace(tt.debrids, tt.customFolders)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validateMountNamespace error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestValidateMountNamespaceAllowsUniqueProviderInstances(t *testing.T) {
	err := validateMountNamespace(
		[]Debrid{
			{Provider: "torbox", Name: "TorBox Primary"},
			{Provider: "realdebrid", Name: "RD Fallback"},
		},
		map[string]CustomFolders{"Recently Added": {}},
	)
	if err != nil {
		t.Fatalf("validateMountNamespace: %v", err)
	}
}

func TestValidateArrDebridSelectionsRejectsUnknownInstance(t *testing.T) {
	err := validateArrDebridSelections(
		[]Arr{{Name: "Sonarr", SelectedDebrid: "torbox"}},
		[]Debrid{{Provider: "torbox", Name: "TorBox Primary"}},
	)
	if err == nil || !strings.Contains(err.Error(), "unknown debrid provider") {
		t.Fatalf("validateArrDebridSelections error = %v, want unknown-provider detail", err)
	}
}
