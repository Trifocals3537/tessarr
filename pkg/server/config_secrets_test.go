package server

import (
	stdjson "encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func secretConfigFixture() *config.Config {
	return &config.Config{
		UseAuth: true,
		Debrids: []config.Debrid{{
			Name:            "primary",
			Provider:        "realdebrid",
			APIKey:          "debrid-secret",
			DownloadAPIKeys: []string{"download-secret-1", "download-secret-2"},
			Proxy:           "https://proxy-user:proxy-secret@proxy.example",
			RcPass:          "legacy-rclone-secret",
		}},
		Arrs: []config.Arr{{
			Name:  "sonarr",
			Host:  "http://sonarr:8989",
			Token: "arr-secret",
		}},
		Usenet: config.Usenet{Providers: []config.UsenetProvider{{
			Host:     "news.example",
			Port:     563,
			Username: "reader",
			Password: "usenet-secret",
		}}},
		Mount: config.Mount{ExternalRclone: config.ExternalRclone{
			RCUrl:      "http://rclone:5572",
			RCUsername: "rclone",
			RCPassword: "rclone-secret",
		}},
		Notifications: config.Notifications{
			WebhookURL:  "https://discord.example/webhook-secret",
			CallbackURL: "https://callback.example/callback-secret",
		},
		Strm:           config.Strm{Enabled: true, Path: "/media/strm", Secret: "strm-signing-secret"},
		DiscordWebhook: "https://legacy.example/discord-secret",
		CallbackURL:    "https://legacy.example/callback-secret",
		Auth: &config.Auth{
			Username:      "admin",
			APIToken:      "control-plane-secret",
			SessionSecret: "session-secret",
		},
	}
}

func TestNewConfigResponseRedactsSecretsWithoutMutatingLiveConfig(t *testing.T) {
	live := secretConfigFixture()
	runtimeArrs := []config.Arr{{
		Name:  "sonarr",
		Host:  "http://sonarr:8989",
		Token: "runtime-arr-secret",
	}}

	response, err := newConfigResponse(live, runtimeArrs)
	if err != nil {
		t.Fatalf("newConfigResponse: %v", err)
	}
	encoded, err := stdjson.Marshal(response)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}

	for _, secret := range []string{
		"debrid-secret",
		"download-secret-1",
		"download-secret-2",
		"proxy-secret",
		"legacy-rclone-secret",
		"arr-secret",
		"runtime-arr-secret",
		"usenet-secret",
		"rclone-secret",
		"webhook-secret",
		"callback-secret",
		"discord-secret",
		"control-plane-secret",
		"session-secret",
		"strm-signing-secret",
	} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("configuration response exposed %q: %s", secret, encoded)
		}
	}
	if !response.APITokenConfigured || response.AuthUsername != "admin" {
		t.Fatalf("auth metadata = configured %t, username %q", response.APITokenConfigured, response.AuthUsername)
	}
	if response.Debrids[0].APIKey != redactedConfigSecret || response.Arrs[0].Token != redactedConfigSecret {
		t.Fatalf("secrets were not represented as redacted: %#v", response.Config)
	}
	if live.Debrids[0].APIKey != "debrid-secret" || live.Arrs[0].Token != "arr-secret" {
		t.Fatalf("live config was mutated: %#v", live)
	}
}

func TestRestoreConfigSecretsPreservesConfiguredValuesAndAcceptsReplacements(t *testing.T) {
	current := secretConfigFixture()
	candidate, err := redactedConfigSnapshot(current)
	if err != nil {
		t.Fatalf("copy config: %v", err)
	}
	redactConfigSecrets(candidate)
	candidate.Debrids[0].Proxy = "https://replacement-proxy.example"
	candidate.Notifications.CallbackURL = ""
	candidate.Usenet.Providers[0].Password = "replacement-usenet-secret"

	if err := restoreConfigSecrets(candidate, current); err != nil {
		t.Fatalf("restoreConfigSecrets: %v", err)
	}
	if candidate.Debrids[0].APIKey != "debrid-secret" {
		t.Fatalf("API key = %q, want configured value", candidate.Debrids[0].APIKey)
	}
	if !slices.Equal(candidate.Debrids[0].DownloadAPIKeys, current.Debrids[0].DownloadAPIKeys) {
		t.Fatalf("download keys = %#v, want %#v", candidate.Debrids[0].DownloadAPIKeys, current.Debrids[0].DownloadAPIKeys)
	}
	if candidate.Debrids[0].Proxy != "https://replacement-proxy.example" {
		t.Fatalf("replacement proxy was not accepted: %q", candidate.Debrids[0].Proxy)
	}
	if candidate.Notifications.CallbackURL != "" {
		t.Fatalf("explicitly cleared callback URL was restored: %q", candidate.Notifications.CallbackURL)
	}
	if candidate.Usenet.Providers[0].Password != "replacement-usenet-secret" {
		t.Fatalf("replacement Usenet password was not accepted: %q", candidate.Usenet.Providers[0].Password)
	}
	if candidate.Strm.Secret != "strm-signing-secret" {
		t.Fatalf("STRM secret = %q, want configured value", candidate.Strm.Secret)
	}
}

func TestRestoreConfigSecretsAcceptsPreCutoverPlaceholder(t *testing.T) {
	current := secretConfigFixture()
	candidate, err := redactedConfigSnapshot(current)
	if err != nil {
		t.Fatal(err)
	}
	redactConfigSecrets(candidate)
	candidate.Debrids[0].APIKey = legacyRedactedConfigSecret
	candidate.Debrids[0].DownloadAPIKeys = []string{legacyRedactedConfigSecret}
	candidate.Arrs[0].Token = legacyRedactedConfigSecret
	candidate.Usenet.Providers[0].Password = legacyRedactedConfigSecret
	candidate.Strm.Secret = legacyRedactedConfigSecret

	if err := restoreConfigSecrets(candidate, current); err != nil {
		t.Fatal(err)
	}
	if candidate.Debrids[0].APIKey != current.Debrids[0].APIKey ||
		!slices.Equal(candidate.Debrids[0].DownloadAPIKeys, current.Debrids[0].DownloadAPIKeys) ||
		candidate.Arrs[0].Token != current.Arrs[0].Token ||
		candidate.Usenet.Providers[0].Password != current.Usenet.Providers[0].Password ||
		candidate.Strm.Secret != current.Strm.Secret {
		t.Fatalf("pre-cutover placeholders were not restored: %#v", candidate)
	}
}

func TestRestoreConfigSecretsRejectsMarkerForUnknownEntry(t *testing.T) {
	current := secretConfigFixture()
	candidate, err := redactedConfigSnapshot(current)
	if err != nil {
		t.Fatalf("copy config: %v", err)
	}
	redactConfigSecrets(candidate)
	candidate.Debrids[0].Name = "renamed"

	err = restoreConfigSecrets(candidate, current)
	if err == nil || !strings.Contains(err.Error(), "no longer matches") {
		t.Fatalf("restore error = %v, want unresolved marker error", err)
	}
}
