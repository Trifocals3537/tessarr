package server

import (
	stdjson "encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/config"
)

// redactedConfigSecret is an API-only placeholder. It is never persisted: a
// matching configured secret is restored before validation, and unresolved
// placeholders are rejected.
const redactedConfigSecret = "__TESSARR_REDACTED__"

type configResponse struct {
	*config.Config
	APITokenConfigured bool   `json:"api_token_configured"`
	AuthUsername       string `json:"auth_username,omitempty"`
}

func newConfigResponse(cfg *config.Config, runtimeArrs []config.Arr) (*configResponse, error) {
	redacted, err := redactedConfigSnapshot(cfg)
	if err != nil {
		return nil, err
	}
	redacted.Arrs = cloneArrs(runtimeArrs)
	redactConfigSecrets(redacted)

	response := &configResponse{Config: redacted}
	if auth := cfg.GetAuth(); auth != nil {
		response.APITokenConfigured = auth.APIToken != ""
		response.AuthUsername = auth.Username
	}
	return response, nil
}

func redactedConfigSnapshot(cfg *config.Config) (*config.Config, error) {
	if cfg == nil {
		return nil, fmt.Errorf("configuration is unavailable")
	}
	data, err := stdjson.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("copy configuration: %w", err)
	}
	var snapshot config.Config
	if err := stdjson.Unmarshal(data, &snapshot); err != nil {
		return nil, fmt.Errorf("copy configuration: %w", err)
	}
	return &snapshot, nil
}

func cloneArrs(arrs []config.Arr) []config.Arr {
	if arrs == nil {
		return nil
	}
	return append([]config.Arr(nil), arrs...)
}

func redactConfigSecrets(cfg *config.Config) {
	for i := range cfg.Debrids {
		redactString(&cfg.Debrids[i].APIKey)
		redactStrings(&cfg.Debrids[i].DownloadAPIKeys)
		redactString(&cfg.Debrids[i].Proxy)
		redactString(&cfg.Debrids[i].RcPass)
	}
	for i := range cfg.Arrs {
		redactString(&cfg.Arrs[i].Token)
	}
	for i := range cfg.Usenet.Providers {
		redactString(&cfg.Usenet.Providers[i].Password)
	}
	redactString(&cfg.Mount.ExternalRclone.RCPassword)
	redactString(&cfg.Notifications.WebhookURL)
	redactString(&cfg.Notifications.CallbackURL)
	redactString(&cfg.Strm.Secret)
	//lint:ignore SA1019 Legacy config fields must remain safe during migration.
	redactString(&cfg.DiscordWebhook)
	//lint:ignore SA1019 Legacy config fields must remain safe during migration.
	redactString(&cfg.CallbackURL)
}

func redactString(value *string) {
	if *value != "" {
		*value = redactedConfigSecret
	}
}

func redactStrings(values *[]string) {
	if len(*values) != 0 {
		*values = []string{redactedConfigSecret}
	}
}

// restoreConfigSecrets resolves redaction placeholders against the live
// configuration. Empty values remain empty, so API clients can deliberately
// clear a secret; the web UI sends the placeholder when a blank field means
// "leave the configured value unchanged".
func restoreConfigSecrets(candidate, current *config.Config) error {
	if candidate == nil || current == nil {
		return fmt.Errorf("configuration is unavailable")
	}

	debrids := make(map[string]config.Debrid, len(current.Debrids))
	for _, debrid := range current.Debrids {
		debrids[debridIdentity(debrid)] = debrid
	}
	for i := range candidate.Debrids {
		old, ok := debrids[debridIdentity(candidate.Debrids[i])]
		if err := restoreString(&candidate.Debrids[i].APIKey, old.APIKey, ok, "debrid API key"); err != nil {
			return err
		}
		if err := restoreStrings(&candidate.Debrids[i].DownloadAPIKeys, old.DownloadAPIKeys, ok, "debrid download API keys"); err != nil {
			return err
		}
		if err := restoreString(&candidate.Debrids[i].Proxy, old.Proxy, ok, "debrid proxy"); err != nil {
			return err
		}
		if err := restoreString(&candidate.Debrids[i].RcPass, old.RcPass, ok, "deprecated debrid rclone password"); err != nil {
			return err
		}
	}

	arrs := make(map[string]config.Arr, len(current.Arrs))
	for _, arr := range current.Arrs {
		arrs[arrIdentity(arr)] = arr
	}
	for i := range candidate.Arrs {
		old, ok := arrs[arrIdentity(candidate.Arrs[i])]
		if err := restoreString(&candidate.Arrs[i].Token, old.Token, ok, "Arr API token"); err != nil {
			return err
		}
	}

	usenetProviders := make(map[string]config.UsenetProvider, len(current.Usenet.Providers))
	for _, provider := range current.Usenet.Providers {
		usenetProviders[usenetProviderIdentity(provider)] = provider
	}
	for i := range candidate.Usenet.Providers {
		old, ok := usenetProviders[usenetProviderIdentity(candidate.Usenet.Providers[i])]
		if err := restoreString(&candidate.Usenet.Providers[i].Password, old.Password, ok, "Usenet password"); err != nil {
			return err
		}
	}

	if err := restoreString(
		&candidate.Mount.ExternalRclone.RCPassword,
		current.Mount.ExternalRclone.RCPassword,
		true,
		"external rclone password",
	); err != nil {
		return err
	}
	if err := restoreString(&candidate.Notifications.WebhookURL, current.Notifications.WebhookURL, true, "notification webhook URL"); err != nil {
		return err
	}
	if err := restoreString(&candidate.Notifications.CallbackURL, current.Notifications.CallbackURL, true, "notification callback URL"); err != nil {
		return err
	}
	if err := restoreString(&candidate.Strm.Secret, current.Strm.Secret, true, "STRM signing secret"); err != nil {
		return err
	}
	//lint:ignore SA1019 Legacy config fields must remain safe during migration.
	if err := restoreString(&candidate.DiscordWebhook, current.DiscordWebhook, true, "deprecated Discord webhook URL"); err != nil {
		return err
	}
	//lint:ignore SA1019 Legacy config fields must remain safe during migration.
	return restoreString(&candidate.CallbackURL, current.CallbackURL, true, "deprecated callback URL")
}

func restoreString(value *string, configured string, identityMatched bool, label string) error {
	if !isRedactedConfigSecret(*value) {
		return nil
	}
	if !identityMatched || configured == "" {
		return fmt.Errorf("cannot preserve %s because its configured entry no longer matches", label)
	}
	*value = configured
	return nil
}

func restoreStrings(values *[]string, configured []string, identityMatched bool, label string) error {
	markerIndex := -1
	for i, value := range *values {
		if isRedactedConfigSecret(value) {
			markerIndex = i
			break
		}
	}
	if markerIndex == -1 {
		return nil
	}
	if !identityMatched || len(configured) == 0 {
		return fmt.Errorf("cannot preserve %s because its configured entry no longer matches", label)
	}

	var restored []string
	for _, value := range *values {
		if isRedactedConfigSecret(value) {
			restored = append(restored, configured...)
			continue
		}
		restored = append(restored, value)
	}
	*values = restored
	return nil
}

func isRedactedConfigSecret(value string) bool {
	return value == redactedConfigSecret || value == legacyRedactedConfigSecret
}

func debridIdentity(debrid config.Debrid) string {
	return strings.ToLower(strings.TrimSpace(debrid.Name)) + "\x00" + strings.ToLower(strings.TrimSpace(debrid.Provider))
}

func arrIdentity(arr config.Arr) string {
	return strings.ToLower(strings.TrimSpace(arr.Name)) + "\x00" + strings.TrimSpace(arr.Host)
}

func usenetProviderIdentity(provider config.UsenetProvider) string {
	return strings.ToLower(strings.TrimSpace(provider.Host)) + "\x00" +
		strconv.Itoa(provider.Port) + "\x00" + strings.TrimSpace(provider.Username)
}
