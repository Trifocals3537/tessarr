package config

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestHotUpdateDoesNotCreateRestartTransaction(t *testing.T) {
	current := useRuntimeConfig(t)
	_, err := Update(func(draft *Config) error {
		draft.RelativeSymlinks = !current.RelativeSymlinks
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(restartTransactionPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restart transaction exists after hot update: %v", err)
	}
}

func TestColdUpdateCommitsOnlyAfterReadiness(t *testing.T) {
	useRuntimeConfig(t)
	result, err := Update(func(draft *Config) error {
		draft.BindAddress = "192.0.2.40"
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.RestartRequired {
		t.Fatal("cold update did not require restart")
	}
	tx, err := loadRestartTransaction()
	if err != nil {
		t.Fatal(err)
	}
	if tx == nil || tx.State != restartStatePrepared {
		t.Fatalf("restart transaction = %#v", tx)
	}
	if err := MarkRestartApplying(); err != nil {
		t.Fatal(err)
	}
	if err := CommitRestart(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(restartTransactionPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restart transaction remains after readiness: %v", err)
	}
	Reset()
	if got := Get().BindAddress; got != "192.0.2.40" {
		t.Fatalf("committed BindAddress = %q", got)
	}
}

func TestInterruptedRestartRestoresLastKnownGoodConfigAndAuth(t *testing.T) {
	previous := useRuntimeConfig(t)
	previousBind := previous.BindAddress
	previousToken := previous.Auth.APIToken

	_, err := Update(func(draft *Config) error {
		draft.BindAddress = "192.0.2.41"
		draft.Auth.APIToken = "candidate-token"
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	authData, err := os.ReadFile(filepath.Join(GetMainPath(), "auth.json"))
	if err != nil {
		t.Fatal(err)
	}
	var desiredAuth Auth
	if err := json.Unmarshal(authData, &desiredAuth); err != nil {
		t.Fatal(err)
	}
	if desiredAuth.APIToken != "candidate-token" {
		t.Fatalf("persisted API token = %q, want candidate token", desiredAuth.APIToken)
	}

	rolledBack, err := PrepareRestartStartup()
	if err != nil {
		t.Fatal(err)
	}
	if rolledBack {
		t.Fatal("first startup attempt rolled back before trying desired config")
	}
	rolledBack, err = PrepareRestartStartup()
	if err != nil {
		t.Fatal(err)
	}
	if !rolledBack {
		t.Fatal("interrupted applying transaction was not rolled back")
	}

	Reset()
	if got := Get().BindAddress; got != previousBind {
		t.Fatalf("restored BindAddress = %q, want %q", got, previousBind)
	}
	if got := Get().Auth.APIToken; got != previousToken {
		t.Fatalf("restored API token = %q, want original", got)
	}
}

func TestPreparedRestartRollsBackDesiredFileTampering(t *testing.T) {
	previous := useRuntimeConfig(t)
	previousBind := previous.BindAddress
	_, err := Update(func(draft *Config) error {
		draft.BindAddress = "192.0.2.42"
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	configFile := filepath.Join(GetMainPath(), "config.json")
	data, err := os.ReadFile(configFile)
	if err != nil {
		t.Fatal(err)
	}
	var changed Config
	if err := json.Unmarshal(data, &changed); err != nil {
		t.Fatal(err)
	}
	changed.Port = "6553"
	data, err = json.MarshalIndent(changed, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configFile, data, privateFileMode); err != nil {
		t.Fatal(err)
	}

	rolledBack, err := PrepareRestartStartup()
	if err != nil {
		t.Fatal(err)
	}
	if !rolledBack {
		t.Fatal("tampered desired configuration was not rolled back")
	}
	if _, err := os.Stat(restartTransactionPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restart transaction remains after rollback: %v", err)
	}
	Reset()
	if got := Get().BindAddress; got != previousBind {
		t.Fatalf("restored BindAddress = %q, want %q", got, previousBind)
	}
}

func TestCorruptRestartTransactionFailsClosed(t *testing.T) {
	current := useRuntimeConfig(t)
	original, err := os.ReadFile(current.JsonFile())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(restartTransactionPath(), []byte(`{"version":1`), privateFileMode); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareRestartStartup(); err == nil {
		t.Fatal("corrupt restart transaction was accepted")
	}
	after, err := os.ReadFile(current.JsonFile())
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(original) {
		t.Fatal("corrupt transaction modified config.json")
	}
}

func TestRestartTransactionUsesPrivatePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows ACLs are not represented by Unix permission bits")
	}
	useRuntimeConfig(t)
	_, err := Update(func(draft *Config) error {
		draft.BindAddress = "192.0.2.43"
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(restartTransactionPath())
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != privateFileMode {
		t.Fatalf("restart transaction permissions = %o", got)
	}
}

func TestStrmUpdateMaterializesSecretBeforeRestartTransaction(t *testing.T) {
	useRuntimeConfig(t)
	result, err := Update(func(draft *Config) error {
		draft.Strm = Strm{
			Enabled: true,
			Path:    filepath.Join(t.TempDir(), "strm"),
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.RestartRequired {
		t.Fatal("enabling STRM did not require restart")
	}
	if len(result.Desired.Strm.Secret) != 64 {
		t.Fatalf("desired STRM secret length = %d, want 64", len(result.Desired.Strm.Secret))
	}
	data, err := os.ReadFile(filepath.Join(GetMainPath(), "config.json"))
	if err != nil {
		t.Fatal(err)
	}
	var persisted Config
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.Strm.Secret != result.Desired.Strm.Secret {
		t.Fatal("persisted STRM secret differs from restart transaction")
	}
	if err := MarkRestartApplying(); err != nil {
		t.Fatalf("MarkRestartApplying() rejected generated STRM secret: %v", err)
	}
}

func TestAuthUpdateMaterializesTokenBeforeRestartTransaction(t *testing.T) {
	useRuntimeConfig(t)
	result, err := Update(func(draft *Config) error {
		draft.UseAuth = true
		draft.BindAddress = "127.0.0.2"
		draft.Auth.APIToken = ""
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.RestartRequired || result.Desired.Auth == nil || len(result.Desired.Auth.APIToken) != 64 {
		t.Fatalf("desired restart auth defaults not materialized: restart=%t auth=%+v", result.RestartRequired, result.Desired.Auth != nil)
	}
	data, err := os.ReadFile(filepath.Join(GetMainPath(), "auth.json"))
	if err != nil {
		t.Fatal(err)
	}
	var persisted Auth
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.APIToken != result.Desired.Auth.APIToken {
		t.Fatal("persisted API token differs from restart transaction")
	}
	if err := MarkRestartApplying(); err != nil {
		t.Fatalf("MarkRestartApplying() rejected generated API token: %v", err)
	}
}
