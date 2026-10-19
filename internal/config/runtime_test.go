package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func useRuntimeConfig(t *testing.T) *Config {
	t.Helper()
	oldPath := GetMainPath()
	SetConfigPath(t.TempDir())
	Reset()
	t.Cleanup(func() {
		Reset()
		SetConfigPath(oldPath)
	})
	return Get()
}

func TestSetConfigPathValidatesAndCanonicalizesRoot(t *testing.T) {
	oldPath := GetMainPath()
	t.Cleanup(func() { _ = SetConfigPath(oldPath) })

	parent := t.TempDir()
	configured := filepath.Join(parent, "nested", "..", "config")
	if err := SetConfigPath(configured); err != nil {
		t.Fatal(err)
	}
	want, err := filepath.Abs(filepath.Clean(configured))
	if err != nil {
		t.Fatal(err)
	}
	if got := GetMainPath(); got != want {
		t.Fatalf("GetMainPath() = %q, want %q", got, want)
	}

	outside := t.TempDir()
	link := filepath.Join(parent, "config-link")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := SetConfigPath(link); err == nil {
		t.Fatal("SetConfigPath() accepted a symlink root")
	}
	if got := GetMainPath(); got != want {
		t.Fatalf("failed SetConfigPath() changed root to %q", got)
	}
}

func TestUpdatePublishesImmutableHotSnapshot(t *testing.T) {
	previous := useRuntimeConfig(t)
	originalRelative := previous.RelativeSymlinks
	var retainedDraft *Config

	result, err := Update(func(draft *Config) error {
		retainedDraft = draft
		draft.RelativeSymlinks = !originalRelative
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RestartRequired {
		t.Fatal("hot update unexpectedly required a restart")
	}
	if previous.RelativeSymlinks != originalRelative {
		t.Fatal("previous snapshot was mutated in place")
	}
	if got := Get().RelativeSymlinks; got == originalRelative {
		t.Fatalf("active RelativeSymlinks = %v", got)
	}

	result.Desired.RelativeSymlinks = originalRelative
	retainedDraft.RelativeSymlinks = originalRelative
	if got := Get().RelativeSymlinks; got == originalRelative {
		t.Fatalf("result exposed mutable active state: %v", got)
	}
}

func TestUpdatePublishesLogLevelWithoutRestart(t *testing.T) {
	current := useRuntimeConfig(t)
	want := "debug"
	if current.LogLevel == want {
		want = "warn"
	}

	result, err := Update(func(draft *Config) error {
		draft.LogLevel = want
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.RestartRequired {
		t.Fatal("log-level update unexpectedly required a restart")
	}
	if got := Get().LogLevel; got != want {
		t.Fatalf("active LogLevel = %q, want %q", got, want)
	}
}

func TestUpdatePersistsColdConfigWithoutPublishing(t *testing.T) {
	previous := useRuntimeConfig(t)
	originalBind := previous.BindAddress

	result, err := Update(func(draft *Config) error {
		draft.BindAddress = "192.0.2.10"
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !result.RestartRequired {
		t.Fatal("listener update did not require a restart")
	}
	if got := Get().BindAddress; got != originalBind {
		t.Fatalf("cold setting became active before restart: %q", got)
	}

	Reset()
	if got := Get().BindAddress; got != "192.0.2.10" {
		t.Fatalf("reloaded BindAddress = %q", got)
	}
}

func TestUpdateAfterColdChangePreservesDesiredState(t *testing.T) {
	useRuntimeConfig(t)
	if _, err := Update(func(draft *Config) error {
		draft.BindAddress = "192.0.2.20"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := Update(func(draft *Config) error {
		draft.RelativeSymlinks = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Neither update becomes active while the listener change is pending, but
	// both must survive into the replacement process.
	if Get().BindAddress == "192.0.2.20" {
		t.Fatal("cold listener change was published early")
	}
	if err := MarkRestartApplying(); err != nil {
		t.Fatal(err)
	}
	if err := CommitRestart(); err != nil {
		t.Fatal(err)
	}
	Reset()
	if got := Get().BindAddress; got != "192.0.2.20" {
		t.Fatalf("pending BindAddress was lost: %q", got)
	}
	if !Get().RelativeSymlinks {
		t.Fatal("follow-up update overwrote pending desired state")
	}
}

func TestUpdateSerializesConcurrentWriters(t *testing.T) {
	useRuntimeConfig(t)

	const writers = 24
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for i := 0; i < writers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := Update(func(draft *Config) error {
				draft.QueueCleanup.Rules = append(draft.QueueCleanup.Rules, QueueCleanupRule{
					Match:  fmt.Sprintf("writer-%d", i),
					Action: "blacklist",
				})
				return nil
			})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	seen := make(map[string]bool, writers)
	for _, rule := range Get().QueueCleanup.Rules {
		if rule.ID == "" {
			seen[rule.Match] = true
		}
	}
	if len(seen) != writers {
		t.Fatalf("retained %d/%d concurrent updates", len(seen), writers)
	}
}

func TestGetAuthReturnsDefensiveCopy(t *testing.T) {
	useRuntimeConfig(t)
	_, err := Update(func(draft *Config) error {
		draft.UseAuth = true
		draft.Auth.Username = "operator"
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	auth := Get().GetAuth()
	auth.Username = "mutated"
	if got := Get().GetAuth().Username; got != "operator" {
		t.Fatalf("active auth was mutated through GetAuth: %q", got)
	}
}

func TestUpdateRollsBackAuthWhenMainConfigCannotCommit(t *testing.T) {
	current := useRuntimeConfig(t)
	originalAuth, err := os.ReadFile(current.AuthFile())
	if err != nil {
		t.Fatal(err)
	}

	configFile := filepath.Join(GetMainPath(), "config.json")
	if err := os.Remove(configFile); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(configFile, 0o700); err != nil {
		t.Fatal(err)
	}

	if _, err := Update(func(draft *Config) error {
		draft.BindAddress = "192.0.2.99"
		draft.Auth.APIToken = "must-not-commit"
		return nil
	}); err == nil {
		t.Fatal("Update succeeded when config.json was not writable as a file")
	}

	after, err := os.ReadFile(current.AuthFile())
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(originalAuth) {
		t.Fatal("auth.json was not rolled back after config.json failed")
	}
	if Get().Auth.APIToken == "must-not-commit" {
		t.Fatal("failed update was published")
	}
	if _, err := os.Stat(restartTransactionPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed update retained restart transaction: %v", err)
	}
}

func TestConcurrentReadersSeeCompleteSnapshots(t *testing.T) {
	useRuntimeConfig(t)
	if _, err := Update(func(draft *Config) error {
		draft.Usenet.AvailabilitySamplePercent = 7
		draft.Usenet.ImportAvailabilitySamplePercent = 7
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	const updates = 20
	var readers sync.WaitGroup
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-done:
					return
				default:
					cfg := Get()
					if cfg.Usenet.AvailabilitySamplePercent != cfg.Usenet.ImportAvailabilitySamplePercent {
						t.Errorf(
							"observed mixed snapshot: availability=%d import=%d",
							cfg.Usenet.AvailabilitySamplePercent,
							cfg.Usenet.ImportAvailabilitySamplePercent,
						)
						return
					}
				}
			}
		}()
	}
	for i := 0; i < updates; i++ {
		_, err := Update(func(draft *Config) error {
			value := i + 20
			draft.Usenet.AvailabilitySamplePercent = value
			draft.Usenet.ImportAvailabilitySamplePercent = value
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	close(done)
	readers.Wait()
}
