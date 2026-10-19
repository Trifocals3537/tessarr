package account

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

func newRecoveryTestAccount(token string, index int) *Account {
	return &Account{
		Debrid: "test",
		Token:  token,
		Index:  index,
		links:  xsync.NewMap[string, types.DownloadLink](),
	}
}

func newRecoveryTestManager(now *time.Time, logger zerolog.Logger, accounts ...*Account) *Manager {
	m := &Manager{
		debrid:   "test",
		accounts: xsync.NewMap[string, *Account](),
		logger:   logger,
		now: func() time.Time {
			return *now
		},
	}
	for _, account := range accounts {
		m.accounts.Store(account.Token, account)
	}
	if len(accounts) > 0 {
		m.current.Store(accounts[0])
	}
	return m
}

func TestTemporarySuspensionUsesBoundedBackoffAndOneProbe(t *testing.T) {
	account := newRecoveryTestAccount("token", 0)
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)

	status, changed := account.SuspendTemporary(now, 0, 0, "bytes_limit_reached")
	if !changed || status.State != StateTemporarilySuspended || status.Failures != 1 || status.RetryAfter != time.Minute {
		t.Fatalf("first suspension = %+v, changed %v", status, changed)
	}
	status, changed = account.SuspendTemporary(now.Add(10*time.Second), 0, 0, "late_duplicate")
	if changed || status.Failures != 1 || status.RetryAfter != 50*time.Second {
		t.Fatalf("duplicate suspension = %+v, changed %v", status, changed)
	}

	probeAt := now.Add(time.Minute)
	acquired, probeID, retryAfter := account.TryAcquire(probeAt)
	if !acquired || probeID == 0 || retryAfter != 0 {
		t.Fatalf("first half-open acquisition = %v/%d/%s", acquired, probeID, retryAfter)
	}
	if acquired, _, retryAfter = account.TryAcquire(probeAt); acquired || retryAfter != recoveryProbeLease {
		t.Fatalf("second half-open acquisition = %v/%s, want blocked for %s", acquired, retryAfter, recoveryProbeLease)
	}

	status, changed = account.SuspendTemporary(probeAt, probeID, 0, "probe_failed")
	if !changed || status.Failures != 2 || status.RetryAfter != 2*time.Minute {
		t.Fatalf("failed probe suspension = %+v, changed %v", status, changed)
	}

	probeAt = probeAt.Add(2 * time.Minute)
	acquired, probeID, _ = account.TryAcquire(probeAt)
	if !acquired || probeID == 0 {
		t.Fatal("second recovery probe was not acquired")
	}
	status, changed = account.SuspendTemporary(probeAt, probeID, 30*time.Minute, "retry_after")
	if !changed || status.RetryAfter != 30*time.Minute {
		t.Fatalf("provider Retry-After suspension = %+v, changed %v", status, changed)
	}
}

func TestTemporarySuspensionAllowsOnlyOneConcurrentHalfOpenProbe(t *testing.T) {
	account := newRecoveryTestAccount("token", 0)
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	account.SuspendTemporary(now, 0, 0, "quota_exceeded")

	var acquired atomic.Int32
	var ids sync.Map
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, probeID, _ := account.TryAcquire(now.Add(recoveryBackoffBase))
			if ok {
				acquired.Add(1)
				ids.Store(probeID, struct{}{})
			}
		}()
	}
	wg.Wait()

	if acquired.Load() != 1 {
		t.Fatalf("acquired probes = %d, want 1", acquired.Load())
	}
	count := 0
	ids.Range(func(_, _ any) bool { count++; return true })
	if count != 1 {
		t.Fatalf("unique probe IDs = %d, want 1", count)
	}
}

func TestOnlyMatchingProbeCanRecoverOrFailAccount(t *testing.T) {
	account := newRecoveryTestAccount("token", 0)
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	account.SuspendTemporary(now, 0, 0, "quota_exceeded")
	probeAt := now.Add(recoveryBackoffBase)
	_, probeID, _ := account.TryAcquire(probeAt)

	if _, changed := account.SuspendTemporary(probeAt, 0, 0, "stale_failure"); changed {
		t.Fatal("stale non-probe request changed recovery state")
	}
	if account.MarkHealthy(probeID + 1) {
		t.Fatal("mismatched probe recovered account")
	}
	if !account.MarkHealthy(probeID) {
		t.Fatal("matching probe did not recover account")
	}
	if status := account.RecoveryStatus(probeAt); status.State != StateActive || status.Failures != 0 {
		t.Fatalf("recovered status = %+v", status)
	}
}

func TestCanceledProbeCanBeReplacedWithoutBackoff(t *testing.T) {
	account := newRecoveryTestAccount("token", 0)
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	account.SuspendTemporary(now, 0, 0, "quota_exceeded")
	probeAt := now.Add(recoveryBackoffBase)
	_, firstID, _ := account.TryAcquire(probeAt)
	if !account.ReleaseProbe(firstID) {
		t.Fatal("matching canceled probe was not released")
	}
	acquired, secondID, _ := account.TryAcquire(probeAt)
	if !acquired || secondID == 0 || secondID == firstID {
		t.Fatalf("replacement probe = %v/%d, first %d", acquired, secondID, firstID)
	}
}

func TestPermanentDisableDoesNotHalfOpen(t *testing.T) {
	account := newRecoveryTestAccount("token", 0)
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	account.SuspendTemporary(now, 0, 0, "quota_exceeded")
	account.MarkDisabled()

	if status := account.RecoveryStatus(now.Add(48 * time.Hour)); status.State != StatePermanentlyDisabled {
		t.Fatalf("permanently disabled status = %+v", status)
	}
	if acquired, _, _ := account.TryAcquire(now.Add(48 * time.Hour)); acquired {
		t.Fatal("permanently disabled account acquired a recovery probe")
	}
	account.Reset()
	if status := account.RecoveryStatus(now); status.State != StateActive {
		t.Fatalf("reset status = %+v", status)
	}
}

func TestManagerReturnsRetryableUnavailableWithoutCallingSuspendedAccount(t *testing.T) {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	account := newRecoveryTestAccount("token", 0)
	manager := newRecoveryTestManager(&now, zerolog.Nop(), account)
	manager.SuspendTemporary(account, 0, 0, "bytes_limit_reached")

	fetches := 0
	_, err := manager.GetDownloadLink("torrent", &types.File{Link: "restricted"}, func(*Account, string, *types.File) (types.DownloadLink, error) {
		fetches++
		return types.DownloadLink{}, errors.New("should not be called")
	})
	var unavailable *UnavailableError
	if !errors.As(err, &unavailable) || !unavailable.Temporary || unavailable.RetryAfter != recoveryBackoffBase {
		t.Fatalf("GetDownloadLink() error = %#v", err)
	}
	if fetches != 0 {
		t.Fatalf("fetches = %d, want 0", fetches)
	}
}

func TestManagerPropagatesProbeIDUntilValidated(t *testing.T) {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	account := newRecoveryTestAccount("token", 0)
	manager := newRecoveryTestManager(&now, zerolog.Nop(), account)
	manager.SuspendTemporary(account, 0, 0, "quota_exceeded")
	now = now.Add(recoveryBackoffBase)

	dl, err := manager.GetDownloadLink("torrent", &types.File{Link: "restricted"}, func(account *Account, _ string, file *types.File) (types.DownloadLink, error) {
		return types.DownloadLink{
			Debrid:       account.Debrid,
			Token:        account.Token,
			Link:         file.Link,
			DownloadLink: "https://cdn.example/video",
		}, nil
	})
	if err != nil || dl.RecoveryProbeID == 0 {
		t.Fatalf("probe link/error = %+v/%v", dl, err)
	}
	if status := account.RecoveryStatus(now); status.State != StateRecoveryProbe {
		t.Fatalf("status before validation = %+v", status)
	}
	if !manager.MarkHealthy(account, dl.RecoveryProbeID) {
		t.Fatal("validated probe did not recover account")
	}
	if status := account.RecoveryStatus(now); status.State != StateActive {
		t.Fatalf("status after validation = %+v", status)
	}
}

func TestManagerResuspendsWhenRecoveryProbeCannotFetchLink(t *testing.T) {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	account := newRecoveryTestAccount("token", 0)
	manager := newRecoveryTestManager(&now, zerolog.Nop(), account)
	manager.SuspendTemporary(account, 0, 0, "quota_exceeded")
	now = now.Add(recoveryBackoffBase)

	fetchErr := errors.New("provider fetch failed")
	_, err := manager.GetDownloadLink("torrent", &types.File{Link: "restricted"}, func(*Account, string, *types.File) (types.DownloadLink, error) {
		return types.DownloadLink{}, fetchErr
	})
	if !errors.Is(err, fetchErr) {
		t.Fatalf("GetDownloadLink() error = %v, want fetch error", err)
	}
	status := account.RecoveryStatus(now)
	if status.State != StateTemporarilySuspended || status.Failures != 2 || status.RetryAfter != 2*time.Minute {
		t.Fatalf("status after failed fetch probe = %+v", status)
	}
}

func TestManagerDoesNotSpamLogsWhileAllAccountsSuspended(t *testing.T) {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	account := newRecoveryTestAccount("token", 0)
	var logs bytes.Buffer
	manager := newRecoveryTestManager(&now, zerolog.New(&logs), account)
	manager.SuspendTemporary(account, 0, 0, "bytes_limit_reached")

	for range 100 {
		if manager.Current() != nil {
			t.Fatal("suspended account returned as current")
		}
	}
	output := logs.String()
	if got := strings.Count(output, "Debrid account temporarily suspended"); got != 1 {
		t.Fatalf("suspension log count = %d, want 1; logs: %s", got, output)
	}
	if strings.Contains(output, "No active accounts available") {
		t.Fatalf("lookup emitted legacy warning spam: %s", output)
	}
}
