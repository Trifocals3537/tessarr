package account

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

func TestGetDownloadLinkContextCancelsWithoutAccountFailover(t *testing.T) {
	t.Parallel()

	first := newLinkCacheTestAccount()
	first.Index = 0
	second := newLinkCacheTestAccount()
	second.Token = "token-2"
	second.Index = 1
	manager := contextTestManager(first, second)
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	type result struct {
		link types.DownloadLink
		err  error
	}
	done := make(chan result, 1)
	var fetches atomic.Int32
	go func() {
		link, err := manager.GetDownloadLinkContext(ctx, "torrent", &types.File{Link: "restricted"}, func(ctx context.Context, _ *Account, _ string, _ *types.File) (types.DownloadLink, error) {
			if fetches.Add(1) == 1 {
				close(started)
			}
			<-ctx.Done()
			return types.DownloadLink{}, ctx.Err()
		})
		done <- result{link: link, err: err}
	}()
	waitForContextSignal(t, started)
	cancel()
	select {
	case result := <-done:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("GetDownloadLinkContext() error = %v, want context.Canceled", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for link cancellation")
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("link fetches = %d, want 1 without account failover", got)
	}
	if first.DownloadLinksCount() != 0 || second.DownloadLinksCount() != 0 {
		t.Fatal("canceled link fetch populated an account cache")
	}
}

func TestRefreshLinksContextCancelsInFlightFetcher(t *testing.T) {
	t.Parallel()

	acc := newLinkCacheTestAccount()
	manager := contextTestManager(acc)
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- manager.RefreshLinksContext(ctx, func(ctx context.Context, _ *Account) ([]types.DownloadLink, error) {
			close(started)
			<-ctx.Done()
			return nil, ctx.Err()
		})
	}()
	waitForContextSignal(t, started)
	cancel()
	if err := waitForContextResult(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("RefreshLinksContext() error = %v, want context.Canceled", err)
	}
}

func TestSyncContextCancelsInFlightSync(t *testing.T) {
	t.Parallel()

	manager := contextTestManager(newLinkCacheTestAccount())
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- manager.SyncContext(ctx, func(ctx context.Context, _ *Account) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	waitForContextSignal(t, started)
	cancel()
	if err := waitForContextResult(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("SyncContext() error = %v, want context.Canceled", err)
	}
}

func contextTestManager(accounts ...*Account) *Manager {
	manager := &Manager{
		debrid:   "test",
		accounts: xsync.NewMap[string, *Account](),
		logger:   zerolog.Nop(),
	}
	for _, acc := range accounts {
		manager.accounts.Store(acc.Token, acc)
	}
	return manager
}

func waitForContextSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for context-aware callback")
	}
}

func waitForContextResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for context-aware operation")
		return nil
	}
}
