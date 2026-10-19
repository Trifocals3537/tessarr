package manager

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestGetProviderTorrentsUsesContextAwareListing(t *testing.T) {
	t.Parallel()

	provider := &contextTorrentProvider{started: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := getProviderTorrents(ctx, provider)
		done <- err
	}()

	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for context-aware listing")
	}
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("getProviderTorrents() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("context-aware listing did not return after cancellation")
	}
	if provider.legacyCalled.Load() {
		t.Fatal("getProviderTorrents() called legacy GetTorrents")
	}
}

type contextTorrentProvider struct {
	debrid.Client
	started      chan struct{}
	legacyCalled atomic.Bool
}

func (p *contextTorrentProvider) GetTorrents() ([]*debridTypes.Torrent, error) {
	p.legacyCalled.Store(true)
	return nil, nil
}

func (p *contextTorrentProvider) GetTorrentsContext(ctx context.Context) ([]*debridTypes.Torrent, error) {
	close(p.started)
	<-ctx.Done()
	return nil, ctx.Err()
}
