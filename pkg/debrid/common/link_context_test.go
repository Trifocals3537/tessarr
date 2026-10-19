package common

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestResolveDownloadLinkPrefersContextCapability(t *testing.T) {
	t.Parallel()
	provider := &contextLinkProvider{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := ResolveDownloadLink(ctx, provider, "torrent", &types.File{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("ResolveDownloadLink() error = %v, want context.Canceled", err)
	}
	if provider.legacyCalled.Load() {
		t.Fatal("ResolveDownloadLink() used the legacy provider method")
	}
}

type contextLinkProvider struct {
	Client
	legacyCalled atomic.Bool
}

func (p *contextLinkProvider) GetDownloadLink(string, *types.File) (types.DownloadLink, error) {
	p.legacyCalled.Store(true)
	return types.DownloadLink{}, nil
}

func (p *contextLinkProvider) GetDownloadLinkContext(ctx context.Context, _ string, _ *types.File) (types.DownloadLink, error) {
	return types.DownloadLink{}, ctx.Err()
}
