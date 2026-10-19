package manager

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestProviderSubmissionPrefersContextAwareCapabilities(t *testing.T) {
	t.Parallel()

	provider := &contextSubmissionProvider{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := submitProviderMagnet(ctx, provider, &types.Torrent{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("submitProviderMagnet() error = %v, want context.Canceled", err)
	}
	if _, err := checkProviderStatus(ctx, provider, &types.Torrent{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("checkProviderStatus() error = %v, want context.Canceled", err)
	}
	if provider.legacySubmitCalled.Load() || provider.legacyStatusCalled.Load() {
		t.Fatal("provider submission used a legacy method")
	}
}

type contextSubmissionProvider struct {
	debrid.Client
	legacySubmitCalled atomic.Bool
	legacyStatusCalled atomic.Bool
}

func (p *contextSubmissionProvider) SubmitMagnet(torrent *types.Torrent) (*types.Torrent, error) {
	p.legacySubmitCalled.Store(true)
	return torrent, nil
}

func (p *contextSubmissionProvider) SubmitMagnetContext(ctx context.Context, torrent *types.Torrent) (*types.Torrent, error) {
	return torrent, ctx.Err()
}

func (p *contextSubmissionProvider) CheckStatus(torrent *types.Torrent) (*types.Torrent, error) {
	p.legacyStatusCalled.Store(true)
	return torrent, nil
}

func (p *contextSubmissionProvider) CheckStatusContext(ctx context.Context, torrent *types.Torrent) (*types.Torrent, error) {
	return torrent, ctx.Err()
}
