package manager

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
)

func TestProviderMaintenancePrefersContextAwareCapabilities(t *testing.T) {
	t.Parallel()

	provider := &contextMaintenanceProvider{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := refreshProviderDownloadLinks(ctx, provider); !errors.Is(err, context.Canceled) {
		t.Fatalf("refreshProviderDownloadLinks() error = %v, want context.Canceled", err)
	}
	if err := syncProviderAccounts(ctx, provider); !errors.Is(err, context.Canceled) {
		t.Fatalf("syncProviderAccounts() error = %v, want context.Canceled", err)
	}
	if provider.legacyRefreshCalled.Load() || provider.legacySyncCalled.Load() {
		t.Fatal("provider maintenance used a legacy method")
	}
}

type contextMaintenanceProvider struct {
	debrid.Client
	legacyRefreshCalled atomic.Bool
	legacySyncCalled    atomic.Bool
}

func (p *contextMaintenanceProvider) RefreshDownloadLinks() error {
	p.legacyRefreshCalled.Store(true)
	return nil
}

func (p *contextMaintenanceProvider) RefreshDownloadLinksContext(ctx context.Context) error {
	return ctx.Err()
}

func (p *contextMaintenanceProvider) SyncAccounts() {
	p.legacySyncCalled.Store(true)
}

func (p *contextMaintenanceProvider) SyncAccountsContext(ctx context.Context) error {
	return ctx.Err()
}
