package torbox

import (
	"context"
	"errors"
	"testing"
)

func TestGetTorrentsContextRejectsCanceledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (&Torbox{}).GetTorrentsContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetTorrentsContext() error = %v, want context.Canceled", err)
	}
}

func TestMaintenanceContextRejectsCanceledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	provider := &Torbox{}
	if err := provider.RefreshDownloadLinksContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("RefreshDownloadLinksContext() error = %v, want context.Canceled", err)
	}
	if err := provider.SyncAccountsContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("SyncAccountsContext() error = %v, want context.Canceled", err)
	}
}

func TestSubmissionContextRejectsCanceledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	provider := &Torbox{}
	if _, err := provider.SubmitMagnetContext(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("SubmitMagnetContext() error = %v, want context.Canceled", err)
	}
	if _, err := provider.CheckStatusContext(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("CheckStatusContext() error = %v, want context.Canceled", err)
	}
	if _, err := provider.GetDownloadLinkContext(ctx, "", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetDownloadLinkContext() error = %v, want context.Canceled", err)
	}
}
