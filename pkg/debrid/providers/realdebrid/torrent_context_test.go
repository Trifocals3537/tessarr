package realdebrid

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestGetTorrentsContextRejectsCanceledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (&RealDebrid{}).GetTorrentsContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetTorrentsContext() error = %v, want context.Canceled", err)
	}
}

func TestMaintenanceContextRejectsCanceledContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	provider := &RealDebrid{}
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
	provider := &RealDebrid{}
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

func TestCheckStatusContextCancelsInitialDelay(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := (&RealDebrid{}).CheckStatusContext(ctx, &types.Torrent{Id: "1"})
		done <- err
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("CheckStatusContext() error = %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("CheckStatusContext() did not cancel its initial status delay")
	}
}
