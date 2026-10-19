package usenet

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

func TestCleanupIdleFSStopsWithLifecycle(t *testing.T) {
	u := &Usenet{
		fs:     xsync.NewMap[string, *fsEntry](),
		logger: zerolog.Nop(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		u.cleanupIdleFS(ctx, time.Millisecond)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("idle cleanup did not stop after lifecycle cancellation")
	}
}

func TestCloseCancelsAndJoinsBackgroundWork(t *testing.T) {
	u := &Usenet{
		fs:     xsync.NewMap[string, *fsEntry](),
		logger: zerolog.Nop(),
	}
	started := make(chan struct{})
	stopped := make(chan struct{})
	u.startBackground(func(ctx context.Context) {
		close(started)
		<-ctx.Done()
		close(stopped)
	})
	<-started

	if err := u.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("Close returned before background work stopped")
	}

	// Closing repeatedly and concurrently must not rerun cleanup or deadlock.
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := u.Close(); err != nil {
				t.Errorf("repeated Close() error = %v", err)
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("concurrent Close calls did not complete")
	}
}

func TestMigrateLegacyContextHonorsCancellationBeforeScan(t *testing.T) {
	root, err := safepath.ValidateRoot(t.TempDir())
	if err != nil {
		t.Fatalf("ValidateRoot() error = %v", err)
	}
	storage := &NZBStorage{
		metaDir: root,
		logger:  zerolog.Nop(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	migrated, err := storage.MigrateLegacyContext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("MigrateLegacyContext() error = %v, want context.Canceled", err)
	}
	if migrated != 0 {
		t.Fatalf("MigrateLegacyContext() migrated = %d, want 0", migrated)
	}
	if storage.migrationMarkerExists() {
		t.Fatal("canceled migration wrote completion marker")
	}
}
