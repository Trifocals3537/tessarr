package stats

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/rs/zerolog"
)

func TestNewDoesNotCollectSynchronously(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	collector := New(nil)
	if collector.Snapshot() == nil {
		t.Fatal("New() returned a nil initial snapshot")
	}
}

func TestCollectorStopCancelsAndWaitsForRefresh(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	returned := make(chan struct{})
	collector := newLifecycleTestCollector(func(ctx context.Context) (*Snapshot, error) {
		close(started)
		<-ctx.Done()
		close(returned)
		return nil, ctx.Err()
	})

	collector.Start(context.Background())
	waitForSignal(t, started, "initial refresh to start")

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := collector.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	waitForSignal(t, returned, "in-flight refresh to return")
}

func TestCollectorSerializesRefreshes(t *testing.T) {
	t.Parallel()

	firstStarted := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	var active atomic.Int32
	var maximum atomic.Int32
	collector := newLifecycleTestCollector(func(context.Context) (*Snapshot, error) {
		call := calls.Add(1)
		current := active.Add(1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		defer active.Add(-1)
		if call == 1 {
			close(firstStarted)
		}
		<-release
		return &Snapshot{}, nil
	})

	errCh := make(chan error, 2)
	go func() {
		_, err := collector.Refresh(context.Background())
		errCh <- err
	}()
	waitForSignal(t, firstStarted, "first refresh to start")
	go func() {
		_, err := collector.Refresh(context.Background())
		errCh <- err
	}()

	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("collect calls while first refresh was blocked = %d, want 1", got)
	}
	close(release)
	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatalf("Refresh() error = %v", err)
		}
	}
	if got := maximum.Load(); got != 1 {
		t.Fatalf("maximum concurrent collects = %d, want 1", got)
	}
}

func TestCollectorRefreshHonorsCancellationWhileQueued(t *testing.T) {
	t.Parallel()

	firstStarted := make(chan struct{})
	release := make(chan struct{})
	collector := newLifecycleTestCollector(func(context.Context) (*Snapshot, error) {
		select {
		case <-firstStarted:
		default:
			close(firstStarted)
		}
		<-release
		return &Snapshot{}, nil
	})

	firstDone := make(chan error, 1)
	go func() {
		_, err := collector.Refresh(context.Background())
		firstDone <- err
	}()
	waitForSignal(t, firstStarted, "first refresh to start")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := collector.Refresh(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("queued Refresh() error = %v, want context.Canceled", err)
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Refresh() error = %v", err)
	}
}

func TestGetProfileUsesContextAwareProvider(t *testing.T) {
	t.Parallel()

	provider := &contextProfileProvider{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := getProfile(ctx, provider); !errors.Is(err, context.Canceled) {
		t.Fatalf("getProfile() error = %v, want context.Canceled", err)
	}
	if !provider.contextMethodCalled.Load() {
		t.Fatal("getProfile() did not use GetProfileContext")
	}
}

func newLifecycleTestCollector(collectFn func(context.Context) (*Snapshot, error)) *Collector {
	return &Collector{
		logger:      zerolog.Nop(),
		snapshot:    &Snapshot{},
		refreshGate: make(chan struct{}, 1),
		collectFn:   collectFn,
	}
}

func waitForSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

type contextProfileProvider struct {
	debrid.Client
	contextMethodCalled atomic.Bool
}

func (p *contextProfileProvider) GetProfile() (*debridTypes.Profile, error) {
	panic("non-context profile method called")
}

func (p *contextProfileProvider) GetProfileContext(ctx context.Context) (*debridTypes.Profile, error) {
	p.contextMethodCalled.Store(true)
	return nil, ctx.Err()
}
