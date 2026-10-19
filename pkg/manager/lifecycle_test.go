package manager

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
)

type readinessTestMount struct {
	ready    atomic.Bool
	startErr error
}

func (m *readinessTestMount) Start(context.Context) error { return m.startErr }
func (m *readinessTestMount) Stop() error                 { return nil }
func (m *readinessTestMount) Stats() map[string]any       { return nil }
func (m *readinessTestMount) IsReady() bool               { return m.ready.Load() }
func (m *readinessTestMount) Type() string                { return "test" }
func (m *readinessTestMount) Refresh([]string) error      { return nil }

func TestReadinessStatusDetectsMountLoss(t *testing.T) {
	mount := &readinessTestMount{}
	mount.ready.Store(true)
	m := &Manager{mountManager: mount}
	m.setLifecyclePhase(LifecyclePhaseReady, "media data path is ready")

	if status := m.ReadinessStatus(); !status.Ready || status.Phase != LifecyclePhaseReady {
		t.Fatalf("initial status = %+v, want ready", status)
	}

	mount.ready.Store(false)
	status := m.ReadinessStatus()
	if status.Ready || status.Phase != LifecyclePhaseDegraded || status.MountReady {
		t.Fatalf("status after mount loss = %+v, want degraded and unready", status)
	}
}

func TestWaitForMountReadyHonorsReadinessAndTimeout(t *testing.T) {
	mount := &readinessTestMount{}
	go func() {
		time.Sleep(10 * time.Millisecond)
		mount.ready.Store(true)
	}()
	if err := waitForMountReady(context.Background(), mount, time.Second, time.Millisecond); err != nil {
		t.Fatalf("waitForMountReady() error = %v", err)
	}

	mount.ready.Store(false)
	if err := waitForMountReady(context.Background(), mount, 10*time.Millisecond, time.Millisecond); err == nil {
		t.Fatal("waitForMountReady() error = nil, want timeout")
	}
}

func TestManagerStartDoesNotPublishReadinessWhenMountFails(t *testing.T) {
	testRoot := t.TempDir()
	config.SetConfigPath(testRoot)
	t.Cleanup(config.Reset)

	wantErr := errors.New("mount failed")
	m := New()
	m.SetMountManager(&readinessTestMount{startErr: wantErr})
	err := m.Start(context.Background())
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("Start() error = %v, want mount failure", err)
	}
	select {
	case <-m.IsReady():
		t.Fatal("readiness was published after mount failure")
	default:
	}
	if status := m.ReadinessStatus(); status.Ready || status.Phase != LifecyclePhaseFailed {
		t.Fatalf("status = %+v, want failed and unready", status)
	}
	if stopErr := m.Stop(); stopErr != nil {
		t.Fatalf("Stop() error = %v", stopErr)
	}
}

func TestManagerStartPublishesReadinessAfterMount(t *testing.T) {
	testRoot := t.TempDir()
	config.SetConfigPath(testRoot)
	t.Cleanup(config.Reset)

	mount := &readinessTestMount{}
	mount.ready.Store(true)
	m := New()
	m.SetMountManager(mount)
	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	select {
	case <-m.IsReady():
	default:
		t.Fatal("readiness was not published after mount became ready")
	}
	if status := m.ReadinessStatus(); !status.Ready || status.Phase != LifecyclePhaseReady {
		t.Fatalf("status = %+v, want ready", status)
	}
	if err := m.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
}
