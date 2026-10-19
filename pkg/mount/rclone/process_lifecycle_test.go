package rclone

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	rcloneHelperProcessEnv  = "TESSARR_RCLONE_HELPER_PROCESS"
	rcloneHelperProcessMode = "TESSARR_RCLONE_HELPER_MODE"
)

func TestRcloneProcessHelper(t *testing.T) {
	t.Helper()
	if os.Getenv(rcloneHelperProcessEnv) != "1" {
		return
	}
	switch os.Getenv(rcloneHelperProcessMode) {
	case "exit":
		os.Exit(23)
	case "wait":
		time.Sleep(10 * time.Minute)
		os.Exit(0)
	default:
		os.Exit(24)
	}
}

func rcloneHelperCommand(mode string) func(string, ...string) *exec.Cmd {
	return func(string, ...string) *exec.Cmd {
		cmd := exec.Command(os.Args[0], "-test.run=^TestRcloneProcessHelper$", "--")
		cmd.Env = append(os.Environ(),
			rcloneHelperProcessEnv+"=1",
			rcloneHelperProcessMode+"="+mode,
		)
		return cmd
	}
}

func waitForRcloneCondition(t *testing.T, name string, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s", name)
		case <-ticker.C:
		}
	}
}

func TestProcessExitClearsReadinessAndMountState(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	})
	m := newRecoveryTestManager(t, handler)
	m.command = rcloneHelperCommand("exit")
	m.serverStarted.Store(false)
	m.serverReady.Store(true)

	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	waitForRcloneCondition(t, "helper process exit", func() bool {
		return !m.serverStarted.Load()
	})

	if m.IsReady() {
		t.Fatal("manager remained ready after the RC process exited")
	}
	info := m.getMountInfo()
	if info == nil || info.Mounted {
		t.Fatalf("mount info after process exit = %#v, want unavailable", info)
	}
	if info.Error == "" {
		t.Fatal("unexpected process exit was not recorded in mount state")
	}
}

func TestStopUsesProcessMonitorAsSingleWaitOwner(t *testing.T) {
	var unmounts atomic.Int32
	var mountOnce sync.Once
	mounted := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/mount/mount":
			mountOnce.Do(func() { close(mounted) })
		case "/mount/unmount":
			unmounts.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	})
	m := newRecoveryTestManager(t, handler)
	m.command = rcloneHelperCommand("wait")
	m.serverStarted.Store(false)
	m.serverReady.Store(false)
	m.info.Store(nil)
	t.Cleanup(func() { _ = m.Stop() })

	if err := m.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	select {
	case <-mounted:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for initial mount")
	}
	waitForRcloneCondition(t, "mounted state", m.IsMounted)
	if !m.IsReady() {
		t.Fatal("manager did not become ready after a successful RC probe")
	}

	if err := m.Stop(); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if m.serverStarted.Load() || m.IsReady() || m.IsMounted() {
		t.Fatalf(
			"state after Stop: started=%v ready=%v mounted=%v",
			m.serverStarted.Load(),
			m.IsReady(),
			m.IsMounted(),
		)
	}
	if got := unmounts.Load(); got != 1 {
		t.Fatalf("RC unmount requests = %d, want 1", got)
	}
}

func TestInitializationDoesNotPublishReadinessAfterProcessExit(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not ready", http.StatusBadRequest)
	})
	m := newRecoveryTestManager(t, handler)
	m.serverReady.Store(false)
	done := make(chan struct{})
	close(done)

	m.initializeProcess(context.Background(), done)
	if m.IsReady() {
		t.Fatal("failed RC probe published readiness")
	}
}
