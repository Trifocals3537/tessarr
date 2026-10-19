package rclone

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	rcloneclient "github.com/Trifocals3537/tessarr/internal/rclone"
	"github.com/rs/zerolog"
)

func newRecoveryTestManager(t *testing.T, handler http.Handler) *Manager {
	t.Helper()

	oldConfigPath := config.GetMainPath()
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() {
		config.Reset()
		config.SetConfigPath(oldConfigPath)
	})

	cfg := config.Get()
	cfg.Mount.MountPath = t.TempDir()
	cfg.Mount.Rclone.VfsCacheMode = "off"

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m := &Manager{
		logger:    zerolog.Nop(),
		ctx:       ctx,
		cancel:    cancel,
		webdavURL: "http://tessarr.example/webdav/",
		client:    rcloneclient.NewClient(server.URL, "", "", zerolog.Nop()),
	}
	m.serverStarted.Store(true)
	m.info.Store(&MountInfo{
		LocalPath:  cfg.Mount.MountPath,
		WebDAVURL:  m.webdavURL,
		Mounted:    true,
		MountedAt:  "2026-01-01T00:00:00Z",
		ConfigName: ConfigName,
	})
	return m
}

func TestRecoverMountRemountsThroughRunningRCServer(t *testing.T) {
	var mu sync.Mutex
	requests := make([]string, 0, 3)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.URL.Path)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	})

	m := newRecoveryTestManager(t, handler)
	if err := m.recoverMountAfter(context.Background(), 0); err != nil {
		t.Fatalf("recoverMountAfter() error = %v", err)
	}

	mu.Lock()
	gotRequests := append([]string(nil), requests...)
	mu.Unlock()
	wantRequests := []string{
		"/mount/unmount",
		"/config/create",
		"/mount/mount",
	}
	if !reflect.DeepEqual(gotRequests, wantRequests) {
		t.Fatalf("RC requests = %v, want %v", gotRequests, wantRequests)
	}
	if !m.serverStarted.Load() {
		t.Fatal("recovery stopped the running RC server")
	}
	info := m.getMountInfo()
	if info == nil || !info.Mounted {
		t.Fatalf("mount info after recovery = %#v, want mounted", info)
	}
	if info.Error != "" {
		t.Fatalf("mount error after recovery = %q, want empty", info.Error)
	}
}

func TestUnmountPublishesNewMountInfoSnapshot(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	})
	m := newRecoveryTestManager(t, handler)
	original := m.getMountInfo()

	if err := m.unmount(context.Background()); err != nil {
		t.Fatalf("unmount() error = %v", err)
	}
	current := m.getMountInfo()
	if current == original {
		t.Fatal("unmount mutated the published mount-info snapshot in place")
	}
	if !original.Mounted {
		t.Fatal("unmount changed a snapshot already held by a reader")
	}
	if current.Mounted {
		t.Fatal("current mount info still reports mounted")
	}
	if current.MountedAt != "" {
		t.Fatalf("current mount timestamp = %q, want empty", current.MountedAt)
	}
}

func TestHealthCheckWaitsForRecoveryToFinish(t *testing.T) {
	mountStarted := make(chan struct{})
	releaseMount := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseMount) })
	})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/operations/list" {
			http.Error(w, "unhealthy", http.StatusBadRequest)
			return
		}
		if r.URL.Path == "/mount/mount" {
			close(mountStarted)
			<-releaseMount
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	})
	m := newRecoveryTestManager(t, handler)

	checkDone := make(chan struct{})
	go func() {
		m.performMountHealthCheckAfter(context.Background(), 0)
		close(checkDone)
	}()

	select {
	case <-mountStarted:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for recovery mount request")
	}
	select {
	case <-checkDone:
		t.Fatal("health check returned while recovery was still mounting")
	default:
	}
	releaseOnce.Do(func() { close(releaseMount) })
	select {
	case <-checkDone:
	case <-time.After(30 * time.Second):
		t.Fatal("timed out waiting for health-check recovery")
	}

	if !m.IsMounted() {
		t.Fatal("health check returned without restoring the mount")
	}
}
