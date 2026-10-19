package rclone

import "testing"

func TestIsReadyRequiresMountedDataPath(t *testing.T) {
	m := &Manager{}
	m.serverReady.Store(true)
	m.info.Store(&MountInfo{Mounted: false})
	if m.IsReady() {
		t.Fatal("IsReady() = true with RC server only, want false")
	}
	m.info.Store(&MountInfo{Mounted: true})
	if !m.IsReady() {
		t.Fatal("IsReady() = false with RC server and mount ready, want true")
	}
}
