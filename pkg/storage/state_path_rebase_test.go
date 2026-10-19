package storage

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
)

func TestRebaseStatePathsUpdatesContainedMainAndQueuePaths(t *testing.T) {
	root := t.TempDir()
	sourceRoot := filepath.Join(root, "old-state")
	targetRoot := filepath.Join(root, "new-state")
	dbPath := filepath.Join(root, "db")
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	mainEntry := &Entry{
		Protocol:    config.ProtocolTorrent,
		InfoHash:    "main-entry",
		Name:        "Main",
		MountPath:   filepath.Join(sourceRoot, "mount"),
		SavePath:    filepath.Join(sourceRoot, "downloads"),
		ContentPath: filepath.Join(sourceRoot, "downloads", "Main"),
		Files:       map[string]*File{},
		Providers:   map[string]*ProviderEntry{},
	}
	if err := store.AddOrUpdateDurable(mainEntry); err != nil {
		t.Fatal(err)
	}
	queueEntry := &Entry{
		Protocol:    config.ProtocolNZB,
		InfoHash:    "queue-entry",
		Name:        "Queue",
		MountPath:   filepath.Join(root, "external-mount"),
		SavePath:    filepath.Join(sourceRoot, "downloads"),
		ContentPath: filepath.Join(sourceRoot, "downloads", "Queue"),
		Magnet:      filepath.Join(sourceRoot, "usenet", "nzbs", "queue-entry.nzb"),
		Files:       map[string]*File{},
		Providers:   map[string]*ProviderEntry{},
	}
	if err := store.AddQueue(queueEntry); err != nil {
		t.Fatal(err)
	}
	if err := store.SyncQueue(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	changed, err := RebaseStatePaths(dbPath, sourceRoot, targetRoot)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 2 {
		t.Fatalf("rebased records = %d, want 2", changed)
	}

	store, err = NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	rebasedMain, err := store.Get(mainEntry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if rebasedMain.MountPath != filepath.Join(targetRoot, "mount") ||
		rebasedMain.SavePath != filepath.Join(targetRoot, "downloads") ||
		rebasedMain.ContentPath != filepath.Join(targetRoot, "downloads", "Main") {
		t.Fatalf("rebased main paths = %#v", rebasedMain)
	}
	rebasedQueue, err := store.GetQueued(queueEntry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if rebasedQueue.MountPath != queueEntry.MountPath {
		t.Fatalf("external queue mount changed from %q to %q", queueEntry.MountPath, rebasedQueue.MountPath)
	}
	if rebasedQueue.SavePath != filepath.Join(targetRoot, "downloads") ||
		rebasedQueue.ContentPath != filepath.Join(targetRoot, "downloads", "Queue") ||
		rebasedQueue.Magnet != filepath.Join(targetRoot, "usenet", "nzbs", "queue-entry.nzb") {
		t.Fatalf("rebased queue paths = %#v", rebasedQueue)
	}
}

func TestRebaseStatePathsUpdatesPendingQueueDeletionSnapshots(t *testing.T) {
	root := t.TempDir()
	sourceRoot := filepath.Join(root, "old-state")
	targetRoot := filepath.Join(root, "new-state")
	dbPath := filepath.Join(root, "db")
	store, err := NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	entry := &Entry{
		Protocol:    config.ProtocolNZB,
		InfoHash:    "pending-delete",
		Name:        "Pending",
		SavePath:    filepath.Join(sourceRoot, "downloads"),
		ContentPath: filepath.Join(sourceRoot, "downloads", "Pending"),
		Magnet:      filepath.Join(sourceRoot, "usenet", "nzbs", "pending-delete.nzb"),
		Files:       map[string]*File{},
		Providers:   map[string]*ProviderEntry{},
	}
	if err := store.AddQueue(entry); err != nil {
		t.Fatal(err)
	}
	placement := *entry
	placement.SavePath = filepath.Join(sourceRoot, "placements")
	placement.ContentPath = filepath.Join(sourceRoot, "placements", "Pending")
	if _, err := store.PrepareQueuedDeletion(entry.InfoHash, true, &placement); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	changed, err := RebaseStatePaths(dbPath, sourceRoot, targetRoot)
	if err != nil {
		t.Fatal(err)
	}
	if changed != 2 {
		t.Fatalf("rebased records = %d, want queue row and tombstone", changed)
	}
	tombstones, err := hybrid.New(hybrid.Config{
		DataPath:            filepath.Join(dbPath, "queue_tombstones.db"),
		SyncInterval:        -1,
		CompactionThreshold: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer tombstones.Close()
	data, err := tombstones.Get(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	var tombstone queueDeletionTombstone
	if err := json.Unmarshal(data, &tombstone); err != nil {
		t.Fatal(err)
	}
	snapshot, err := decodeQueuedEntry(entry.InfoHash, tombstone.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.SavePath != filepath.Join(targetRoot, "downloads") ||
		snapshot.ContentPath != filepath.Join(targetRoot, "downloads", "Pending") ||
		snapshot.Magnet != filepath.Join(targetRoot, "usenet", "nzbs", "pending-delete.nzb") {
		t.Fatalf("rebased queue deletion snapshot = %#v", snapshot)
	}
	if len(tombstone.PlacementSnapshots) != 1 {
		t.Fatalf("placement snapshots = %d, want 1", len(tombstone.PlacementSnapshots))
	}
	placementSnapshot, err := decodeQueuedEntry(entry.InfoHash, tombstone.PlacementSnapshots[0])
	if err != nil {
		t.Fatal(err)
	}
	if placementSnapshot.SavePath != filepath.Join(targetRoot, "placements") ||
		placementSnapshot.ContentPath != filepath.Join(targetRoot, "placements", "Pending") {
		t.Fatalf("rebased placement snapshot = %#v", placementSnapshot)
	}
}
