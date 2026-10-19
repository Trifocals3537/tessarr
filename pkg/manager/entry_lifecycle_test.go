package manager

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	"github.com/Trifocals3537/tessarr/pkg/notifications"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/rs/zerolog"
)

func TestQueueDeleteCancelsAndDrainsActiveJobBeforeRemovingFiles(t *testing.T) {
	store := newLifecycleTestStorage(t)
	lifecycle := newEntryLifecycle()
	queue := newLifecycleTestQueue(store, lifecycle)
	entry, outputPath := addLifecycleTestEntry(t, queue, "active-delete")

	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	jobQueue := newLifecycleTestJobQueue(t, lifecycle, func(ctx context.Context, _ *Job) {
		close(started)
		<-ctx.Done()
		close(canceled)
		<-release
	})
	queue.removePendingJobs = jobQueue.DeleteJobs

	if err := jobQueue.Submit(&Job{
		ID:         entry.InfoHash,
		Type:       JobTypeNZB,
		Entry:      entry,
		Generation: entry.QueueGeneration,
	}); err != nil {
		t.Fatal(err)
	}
	<-started

	deleteResult := make(chan error, 1)
	go func() {
		deleteResult <- queue.Delete(entry.InfoHash, nil)
	}()
	<-canceled

	select {
	case err := <-deleteResult:
		t.Fatalf("Delete() returned before active work drained: %v", err)
	default:
	}
	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("output removed before active work drained: %v", err)
	}
	if err := queue.Update(entry); !errors.Is(err, ErrQueueEntryDeleting) {
		t.Fatalf("Update() during delete error = %v, want ErrQueueEntryDeleting", err)
	}

	close(release)
	if err := <-deleteResult; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("output still exists after drained delete: %v", err)
	}
	if _, err := store.GetQueued(entry.InfoHash); !storage.IsQueuedEntryNotFound(err) {
		t.Fatalf("queue row after delete error = %v, want queued-entry-not-found", err)
	}

	replacement, _ := addLifecycleTestEntry(t, queue, entry.InfoHash)
	if replacement.QueueGeneration == entry.QueueGeneration {
		t.Fatalf("replacement generation = %d, want a new generation", replacement.QueueGeneration)
	}
	if err := queue.Update(entry); !errors.Is(err, ErrStaleQueueGeneration) {
		t.Fatalf("stale Update() error = %v, want ErrStaleQueueGeneration", err)
	}
	replacement.Progress = 0.5
	if err := queue.Update(replacement); err != nil {
		t.Fatalf("replacement Update() failed: %v", err)
	}
}

func TestQueueDeleteTimeoutRetainsRowAndFilesForRetry(t *testing.T) {
	store := newLifecycleTestStorage(t)
	lifecycle := newEntryLifecycle()
	queue := newLifecycleTestQueue(store, lifecycle)
	queue.deleteDrainTimeout = 25 * time.Millisecond
	entry, outputPath := addLifecycleTestEntry(t, queue, "delete-timeout")

	work, err := lifecycle.startWork(context.Background(), entry.InfoHash, entry.QueueGeneration)
	if err != nil {
		t.Fatal(err)
	}
	canceled := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		<-work.Context().Done()
		close(canceled)
		<-release
		work.Close()
	}()

	err = queue.Delete(entry.InfoHash, nil)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Delete() error = %v, want context deadline exceeded", err)
	}
	<-canceled
	if _, err := store.GetQueued(entry.InfoHash); err != nil {
		t.Fatalf("queue row removed after drain timeout: %v", err)
	}
	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("output removed after drain timeout: %v", err)
	}

	close(release)
	<-finished
	if err := queue.Delete(entry.InfoHash, nil); err != nil {
		t.Fatalf("retry Delete() failed: %v", err)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("output still exists after retry: %v", err)
	}
}

func TestQueueDeleteEntryOnlyPreservesDownloadedFiles(t *testing.T) {
	store := newLifecycleTestStorage(t)
	lifecycle := newEntryLifecycle()
	queue := newLifecycleTestQueue(store, lifecycle)
	entry, outputPath := addLifecycleTestEntry(t, queue, "preserve-files")

	if err := queue.DeleteEntryOnly(entry.InfoHash); err != nil {
		t.Fatalf("DeleteEntryOnly() error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outputPath, "payload")); err != nil {
		t.Fatalf("downloaded data was removed: %v", err)
	}
	if _, err := store.GetQueued(entry.InfoHash); !storage.IsQueuedEntryNotFound(err) {
		t.Fatalf("queue row after entry-only delete = %v, want queued-entry-not-found", err)
	}

	replacement, _ := addLifecycleTestEntry(t, queue, entry.InfoHash)
	if replacement.QueueGeneration == entry.QueueGeneration {
		t.Fatal("entry-only delete did not invalidate the retired generation")
	}
}

func TestQueueDeleteEntryOnlyPreservesFilesDuringRestartRecovery(t *testing.T) {
	root := t.TempDir()
	config.SetConfigPath(root)
	downloadRoot := filepath.Join(root, "downloads")
	if err := os.MkdirAll(downloadRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	config.Get().DownloadFolder = downloadRoot
	dbPath := filepath.Join(root, "db")

	store, err := storage.NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	queue := newLifecycleTestQueue(store, newEntryLifecycle())
	entry := &storage.Entry{
		InfoHash: "preserve-files-recovery",
		Name:     "release.mkv",
		Protocol: config.ProtocolTorrent,
		SavePath: downloadRoot,
	}
	outputPath := entry.DownloadPath()
	if _, _, err := claimTorrentEntryDirectory(downloadRoot, entry, torrentLegacyProof{}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outputPath, "payload"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := queue.Add(entry); err != nil {
		t.Fatal(err)
	}
	intent, err := store.PrepareQueuedDeletionPreservingFiles(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StartQueuedDeletionCleanup(intent.InfoHash, intent.QueueIncarnation); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store, err = storage.NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close reopened storage: %v", err)
		}
	})
	manager := &Manager{
		storage: store,
		queue:   newLifecycleTestQueue(store, newEntryLifecycle()),
		logger:  zerolog.Nop(),
	}
	residual, fatal := manager.recoverQueuedDeletions()
	if fatal != nil || residual != nil {
		t.Fatalf("recoverQueuedDeletions() = residual %v, fatal %v", residual, fatal)
	}
	if _, err := os.Stat(filepath.Join(outputPath, "payload")); err != nil {
		t.Fatalf("restart recovery removed preserved data: %v", err)
	}
	if _, err := store.GetQueued(entry.InfoHash); !storage.IsQueuedEntryNotFound(err) {
		t.Fatalf("queue row after recovery = %v, want queued-entry-not-found", err)
	}
}

func TestJobQueueDeleteJobsRemovesEveryPendingGeneration(t *testing.T) {
	queue := &JobQueue{jobs: make([]*Job, 0, 5)}
	queue.cond = sync.NewCond(&queue.mu)
	queue.jobs = append(queue.jobs,
		&Job{ID: "other"},
		&Job{ID: "Release-ID", Generation: 1},
		&Job{ID: "release-id", Generation: 2},
		&Job{ID: "last"},
	)

	if removed := queue.DeleteJobs("RELEASE-ID"); removed != 2 {
		t.Fatalf("DeleteJobs() removed %d jobs, want 2", removed)
	}
	if got := queue.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
	if queue.FindJob("other") == nil || queue.FindJob("last") == nil {
		t.Fatal("DeleteJobs() removed an unrelated pending job")
	}
}

func TestScheduledStaleSnapshotCannotStartAgainstSameIDReplacement(t *testing.T) {
	store := newLifecycleTestStorage(t)
	lifecycle := newEntryLifecycle()
	queue := newLifecycleTestQueue(store, lifecycle)
	stale, _ := addLifecycleTestEntry(t, queue, "scheduler-stale")

	if err := queue.Delete(stale.InfoHash, nil); err != nil {
		t.Fatal(err)
	}
	replacement, _ := addLifecycleTestEntry(t, queue, stale.InfoHash)

	manager := &Manager{
		queue:          queue,
		entryLifecycle: lifecycle,
		logger:         zerolog.Nop(),
	}
	manager.resetLifecycle()
	t.Cleanup(manager.cancel)
	started := false
	if manager.startEntryBackground(context.Background(), "stale scheduler test", stale, func(context.Context) error {
		started = true
		return nil
	}) {
		t.Fatal("stale scheduler snapshot was admitted")
	}
	if started {
		t.Fatal("stale scheduler work ran")
	}
	if replacement.QueueGeneration == stale.QueueGeneration {
		t.Fatal("replacement reused stale lifecycle generation")
	}
}

func TestDelayedRetryCannotEnterSameIDReplacementGeneration(t *testing.T) {
	store := newLifecycleTestStorage(t)
	lifecycle := newEntryLifecycle()
	queue := newLifecycleTestQueue(store, lifecycle)
	stale, _ := addLifecycleTestEntry(t, queue, "delayed-retry")

	ctx, cancel := context.WithCancel(context.Background())
	jobQueue := NewJobQueueWithCapacity(
		ctx,
		1,
		1,
		func(context.Context, *Job) {},
		lifecycle,
	)
	jobQueue.logger = zerolog.Nop()
	t.Cleanup(jobQueue.Close)
	t.Cleanup(cancel)

	if err := jobQueue.Retry(&Job{
		ID:         stale.InfoHash,
		Type:       JobTypeTorrent,
		Entry:      stale,
		Generation: stale.QueueGeneration,
	}, 20*time.Millisecond); err != nil {
		t.Fatalf("Retry() error = %v", err)
	}
	if err := queue.Delete(stale.InfoHash, nil); err != nil {
		t.Fatal(err)
	}
	replacement, _ := addLifecycleTestEntry(t, queue, stale.InfoHash)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if time.Until(deadline) < 900*time.Millisecond {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if got := jobQueue.Len(); got != 0 {
		t.Fatalf("delayed stale retry queued %d jobs against replacement", got)
	}
	if replacement.QueueGeneration == stale.QueueGeneration {
		t.Fatal("replacement reused stale lifecycle generation")
	}
}

func TestActionNoneDeletesOnlyAfterWorkerLeaseCloses(t *testing.T) {
	store := newLifecycleTestStorage(t)
	lifecycle := newEntryLifecycle()
	queue := newLifecycleTestQueue(store, lifecycle)
	entry, outputPath := addLifecycleTestEntry(t, queue, "action-none")
	entry.Action = config.DownloadActionNone
	if err := queue.Update(entry); err != nil {
		t.Fatal(err)
	}

	cfg := config.Get()
	manager := &Manager{
		config:         cfg,
		queue:          queue,
		entryLifecycle: lifecycle,
		logger:         zerolog.Nop(),
		arr:            arr.NewStorage(),
		Notifications:  notifications.New(&cfg.Notifications, zerolog.Nop()),
	}
	manager.resetLifecycle()
	downloader := &Downloader{
		manager: manager,
		dest:    cfg.DownloadFolder,
		logger:  zerolog.Nop(),
	}

	rowExistedInsideLease := make(chan bool, 1)
	deleted := make(chan error, 1)
	jobQueue := newLifecycleTestJobQueue(t, lifecycle, func(ctx context.Context, job *Job) {
		err := downloader.process(ctx, entry, "")
		if errors.Is(err, errDeleteQueueEntryOnJobFinish) {
			_, getErr := store.GetQueued(entry.InfoHash)
			rowExistedInsideLease <- getErr == nil
			job.DeleteOnFinish = true
		}
	})
	queue.removePendingJobs = jobQueue.DeleteJobs
	jobQueue.afterFunc = func(job *Job) {
		deleted <- queue.Delete(job.ID, nil)
	}

	if err := jobQueue.Submit(&Job{
		ID:         entry.InfoHash,
		Type:       JobTypeTorrent,
		Entry:      entry,
		Generation: entry.QueueGeneration,
	}); err != nil {
		t.Fatal(err)
	}
	if !<-rowExistedInsideLease {
		t.Fatal("ActionNone deleted its queue row while its own lease was active")
	}
	if err := <-deleted; err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetQueued(entry.InfoHash); !storage.IsQueuedEntryNotFound(err) {
		t.Fatalf("queue row after ActionNone = %v", err)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("ActionNone output still exists after post-lease delete: %v", err)
	}
	manager.cancel()
	if err := manager.waitForBackground(); err != nil {
		t.Fatal(err)
	}
}

func TestMainDeleteCancelsAndDrainsQueueWorkerBeforeCleanup(t *testing.T) {
	store := newLifecycleTestStorage(t)
	lifecycle := newEntryLifecycle()
	queue := newLifecycleTestQueue(store, lifecycle)
	entry, outputPath := addLifecycleTestEntry(t, queue, "browse-delete")
	entry.Files = map[string]*storage.File{}
	entry.Providers = map[string]*storage.ProviderEntry{}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}

	work, err := lifecycle.startWork(context.Background(), entry.InfoHash, entry.QueueGeneration)
	if err != nil {
		t.Fatal(err)
	}
	canceled := make(chan struct{})
	release := make(chan struct{})
	workerDone := make(chan error, 1)
	go func() {
		<-work.Context().Done()
		close(canceled)
		<-release
		entry.Progress = 0.9
		workerDone <- queue.Update(entry)
		work.Close()
	}()

	manager := &Manager{
		storage:        store,
		queue:          queue,
		entryLifecycle: lifecycle,
		logger:         zerolog.Nop(),
	}
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- manager.DeleteEntry(entry.InfoHash, false)
	}()
	<-canceled

	select {
	case err := <-deleteDone:
		t.Fatalf("DeleteEntry returned before worker drained: %v", err)
	default:
	}
	if _, err := store.Get(entry.InfoHash); !errors.Is(err, storage.ErrEntryDeleting) {
		t.Fatalf("main row was not hidden by durable delete intent: %v", err)
	}
	if _, err := os.Stat(outputPath); err != nil {
		t.Fatalf("files removed before worker drained: %v", err)
	}

	close(release)
	if err := <-workerDone; !errors.Is(err, ErrQueueEntryDeleting) {
		t.Fatalf("late worker update error = %v, want deleting tombstone", err)
	}
	if err := <-deleteDone; err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(entry.InfoHash); !storage.IsEntryNotFound(err) {
		t.Fatalf("main row after delete = %v", err)
	}
	if _, err := store.GetQueued(entry.InfoHash); !storage.IsQueuedEntryNotFound(err) {
		t.Fatalf("queue row after delete = %v", err)
	}
	if _, err := os.Stat(outputPath); !os.IsNotExist(err) {
		t.Fatalf("files remain after drained delete: %v", err)
	}
}

func TestQueueDeleteMissingIsIdempotentAndAdvancesGeneration(t *testing.T) {
	store := newLifecycleTestStorage(t)
	lifecycle := newEntryLifecycle()
	queue := newLifecycleTestQueue(store, lifecycle)
	state, err := lifecycle.stateFor("already-gone", true)
	if err != nil {
		t.Fatal(err)
	}
	state.mu.Lock()
	before := state.generation
	state.mu.Unlock()

	if err := queue.Delete("already-gone", nil); err != nil {
		t.Fatalf("Delete() missing row error = %v", err)
	}
	state.mu.Lock()
	after := state.generation
	state.mu.Unlock()
	if after == before {
		t.Fatal("idempotent missing delete did not invalidate stale generation")
	}
}

func TestLifecycleReadCannotBindStalePayloadToReplacementGeneration(t *testing.T) {
	lifecycle := newEntryLifecycle()
	original := &storage.Entry{InfoHash: "read-aba"}
	if err := lifecycle.bindEntry(original); err != nil {
		t.Fatal(err)
	}

	readStarted := make(chan struct{})
	releaseRead := make(chan struct{})
	readDone := make(chan *storage.Entry, 1)
	go func() {
		entry, err := lifecycle.withRead(original.InfoHash, func() (*storage.Entry, error) {
			close(readStarted)
			<-releaseRead
			return &storage.Entry{InfoHash: original.InfoHash, Name: "old payload"}, nil
		})
		if err != nil {
			readDone <- nil
			return
		}
		readDone <- entry
	}()
	<-readStarted

	deleteAttempted := make(chan struct{})
	deleteDone := make(chan struct{})
	go func() {
		close(deleteAttempted)
		deletion, err := lifecycle.beginDelete(original.InfoHash)
		if err == nil {
			deletion.Finish(true)
		}
		close(deleteDone)
	}()
	<-deleteAttempted
	select {
	case <-deleteDone:
		t.Fatal("delete crossed the read/bind gate before the durable read returned")
	case <-time.After(10 * time.Millisecond):
	}

	close(releaseRead)
	stale := <-readDone
	if stale == nil {
		t.Fatal("gated read failed")
	}
	<-deleteDone

	replacement := &storage.Entry{InfoHash: original.InfoHash, Name: "replacement"}
	if err := lifecycle.bindEntry(replacement); err != nil {
		t.Fatal(err)
	}
	if replacement.QueueGeneration == stale.QueueGeneration {
		t.Fatal("stale payload received replacement generation")
	}
	if err := lifecycle.withUpdate(stale, func() error { return nil }); !errors.Is(err, ErrStaleQueueGeneration) {
		t.Fatalf("stale update error = %v, want stale generation", err)
	}
}

func TestQueueDeletePropagatesExactLeaseContextIntoUsenetDownload(t *testing.T) {
	store := newLifecycleTestStorage(t)
	lifecycle := newEntryLifecycle()
	queue := newLifecycleTestQueue(store, lifecycle)
	downloadRoot := t.TempDir()
	config.Get().DownloadFolder = downloadRoot
	entry := &storage.Entry{
		InfoHash: "11111111-1111-4111-8111-111111111111",
		Name:     "release.mkv",
		Protocol: config.ProtocolNZB,
		SavePath: downloadRoot,
		Files: map[string]*storage.File{
			"release.mkv": {Name: "release.mkv", Size: 1},
		},
	}
	if err := queue.Add(entry); err != nil {
		t.Fatal(err)
	}

	manager := &Manager{
		config:         config.Get(),
		queue:          queue,
		entryLifecycle: lifecycle,
		logger:         zerolog.Nop(),
	}
	manager.resetLifecycle()
	t.Cleanup(manager.cancel)

	type contextKey string
	parent := context.WithValue(context.Background(), contextKey("lease"), "preserved")
	workContext := make(chan context.Context, 1)
	downloadContext := make(chan context.Context, 1)
	downloadCanceled := make(chan struct{})
	downloader := &Downloader{
		manager: manager,
		dest:    downloadRoot,
		logger:  zerolog.Nop(),
		usenetDownload: func(ctx context.Context, _, _ string, _ io.Writer, _ func(int64, int64)) error {
			downloadContext <- ctx
			<-ctx.Done()
			close(downloadCanceled)
			return ctx.Err()
		},
	}

	if !manager.startEntryBackground(parent, "usenet context test", entry, func(ctx context.Context) error {
		workContext <- ctx
		return downloader.processUsenetDownload(ctx, entry)
	}) {
		t.Fatal("failed to start lifecycle-owned Usenet work")
	}
	leaseCtx := <-workContext
	actualDownloadCtx := <-downloadContext
	if actualDownloadCtx != leaseCtx {
		t.Fatal("Usenet download did not receive the exact lifecycle lease context")
	}
	if got := actualDownloadCtx.Value(contextKey("lease")); got != "preserved" {
		t.Fatalf("lease context value = %v, want preserved", got)
	}

	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- queue.Delete(entry.InfoHash, nil)
	}()
	<-downloadCanceled
	if err := <-deleteDone; err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetQueued(entry.InfoHash); !storage.IsQueuedEntryNotFound(err) {
		t.Fatalf("queue row after canceled Usenet download = %v", err)
	}
}

func TestMainDeleteStillCompletesWhenQueueDisappearedBeforeDeleteGate(t *testing.T) {
	store := newLifecycleTestStorage(t)
	lifecycle := newEntryLifecycle()
	queue := newLifecycleTestQueue(store, lifecycle)
	entry, _ := addLifecycleTestEntry(t, queue, "queue-disappeared")
	entry.Files = map[string]*storage.File{}
	entry.Providers = map[string]*storage.ProviderEntry{}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	if err := queue.Delete(entry.InfoHash, nil); err != nil {
		t.Fatal(err)
	}

	manager := &Manager{
		storage: store,
		queue:   queue,
		logger:  zerolog.Nop(),
	}
	if err := manager.DeleteEntry(entry.InfoHash, false); err != nil {
		t.Fatalf("DeleteEntry() after queue disappeared: %v", err)
	}
	if _, err := store.Get(entry.InfoHash); !storage.IsEntryNotFound(err) {
		t.Fatalf("main row after delete = %v", err)
	}
}

func TestMainDeleteHidesQueueWhenFilesystemCleanupFailsThenRetrySucceeds(t *testing.T) {
	store := newLifecycleTestStorage(t)
	lifecycle := newEntryLifecycle()
	queue := newLifecycleTestQueue(store, lifecycle)
	downloadRoot := t.TempDir()
	config.Get().DownloadFolder = downloadRoot
	outsideRoot := t.TempDir()
	entry := &storage.Entry{
		InfoHash:  "filesystem-retry",
		Name:      "release.mkv",
		Protocol:  config.ProtocolTorrent,
		SavePath:  outsideRoot,
		Files:     map[string]*storage.File{},
		Providers: map[string]*storage.ProviderEntry{},
	}
	if err := queue.Add(entry); err != nil {
		t.Fatal(err)
	}
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	manager := &Manager{
		storage: store,
		queue:   queue,
		logger:  zerolog.Nop(),
	}

	if err := manager.DeleteEntry(entry.InfoHash, false); err == nil {
		t.Fatal("DeleteEntry() accepted queue output outside configured root")
	}
	if _, err := store.Get(entry.InfoHash); err != nil {
		t.Fatalf("main row removed after filesystem cleanup failure: %v", err)
	}
	if _, err := queue.GetTorrent(entry.InfoHash); !storage.IsQueuedEntryNotFound(err) {
		t.Fatalf("queue row remained visible after filesystem cleanup failure: %v", err)
	}

	// The snapshotted delete cannot be rewritten after cleanup may have
	// started. Correct the configured ownership boundary, then resume the
	// exact durable intent.
	config.Get().DownloadFolder = outsideRoot
	if _, _, err := claimTorrentEntryDirectory(outsideRoot, entry, torrentLegacyProof{}); err != nil {
		t.Fatal(err)
	}
	if err := queue.Update(entry); !errors.Is(err, ErrQueueEntryDeleting) &&
		!errors.Is(err, storage.ErrQueuedEntryDeleting) {
		t.Fatalf("queue update during durable deletion = %v", err)
	}
	if err := manager.DeleteEntry(entry.InfoHash, false); err != nil {
		t.Fatalf("DeleteEntry() retry error = %v", err)
	}
	if _, err := store.Get(entry.InfoHash); !storage.IsEntryNotFound(err) {
		t.Fatalf("main row after retry = %v", err)
	}
	if _, err := store.GetQueued(entry.InfoHash); !storage.IsQueuedEntryNotFound(err) {
		t.Fatalf("queue row after retry = %v", err)
	}
}

func newLifecycleTestQueue(store *storage.Storage, lifecycle *entryLifecycle) *Queue {
	return &Queue{
		storage:            store,
		logger:             zerolog.Nop(),
		lifecycle:          lifecycle,
		deleteDrainTimeout: defaultEntryDeleteDrainTimeout,
	}
}

func newLifecycleTestJobQueue(
	t *testing.T,
	lifecycle *entryLifecycle,
	process func(context.Context, *Job),
) *JobQueue {
	t.Helper()
	queue := NewJobQueueWithCapacity(
		context.Background(),
		1,
		4,
		process,
		lifecycle,
	)
	queue.logger = zerolog.Nop()
	t.Cleanup(queue.Close)
	return queue
}

func newLifecycleTestStorage(t *testing.T) *storage.Storage {
	t.Helper()
	testRoot := t.TempDir()
	config.SetConfigPath(testRoot)
	store, err := storage.NewStorage(filepath.Join(testRoot, "db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close storage: %v", err)
		}
	})
	return store
}

func addLifecycleTestEntry(t *testing.T, queue *Queue, id string) (*storage.Entry, string) {
	t.Helper()
	downloadRoot := t.TempDir()
	config.Get().DownloadFolder = downloadRoot
	entry := &storage.Entry{
		InfoHash: id,
		Name:     "release.mkv",
		Protocol: config.ProtocolTorrent,
		SavePath: downloadRoot,
	}
	outputPath := entry.DownloadPath()
	if _, _, err := claimTorrentEntryDirectory(downloadRoot, entry, torrentLegacyProof{}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outputPath, "payload"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := queue.Add(entry); err != nil {
		t.Fatal(err)
	}
	return entry, outputPath
}
