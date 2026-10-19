package manager

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/proto"
)

func TestInitializeActiveDownloadsClearsInterruptedWorkBeforeRestore(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "fresh startup"
		if restart {
			name = "drained manager reset"
		}
		t.Run(name, func(t *testing.T) {
			m := newQueueRecoveryTestManager(t)
			var previous *JobQueue
			if restart {
				// Reset has drained the old workers but retains their closed
				// handle until initialization constructs the replacement queue.
				previous = NewJobQueueWithCapacity(m.ctx, 1, 4, m.processJob, m.entryLifecycle)
				m.jobQueue = previous
				m.cancel()
				previous.Close()
				m.resetLifecycle()
			}
			entry := interruptedQueueTestEntry("interrupted-download")
			if err := m.queue.Add(entry); err != nil {
				t.Fatal(err)
			}
			m.initializeActiveDownloads(func() error { return nil })
			if m.initializationErr != nil {
				t.Fatal(m.initializationErr)
			}
			current, err := m.queue.GetTorrent(entry.InfoHash)
			if err != nil {
				t.Fatal(err)
			}
			if current.IsDownloading {
				t.Fatal("startup left interrupted local work marked as running")
			}
			if m.jobQueue == nil || m.jobQueue == previous {
				t.Fatal("startup did not initialize a new worker queue")
			}
		})
	}
}

func TestInterruptedDownloadRecoverySurvivesRestartAndPreservesMetadata(t *testing.T) {
	root := t.TempDir()
	config.SetConfigPath(root)
	dbPath := filepath.Join(root, "db")
	store, err := storage.NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if store != nil {
			if err := store.Close(); err != nil {
				t.Errorf("close recovery storage: %v", err)
			}
		}
	})
	queue := newLifecycleTestQueue(store, newEntryLifecycle())
	entries := []*storage.Entry{
		interruptedQueueTestEntry("torrent-local-download"),
		interruptedQueueTestEntry("torrent-provider-download"),
		interruptedQueueTestEntry("torrent-queued"),
		interruptedQueueTestEntry("nzb-local-download"),
		interruptedQueueTestEntry("already-idle"),
		interruptedQueueTestEntry("paused"),
		interruptedQueueTestEntry("complete"),
		interruptedQueueTestEntry("failed"),
	}
	entries[1].Status = debridTypes.TorrentStatusDownloading
	entries[2].Status = debridTypes.TorrentStatusQueued
	entries[2].DownloadUncached = false
	entries[3].Protocol = config.ProtocolNZB
	entries[3].ActiveProvider = "usenet"
	entries[3].Providers = nil
	entries[4].IsDownloading = false
	entries[5].State = storage.EntryStatePausedDL
	entries[6].MarkAsCompleted("completed-output")
	entries[7].MarkAsError(errors.New("existing failure"))
	for _, entry := range entries {
		if err := queue.Add(entry); err != nil {
			t.Fatal(err)
		}
	}
	// Close/reopen the actual store: QueueGeneration cannot survive this, but
	// the abandoned local-work flag and provider placements do.
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = storage.NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{queue: newLifecycleTestQueue(store, newEntryLifecycle()), logger: zerolog.Nop()}
	if err := m.recoverInterruptedDownloads(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = storage.NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	m.queue = newLifecycleTestQueue(store, newEntryLifecycle())
	for _, original := range entries {
		current, err := m.queue.GetTorrent(original.InfoHash)
		if err != nil {
			t.Fatal(err)
		}
		want, got := storage.EntryToProto(original), storage.EntryToProto(current)
		if original.State == storage.EntryStateDownloading && original.IsDownloading {
			want.IsDownloading = false
			want.UpdatedAtUnix = got.UpdatedAtUnix
		}
		if !proto.Equal(want, got) {
			t.Errorf("%s recovery changed fields other than the local-work flag and update time\nwant: %v\ngot: %v", original.InfoHash, want, got)
		}
		if current.QueueGeneration == 0 {
			t.Error("reopened queue entry lacks a fresh lifecycle binding")
		}
	}
	// A second pass has nothing left to write.
	beforeSize := store.DiskSize()
	if err := m.recoverInterruptedDownloads(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.DiskSize() != beforeSize {
		t.Fatal("idempotent recovery appended another queue update")
	}
	queued, err := m.queue.GetTorrent("torrent-queued")
	if err != nil {
		t.Fatal(err)
	}
	job, err := m.rebuildQueuedTorrentJob(queued)
	if err != nil || !job.ResumeExisting || job.Request != nil || job.Entry.DownloadUncached {
		t.Fatalf("queued placement was not resumed with its cached-only policy: job=%v err=%v", job, err)
	}
}

func TestRecoveredDownloadIsProcessedWithoutNewProviderSubmission(t *testing.T) {
	m := newQueueRecoveryTestManager(t)
	entry := interruptedQueueTestEntry("1111111111111111111111111111111111111111")
	if err := m.queue.Add(entry); err != nil {
		t.Fatal(err)
	}
	var checks, submissions atomic.Int32
	provider := &routingTestClient{
		cfg: config.Debrid{Name: "torbox"},
		submit: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			submissions.Add(1)
			return torrent, nil
		},
		check: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			checks.Add(1)
			if torrent.Id != "existing-provider-id" {
				t.Errorf("provider check ID = %q, want existing placement", torrent.Id)
			}
			torrent.Status = debridTypes.TorrentStatusDownloading
			torrent.Progress = 61
			return torrent, nil
		},
	}
	m.clients.Store("torbox", provider)
	if err := m.recoverInterruptedDownloads(m.ctx); err != nil {
		t.Fatal(err)
	}
	m.processQueuedEntries(m.ctx)
	if err := m.waitForBackground(); err != nil {
		t.Fatal(err)
	}
	current, err := m.queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if checks.Load() != 1 || submissions.Load() != 0 || current.Progress != .61 {
		t.Fatalf("recovery did not resume existing placement: checks=%d submissions=%d progress=%v", checks.Load(), submissions.Load(), current.Progress)
	}
}

func TestRestoredDownloadWaitersPreserveLimitsDeletionAndCancellation(t *testing.T) {
	m := newQueueRecoveryTestManager(t)
	for i, key := range []string{"first", "second", "third"} {
		entry := interruptedQueueTestEntry(key)
		entry.AddedOn = time.Unix(int64(i+1), 0)
		if err := m.queue.Add(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.recoverInterruptedDownloads(m.ctx); err != nil {
		t.Fatal(err)
	}
	started := make(chan *Job, 3)
	finished := make(chan string, 3)
	m.jobQueue = NewJobQueueWithCapacity(m.ctx, 1, 4, func(ctx context.Context, job *Job) {
		started <- job
		m.processJob(ctx, job)
		finished <- job.ID
	}, m.entryLifecycle)
	m.queue.removePendingJobs = m.jobQueue.DeleteJobs
	m.restoreActiveDownloadJobs(m.ctx)
	first := awaitAsyncAdmissionValue(t, started, "first restored download")
	if first.ID != "first" || first.Request != nil || first.ResumeExisting || first.DebridTorrent != nil || first.NZBMeta != nil {
		t.Fatal("restored active job was rebuilt as a new provider submission")
	}
	if m.jobQueue.ActiveCount() != 1 || m.jobQueue.PendingCount("") != 2 {
		t.Fatal("restored jobs did not retain the one-worker limit")
	}
	select {
	case job := <-started:
		t.Fatalf("%s bypassed the active-download limit", job.ID)
	case <-time.After(30 * time.Millisecond):
	}
	current, err := m.queue.GetTorrent(first.ID)
	if err != nil {
		t.Fatal(err)
	}
	current.MarkAsCompleted("completed-output")
	if err := m.queue.Update(current); err != nil {
		t.Fatal(err)
	}
	if got := awaitAsyncAdmissionValue(t, finished, "completed restored waiter"); got != "first" {
		t.Fatalf("finished = %s, want first", got)
	}
	second := awaitAsyncAdmissionValue(t, started, "second restored download")
	if second.ID != "second" {
		t.Fatalf("restored order = %s, want second", second.ID)
	}
	if err := m.queue.DeleteEntryOnly(second.ID); err != nil {
		t.Fatal(err)
	}
	if got := awaitAsyncAdmissionValue(t, finished, "deleted restored waiter"); got != "second" {
		t.Fatalf("finished = %s, want second", got)
	}
	if err := m.queue.Update(second.Entry); !errors.Is(err, ErrStaleQueueGeneration) {
		t.Fatalf("late restored update = %v, want stale generation", err)
	}
	third := awaitAsyncAdmissionValue(t, started, "third restored download")
	if third.ID != "third" {
		t.Fatalf("restored order = %s, want third", third.ID)
	}
	m.cancel()
	if got := awaitAsyncAdmissionValue(t, finished, "canceled restored waiter"); got != "third" {
		t.Fatalf("finished = %s, want third", got)
	}
	if _, err := m.queue.GetTorrent(third.ID); err != nil {
		t.Fatalf("shutdown removed unfinished work: %v", err)
	}
}

func TestInterruptedDownloadRecoverySkipsDeletionTombstone(t *testing.T) {
	m := newQueueRecoveryTestManager(t)
	entry := interruptedQueueTestEntry("deleted-download")
	if err := m.queue.Add(entry); err != nil {
		t.Fatal(err)
	}
	if _, err := m.storage.PrepareQueuedDeletionPreservingFiles(entry.InfoHash); err != nil {
		t.Fatal(err)
	}
	if err := m.recoverInterruptedDownloads(m.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := m.queue.GetTorrent(entry.InfoHash); !storage.IsQueuedEntryNotFound(err) {
		t.Fatalf("recovery exposed a deleted row: %v", err)
	}
	intents, err := m.storage.QueuedDeletionIntents()
	if err != nil || len(intents) != 1 || !intents[0].Entry.IsDownloading {
		t.Fatalf("recovery changed deletion intent: intents=%v err=%v", intents, err)
	}
}

func TestInterruptedDownloadRecoveryFailsClosed(t *testing.T) {
	t.Run("canceled startup", func(t *testing.T) {
		m := newQueueRecoveryTestManager(t)
		entry := interruptedQueueTestEntry("canceled")
		if err := m.queue.Add(entry); err != nil {
			t.Fatal(err)
		}
		m.cancel()
		m.initializeActiveDownloads(func() error { return nil })
		if !errors.Is(m.initializationErr, context.Canceled) || m.jobQueue != nil {
			t.Fatalf("canceled startup: error=%v queue=%v", m.initializationErr, m.jobQueue)
		}
		if err := m.Start(context.Background()); !errors.Is(err, context.Canceled) {
			t.Fatalf("Start = %v, want cancellation", err)
		}
		current, err := m.queue.GetTorrent(entry.InfoHash)
		if err != nil || !current.IsDownloading {
			t.Fatalf("canceled recovery changed the entry: entry=%v err=%v", current, err)
		}
	})
	t.Run("unreadable queue", func(t *testing.T) {
		root := t.TempDir()
		config.SetConfigPath(root)
		store, err := storage.NewStorage(filepath.Join(root, "db"))
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		m := &Manager{queue: newLifecycleTestQueue(store, newEntryLifecycle())}
		if err := m.recoverInterruptedDownloads(context.Background()); err == nil {
			t.Fatal("unreadable queue was treated as an empty successful recovery")
		}
	})
}

func interruptedQueueTestEntry(key string) *storage.Entry {
	return &storage.Entry{
		InfoHash:         key,
		Name:             "release.mkv",
		Protocol:         config.ProtocolTorrent,
		State:            storage.EntryStateDownloading,
		Status:           debridTypes.TorrentStatusDownloaded,
		IsDownloading:    true,
		ActiveProvider:   "torbox",
		Providers:        map[string]*storage.ProviderEntry{"torbox": {ID: "existing-provider-id"}},
		Files:            map[string]*storage.File{"release.mkv": {Name: "release.mkv", Size: 12345}},
		Progress:         .5,
		SizeDownloaded:   6789,
		DownloadUncached: true,
		Category:         "sonarr",
		SavePath:         "download-output",
		Action:           config.DownloadActionDownload,
		SkipMultiSeason:  true,
	}
}

func newQueueRecoveryTestManager(t *testing.T) *Manager {
	t.Helper()
	store := newLifecycleTestStorage(t)
	lifecycle := newEntryLifecycle()
	m := &Manager{
		config:            &config.Config{MaxActiveDownloads: 1},
		storage:           store,
		queue:             newLifecycleTestQueue(store, lifecycle),
		entryLifecycle:    lifecycle,
		logger:            zerolog.Nop(),
		clients:           xsync.NewMap[string, debrid.Client](),
		arr:               arr.NewStorage(),
		processingEntries: xsync.NewMap[string, struct{}](),
	}
	m.resetLifecycle()
	t.Cleanup(func() {
		m.stopAcceptingBackgroundWork()
		m.cancel()
		if m.jobQueue != nil {
			m.jobQueue.Close()
		}
		if err := m.waitForBackground(); err != nil {
			t.Errorf("stop restore: %v", err)
		}
	})
	return m
}
