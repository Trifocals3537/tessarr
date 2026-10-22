package manager

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/workflow"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/rs/zerolog"
)

type failingOutcomeStore struct {
	calls atomic.Int32
	err   error
}

func (s *failingOutcomeStore) CommitQueuedOutcome(*storage.Entry, storage.QueueOutcome) (*storage.Entry, error) {
	s.calls.Add(1)
	return nil, s.err
}

func TestWorkerPanicDurablyFailsAcceptedJobAndKeepsPoolAlive(t *testing.T) {
	var calls atomic.Int32
	provider := &routingTestClient{cfg: config.Debrid{Name: "torbox"}, submit: func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		calls.Add(1)
		panic("secret-signed-url")
	}}
	m, root := newAsyncAdmissionTestManager(t, provider, 1)
	for _, name := range []string{"first", "second"} {
		request := asyncAdmissionTestRequest(root, name, nil)
		if err := m.AdmitNewTorrent(context.Background(), request); err != nil {
			t.Fatal(err)
		}
		waitForQueuedEntryState(t, m.queue, request.Magnet.InfoHash, storage.EntryStateError)
		entry, err := m.queue.GetTorrent(request.Magnet.InfoHash)
		if err != nil {
			t.Fatal(err)
		}
		if entry.LastError != workflow.ErrPanicked.Error() || strings.Contains(entry.LastError, "secret") {
			t.Fatal("unsafe or missing panic result", entry.LastError)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("worker pool did not survive panic")
	}
	if err := m.workflowAvailable(); err != nil {
		t.Fatal("successfully recorded failure degraded the service", err)
	}
}

func TestOutcomeCommitFailureBlocksFurtherAdmissionsAndDegradesReadiness(t *testing.T) {
	provider := &routingTestClient{cfg: config.Debrid{Name: "torbox"}, submit: func(*debridTypes.Torrent) (*debridTypes.Torrent, error) { panic("injected") }}
	m, root := newAsyncAdmissionTestManager(t, provider, 1)
	repository := &failingOutcomeStore{err: errors.New("checkpoint failed")}
	m.queue.outcomes = repository
	m.setLifecyclePhase(LifecyclePhaseReady, "ready")
	request := asyncAdmissionTestRequest(root, "first", nil)
	if err := m.AdmitNewTorrent(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for m.workflowFault.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !errors.Is(m.workflowAvailable(), ErrWorkflowUnavailable) {
		t.Fatal("checkpoint failure not surfaced")
	}
	if status := m.ReadinessStatus(); status.Ready || status.Phase != LifecyclePhaseDegraded {
		t.Fatal(status)
	}
	if err := m.AdmitNewTorrent(context.Background(), asyncAdmissionTestRequest(root, "second", nil)); !errors.Is(err, ErrWorkflowUnavailable) {
		t.Fatal("new admission accepted after failed checkpoint", err)
	}
	current, err := m.queue.GetTorrent(request.Magnet.InfoHash)
	if err != nil || current.Status != debridTypes.TorrentStatusQueued {
		t.Fatal("recoverable original row was lost", err)
	}
}

func TestCompletionFailureDoesNotNotifyOrReleaseDeletionSafeguards(t *testing.T) {
	m := newQueueRecoveryTestManager(t)
	entry := interruptedQueueTestEntry("completion-failure")
	entry.IsDownloading = false
	if err := m.queue.Add(entry); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("cannot sync completion")
	m.queue.outcomes = &failingOutcomeStore{err: failure}
	d := &Downloader{manager: m, logger: zerolog.Nop()}
	// Reaching a nil notification service would panic: failed completion must
	// return before publishing any success side effects.
	if err := d.completeEntry(entry); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if entry.IsComplete {
		t.Fatal("uncommitted completion published to caller")
	}
	if _, err := m.queue.GetTorrent(entry.InfoHash); err != nil {
		t.Fatal("failed completion removed queue ownership", err)
	}
}

func TestStaleOutcomeCannotMarkReplacementFailedOrDegradeService(t *testing.T) {
	m := newQueueRecoveryTestManager(t)
	old := interruptedQueueTestEntry("stale-outcome")
	if err := m.queue.Add(old); err != nil {
		t.Fatal(err)
	}
	if err := m.queue.DeleteEntryOnly(old.InfoHash); err != nil {
		t.Fatal(err)
	}
	replacement := interruptedQueueTestEntry(old.InfoHash)
	if err := m.queue.Add(replacement); err != nil {
		t.Fatal(err)
	}
	err := m.commitJobOutcome(&Job{Entry: old}, storage.QueueOutcome{Kind: storage.QueueOutcomeFailed, Err: errors.New("late panic")})
	if !obsoleteWorkflow(err) {
		t.Fatal("old generation was accepted", err)
	}
	if err := m.workflowAvailable(); err != nil {
		t.Fatal("benign stale result degraded service", err)
	}
	current, err := m.queue.GetTorrent(old.InfoHash)
	if err != nil || current.State != storage.EntryStateDownloading || current.LastError != "" {
		t.Fatal("replacement was modified", err)
	}
}

func TestScheduledWorkflowRecordsPanicAndPreservesCanceledWork(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "panic", true: "shutdown"}[canceled], func(t *testing.T) {
			m := newQueueRecoveryTestManager(t)
			entry := interruptedQueueTestEntry("scheduled-outcome")
			if err := m.queue.Add(entry); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(m.ctx)
			defer cancel()
			if !m.startEntryBackground(ctx, "outcome-test", entry, func(ctx context.Context) error {
				if canceled {
					cancel()
					return ctx.Err()
				}
				panic("secret provider payload")
			}) {
				t.Fatal("scheduled work rejected")
			}
			m.stopAcceptingBackgroundWork()
			if err := m.waitForBackground(); err != nil {
				t.Fatal(err)
			}
			current, err := m.queue.GetTorrent(entry.InfoHash)
			if err != nil {
				t.Fatal(err)
			}
			if canceled {
				if current.State != storage.EntryStateDownloading || current.LastError != "" {
					t.Fatal("shutdown destroyed resumable work", current)
				}
			} else if current.State != storage.EntryStateError || current.LastError != workflow.ErrPanicked.Error() {
				t.Fatal("scheduled panic did not persist failure", current)
			}
		})
	}
}

func TestOutcomeRefreshPreservesMainMutationFences(t *testing.T) {
	m := newQueueRecoveryTestManager(t)
	entry := interruptedQueueTestEntry("fenced-outcome")
	if err := m.queue.Add(entry); err != nil {
		t.Fatal(err)
	}
	entry.MainGeneration = 41
	entry.MainProviderSnapshot = 37
	entry.MainMutationProvider = "torbox"
	entry.MainReimportIncarnation = entry.QueueIncarnation
	if err := m.commitJobOutcome(&Job{Entry: entry}, storage.QueueOutcome{Kind: storage.QueueOutcomeDeferred}); err != nil {
		t.Fatal(err)
	}
	if entry.MainGeneration != 41 || entry.MainProviderSnapshot != 37 || entry.MainMutationProvider != "torbox" || entry.MainReimportIncarnation != entry.QueueIncarnation {
		t.Fatal("outcome reset main-entry authorization", entry)
	}
}
