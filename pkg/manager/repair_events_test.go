package manager

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
)

func TestRepairEventQueueCoalescesAndBoundsAdmission(t *testing.T) {
	r := NewRepair(&Manager{})
	r.eventCapacity = 2
	r.eventCooldown = 0
	r.eventMu.Lock()
	r.eventStarted = true
	r.eventMu.Unlock()

	if !r.QueueDirtyEntry("alpha") || !r.QueueDirtyEntry("beta") {
		t.Fatal("initial bounded queue admission failed")
	}
	if r.QueueDirtyEntry("alpha") {
		t.Fatal("duplicate entry was not coalesced")
	}
	if r.QueueDirtyEntry("gamma") {
		t.Fatal("entry was admitted beyond queue capacity")
	}
	status := r.eventQueueStatus()
	if status.Pending != 2 || status.Coalesced != 1 || status.Dropped != 1 {
		t.Fatalf("event queue status = %+v, want pending/coalesced/dropped 2/1/1", status)
	}
}

func TestRepairEventWorkerIsSingleAndCoalescesInFlightEntry(t *testing.T) {
	r := NewRepair(&Manager{})
	r.eventCooldown = 0
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	processed := make(chan string, 2)
	var active atomic.Int32
	var maximum atomic.Int32
	r.eventProcess = func(ctx context.Context, entryName string) repairEventOutcome {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			observed := maximum.Load()
			if current <= observed || maximum.CompareAndSwap(observed, current) {
				break
			}
		}
		if entryName == "alpha" {
			select {
			case <-firstStarted:
			default:
				close(firstStarted)
			}
			select {
			case <-releaseFirst:
			case <-ctx.Done():
				return repairEventIncomplete
			}
		}
		processed <- entryName
		return repairEventHealthy
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := r.startEventWorker(ctx); err != nil {
		t.Fatal(err)
	}
	if !r.QueueDirtyEntry("alpha") {
		t.Fatal("failed to queue alpha")
	}
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("event worker did not start alpha")
	}
	if !r.QueueDirtyEntry("beta") {
		t.Fatal("failed to queue beta")
	}
	if r.QueueDirtyEntry("alpha") {
		t.Fatal("in-flight alpha was not coalesced")
	}
	close(releaseFirst)

	got := make([]string, 0, 2)
	for len(got) < 2 {
		select {
		case entryName := <-processed:
			got = append(got, entryName)
		case <-time.After(time.Second):
			t.Fatalf("processed entries = %v, want alpha and beta", got)
		}
	}
	if !slices.Equal(got, []string{"alpha", "beta"}) || maximum.Load() != 1 {
		t.Fatalf("processed/max concurrency = %v/%d, want [alpha beta]/1", got, maximum.Load())
	}
	status := waitForRepairEventProcessed(t, r, 2)
	if status.Processed != 2 || status.Healthy != 1 || status.Incomplete != 1 || status.Coalesced != 1 {
		t.Fatalf("event queue status = %+v", status)
	}

	r.stopEventWorker()
	done := make(chan struct{})
	go func() {
		r.runWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("event worker did not stop")
	}
}

func TestRepairEventTimeoutDoesNotBlockFollowingEntry(t *testing.T) {
	r := NewRepair(&Manager{})
	r.eventCooldown = 0
	r.eventProbeTimeout = 20 * time.Millisecond
	processed := make(chan string, 2)
	r.eventProcess = func(ctx context.Context, entryName string) repairEventOutcome {
		if entryName == "blocked" {
			<-ctx.Done()
			processed <- entryName
			return repairEventIncomplete
		}
		processed <- entryName
		return repairEventHealthy
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := r.startEventWorker(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		r.stopEventWorker()
		r.runWG.Wait()
	}()
	if !r.QueueDirtyEntry("blocked") || !r.QueueDirtyEntry("healthy") {
		t.Fatal("failed to queue timeout-isolation entries")
	}

	want := []string{"blocked", "healthy"}
	got := make([]string, 0, len(want))
	for len(got) < len(want) {
		select {
		case entryName := <-processed:
			got = append(got, entryName)
		case <-time.After(time.Second):
			t.Fatalf("processed entries = %v, want %v", got, want)
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("processed entries = %v, want %v", got, want)
	}
	status := waitForRepairEventProcessed(t, r, 2)
	if status.Processed != 2 || status.Incomplete != 1 || status.Healthy != 1 {
		t.Fatalf("event queue status = %+v", status)
	}
}

func TestRepairEventQueueBootstrapsDurableDirtyRecords(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.MarkEntryDirtyChecked("alpha", config.ProtocolTorrent, "read_failure:timeout"); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkEntryDirtyChecked("beta", config.ProtocolNZB, "read_failure:content_missing"); err != nil {
		t.Fatal(err)
	}
	r := NewRepair(&Manager{storage: store})
	r.eventMu.Lock()
	r.eventStarted = true
	r.eventMu.Unlock()
	r.enqueuePersistedDirty()
	if status := r.eventQueueStatus(); status.Pending != 2 {
		t.Fatalf("event queue status = %+v, want two durable dirty records", status)
	}
}

func TestRepairEventIncompleteProbePreservesDirtyState(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.MarkEntryDirtyChecked("missing-entry", config.ProtocolTorrent, "read_failure:timeout"); err != nil {
		t.Fatal(err)
	}
	r := NewRepair(&Manager{storage: store})
	if outcome := r.reconcileDirtyEntry(context.Background(), "missing-entry"); outcome != repairEventIncomplete {
		t.Fatalf("outcome = %q, want incomplete", outcome)
	}
	health, err := store.GetEntryHealth("missing-entry")
	if err != nil {
		t.Fatal(err)
	}
	if !health.Dirty || health.DirtyReason != "read_failure:timeout" {
		t.Fatalf("health = %+v, want original durable dirty signal", health)
	}
}

func TestRepairEventConcurrentFailurePreservesDurableDirtyState(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.MarkEntryDirtyChecked("alpha", config.ProtocolTorrent, "read_failure:timeout"); err != nil {
		t.Fatal(err)
	}

	r := NewRepair(&Manager{storage: store})
	r.eventCooldown = 0
	r.eventMu.Lock()
	r.eventStarted = true
	r.eventMu.Unlock()
	if !r.QueueDirtyEntry("alpha") {
		t.Fatal("failed to queue alpha")
	}
	work, ok := r.nextDirtyEvent()
	if !ok {
		t.Fatal("failed to begin alpha event")
	}

	// Model a recheck loading revision 1, followed by another terminal read
	// failure before the stale recheck result is saved and finalized.
	probeState, err := store.GetEntryHealth("alpha")
	if err != nil {
		t.Fatal(err)
	}
	probeState.Status = storage.HealthHealthy
	probeState.Dirty = false
	probeState.DirtyReason = ""
	if err := store.MarkEntryDirtyChecked("alpha", config.ProtocolTorrent, "read_failure:unexpected_eof"); err != nil {
		t.Fatal(err)
	}
	if r.QueueDirtyEntry("alpha") {
		t.Fatal("in-flight alpha was not coalesced")
	}
	if err := store.SaveEntryHealth(probeState); err != nil {
		t.Fatal(err)
	}
	r.finishDirtyEvent(work, repairEventHealthy)

	health, err := store.GetEntryHealth("alpha")
	if err != nil {
		t.Fatal(err)
	}
	if !health.Dirty || health.DirtyReason != "read_failure_during_recheck" {
		t.Fatalf("health = %+v, want concurrent failure preserved as dirty", health)
	}
	if health.DirtyRevision < 2 {
		t.Fatalf("dirty revision = %d, want at least 2", health.DirtyRevision)
	}
	status := r.eventQueueStatus()
	if status.Processed != 1 || status.Healthy != 0 || status.Incomplete != 1 || status.Coalesced != 1 {
		t.Fatalf("event queue status = %+v", status)
	}
}

func TestProbeTorrentFileTreatsRemovedProviderClientAsBroken(t *testing.T) {
	manager := &Manager{clients: xsync.NewMap[string, debrid.Client]()}
	r := NewRepair(manager)
	entry := streamFailoverEntry("removed-provider")
	file := entry.Files["video.mkv"]
	file.InfoHash = entry.InfoHash
	result := r.probeTorrentFile(
		context.Background(),
		entry,
		file,
		"video.mkv",
		fileResult{name: "video.mkv", infoHash: entry.InfoHash, protocol: config.ProtocolTorrent},
		RepairRunOptions{},
	)
	if !result.broken || result.reason != "provider_client_not_found" {
		t.Fatalf("probe result = %+v, want broken removed provider", result)
	}
}

func waitForRepairEventProcessed(t *testing.T, r *Repair, want uint64) RepairEventQueueStatus {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		status := r.eventQueueStatus()
		classified := status.Healthy + status.Broken + status.Incomplete
		if status.Processed >= want && classified >= want {
			return status
		}
		if time.Now().After(deadline) {
			t.Fatalf("event queue status = %+v, want at least %d processed", status, want)
		}
		time.Sleep(time.Millisecond)
	}
}
