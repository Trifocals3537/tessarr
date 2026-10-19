package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/rs/zerolog"
)

type recoveryArrFixture struct {
	mu                                    sync.Mutex
	filePresent                           bool
	replacement                           bool
	history                               []arr.HistoryRecord
	releaseSource                         string
	historyStatus                         int
	searchStatus                          int
	failureStatus                         int
	autoSearch                            bool
	interactiveAuto                       bool
	commandVisible                        bool
	searches, failures, deletes, requests int
	onMutation                            func(string)
	file                                  arr.RepairFile
}

func (f *recoveryArrFixture) serve(w http.ResponseWriter, req *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests++
	w.Header().Set("Content-Type", "application/json")
	if req.Method != http.MethodGet && f.onMutation != nil {
		f.onMutation(req.URL.Path)
	}
	switch req.URL.Path {
	case "/api/v3/moviefile/10":
		if req.Method == http.MethodDelete {
			f.deletes++
			f.filePresent = false
			w.WriteHeader(204)
			return
		}
		if !f.filePresent {
			w.WriteHeader(404)
			return
		}
		json.NewEncoder(w).Encode(f.file)
	case "/api/v3/movie/7":
		file := arr.RepairFile{}
		if f.filePresent {
			file = f.file
		}
		if f.replacement {
			file = arr.RepairFile{ID: 20, MovieID: 7, Path: "/movies/new.mkv", Size: 150}
		}
		json.NewEncoder(w).Encode(map[string]any{"id": 7, "movieFile": file})
	case "/api/v3/movie":
		fmt.Fprint(w, "[]")
	case "/api/v3/config/downloadclient":
		json.NewEncoder(w).Encode(arr.RepairDownloadConfig{EnableCompletedDownloadHandling: true, AutoRedownloadFailed: f.autoSearch, AutoRedownloadFailedFromInteractiveSearch: f.interactiveAuto})
	case "/api/v3/history":
		if f.historyStatus != 0 {
			w.WriteHeader(f.historyStatus)
			return
		}
		records := []arr.RepairHistoryRecord{}
		for _, record := range f.history {
			records = append(records, arr.RepairHistoryRecord{HistoryRecord: record, Data: map[string]string{"releaseSource": f.releaseSource}})
		}
		json.NewEncoder(w).Encode(map[string]any{"totalRecords": len(records), "records": records})
	case "/api/v3/history/failed/42":
		f.failures++
		if f.failureStatus != 0 {
			w.WriteHeader(f.failureStatus)
			return
		}
		f.history = append(f.history, arr.HistoryRecord{ID: 43, DownloadID: "original-download", MovieID: 7, EventType: "downloadFailed"})
		w.WriteHeader(200)
	case "/api/v3/command":
		if req.Method == http.MethodPost {
			f.searches++
			if f.searchStatus != 0 {
				w.WriteHeader(f.searchStatus)
				return
			}
			fmt.Fprint(w, `{"id":123}`)
			return
		}
		if f.commandVisible {
			json.NewEncoder(w).Encode([]map[string]any{{"id": 123, "name": "MoviesSearch", "queued": time.Now().UTC(), "body": map[string]any{"movieIds": []int{7}}}})
		} else {
			fmt.Fprint(w, "[]")
		}
	default:
		w.WriteHeader(404)
	}
}

func newRecoveryFixture(t *testing.T) (*Repair, *recoveryArrFixture, *storage.RepairRecovery, string) {
	t.Helper()
	config.SetConfigPath(t.TempDir())
	cfg := config.Get()
	old := cfg.Repair
	cfg.Repair = config.RepairConfig{Source: config.RepairSourceArr}
	t.Cleanup(func() { cfg.Repair = old })
	dir := t.TempDir()
	s, err := storage.NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	f := &recoveryArrFixture{filePresent: true, file: arr.RepairFile{ID: 10, MovieID: 7, Path: "/movies/old.mkv", Size: 100}}
	server := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(server.Close)
	a := &arr.Arr{Name: "radarr-recovery-test", Type: arr.Radarr, Host: server.URL, Token: "test-token"}
	arrs := arr.NewStorage()
	arrs.AddOrUpdate(a)
	r := NewRepair(&Manager{storage: s, arr: arrs})
	r.logger = zerolog.Nop()
	bf := storage.BrokenFile{EntryName: "original-entry", FileName: "old.mkv", InfoHash: "original-download", Protocol: config.ProtocolTorrent, ArrName: a.Name, ArrKind: storage.ArrKindRadarr, MediaID: 7, ArrFileID: 10, SourcePath: f.file.Path, Size: 100}
	job := &storage.RepairRecovery{ID: recoveryID(bf), Original: bf, State: storage.RecoveryPending, EndpointBinding: recoveryEndpointBinding(a)}
	if err := s.SaveRepairRecovery(job); err != nil {
		t.Fatal(err)
	}
	return r, f, job, dir
}

func runRecoveryTest(t *testing.T, r *Repair, job *storage.RepairRecovery, id string) *storage.RepairRun {
	t.Helper()
	job.LastRunID = ""
	job.NextCheckAt = time.Time{}
	if err := r.manager.storage.SaveRepairRecovery(job); err != nil {
		t.Fatal(err)
	}
	run := &storage.RepairRun{ID: id, StartedAt: time.Now()}
	var mu sync.Mutex
	r.processRecovery(context.Background(), run, &mu, job, true)
	return run
}

func TestRecoverySearchAcceptanceIsNotRepairCompletion(t *testing.T) {
	r, f, job, _ := newRecoveryFixture(t)
	f.onMutation = func(path string) {
		stored, err := r.manager.storage.GetRepairRecovery(job.ID)
		if err != nil || stored == nil || !stored.Prepared {
			t.Error("mutation preceded durable binding")
			return
		}
		if path == "/api/v3/command" && stored.SearchIntentAt.IsZero() {
			t.Error("search preceded durable intent")
		}
	}
	run := runRecoveryTest(t, r, job, "first")
	if run.Stats.Repaired != 0 || run.Stats.RepairPending != 1 || job.State != storage.RecoveryWaiting || f.searches != 1 || f.deletes != 1 {
		t.Fatalf("run=%+v job=%+v calls=%+v", run.Stats, job, f)
	}
	f.mu.Lock()
	f.replacement = true
	f.mu.Unlock()
	run = runRecoveryTest(t, r, job, "second")
	if run.Stats.Repaired != 1 || job.State != storage.RecoveryComplete || f.searches != 1 {
		t.Fatalf("run=%+v state=%s searches=%d", run.Stats, job.State, f.searches)
	}
	run = runRecoveryTest(t, r, job, "third")
	if run.Stats.Repaired != 0 {
		t.Fatal("completion counted twice")
	}
}

func TestRecoveryRestartResumesWithoutHealthOrArrCandidate(t *testing.T) {
	r, f, job, dir := newRecoveryFixture(t)
	f.searchStatus = 403
	run := runRecoveryTest(t, r, job, "before-restart")
	if !job.Deleted || run.Stats.Repaired != 0 || run.Stats.RepairFailed != 1 || !job.SearchIntentAt.IsZero() {
		t.Fatalf("job=%+v run=%+v", job, run.Stats)
	}
	if err := r.manager.storage.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := storage.NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r = NewRepair(&Manager{storage: s, arr: r.manager.arr})
	r.logger = zerolog.Nop()
	job, err = s.GetRepairRecovery(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	job.NextCheckAt = time.Time{}
	if err := s.SaveRepairRecovery(job); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.searchStatus = 0
	f.mu.Unlock()
	run = &storage.RepairRun{ID: "after-restart"}
	if err := r.resumeRecoveries(context.Background(), run, nil, "all"); err != nil {
		t.Fatal(err)
	}
	if f.searches != 2 || f.deletes != 1 || run.Stats.Repaired != 0 || run.Stats.RepairPending != 1 {
		t.Fatalf("searches=%d deletes=%d stats=%+v", f.searches, f.deletes, run.Stats)
	}
}

func TestRecoveryUnknownSearchReconcilesWithoutRedispatch(t *testing.T) {
	r, f, job, _ := newRecoveryFixture(t)
	f.searchStatus = 500
	runRecoveryTest(t, r, job, "first")
	if job.SearchIntentAt.IsZero() || job.State != storage.RecoveryAttention || f.searches != 1 {
		t.Fatalf("job=%+v searches=%d", job, f.searches)
	}
	run := runRecoveryTest(t, r, job, "unknown")
	if f.searches != 1 || run.Stats.Repaired != 0 || run.Stats.RepairFailed != 1 {
		t.Fatalf("duplicate unknown request: %d %+v", f.searches, run.Stats)
	}
	f.mu.Lock()
	f.commandVisible = true
	f.mu.Unlock()
	run = runRecoveryTest(t, r, job, "reconciled")
	if f.searches != 1 || job.SearchReceiptID != 123 || job.State != storage.RecoveryWaiting || run.Stats.Repaired != 0 {
		t.Fatalf("job=%+v stats=%+v", job, run.Stats)
	}
}

func TestRecoveryUsesExactHistoryAndRespectsRedownloadSettings(t *testing.T) {
	for _, tc := range []struct {
		name                               string
		auto, interactive, interactiveAuto bool
		wantSearch                         int
	}{
		{"automatic", true, false, false, 0},
		{"disabled", false, false, false, 1},
		{"interactive-disabled", true, true, false, 1},
		{"interactive-enabled", true, true, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, f, job, _ := newRecoveryFixture(t)
			f.autoSearch, f.interactiveAuto = tc.auto, tc.interactiveAuto
			if tc.interactive {
				f.releaseSource = "InteractiveSearch"
			}
			f.history = []arr.HistoryRecord{
				{ID: 99, DownloadID: "newer-download", MovieID: 7, EventType: "grabbed"},
				{ID: 42, DownloadID: "original-download", MovieID: 7, EventType: "grabbed"},
			}
			f.onMutation = func(path string) {
				if path == "/api/v3/history/failed/42" {
					stored, _ := r.manager.storage.GetRepairRecovery(job.ID)
					if stored == nil || stored.FailureIntentAt.IsZero() {
						t.Error("failure preceded durable intent")
					}
				}
			}
			run := runRecoveryTest(t, r, job, "first")
			if job.HistoryID != 42 || f.failures != 1 || f.searches != tc.wantSearch || run.Stats.Repaired != 0 || run.Stats.RepairFailed != 0 {
				t.Fatalf("job=%+v failures=%d searches=%d stats=%+v", job, f.failures, f.searches, run.Stats)
			}
		})
	}
}

func TestRecoveryHistoryFailureDoesNotFallThroughToSearch(t *testing.T) {
	r, f, job, _ := newRecoveryFixture(t)
	f.history = []arr.HistoryRecord{{ID: 42, DownloadID: "original-download", MovieID: 7, EventType: "grabbed"}}
	f.failureStatus = 500
	run := runRecoveryTest(t, r, job, "first")
	if run.Stats.Repaired != 0 || f.searches != 0 || f.failures != 1 || job.State != storage.RecoveryAttention {
		t.Fatalf("stats=%+v job=%+v", run.Stats, job)
	}
	runRecoveryTest(t, r, job, "retry")
	if f.failures != 1 || f.searches != 0 {
		t.Fatal("unknown failure was resent or bypassed")
	}
	f.mu.Lock()
	f.history = append(f.history, arr.HistoryRecord{ID: 43, DownloadID: "original-download", MovieID: 7, EventType: "downloadFailed"})
	f.mu.Unlock()
	runRecoveryTest(t, r, job, "reconcile")
	if f.failures != 1 || f.searches != 1 || job.State != storage.RecoveryWaiting {
		t.Fatalf("job=%+v", job)
	}
}

func TestRecoveryPreflightFailureLeavesLibraryUntouched(t *testing.T) {
	for _, mode := range []string{"history-outage", "changed-file", "changed-endpoint", "store-closed"} {
		t.Run(mode, func(t *testing.T) {
			r, f, job, _ := newRecoveryFixture(t)
			switch mode {
			case "history-outage":
				f.historyStatus = 503
			case "changed-file":
				f.file.MovieID = 8
			case "changed-endpoint":
				job.EndpointBinding = "different-endpoint"
			case "store-closed":
				r.manager.storage.Close()
			}
			run := &storage.RepairRun{ID: "preflight"}
			var mu sync.Mutex
			r.processRecovery(context.Background(), run, &mu, job, true)
			if f.deletes != 0 || f.searches != 0 || f.failures != 0 || run.Stats.Repaired != 0 || run.Stats.RepairFailed != 1 {
				t.Fatalf("mutations or success after failed preflight: %+v %+v", f, run.Stats)
			}
			if (mode == "store-closed" || mode == "changed-endpoint") && f.requests != 0 {
				t.Fatal("sent request despite unavailable store or changed endpoint")
			}
		})
	}
}

func TestRecoveryReplayHonorsFiltersAndHealthOnlyMode(t *testing.T) {
	for _, mode := range []string{"arr-opt-out", "arr-filter", "protocol", "names", "health-only"} {
		t.Run(mode, func(t *testing.T) {
			r, f, job, _ := newRecoveryFixture(t)
			job.Prepared, job.Deleted = true, true
			job.File = f.file
			f.filePresent = false
			if err := r.manager.storage.SaveRepairRecovery(job); err != nil {
				t.Fatal(err)
			}
			scope := "all"
			var names []string
			switch mode {
			case "arr-opt-out":
				r.manager.arr.Get(job.Original.ArrName).SkipRepair = true
			case "arr-filter":
				config.Get().Repair.Arrs = []string{"other"}
			case "protocol":
				scope = "nzb"
			case "names":
				names = []string{"other"}
			}
			run := &storage.RepairRun{ID: mode}
			if mode == "health-only" {
				off := false
				r.executeSweep(context.Background(), run, RepairRunOptions{AutoRepair: &off}, nil)
			} else if err := r.resumeRecoveries(context.Background(), run, names, scope); err != nil {
				t.Fatal(err)
			}
			if f.deletes != 0 || f.searches != 0 || f.failures != 0 {
				t.Fatalf("filter allowed mutation: %+v", f)
			}
		})
	}
}

func TestRecoveryReplayDoesNotDeleteFromStaleHealth(t *testing.T) {
	r, f, job, _ := newRecoveryFixture(t)
	job.Prepared = true
	job.File = f.file
	if err := r.manager.storage.SaveRepairRecovery(job); err != nil {
		t.Fatal(err)
	}
	run := &storage.RepairRun{ID: "replay"}
	if err := r.resumeRecoveries(context.Background(), run, nil, "all"); err != nil {
		t.Fatal(err)
	}
	if f.deletes != 0 || f.searches != 0 {
		t.Fatal("restart authorized deletion without fresh broken confirmation")
	}
	job, _ = r.manager.storage.GetRepairRecovery(job.ID)
	if job.LastRunID != "" || !job.NextCheckAt.IsZero() {
		t.Fatal("deferred job cannot continue after this run probes it")
	}
	// A crash after a successful delete but before its acknowledgement is safe
	// to reconcile: a 404 on the immutable old ID does not require another delete.
	f.mu.Lock()
	f.filePresent = false
	f.mu.Unlock()
	if err := r.resumeRecoveries(context.Background(), run, nil, "all"); err != nil {
		t.Fatal(err)
	}
	if f.deletes != 0 || f.searches != 1 {
		t.Fatalf("deletes=%d searches=%d", f.deletes, f.searches)
	}
}

func TestRecoveryConcurrentSightingsRequestReplacementOnce(t *testing.T) {
	r, f, job, _ := newRecoveryFixture(t)
	health := &storage.EntryHealth{EntryName: job.Original.EntryName, Status: storage.HealthBroken, BrokenFiles: []storage.BrokenFile{job.Original}}
	run := &storage.RepairRun{ID: "concurrent"}
	var statsMu sync.Mutex
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() { r.healBrokenEntry(context.Background(), run, &statsMu, health.EntryName, health) })
	}
	wg.Wait()
	if f.deletes != 1 || f.searches != 1 || run.Stats.RepairPending != 1 {
		t.Fatalf("deletes=%d searches=%d stats=%+v", f.deletes, f.searches, run.Stats)
	}
}

func TestRecoveryLongWaitNeedsAttentionWithoutDuplicateSearch(t *testing.T) {
	r, f, job, _ := newRecoveryFixture(t)
	runRecoveryTest(t, r, job, "first")
	job.SearchIntentAt = time.Now().Add(-25 * time.Hour)
	run := runRecoveryTest(t, r, job, "long-wait")
	if job.State != storage.RecoveryAttention || job.ErrorCode != "import_not_observed" || f.searches != 1 || run.Stats.Repaired != 0 {
		t.Fatalf("job=%+v stats=%+v", job, run.Stats)
	}
}

func TestRecoveryCancelledBeforeStartSendsNoRequests(t *testing.T) {
	r, f, job, _ := newRecoveryFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var mu sync.Mutex
	if r.processRecovery(ctx, &storage.RepairRun{ID: "cancelled"}, &mu, job, true) {
		t.Fatal("cancelled job ran")
	}
	if f.requests != 0 {
		t.Fatal("cancelled job sent requests")
	}
}

func TestRecoveryReplayPreparesInitialIntentWithoutDeleting(t *testing.T) {
	r, f, job, dir := newRecoveryFixture(t)
	// Only the initial durable intent exists: the process died before preflight
	// could persist Prepared. No health record or new probe drives this replay.
	if err := r.manager.storage.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := storage.NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r = NewRepair(&Manager{storage: s, arr: r.manager.arr})
	r.logger = zerolog.Nop()
	run := &storage.RepairRun{ID: "initial-intent-restart"}
	if err := r.resumeRecoveries(context.Background(), run, nil, "all"); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetRepairRecovery(job.ID)
	if err != nil || got == nil {
		t.Fatalf("read recovery: %v", err)
	}
	if !got.Prepared || got.File.ID != f.file.ID || got.Deleted {
		t.Fatalf("preflight not recovered safely: %+v", got)
	}
	if got.ErrorCode != "awaiting_broken_confirmation" || got.LastRunID != "" || !got.NextCheckAt.IsZero() {
		t.Fatalf("fresh probe cannot continue this job: %+v", got)
	}
	if f.deletes != 0 || f.searches != 0 || f.failures != 0 || run.Stats.Repaired != 0 {
		t.Fatalf("read-only preflight mutated Arr or claimed success: deletes=%d searches=%d failures=%d stats=%+v", f.deletes, f.searches, f.failures, run.Stats)
	}
}
