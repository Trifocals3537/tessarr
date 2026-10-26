package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

type healthEvidenceClient struct {
	debrid.Client
	apiKey string
	check  func(context.Context, string) error
}

func (*healthEvidenceClient) SupportsCheck() bool { return true }
func (c *healthEvidenceClient) Config() config.Debrid {
	return config.Debrid{Name: "primary", Provider: "realdebrid", APIKey: c.apiKey}
}
func (c *healthEvidenceClient) CheckFile(ctx context.Context, _ string, link string) error {
	return c.check(ctx, link)
}

func newEvidenceRepair(t *testing.T, count int) (*Repair, *storage.Entry, *storage.EntryItem) {
	t.Helper()
	if err := config.SetConfigPath(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	s, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	e := streamFailoverEntry("primary")
	e.Name = "evidence-release"
	e.OriginalFilename = e.Name
	e.Files = make(map[string]*storage.File)
	e.Providers["primary"].Files = make(map[string]*storage.ProviderFile)
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("video-%03d.mkv", i)
		e.Files[name] = &storage.File{Name: name, InfoHash: e.InfoHash, Size: 1024}
		e.Providers["primary"].Files[name] = &storage.ProviderFile{Link: name}
	}
	if err := s.AddOrUpdate(e); err != nil {
		t.Fatal(err)
	}
	item, err := s.GetEntryItem(e.GetFolder())
	if err != nil {
		t.Fatal(err)
	}
	m := &Manager{storage: s, clients: xsync.NewMap[string, debrid.Client](), logger: zerolog.Nop()}
	m.clients.Store("primary", &healthEvidenceClient{check: func(context.Context, string) error { return nil }})
	return NewRepair(m), e, item
}

func TestHealthRollupRequiresEveryFile(t *testing.T) {
	for _, tt := range []struct {
		name    string
		results []fileResult
		want    storage.HealthStatus
	}{
		{"all", []fileResult{{healthy: true}, {healthy: true}}, storage.HealthHealthy},
		{"partial", []fileResult{{healthy: true}, {reason: "timeout"}}, storage.HealthUnknown},
		{"broken and unknown", []fileResult{{broken: true}, {}}, storage.HealthBroken},
		{"empty", nil, storage.HealthUnknown},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := rollupStatus(tt.results); got != tt.want {
				t.Fatalf("got %s want %s", got, tt.want)
			}
		})
	}
}

func TestHealthPackResumeRetainsIncompleteFilesAndRechecksNewRevision(t *testing.T) {
	r, _, item := newEvidenceRepair(t, 6)
	var mu sync.Mutex
	calls := make(map[string]int)
	var incomplete atomic.Bool
	incomplete.Store(true)
	r.manager.clients.Store("primary", &healthEvidenceClient{check: func(_ context.Context, name string) error {
		mu.Lock()
		calls[name]++
		mu.Unlock()
		if name == "video-005.mkv" && incomplete.Load() {
			return context.DeadlineExceeded
		}
		return nil
	}})
	probe := func() *storage.EntryHealth {
		return r.probeEntry(context.Background(), "test", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	}
	h := probe()
	if h == nil || h.Status != storage.HealthUnknown || !h.Dirty || len(h.IncompleteFiles) != 1 || h.ProbeProgress == nil || len(h.ProbeProgress.Verified) != 5 {
		t.Fatalf("partial falsely healthy/progress lost: %+v", h)
	}
	// Recreate the service to prove progress is persisted, not queue-local.
	r = NewRepair(r.manager)
	incomplete.Store(false)
	h = probe()
	if h == nil || h.Status != storage.HealthHealthy || h.Dirty || h.ProbeProgress != nil {
		t.Fatalf("resume failed: %+v", h)
	}
	mu.Lock()
	for name, n := range calls {
		want := 1
		if name == "video-005.mkv" {
			want = 2
		}
		if n != want {
			t.Errorf("%s called %d want %d", name, n, want)
		}
	}
	mu.Unlock()
	if err := r.manager.storage.MarkEntryDirtyChecked(item.Name, config.ProtocolTorrent, "entry_item_changed"); err != nil {
		t.Fatal(err)
	}
	h = probe()
	if h == nil || h.Dirty {
		t.Fatalf("new revision failed: %+v", h)
	}
	mu.Lock()
	defer mu.Unlock()
	if calls["video-000.mkv"] != 2 {
		t.Fatalf("new revision reused old evidence: %v", calls)
	}
}

func TestHealthConcurrentFailureCannotBeCleared(t *testing.T) {
	r, e, item := newEvidenceRepair(t, 1)
	r.manager.clients.Store("primary", &healthEvidenceClient{check: func(context.Context, string) error {
		return r.manager.storage.RecordFailedRead(item.Name, e.Protocol, "read_failure:timeout", storage.FailedReadRange{InfoHash: e.InfoHash, FileName: "video-000.mkv", Offset: 100, Length: 20})
	}})
	h := r.probeEntry(context.Background(), "test", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	if h == nil || !h.Dirty || len(h.FailedReads) != 1 || h.ProbeProgress != nil {
		t.Fatalf("new failure cleared: %+v", h)
	}
}

func TestHealthTargetReplacementDiscardsSuccessfulProbe(t *testing.T) {
	r, e, item := newEvidenceRepair(t, 1)
	r.manager.clients.Store("primary", &healthEvidenceClient{check: func(context.Context, string) error {
		current, err := r.manager.storage.Get(e.InfoHash)
		if err != nil {
			return err
		}
		current.Providers["primary"].ID = "replacement"
		return r.manager.storage.AddOrUpdate(current)
	}})
	h := r.probeEntry(context.Background(), "test", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	if h == nil || h.Status == storage.HealthHealthy || !h.Dirty || (h.ProbeProgress != nil && len(h.ProbeProgress.Verified) != 0) {
		t.Fatalf("changed target accepted: %+v", h)
	}
}

func TestHealthProgressCannotCrossProviderAccounts(t *testing.T) {
	r, _, item := newEvidenceRepair(t, 2)
	var complete atomic.Bool
	var calls atomic.Int32
	check := func(_ context.Context, name string) error {
		if name == "video-000.mkv" {
			calls.Add(1)
		}
		if name == "video-001.mkv" && !complete.Load() {
			return context.DeadlineExceeded
		}
		return nil
	}
	r.manager.clients.Store("primary", &healthEvidenceClient{apiKey: "first-account", check: check})
	h := r.probeEntry(context.Background(), "test", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	if h == nil || !h.Dirty || h.ProbeProgress == nil || len(h.ProbeProgress.Verified) != 1 {
		t.Fatalf("fixture did not retain progress: %+v", h)
	}
	// No entry mutation, dirty signal or process restart accompanies a client
	// replacement. The cached successful file must still be checked again.
	r.manager.clients.Store("primary", &healthEvidenceClient{apiKey: "second-account", check: check})
	complete.Store(true)
	h = r.probeEntry(context.Background(), "test", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	if h == nil || h.Dirty || h.Status != storage.HealthHealthy || calls.Load() != 2 {
		t.Fatalf("proof reused across accounts: %+v calls=%d", h, calls.Load())
	}
}

func TestHealthMissingAndLegacyRangeEvidenceStaysIncomplete(t *testing.T) {
	r, _, item := newEvidenceRepair(t, 1)
	if err := r.manager.storage.MarkEntryDirtyChecked(item.Name, config.ProtocolTorrent, "read_failure:timeout"); err != nil {
		t.Fatal(err)
	}
	if outcome := r.reconcileDirtyEntry(context.Background(), item.Name); outcome != repairEventIncomplete {
		t.Fatalf("legacy range cleared: %s", outcome)
	}
	h, _ := r.manager.storage.GetEntryHealth(item.Name)
	if !h.Dirty || h.Status == storage.HealthHealthy {
		t.Fatalf("legacy evidence cleared: %+v", h)
	}
	h.FailedReads = []storage.FailedReadRange{{InfoHash: "other", FileName: "video-000.mkv", Length: 10}}
	if _, err := failedRangesFor(h, item); err == nil {
		t.Fatal("mismatched owner accepted")
	}
}

func TestHealthFailedTorrentRangeRequiresActualBytes(t *testing.T) {
	r, e, item := newEvidenceRepair(t, 1)
	var truncated atomic.Bool
	var rangeHeader atomic.Value
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		rangeHeader.Store(request.Header.Get("Range"))
		w.Header().Set("Content-Range", "bytes 900-999/1024")
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusPartialContent)
		size := 100
		if truncated.Load() {
			size = 50
		}
		_, _ = w.Write(make([]byte, size))
	}))
	defer server.Close()
	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{"primary": {DownloadLink: server.URL, Size: 1024}}}
	m := newStreamFailoverTestManager(links, server.Client(), "primary")
	m.storage = r.manager.storage
	m.clients = r.manager.clients
	r = NewRepair(m)
	if err := m.storage.RecordFailedRead(item.Name, e.Protocol, "read_failure:unexpected_eof", storage.FailedReadRange{InfoHash: e.InfoHash, FileName: "video-000.mkv", Offset: 900, Length: 100}); err != nil {
		t.Fatal(err)
	}
	h := r.probeEntry(context.Background(), "test", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	if h == nil || h.Status != storage.HealthHealthy || h.Dirty || len(h.FailedReads) != 0 || rangeHeader.Load() != "bytes=900-999" {
		t.Fatalf("exact failed range not proven: %+v %s", h, rangeHeader.Load())
	}
	truncated.Store(true)
	if err := m.storage.RecordFailedRead(item.Name, e.Protocol, "read_failure:unexpected_eof", storage.FailedReadRange{InfoHash: e.InfoHash, FileName: "video-000.mkv", Offset: 900, Length: 100}); err != nil {
		t.Fatal(err)
	}
	h = r.probeEntry(context.Background(), "test", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	if h == nil || h.Status == storage.HealthHealthy || !h.Dirty || len(h.FailedReads) != 1 {
		t.Fatalf("truncated failed range cleared: %+v", h)
	}
}

func TestHealthCanceledPackKeepsRevisionAndCompletedProgress(t *testing.T) {
	r, _, item := newEvidenceRepair(t, 8)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.manager.clients.Store("primary", &healthEvidenceClient{check: func(_ context.Context, name string) error {
		if name == "video-007.mkv" {
			cancel()
			return context.Canceled
		}
		return nil
	}})
	before, _ := r.manager.storage.GetEntryHealth(item.Name)
	h := r.probeEntry(ctx, "test", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	if h == nil || !h.Dirty || h.Status == storage.HealthHealthy || h.DirtyRevision != before.DirtyRevision || h.ProbeProgress == nil || len(h.ProbeProgress.Verified) == 0 {
		t.Fatalf("cancel lost progress/revision: %+v", h)
	}
	r.manager.clients.Store("primary", &healthEvidenceClient{check: func(context.Context, string) error { return nil }})
	if outcome := r.reconcileDirtyEntry(context.Background(), item.Name); outcome != repairEventHealthy {
		t.Fatalf("resume outcome %s", outcome)
	}
}

func TestHealthCoalescedValidatedFailureDoesNotInventEvidenceGap(t *testing.T) {
	r, e, item := newEvidenceRepair(t, 1)
	r.eventMu.Lock()
	r.eventStarted = true
	r.eventMu.Unlock()
	if !r.QueueDirtyEntry(item.Name) {
		t.Fatal("queue admission failed")
	}
	work, _ := r.nextDirtyEvent()
	if err := r.manager.storage.RecordFailedRead(item.Name, e.Protocol, "read_failure:timeout", storage.FailedReadRange{InfoHash: e.InfoHash, FileName: "video-000.mkv", Offset: 10, Length: 20}); err != nil {
		t.Fatal(err)
	}
	r.QueueDirtyEntry(item.Name)
	// The probe loaded the latest revision after the second queue admission
	// and successfully validated it. The generation guard still asks for a
	// fresh check, but must not invent a permanently missing failed range.
	h, _ := r.manager.storage.GetEntryHealth(item.Name)
	h.Dirty = false
	h.DirtyReason = ""
	h.FailedReads = nil
	h.Status = storage.HealthHealthy
	if err := r.manager.storage.SaveEntryHealth(h); err != nil {
		t.Fatal(err)
	}
	if outcome := r.finishDirtyEvent(work, repairEventHealthy); outcome != repairEventIncomplete {
		t.Fatalf("lost generation guard: %s", outcome)
	}
	h, _ = r.manager.storage.GetEntryHealth(item.Name)
	if !h.Dirty || h.FailedReadsIncomplete {
		t.Fatalf("synthetic event invented lost evidence: %+v", h)
	}
	if outcome := r.reconcileDirtyEntry(context.Background(), item.Name); outcome != repairEventHealthy {
		t.Fatalf("validated coalesced range stranded: %s", outcome)
	}
}

func TestHealthAliasUsesExactOwnerAndRejectsMixedOwnership(t *testing.T) {
	for _, tt := range []struct {
		runtime, original string
		naming            config.WebDavFolderNaming
	}{
		{"Friends.Release.mkv", "Friends.Release.mkv", config.WebDavUseFileNameNoExt},
		{"You.S01.Release", "YOU.S01.Release", config.WebDavUseOriginalName},
	} {
		for _, mode := range []string{"valid", "mixed", "ambiguous"} {
			t.Run(tt.runtime+"/"+mode, func(t *testing.T) {
				cmd := exec.Command(os.Args[0], "-test.run=^TestHealthAliasProducerHelper$")
				cmd.Env = append(os.Environ(), "TESSARR_ALIAS_RUNTIME="+tt.runtime, "TESSARR_ALIAS_ORIGINAL="+tt.original, "TESSARR_ALIAS_NAMING="+string(tt.naming), "TESSARR_ALIAS_MODE="+mode)
				if out, err := cmd.CombinedOutput(); err != nil {
					t.Fatalf("real producer fixture: %v %s", err, out)
				}
			})
		}
	}
}

func TestHealthAliasProducerHelper(t *testing.T) {
	runtime := os.Getenv("TESSARR_ALIAS_RUNTIME")
	if runtime == "" {
		return
	}
	defer logger.Close()
	root := t.TempDir()
	if err := config.SetConfigPath(root); err != nil {
		t.Fatal(err)
	}
	cfg := map[string]any{"folder_naming": os.Getenv("TESSARR_ALIAS_NAMING"), "debrids": []map[string]any{{"name": "realdebrid", "api_key": "fixture"}}}
	data, _ := json.Marshal(cfg)
	if err := os.WriteFile(filepath.Join(root, "config.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	config.Reset()
	config.Get()
	r, e, _ := newEvidenceRepair(t, 1)
	e.Name = runtime
	e.OriginalFilename = os.Getenv("TESSARR_ALIAS_ORIGINAL")
	if err := r.manager.storage.AddOrUpdate(e); err != nil {
		t.Fatal(err)
	}
	canonical := e.GetFolder()
	if _, err := r.manager.storage.GetEntryItem(runtime); err == nil {
		t.Fatal("fixture did not reproduce absent runtime key")
	}
	if err := r.manager.storage.RecordFailedRead(runtime, e.Protocol, "read_failure:timeout", storage.FailedReadRange{InfoHash: e.InfoHash, FileName: "video-000.mkv", Offset: 100, Length: 20}); err != nil {
		t.Fatal(err)
	}
	mode := os.Getenv("TESSARR_ALIAS_MODE")
	if mode == "mixed" {
		item, _ := r.manager.storage.GetEntryItem(canonical)
		item.Files["video-000.mkv"].InfoHash = "another-owner"
		if err := r.manager.storage.UpdateItem(item); err != nil {
			t.Fatal(err)
		}
	}
	if mode == "ambiguous" {
		second := streamFailoverEntry("primary")
		second.InfoHash = "another-owner"
		second.Name = runtime
		second.OriginalFilename = e.OriginalFilename
		if err := r.manager.storage.AddOrUpdate(second); err != nil {
			t.Fatal(err)
		}
	}
	c := &candidate{name: runtime}
	err := r.resolveHealthCandidate(c)
	if mode != "valid" {
		if err == nil {
			t.Fatalf("%s ownership accepted", mode)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if c.name != canonical {
		t.Fatalf("alias not resolved: %s", c.name)
	}
	h, _ := r.manager.storage.GetEntryHealth(runtime)
	if h.EntryName != canonical || !h.Dirty || len(h.FailedReads) != 1 {
		t.Fatalf("handoff lost signal: %+v", h)
	}
	// A failure under the old runtime name follows the durable binding.
	if err := r.manager.storage.MarkEntryDirtyChecked(runtime, e.Protocol, "read_failure:timeout"); err != nil {
		t.Fatal(err)
	}
	current, _ := r.manager.storage.GetEntryHealth(canonical)
	if current.DirtyRevision <= h.DirtyRevision {
		t.Fatal("alias failure did not reach canonical")
	}
}
