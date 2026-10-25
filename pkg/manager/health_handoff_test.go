package manager

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/customerror"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestHealthHandoffDirectNewCanonicalMustMigrateExistingFailures(t *testing.T) {
	r, e, item := newEvidenceRepair(t, 1)
	var restored atomic.Bool
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		var start, end int
		if _, err := fmt.Sscanf(req.Header.Get("Range"), "bytes=%d-%d", &start, &end); err != nil || start < 900 || end != 999 {
			t.Errorf("unexpected range: %s", req.Header.Get("Range"))
		}
		length := end - start + 1
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/1024", start, end))
		w.Header().Set("Content-Length", fmt.Sprint(length))
		w.WriteHeader(http.StatusPartialContent)
		if !restored.Load() {
			length /= 2
		}
		_, _ = w.Write(make([]byte, length))
	}))
	defer server.Close()
	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{"primary": {DownloadLink: server.URL, Size: 1024}}}
	m := newStreamFailoverTestManager(links, server.Client(), "primary")
	circuitTime := time.Now()
	m.streamFileCircuits.now = func() time.Time { return circuitTime }
	m.storage, m.clients = r.manager.storage, r.manager.clients
	r = NewRepair(m)
	oldName := item.Name
	if err := r.manager.storage.RecordFailedRead(oldName, e.Protocol, "read_failure:unexpected_eof", storage.FailedReadRange{InfoHash: e.InfoHash, FileName: "video-000.mkv", Offset: 900, Length: 100}); err != nil {
		t.Fatal(err)
	}
	oldHealth, _ := r.manager.storage.GetEntryHealth(oldName)
	if oldHealth == nil || len(oldHealth.FailedReads) != 1 || oldHealth.OwnerInfoHash != e.InfoHash {
		t.Fatalf("old owner fixture not bound: %+v", oldHealth)
	}
	current, err := r.manager.storage.Get(e.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	current.Name, current.OriginalFilename = "renamed-direct-candidate", "renamed-direct-candidate"
	if err := r.manager.storage.AddOrUpdateDurable(current); err != nil {
		t.Fatal(err)
	}
	newName := current.GetFolder()
	// The normal inventory sweep enumerates the new index name. No runtime
	// feedback or request through the old key triggers lazy migration first.
	h := r.probeEntry(context.Background(), "independent-direct-new-name", &candidate{name: newName}, newHealCache(), RepairRunOptions{}, false)
	oldHealth, oldErr := r.manager.storage.GetEntryHealth(oldName)
	t.Logf("new health=%+v; old health=%+v oldError=%v", h, oldHealth, oldErr)
	if h == nil || h.Status != storage.HealthUnknown || !h.Dirty || len(h.FailedReads) != 1 || requests.Load() == 0 || oldHealth.EntryName != newName {
		t.Fatal("new canonical row did not validate and retain the owner's failed range")
	}
	before := requests.Load()
	restored.Store(true)
	circuitTime = circuitTime.Add(10 * time.Minute)
	h = r.probeEntry(context.Background(), "independent-restored-range", &candidate{name: newName}, newHealCache(), RepairRunOptions{}, false)
	if h == nil || h.Status != storage.HealthHealthy || h.Dirty || len(h.FailedReads) != 0 || requests.Load() <= before {
		t.Fatalf("restored renamed range could not finish: %+v requests=%d->%d", h, before, requests.Load())
	}
}

func TestHealthHandoffLegacyWarningMustSurviveCanonicalRename(t *testing.T) {
	r, e, item := newEvidenceRepair(t, 1)
	oldName := item.Name
	// This is the existing deployed schema's unresolved warning: no new
	// OwnerInfoHash, FailedReads, or FailedReadsIncomplete fields yet.
	if err := r.manager.storage.SaveEntryHealth(&storage.EntryHealth{EntryName: oldName, Protocol: e.Protocol, Status: storage.HealthUnknown, Dirty: true, DirtyReason: "read_failure:unexpected_eof", DirtyRevision: 7}); err != nil {
		t.Fatal(err)
	}
	current, err := r.manager.storage.Get(e.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	current.Name, current.OriginalFilename = "legacy-warning-renamed", "legacy-warning-renamed"
	if err := r.manager.storage.AddOrUpdateDurable(current); err != nil {
		t.Fatal(err)
	}
	oldHealth, oldErr := r.manager.storage.GetEntryHealth(oldName)
	h := r.probeEntry(context.Background(), "independent-legacy-rename", &candidate{name: current.GetFolder()}, newHealCache(), RepairRunOptions{}, false)
	t.Logf("new health=%+v; old health=%+v oldError=%v", h, oldHealth, oldErr)
	if h == nil || h.Status != storage.HealthUnknown || !h.Dirty || !h.FailedReadsIncomplete {
		t.Fatal("real producer rename erased legacy missing-evidence warning and availability alone cleared it")
	}
}

func TestHealthHandoffClearBrokenCanResolveSharedReplacement(t *testing.T) {
	r, e, item := newEvidenceRepair(t, 2)
	const bad = "video-000.mkv"
	r.manager.clients.Store("primary", &healthEvidenceClient{check: func(_ context.Context, name string) error {
		if name == bad {
			return customerror.HosterUnavailableError
		}
		return nil
	}})
	clear := func(owner string) {
		t.Helper()
		if err := r.manager.storage.RecordFailedRead(item.Name, e.Protocol, "read_failure:content_missing", storage.FailedReadRange{InfoHash: owner, FileName: bad, Offset: 100, Length: 20}); err != nil {
			t.Fatal(err)
		}
		h := r.probeEntry(context.Background(), "operator-replacement", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
		if h == nil || h.Status != storage.HealthBroken || h.Dirty || len(h.BrokenFiles) != 1 || h.BrokenFiles[0].InfoHash != owner {
			t.Fatalf("current broken-file proof not available: %+v", h)
		}
		if _, err := r.ClearBroken(context.Background(), []string{item.Name}); err != nil {
			t.Fatal(err)
		}
		r.runWG.Wait()
	}
	clear(e.InfoHash)
	other := streamFailoverEntry("primary")
	other.InfoHash, other.Name, other.OriginalFilename = "operator-replacement", e.Name, e.OriginalFilename
	other.Files = map[string]*storage.File{bad: {Name: bad, InfoHash: other.InfoHash, Size: 1024, AddedOn: time.Now().Add(time.Second)}}
	other.Providers["primary"].ID = "replacement-primary"
	other.Providers["primary"].Files = map[string]*storage.ProviderFile{bad: {Link: bad}}
	if err := r.manager.storage.AddOrUpdateDurable(other); err != nil {
		t.Fatal(err)
	}
	clear(other.InfoHash)
	h := r.probeEntry(context.Background(), "after-operator-replacement", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	if h == nil || h.Status != storage.HealthHealthy || h.Dirty || len(h.FailedReads) != 0 || !h.FileRetirements[bad].Confirmed || h.FileRetirements[bad].InfoHash != other.InfoHash || len(h.FileRetirementHistory[bad]) != 1 {
		t.Fatalf("operator resolution did not finish: %+v", h)
	}
	current, _ := r.manager.storage.GetEntryItem(item.Name)
	if !current.Files[bad].Deleted || current.Files["video-001.mkv"].Deleted {
		t.Fatalf("operator resolution changed an unrelated file: %+v", current)
	}
	for _, hash := range []string{e.InfoHash, other.InfoHash} {
		if _, err := r.manager.storage.Get(hash); err != nil {
			t.Fatalf("file retirement removed a parent entry: %s %v", hash, err)
		}
	}
}
