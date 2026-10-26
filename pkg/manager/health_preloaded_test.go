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
	"github.com/Trifocals3537/tessarr/pkg/arr"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestHealthProbePreloadedArrCandidateRetainsOwnerEvidence(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint("legacy=", legacy), func(t *testing.T) {
			r, e, item := newEvidenceRepair(t, 1)
			var requests atomic.Int32
			var restored atomic.Bool
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
			if legacy {
				if err := m.storage.SaveEntryHealth(&storage.EntryHealth{EntryName: oldName, Protocol: e.Protocol, Status: storage.HealthUnknown, Dirty: true, DirtyReason: "read_failure:unexpected_eof", DirtyRevision: 7}); err != nil {
					t.Fatal(err)
				}
			} else if err := m.storage.RecordFailedRead(oldName, e.Protocol, "read_failure:unexpected_eof", storage.FailedReadRange{InfoHash: e.InfoHash, FileName: "video-000.mkv", Offset: 900, Length: 100}); err != nil {
				t.Fatal(err)
			}
			current, err := m.storage.Get(e.InfoHash)
			if err != nil {
				t.Fatal(err)
			}
			current.Name, current.OriginalFilename = "arr-preloaded-renamed", "arr-preloaded-renamed"
			if err := m.storage.AddOrUpdateDurable(current); err != nil {
				t.Fatal(err)
			}
			// This candidate boundary runs on every platform, including hosts
			// that cannot create the symlinks used by the actual Arr collector.
			preloaded, err := m.GetEntryItem(current.GetFolder())
			if err != nil {
				t.Fatal(err)
			}
			c := &candidate{name: preloaded.Name, item: preloaded, arrName: "fixture-radarr", arrKind: storage.ArrKindRadarr, contentMap: map[string]arr.ContentFile{"video-000.mkv": {Id: 7, FileId: 10, TargetPath: "video-000.mkv"}}}
			h := r.probeEntry(context.Background(), "arr-preloaded", c, newHealCache(), RepairRunOptions{}, false)
			old, oldErr := m.storage.GetEntryHealth(oldName)
			t.Logf("preloaded result=%+v old=%+v oldErr=%v rangeRequests=%d", h, old, oldErr, requests.Load())
			if h == nil || h.Status != storage.HealthUnknown || !h.Dirty || h.OwnerInfoHash != e.InfoHash {
				t.Fatal("preloaded Arr candidate bypassed owner handoff")
			}
			if c.arrName != "fixture-radarr" || c.arrKind != storage.ArrKindRadarr || len(c.contentMap) != 1 || c.contentMap["video-000.mkv"].FileId != 10 {
				t.Fatal("item refresh discarded Arr operator context")
			}
			if legacy {
				if !h.FailedReadsIncomplete || requests.Load() != 0 {
					t.Fatalf("legacy missing proof was invented: %+v requests=%d", h, requests.Load())
				}
			} else {
				if len(h.FailedReads) != 1 || requests.Load() == 0 {
					t.Fatalf("failed range was not read and retained: %+v", h)
				}
				before := requests.Load()
				restored.Store(true)
				circuitTime = circuitTime.Add(10 * time.Minute)
				repaired := r.probeEntry(context.Background(), "arr-restored", c, newHealCache(), RepairRunOptions{}, false)
				if repaired == nil || repaired.Status != storage.HealthHealthy || repaired.Dirty || len(repaired.FailedReads) != 0 || requests.Load() <= before {
					t.Fatalf("restored Arr range could not complete: %+v", repaired)
				}
			}
		})
	}
}

func TestHealthProbeRetiredParentSurvivorCanResolveThroughClearBroken(t *testing.T) {
	r, a, item := newEvidenceRepair(t, 1)
	if err := r.manager.storage.RecordFailedRead(item.Name, a.Protocol, "read_failure:timeout", storage.FailedReadRange{InfoHash: a.InfoHash, FileName: "video-000.mkv", Offset: 100, Length: 20}); err != nil {
		t.Fatal(err)
	}
	b := streamFailoverEntry("primary")
	b.InfoHash, b.Name, b.OriginalFilename = "surviving-operator-parent", a.Name, a.OriginalFilename
	b.Files = map[string]*storage.File{
		"surviving.mkv": {InfoHash: b.InfoHash, Name: "surviving.mkv", Size: 1024},
		"good.mkv":      {InfoHash: b.InfoHash, Name: "good.mkv", Size: 1024},
	}
	b.Providers["primary"].ID = "surviving-operator-transfer"
	b.Providers["primary"].Files = map[string]*storage.ProviderFile{"surviving.mkv": {Link: "surviving.mkv"}, "good.mkv": {Link: "good.mkv"}}
	if err := r.manager.storage.AddOrUpdateDurable(b); err != nil {
		t.Fatal(err)
	}
	if err := r.manager.storage.RecordFailedRead(item.Name, b.Protocol, "read_failure:content_missing", storage.FailedReadRange{InfoHash: b.InfoHash, FileName: "surviving.mkv", Offset: 100, Length: 20}); err != nil {
		t.Fatal(err)
	}
	if err := r.manager.storage.Delete(a.InfoHash); err != nil {
		t.Fatal(err)
	}
	r.manager.clients.Store("primary", &healthEvidenceClient{check: func(_ context.Context, name string) error {
		if name == "surviving.mkv" {
			return customerror.HosterUnavailableError
		}
		return nil
	}})
	preloaded, _ := r.manager.GetEntryItem(item.Name)
	h := r.probeEntry(context.Background(), "survivor-operator", &candidate{name: item.Name, item: preloaded}, newHealCache(), RepairRunOptions{}, false)
	if h == nil || h.Status != storage.HealthBroken || h.Dirty || len(h.BrokenFiles) != 1 || h.BrokenFiles[0].InfoHash != b.InfoHash || len(h.FailedReads) != 1 {
		t.Fatalf("survivor did not obtain its own broken proof: %+v", h)
	}
	if _, err := r.ClearBroken(context.Background(), []string{item.Name}); err != nil {
		t.Fatal(err)
	}
	r.runWG.Wait()
	h = r.probeEntry(context.Background(), "survivor-after-clear", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	if h == nil || h.Status != storage.HealthHealthy || h.Dirty || len(h.FailedReads) != 0 || h.OwnerInfoHash != b.InfoHash {
		t.Fatalf("survivor operator resolution could not finish: %+v", h)
	}
	current, _ := r.manager.storage.GetEntryItem(item.Name)
	if !current.Files["surviving.mkv"].Deleted || current.Files["good.mkv"].Deleted {
		t.Fatalf("operator resolution changed the other file: %+v", current)
	}
	if _, err := r.manager.storage.Get(a.InfoHash); !storage.IsEntryNotFound(err) {
		t.Fatalf("old parent retirement was undone: %v", err)
	}
	if _, err := r.manager.storage.Get(b.InfoHash); err != nil {
		t.Fatalf("surviving parent was removed: %v", err)
	}
}
