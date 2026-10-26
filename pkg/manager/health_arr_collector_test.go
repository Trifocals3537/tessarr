package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/arr"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestHealthProbeActualArrCollectorRetainsOwnerEvidence(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(fmt.Sprint("legacy=", legacy), func(t *testing.T) {
			r, e, item := newEvidenceRepair(t, 1)
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				requests.Add(1)
				w.Header().Set("Content-Range", "bytes 900-999/1024")
				w.Header().Set("Content-Length", "100")
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(make([]byte, 50))
			}))
			defer server.Close()
			links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{"primary": {DownloadLink: server.URL, Size: 1024}}}
			m := newStreamFailoverTestManager(links, server.Client(), "primary")
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
			// Exercise the actual Arr producer, including its symlink lookup,
			// against a local GET-only Radarr fixture.
			libraryLink := filepath.Join(t.TempDir(), "library-video.mkv")
			target := filepath.Join(t.TempDir(), current.GetFolder(), "video-000.mkv")
			if err := os.Symlink(target, libraryLink); err != nil {
				if runtime.GOOS == "windows" {
					t.Skipf("Windows symlink privilege unavailable; the preloaded-candidate regression runs separately: %v", err)
				}
				t.Fatal(err)
			}
			arrServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.Method != http.MethodGet || req.URL.Path != "/api/v3/movie" {
					t.Errorf("unexpected Arr request %s %s", req.Method, req.URL.Path)
					w.WriteHeader(404)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 7, "title": "local probe", "movieFile": map[string]any{"id": 10, "path": libraryLink, "size": 1024}}})
			}))
			defer arrServer.Close()
			a := &arr.Arr{Name: "local-radarr-probe", Type: arr.Radarr, Host: arrServer.URL, Token: "fixture"}
			candidates, err := r.collectArrMediaCandidates(context.Background(), a, "")
			if err != nil {
				t.Fatal(err)
			}
			c := candidates[current.GetFolder()]
			if c == nil || c.item == nil || len(c.contentMap) != 1 {
				t.Fatalf("actual Arr collection did not materialize fixture: %+v", candidates)
			}
			h := r.probeEntry(context.Background(), "arr-preloaded", c, newHealCache(), RepairRunOptions{}, false)
			old, oldErr := m.storage.GetEntryHealth(oldName)
			t.Logf("preloaded result=%+v old=%+v oldErr=%v rangeRequests=%d", h, old, oldErr, requests.Load())
			if h == nil || h.Status != storage.HealthUnknown || !h.Dirty || h.OwnerInfoHash != e.InfoHash {
				t.Fatal("actual Arr candidate bypassed owner handoff")
			}
			if c.arrName != a.Name || len(c.contentMap) != 1 || c.contentMap["video-000.mkv"].FileId != 10 {
				t.Fatal("refresh lost Arr context")
			}
			if legacy && !h.FailedReadsIncomplete || !legacy && (len(h.FailedReads) != 1 || requests.Load() == 0) {
				t.Fatal("Arr collection did not preserve required proof")
			}
		})
	}
}
