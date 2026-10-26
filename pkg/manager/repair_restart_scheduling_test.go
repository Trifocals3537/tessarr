package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestInterruptedFreshRecheckResumesAfterStorageRestart(t *testing.T) {
	for _, previous := range []storage.HealthStatus{storage.HealthHealthy, storage.HealthUnsupported} {
		t.Run(string(previous), func(t *testing.T) {
			r, entry, _ := newEvidenceRepair(t, 2)
			if err := r.manager.storage.Close(); err != nil {
				t.Fatal(err)
			}
			path := t.TempDir()
			s, err := storage.NewStorage(path)
			if err != nil {
				t.Fatal(err)
			}
			r.manager.storage = s
			t.Cleanup(func() { _ = r.manager.storage.Close() })
			entry.MainGeneration = 0
			if err := s.AddOrUpdate(entry); err != nil {
				t.Fatal(err)
			}
			item, err := s.GetEntryItem(entry.GetFolder())
			if err != nil {
				t.Fatal(err)
			}
			h, err := s.GetEntryHealth(item.Name)
			if err != nil {
				t.Fatal(err)
			}
			fingerprint, err := r.fileProbeFingerprint(item, "video-000.mkv")
			if err != nil {
				t.Fatal(err)
			}
			h.Status, h.PreviousStatus, h.ActiveRunID = storage.HealthRepairing, previous, "interrupted-recheck"
			h.Dirty, h.DirtyReason = false, ""
			h.LastCheckedAt = time.Now()
			h.LastOKAt = h.LastCheckedAt
			h.NextCheckDueAt = time.Now().Add(24 * time.Hour)
			h.ProbeProgress = &storage.HealthProbeProgress{
				Revision: h.DirtyRevision, Fingerprint: storage.EntryItemRepairFingerprint(item),
				Mode:      fmt.Sprintf("v2:content=true:unrestrict=false:scope=%s", r.effectiveProtocolScope(RepairRunOptions{})),
				StartedAt: time.Now(), Verified: map[string]string{"video-000.mkv": fingerprint},
			}
			before, _ := json.Marshal(h.ProbeProgress)
			if err := s.SaveEntryHealth(h); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			r.manager.storage, err = storage.NewStorage(path)
			if err != nil {
				t.Fatal(err)
			}
			r = NewRepair(r.manager)
			r.reconcileOrphans()
			recovered, err := r.manager.storage.GetEntryHealth(item.Name)
			if err != nil {
				t.Fatal(err)
			}
			after, _ := json.Marshal(recovered.ProbeProgress)
			if !recovered.Dirty || !recovered.IsDue(time.Now(), 24*time.Hour) || recovered.Status != previous || recovered.ActiveRunID != "" || !recovered.NextCheckDueAt.IsZero() {
				t.Fatalf("unfinished check was not made due: %+v", recovered)
			}
			if string(before) != string(after) || recovered.DirtyRevision != h.DirtyRevision || !recovered.LastOKAt.Equal(h.LastOKAt) {
				t.Fatal("restart changed probe evidence, revision, or historical success")
			}
			// The normal scheduler and startup event admission must both see it.
			due, skipped := r.filterDueCandidates(map[string]*candidate{item.Name: {name: item.Name}}, false)
			if len(due) != 1 || skipped != 0 {
				t.Fatal("normal sweep skipped interrupted work")
			}
			r.eventStarted = true
			r.enqueuePersistedDirty()
			if r.eventQueueStatus().Pending != 1 {
				t.Fatal("startup did not enqueue interrupted work")
			}
			r.eventStarted = false
			var callsMu sync.Mutex
			calls := make(map[string]int)
			client := r.manager.ProviderClient("primary").(*healthEvidenceClient)
			client.check = func(_ context.Context, name string) error {
				callsMu.Lock()
				defer callsMu.Unlock()
				calls[name]++
				return nil
			}
			final := r.probeEntry(context.Background(), "resumed-check", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
			if final == nil || final.Dirty || final.Status != storage.HealthHealthy || final.ProbeProgress != nil {
				t.Fatalf("resumed check did not complete: %+v", final)
			}
			if calls["video-000.mkv"] != 0 || calls["video-001.mkv"] != 1 {
				t.Fatalf("resume failed to reuse valid persisted proof: %v", calls)
			}
		})
	}
}

func TestInterruptedRecheckPreservesExistingFailureEvidence(t *testing.T) {
	r, entry, item := newEvidenceRepair(t, 1)
	failed := storage.FailedReadRange{InfoHash: entry.InfoHash, FileName: "video-000.mkv", FileID: item.Files["video-000.mkv"].ID, Offset: 100, Length: 20}
	if err := r.manager.storage.RecordFailedRead(item.Name, config.ProtocolTorrent, "read_failure:unexpected_eof", failed); err != nil {
		t.Fatal(err)
	}
	h, err := r.manager.storage.GetEntryHealth(item.Name)
	if err != nil {
		t.Fatal(err)
	}
	h.Status = storage.HealthRepairing
	h.PreviousStatus = storage.HealthBroken
	h.ActiveRunID = "interrupted-dirty-check"
	if err := r.manager.storage.SaveEntryHealth(h); err != nil {
		t.Fatal(err)
	}
	r.reconcileOrphans()
	got, err := r.manager.storage.GetEntryHealth(item.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Dirty || got.DirtyRevision != h.DirtyRevision || got.DirtyReason != h.DirtyReason || len(got.FailedReads) != 1 || got.FailedReads[0] != failed {
		t.Fatal("restart changed existing failure evidence")
	}
	r.reconcileOrphans()
	again, _ := r.manager.storage.GetEntryHealth(item.Name)
	if again.DirtyRevision != got.DirtyRevision || again.DirtyReason != got.DirtyReason {
		t.Fatal("reconciliation is not idempotent")
	}
}
