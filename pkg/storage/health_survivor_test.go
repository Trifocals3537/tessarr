package storage

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

const survivorAlias = "old-retired-runtime"

func seedSharedHealthSurvivor(t *testing.T, s *Storage) (*Entry, *Entry) {
	t.Helper()
	a := seedHealthResolution(t, s)
	// An old-format forwarding key must not begin pointing at the survivor.
	if err := s.MarkEntryDirtyChecked(survivorAlias, a.Protocol, "entry_item_changed"); err != nil {
		t.Fatal(err)
	}
	if err := s.BindHealthAlias(survivorAlias, a.GetFolder()); err != nil {
		t.Fatal(err)
	}
	// Retain an interrupted local removal belonging to the retiring parent.
	item, _ := s.GetEntryItem(a.GetFolder())
	func() {
		defer func() {
			if v := recover(); v != "intent-stop" {
				t.Fatalf("pending removal fixture: %v", v)
			}
		}()
		s.healthRetirementForTest = func(phase string) {
			if phase == "intent" {
				panic("intent-stop")
			}
		}
		_, _ = s.RetireEntryItemFile(a.GetFolder(), "bad.mkv", item.Files["bad.mkv"])
	}()
	s.healthRetirementForTest = nil
	b := entryItemTransactionEntry("surviving-health-owner", a.GetFolder(), "surviving.mkv")
	b.OriginalFilename, b.Protocol = a.GetFolder(), config.ProtocolNZB
	b.Files["surviving.mkv"].Size = 100
	if err := s.AddOrUpdateDurable(b); err != nil {
		t.Fatal(err)
	}
	// Shared-folder evidence already durably belonging to B must stay active.
	if err := s.RecordFailedRead(b.GetFolder(), b.Protocol, "read_failure:timeout", FailedReadRange{InfoHash: b.InfoHash, FileName: "surviving.mkv", Offset: 10, Length: 20}); err != nil {
		t.Fatal(err)
	}
	return a, b
}

func survivorArchives(t *testing.T, s *Storage) []*EntryHealth {
	t.Helper()
	var out []*EntryHealth
	if err := s.repairState.ForEach(func(key string, _ []byte) error {
		h, err := s.getEntryHealthLocked(key)
		if err != nil {
			return err
		}
		if h.Archived {
			out = append(out, h)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func assertSharedHealthSurvivor(t *testing.T, s *Storage) {
	t.Helper()
	if _, err := s.ResolveHealthEntry("release", config.Get().FolderNaming); err != nil {
		t.Fatal(err)
	}
	h, err := s.GetEntryHealth("release")
	if err != nil || !h.Dirty || h.OwnerInfoHash != "surviving-health-owner" || len(h.FailedReads) != 1 || h.FailedReads[0].InfoHash != h.OwnerInfoHash || len(h.FileRetirements) != 0 {
		t.Fatalf("survivor lost/mixed proof or inherited retired work: %+v %v", h, err)
	}
	old, err := s.GetEntryHealth(survivorAlias)
	if err != nil || !old.Archived || old.OwnerInfoHash != "health-owner" || old.ArchivedFrom != "release" || len(old.FailedReads) != 3 || len(old.FileRetirements) != 1 {
		t.Fatalf("old alias lost archived proof: %+v %v", old, err)
	}
	if old.FileRetirements["bad.mkv"].Confirmed {
		t.Fatal("archiving acknowledged an interrupted removal")
	}
	oldData, _ := s.repairState.Get(old.EntryName)
	if err := s.MarkEntryDirtyChecked(survivorAlias, config.ProtocolNZB, "read_failure:timeout"); !errors.Is(err, ErrEntryDeleting) {
		t.Fatalf("old alias admitted new work: %v", err)
	}
	if err := s.SaveEntryHealth(old); !errors.Is(err, ErrHealthIdentity) {
		t.Fatalf("archive accepted a probe save: %v", err)
	}
	if err := s.DeleteEntryHealth(old.EntryName); err != nil {
		t.Fatal(err)
	}
	after, _ := s.repairState.Get(old.EntryName)
	if !bytes.Equal(after, oldData) {
		t.Fatal("retired proof was mutated or deleted")
	}
	count := 0
	if err := s.ForEachEntryHealth(func(h *EntryHealth) error { count++; return nil }); err != nil || count != 1 {
		t.Fatalf("archives entered active recovery: count=%d err=%v", count, err)
	}
	counts := s.CountEntryHealthByStatus()
	total := 0
	for _, count := range counts {
		total += count
	}
	if total != 1 {
		t.Fatalf("archives entered active counts: %+v", counts)
	}
	key, err := s.RecordFailedReadAt("release", config.ProtocolNZB, "read_failure:timeout", FailedReadRange{InfoHash: h.OwnerInfoHash, FileName: "surviving.mkv", Offset: 40, Length: 10})
	if err != nil || key != "release" {
		t.Fatalf("survivor failure intake blocked: %q %v", key, err)
	}
	h, _ = s.GetEntryHealth("release")
	if len(h.FailedReads) != 2 || !h.Dirty {
		t.Fatalf("new survivor failure not durable: %+v", h)
	}
}

func TestHealthSurvivorRetirementPreservesEvidenceAndFences(t *testing.T) {
	path := t.TempDir()
	s, err := NewStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := seedSharedHealthSurvivor(t, s)
	stale, _ := s.GetEntryHealth("release")
	if err := s.Delete(a.InfoHash); err != nil {
		t.Fatal(err)
	}
	before, _ := s.entryTombstones.Get(a.InfoHash)
	assertSharedHealthSurvivor(t, s)
	if err := s.SaveEntryHealth(stale); !errors.Is(err, ErrHealthIdentity) {
		t.Fatalf("old-owner save contaminated survivor: %v", err)
	}
	after, _ := s.entryTombstones.Get(a.InfoHash)
	if !bytes.Equal(before, after) {
		t.Fatal("health handoff rewrote deletion authority")
	}
	archiveCount := len(survivorArchives(t, s))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.ResolveHealthEntry("release", config.Get().FolderNaming); err != nil {
		t.Fatal(err)
	}
	h, err := s.GetEntryHealth("release")
	if err != nil || len(h.FailedReads) != 2 || !h.Dirty || len(survivorArchives(t, s)) != archiveCount {
		t.Fatalf("reopen lost proof or replayed a completed archive: %+v %v", h, err)
	}
}

func TestHealthSurvivorRequiresAuthoritativeRetirement(t *testing.T) {
	for _, mode := range []string{"missing", "corrupt", "replacement", "queue", "closed"} {
		t.Run(mode, func(t *testing.T) {
			s := newMainLifecycleTestStorage(t)
			a, b := seedSharedHealthSurvivor(t, s)
			if err := s.entries.Delete(a.InfoHash); err != nil {
				t.Fatal(err)
			}
			if _, err := s.reconcileEntryItems(); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "corrupt":
				if err := s.entryTombstones.Put(a.InfoHash, []byte("{"), nil); err != nil {
					t.Fatal(err)
				}
			case "replacement", "queue":
				phase := mainEntryTombstoneReplacementPending
				if mode == "queue" {
					phase = mainEntryTombstoneQueuePending
				}
				data, _ := json.Marshal(mainEntryTombstone{Version: mainEntryTombstoneVersion, Phase: phase, QueueIncarnation: "pending-replacement"})
				if err := s.entryTombstones.Put(a.InfoHash, data, nil); err != nil {
					t.Fatal(err)
				}
			case "closed":
				if err := s.entryTombstones.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := s.ResolveHealthEntry(b.GetFolder(), config.Get().FolderNaming); err == nil {
				t.Fatal("uncertain ownership was treated as retirement")
			}
			if len(survivorArchives(t, s)) != 0 {
				t.Fatal("uncertain data was archived")
			}
			h, _ := s.GetEntryHealth(b.GetFolder())
			if h.OwnerInfoHash != a.InfoHash || len(h.FailedReads) != 3 {
				t.Fatalf("failed proof changed evidence: %+v", h)
			}
		})
	}
}

func TestHealthSurvivorArchiveCrashRecovery(t *testing.T) {
	for _, phase := range []string{"archive", "aliases", "current", "delete"} {
		t.Run(phase, func(t *testing.T) {
			path := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestHealthSurvivorCrashHelper$")
			cmd.Env = append(os.Environ(), "TESSARR_SURVIVOR_PATH="+path, "TESSARR_SURVIVOR_PHASE="+phase)
			out, err := cmd.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 84 {
				t.Fatalf("crash helper: %v %s", err, out)
			}
			s, err := NewStorage(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			assertSharedHealthSurvivor(t, s)
			if _, err := s.Get("health-owner"); !IsEntryNotFound(err) {
				t.Fatalf("retired parent reappeared: %v", err)
			}
			if _, found, err := s.loadMainEntryTombstone("health-owner"); err != nil || !found {
				t.Fatalf("deletion fence lost: found=%t err=%v", found, err)
			}
		})
	}
}

func TestHealthSurvivorCrashHelper(t *testing.T) {
	path := os.Getenv("TESSARR_SURVIVOR_PATH")
	if path == "" {
		return
	}
	s, err := NewStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	a, b := seedSharedHealthSurvivor(t, s)
	phase := os.Getenv("TESSARR_SURVIVOR_PHASE")
	if phase == "delete" {
		if err := s.DeleteWithCleanup(a.InfoHash, func(*Entry) error { os.Exit(84); return nil }); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := s.Delete(a.InfoHash); err != nil {
			t.Fatal(err)
		}
		s.healthOwnerArchiveForTest = func(at string) {
			if at == phase {
				os.Exit(84)
			}
		}
		if _, err := s.ResolveHealthEntry(b.GetFolder(), config.Get().FolderNaming); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("expected process death without Close")
}

func TestHealthSurvivorArchiveSyncFailureRetriesBarrier(t *testing.T) {
	for _, phase := range []string{"archive", "aliases", "current"} {
		t.Run(phase, func(t *testing.T) {
			s := newMainLifecycleTestStorage(t)
			a, b := seedSharedHealthSurvivor(t, s)
			if err := s.Delete(a.InfoHash); err != nil {
				t.Fatal(err)
			}
			failed := errors.New("injected health archive sync failure")
			s.healthOwnerArchiveSyncForTest = func(at string) error {
				if at == phase {
					return failed
				}
				return nil
			}
			if _, err := s.ResolveHealthEntry(b.GetFolder(), config.Get().FolderNaming); !errors.Is(err, failed) {
				t.Fatalf("failure did not stop publication: %v", err)
			}
			h, _ := s.GetEntryHealth(b.GetFolder())
			if phase != "current" && h.OwnerInfoHash != a.InfoHash {
				t.Fatal("current owner published ahead of its barrier")
			}
			s.healthOwnerArchiveSyncForTest = nil
			assertSharedHealthSurvivor(t, s)
		})
	}
}

func TestHealthSurvivorRetainsUnscopedMissingEvidence(t *testing.T) {
	s := newMainLifecycleTestStorage(t)
	a, b := seedSharedHealthSurvivor(t, s)
	h, _ := s.GetEntryHealth(a.GetFolder())
	h.FailedReadsIncomplete = true
	if err := s.SaveEntryHealth(h); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(a.InfoHash); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveHealthEntry(b.GetFolder(), config.Get().FolderNaming); err != nil {
		t.Fatal(err)
	}
	h, _ = s.GetEntryHealth(b.GetFolder())
	if !h.Dirty || !h.FailedReadsIncomplete || len(h.FailedReads) != 1 || h.FailedReads[0].InfoHash != b.InfoHash {
		t.Fatalf("handoff invented a scope for missing evidence: %+v", h)
	}
	if count, err := s.ClearEntryHealthByStatuses([]HealthStatus{HealthUnknown}); err != nil || count != 0 {
		t.Fatalf("display reset erased missing proof: count=%d err=%v", count, err)
	}
}

func TestHealthSurvivorDoesNotHideThirdOwnerEvidence(t *testing.T) {
	s := newMainLifecycleTestStorage(t)
	a, b := seedSharedHealthSurvivor(t, s)
	h, _ := s.GetEntryHealth(a.GetFolder())
	addFailedRead(h, FailedReadRange{InfoHash: "unresolved-third-owner", FileName: "unknown.mkv", Length: 1})
	if err := s.SaveEntryHealth(h); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(a.InfoHash); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveHealthEntry(b.GetFolder(), config.Get().FolderNaming); !errors.Is(err, ErrHealthIdentity) {
		t.Fatalf("unproved third owner was hidden: %v", err)
	}
	if len(survivorArchives(t, s)) != 0 {
		t.Fatal("ambiguous active proof entered an archive")
	}
	h, _ = s.GetEntryHealth(b.GetFolder())
	if len(h.FailedReads) != 4 || !h.Dirty {
		t.Fatalf("ambiguous evidence changed: %+v", h)
	}
}

func TestHealthSurvivorRetiredAliasCanBindAnotherCurrentOwner(t *testing.T) {
	s := newMainLifecycleTestStorage(t)
	a, b := seedSharedHealthSurvivor(t, s)
	if err := s.Delete(a.InfoHash); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveHealthEntry(b.GetFolder(), config.Get().FolderNaming); err != nil {
		t.Fatal(err)
	}
	old, _ := s.GetEntryHealth(survivorAlias)
	proof, _ := s.repairState.Get(old.EntryName)
	c := entryItemTransactionEntry("reused-retired-alias-owner", survivorAlias, "new.mkv")
	c.OriginalFilename, c.Protocol = survivorAlias, config.ProtocolNZB
	c.Files["new.mkv"].Size = 100
	if err := s.AddOrUpdateDurable(c); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordFailedRead(c.GetFolder(), c.Protocol, "read_failure:timeout", FailedReadRange{InfoHash: c.InfoHash, FileName: "new.mkv", Offset: 30, Length: 10}); err != nil {
		t.Fatal(err)
	}
	h, err := s.GetEntryHealth(c.GetFolder())
	if err != nil || h.Archived || h.OwnerInfoHash != c.InfoHash || len(h.FailedReads) != 1 || h.FailedReads[0].InfoHash != c.InfoHash {
		t.Fatalf("reused alias inherited historical proof: %+v %v", h, err)
	}
	survivor, _ := s.GetEntryHealth(b.GetFolder())
	if len(survivor.FailedReads) != 1 || survivor.OwnerInfoHash != b.InfoHash {
		t.Fatalf("alias reuse changed survivor scope: %+v", survivor)
	}
	after, _ := s.repairState.Get(old.EntryName)
	if !bytes.Equal(proof, after) || len(survivorArchives(t, s)) != 1 {
		t.Fatal("alias reuse changed or duplicated historical evidence")
	}
}
