package storage

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
)

func TestHealthHandoffFreshRetirementCanAuthorizeReplacement(t *testing.T) {
	s := newMainLifecycleTestStorage(t)
	e := seedHealthResolution(t, s)
	item, _ := s.GetEntryItem(e.GetFolder())
	if _, err := s.RetireEntryItemFile(item.Name, "bad.mkv", item.Files["bad.mkv"]); err != nil {
		t.Fatal(err)
	}
	item, _ = s.GetEntryItem(item.Name)
	item.Files["bad.mkv"].ID = "replacement-incarnation"
	item.Files["bad.mkv"].Deleted = false
	if err := s.UpdateItem(item); err != nil {
		t.Fatal(err)
	}
	// This is a fresh operator instruction with an exact current snapshot,
	// not replay of the old receipt or stale broken-file proof.
	_, err := s.RetireEntryItemFile(item.Name, "bad.mkv", item.Files["bad.mkv"])
	t.Logf("fresh exact retirement of replacement: %v", err)
	if err != nil {
		t.Fatal("historical filename receipt blocks a new exact operator authorization")
	}
}

func TestHealthHandoffFreshRetirementAfterPendingSourceReplacement(t *testing.T) {
	s := newMainLifecycleTestStorage(t)
	e := seedHealthResolution(t, s)
	item, _ := s.GetEntryItem(e.GetFolder())
	func() {
		defer func() {
			if v := recover(); v != "intent-stop" {
				t.Fatalf("unexpected interruption: %v", v)
			}
		}()
		s.healthRetirementForTest = func(phase string) {
			if phase == "intent" {
				panic("intent-stop")
			}
		}
		_, _ = s.RetireEntryItemFile(item.Name, "bad.mkv", item.Files["bad.mkv"])
	}()
	s.healthRetirementForTest = nil
	current, err := s.Get(e.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	current.Files["bad.mkv"].ID = "replacement-incarnation"
	current.Files["bad.mkv"].Size = 120
	current.Files["bad.mkv"].AddedOn = time.Now().Add(time.Second)
	if err := s.AddOrUpdateDurable(current); err != nil {
		t.Fatal(err)
	}
	item, _ = s.GetEntryItem(e.GetFolder())
	if item.Files["bad.mkv"].Deleted || item.Files["bad.mkv"].ID != "replacement-incarnation" {
		t.Fatalf("producer did not publish replacement: %+v", item.Files["bad.mkv"])
	}
	_, replayErr := s.ReconcileHealthFileRetirements(item.Name)
	_, freshErr := s.RetireEntryItemFile(item.Name, "bad.mkv", item.Files["bad.mkv"])
	t.Logf("old replay rejection=%v fresh exact authorization rejection=%v", replayErr, freshErr)
	if !errors.Is(replayErr, ErrHealthRetirementIdentity) || freshErr != nil {
		t.Fatal("old pending intent permanently blocks a new exact operator authorization after real producer replacement")
	}
	h, _ := s.GetEntryHealth(item.Name)
	if len(h.FileRetirementHistory["bad.mkv"]) != 1 || h.FileRetirementHistory["bad.mkv"][0].Confirmed || !h.FileRetirements["bad.mkv"].Confirmed || h.FileRetirements["bad.mkv"].FileID != "replacement-incarnation" || len(h.FailedReads) != 2 {
		t.Fatalf("supersession lost the old intent or unrelated evidence: %+v", h)
	}
}

func TestHealthHandoffUnboundPreviousRecordSurvivesCanonicalRename(t *testing.T) {
	s := newMainLifecycleTestStorage(t)
	e := healthResolutionEntry()
	if err := s.AddOrUpdateDurable(e); err != nil {
		t.Fatal(err)
	}
	oldName := e.GetFolder()
	// On-disk record format emitted before these four fixes: exact ranges,
	// but no OwnerInfoHash field yet.
	if err := s.SaveEntryHealth(&EntryHealth{EntryName: oldName, Status: HealthUnknown, Dirty: true, DirtyReason: "read_failure:timeout", DirtyRevision: 7, FailedReads: []FailedReadRange{{InfoHash: e.InfoHash, FileName: "bad.mkv", Offset: 10, Length: 20}}}); err != nil {
		t.Fatal(err)
	}
	e.Name, e.OriginalFilename = "legacy-record-renamed", "legacy-record-renamed"
	if err := s.AddOrUpdateDurable(e); err != nil {
		t.Fatal(err)
	}
	old, oldErr := s.GetEntryHealth(oldName)
	current, currentErr := s.GetEntryHealth(e.GetFolder())
	t.Logf("old=%+v error=%v new=%+v error=%v", old, oldErr, current, currentErr)
	if old == nil && (current == nil || len(current.FailedReads) == 0) {
		t.Fatal("canonical rename deleted pre-fix exact range evidence")
	}
	if _, err := s.ResolveHealthEntry(e.GetFolder(), config.Get().FolderNaming); err != nil {
		t.Fatal(err)
	}
	current, err := s.GetEntryHealth(e.GetFolder())
	if err != nil || !current.Dirty || len(current.FailedReads) != 1 || current.OwnerInfoHash != e.InfoHash {
		t.Fatalf("direct new-key resolution lost legacy ranges: %+v %v", current, err)
	}
}

func TestHealthHandoffCanonicalReuseDoesNotMixOwners(t *testing.T) {
	s := newMainLifecycleTestStorage(t)
	e := seedHealthResolution(t, s)
	oldName := e.GetFolder()
	current, _ := s.Get(e.InfoHash)
	current.Name, current.OriginalFilename = "moved-original-owner", "moved-original-owner"
	if err := s.AddOrUpdateDurable(current); err != nil {
		t.Fatal(err)
	}
	other := entryItemTransactionEntry("new-health-owner", oldName, "bad.mkv")
	other.OriginalFilename, other.Protocol = oldName, config.ProtocolNZB
	other.Files["bad.mkv"].Size = 100
	if err := s.AddOrUpdateDurable(other); err != nil {
		t.Fatal(err)
	}
	key, err := s.RecordFailedReadAt(oldName, other.Protocol, "read_failure:timeout", FailedReadRange{InfoHash: other.InfoHash, FileName: "bad.mkv", Offset: 40, Length: 20})
	h, healthErr := s.GetEntryHealth(oldName)
	_, resolveErr := s.ResolveHealthEntry(oldName, config.Get().FolderNaming)
	t.Logf("record key=%q err=%v health=%+v healthErr=%v resolution=%v", key, err, h, healthErr, resolveErr)
	if err != nil || healthErr != nil || resolveErr != nil || key != oldName || h == nil || len(h.FailedReads) != 1 || h.OwnerInfoHash != other.InfoHash || h.FailedReads[0].InfoHash != other.InfoHash {
		t.Fatal("new owner's exact failure was lost, mixed, or stranded")
	}
	previous, err := s.GetEntryHealth(current.GetFolder())
	if err != nil || !previous.Dirty || len(previous.FailedReads) != 2 || previous.OwnerInfoHash != e.InfoHash {
		t.Fatalf("reused name lost the previous owner's proof: %+v %v", previous, err)
	}
}

func TestHealthHandoffSharedFolderReplacementCanBeExplicitlyRetired(t *testing.T) {
	s := newMainLifecycleTestStorage(t)
	e := seedHealthResolution(t, s)
	item, _ := s.GetEntryItem(e.GetFolder())
	if _, err := s.RetireEntryItemFile(item.Name, "bad.mkv", item.Files["bad.mkv"]); err != nil {
		t.Fatal(err)
	}
	other := entryItemTransactionEntry("shared-folder-replacement-owner", e.GetFolder(), "bad.mkv")
	other.OriginalFilename, other.Protocol = e.GetFolder(), config.ProtocolNZB
	other.Files["bad.mkv"].Size = 120
	other.Files["bad.mkv"].AddedOn = time.Now().Add(time.Second)
	if err := s.AddOrUpdateDurable(other); err != nil {
		t.Fatal(err)
	}
	item, _ = s.GetEntryItem(e.GetFolder())
	replacement := item.Files["bad.mkv"]
	if replacement == nil || replacement.InfoHash != other.InfoHash || replacement.Deleted {
		t.Fatalf("supported shared-folder producer did not expose replacement: %+v", item)
	}
	if good := item.Files["good.mkv"]; good == nil || good.InfoHash != e.InfoHash || good.Deleted {
		t.Fatalf("other original file not retained: %+v", item)
	}
	_, err := s.RetireEntryItemFile(item.Name, "bad.mkv", replacement)
	t.Logf("visible replacement owner=%s id=%s size=%d deleted=%t; fresh exact retirement error=%v", replacement.InfoHash, replacement.ID, replacement.Size, replacement.Deleted, err)
	if err != nil {
		t.Fatal("previous owner's filename receipt permanently blocks explicit retirement of the current shared-folder file")
	}
}

func TestHealthHandoffOwnerIndexUpgradesPriorMetadata(t *testing.T) {
	path := t.TempDir()
	prior, err := NewStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	e := seedHealthResolution(t, prior)
	h, _ := prior.GetEntryHealth(e.GetFolder())
	data, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	// Prior PR heads had the owner in JSON but only status in index metadata.
	if err := prior.repairState.Put(e.GetFolder(), data, &hybrid.EntryMeta{Status: string(h.Status)}); err != nil {
		t.Fatal(err)
	}
	if err := prior.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := NewStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.entryHealthMu.Lock()
	err = s.ensureHealthIdentityIndexLocked()
	s.entryHealthMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	indexed, err := s.GetEntryHealth(e.GetFolder())
	if err != nil || indexed.DirtyRevision != h.DirtyRevision || len(indexed.FailedReads) != 2 || len(s.repairState.FilterByCategory(healthOwnerCategory(e.InfoHash))) != 1 {
		t.Fatalf("index backfill changed or missed proof: %+v %v", indexed, err)
	}
	current, _ := s.Get(e.InfoHash)
	current.Name, current.OriginalFilename = "after-index-upgrade", "after-index-upgrade"
	if err := s.AddOrUpdateDurable(current); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveHealthEntry(current.GetFolder(), config.Get().FolderNaming); err != nil {
		t.Fatal(err)
	}
	indexed, err = s.GetEntryHealth(current.GetFolder())
	if err != nil || !indexed.Dirty || len(indexed.FailedReads) != 2 {
		t.Fatalf("upgraded owner evidence was not handed off: %+v %v", indexed, err)
	}
}

func TestHealthHandoffSharedLegacyWarningRejectsUnprovableRename(t *testing.T) {
	s := newMainLifecycleTestStorage(t)
	e := healthResolutionEntry()
	if err := s.AddOrUpdateDurable(e); err != nil {
		t.Fatal(err)
	}
	other := entryItemTransactionEntry("shared-health-owner", e.Name, "another.mkv")
	other.OriginalFilename, other.Protocol = e.OriginalFilename, e.Protocol
	if err := s.AddOrUpdateDurable(other); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveEntryHealth(&EntryHealth{EntryName: e.GetFolder(), Dirty: true, DirtyReason: "read_failure:timeout", DirtyRevision: 7}); err != nil {
		t.Fatal(err)
	}
	current, _ := s.Get(e.InfoHash)
	current.Name, current.OriginalFilename = "unproven-shared-rename", "unproven-shared-rename"
	if err := s.AddOrUpdateDurable(current); !errors.Is(err, ErrHealthIdentity) {
		t.Fatalf("ambiguous legacy proof allowed a rename: %v", err)
	}
	unchanged, err := s.Get(e.InfoHash)
	if err != nil || unchanged.GetFolder() != e.GetFolder() {
		t.Fatalf("rejected rename changed the authoritative row: %+v %v", unchanged, err)
	}
	h, err := s.GetEntryHealth(e.GetFolder())
	if err != nil || !h.Dirty || !missingLegacyReadEvidence(h) {
		t.Fatalf("rejected rename lost legacy uncertainty: %+v %v", h, err)
	}
}
