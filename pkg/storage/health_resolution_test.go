package storage

import (
	"errors"
	"os"
	"os/exec"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func healthResolutionEntry() *Entry {
	e := entryItemTransactionEntry("health-owner", "release", "bad.mkv")
	e.OriginalFilename = e.Name
	e.Protocol = config.ProtocolNZB
	e.Files["bad.mkv"].Size = 100
	e.Files["good.mkv"] = &File{InfoHash: e.InfoHash, Name: "good.mkv", Size: 100}
	return e
}

func seedHealthResolution(t *testing.T, s *Storage) *Entry {
	t.Helper()
	e := healthResolutionEntry()
	if err := s.AddOrUpdateDurable(e); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"bad.mkv", "good.mkv"} {
		if err := s.RecordFailedRead(e.GetFolder(), e.Protocol, "read_failure:timeout", FailedReadRange{InfoHash: e.InfoHash, FileName: name, Offset: 10, Length: 20}); err != nil {
			t.Fatal(err)
		}
	}
	return e
}

func TestHealthClearProtectsEvidenceAndRevisionFence(t *testing.T) {
	s := newMainLifecycleTestStorage(t)
	for _, h := range []*EntryHealth{
		{EntryName: "failed", FailedReads: []FailedReadRange{{InfoHash: "h", FileName: "f", Length: 1}}},
		{EntryName: "legacy", FailedReadsIncomplete: true},
		{EntryName: "progress", ProbeProgress: &HealthProbeProgress{}},
		{EntryName: "active", ActiveRunID: "run"},
		{EntryName: "dirty", Dirty: true},
		{EntryName: "broken", BrokenFiles: []BrokenFile{{InfoHash: "h"}}},
	} {
		h.Status = HealthUnknown
		if err := s.SaveEntryHealth(h); err != nil {
			t.Fatal(err)
		}
	}
	stale := &EntryHealth{EntryName: "display", Status: HealthUnknown, DirtyRevision: 200}
	if err := s.SaveEntryHealth(stale); err != nil {
		t.Fatal(err)
	}
	if count, err := s.ClearEntryHealthByStatuses([]HealthStatus{HealthUnknown}); err != nil || count != 1 {
		t.Fatalf("clear count=%d err=%v", count, err)
	}
	f := FailedReadRange{InfoHash: "h", FileName: "f", Length: 1}
	if err := s.RecordFailedRead("display", config.ProtocolNZB, "read_failure:timeout", f); err != nil {
		t.Fatal(err)
	}
	stale.Status, stale.Dirty, stale.FailedReads = HealthHealthy, false, nil
	if err := s.SaveEntryHealth(stale); err != nil {
		t.Fatal(err)
	}
	h, _ := s.GetEntryHealth("display")
	if !h.Dirty || h.Status != HealthUnknown || len(h.FailedReads) != 1 || h.DirtyRevision <= 200 {
		t.Fatalf("stale pre-clear probe erased new evidence: %+v", h)
	}
	if count, err := s.ClearEntryHealthByStatuses([]HealthStatus{HealthUnknown}); err != nil || count != 0 {
		t.Fatalf("protected state cleared: count=%d err=%v", count, err)
	}
}

func TestHealthFileRetirementIsExactAndPreservesOtherEvidence(t *testing.T) {
	s := newMainLifecycleTestStorage(t)
	e := seedHealthResolution(t, s)
	item, _ := s.GetEntryItem(e.GetFolder())
	expected := *item.Files["bad.mkv"]
	wrong := expected
	wrong.ID = "replacement"
	if _, err := s.RetireEntryItemFile(item.Name, "bad.mkv", &wrong); !errors.Is(err, ErrHealthRetirementIdentity) {
		t.Fatalf("changed file accepted: %v", err)
	}
	stale, _ := s.GetEntryHealth(item.Name)
	if _, err := s.RetireEntryItemFile(item.Name, "bad.mkv", &expected); err != nil {
		t.Fatal(err)
	}
	// Delayed feedback from the retired incarnation must not resurrect it.
	key, err := s.RecordFailedReadAt(item.Name, e.Protocol, "read_failure:timeout", FailedReadRange{InfoHash: e.InfoHash, FileName: "bad.mkv", FileID: expected.ID, Offset: 30, Length: 1})
	if err != nil || key != "" {
		t.Fatalf("retired feedback requeued: %q %v", key, err)
	}
	if err := s.RecordFailedRead(item.Name, e.Protocol, "read_failure:timeout", FailedReadRange{InfoHash: e.InfoHash, FileName: "good.mkv", Offset: 70, Length: 1}); err != nil {
		t.Fatal(err)
	}
	stale.Status, stale.Dirty, stale.FailedReads = HealthHealthy, false, nil
	if err := s.SaveEntryHealth(stale); err != nil {
		t.Fatal(err)
	}
	h, _ := s.GetEntryHealth(item.Name)
	if !h.Dirty || len(h.FailedReads) != 2 || !h.FileRetirements["bad.mkv"].Confirmed {
		t.Fatalf("lost unrelated/newer evidence or receipt: %+v", h)
	}
	for _, f := range h.FailedReads {
		if f.FileName != "good.mkv" {
			t.Fatalf("retired range remained: %+v", f)
		}
	}
	item, _ = s.GetEntryItem(item.Name)
	if !item.Files["bad.mkv"].Deleted || item.Files["good.mkv"].Deleted {
		t.Fatalf("retirement affected another file: %+v", item)
	}
}

func TestHealthFileRetirementCrashRecovery(t *testing.T) {
	for _, phase := range []string{"intent", "index", "ack"} {
		t.Run(phase, func(t *testing.T) {
			path := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestHealthResolutionCrashHelper$")
			cmd.Env = append(os.Environ(), "TESSARR_RESOLUTION_PATH="+path, "TESSARR_RESOLUTION_PHASE="+phase)
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
			name := healthResolutionEntry().GetFolder()
			item, err := s.GetEntryItem(name)
			if err != nil || !item.Files["bad.mkv"].Deleted || item.Files["good.mkv"].Deleted {
				t.Fatalf("wrong recovered retirement: %+v %v", item, err)
			}
			h, err := s.GetEntryHealth(name)
			if err != nil || !h.Dirty || len(h.FailedReads) != 1 || h.FailedReads[0].FileName != "good.mkv" || !h.FileRetirements["bad.mkv"].Confirmed {
				t.Fatalf("wrong recovered evidence: %+v %v", h, err)
			}
			before := h.DirtyRevision
			if changed, err := s.ReconcileHealthFileRetirements(name); err != nil || changed {
				t.Fatalf("acknowledged intent replayed: %t %v", changed, err)
			}
			h, _ = s.GetEntryHealth(name)
			if h.DirtyRevision != before {
				t.Fatal("replay duplicated the retirement")
			}
		})
	}
}

func TestHealthPendingRetirementCannotDeleteReplacement(t *testing.T) {
	s := newMainLifecycleTestStorage(t)
	e := seedHealthResolution(t, s)
	item, _ := s.GetEntryItem(e.GetFolder())
	// Stop after the durable intent, before publishing its index tombstone.
	stopped := errors.New("stop at intent")
	s.healthRetirementForTest = func(phase string) {
		if phase == "intent" {
			panic(stopped)
		}
	}
	func() {
		defer func() {
			if got := recover(); got != stopped {
				t.Fatalf("unexpected interruption: %v", got)
			}
		}()
		_, _ = s.RetireEntryItemFile(item.Name, "bad.mkv", item.Files["bad.mkv"])
	}()
	s.healthRetirementForTest = nil
	item, _ = s.GetEntryItem(item.Name)
	item.Files["bad.mkv"].ID = "replacement-incarnation"
	if err := s.UpdateItem(item); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReconcileHealthFileRetirements(item.Name); !errors.Is(err, ErrHealthRetirementIdentity) {
		t.Fatalf("pending receipt accepted replacement: %v", err)
	}
	item, _ = s.GetEntryItem(item.Name)
	h, _ := s.GetEntryHealth(item.Name)
	if item.Files["bad.mkv"].Deleted || len(h.FailedReads) != 2 || h.FileRetirements["bad.mkv"].Confirmed {
		t.Fatalf("replacement deleted or evidence acknowledged: %+v %+v", item, h)
	}
}

func TestHealthFileRetirementRespectsMainDeletionFence(t *testing.T) {
	s := newMainLifecycleTestStorage(t)
	e := seedHealthResolution(t, s)
	item, _ := s.GetEntryItem(e.GetFolder())
	blocked := errors.New("operator cleanup still pending")
	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- s.DeleteWithCleanup(e.InfoHash, func(*Entry) error {
			close(started)
			<-release
			return blocked
		})
	}()
	<-started
	_, retireErr := s.RetireEntryItemFile(item.Name, "bad.mkv", item.Files["bad.mkv"])
	h, _ := s.GetEntryHealth(item.Name)
	close(release)
	if err := <-done; !errors.Is(err, blocked) {
		t.Fatalf("deletion fixture: %v", err)
	}
	if !errors.Is(retireErr, ErrEntryDeleting) {
		t.Fatalf("retirement bypassed main deletion: %v", retireErr)
	}
	if len(h.FailedReads) != 2 || len(h.FileRetirements) != 0 {
		t.Fatalf("fenced retirement changed evidence: %+v", h)
	}
}

func TestHealthResolutionCrashHelper(t *testing.T) {
	path := os.Getenv("TESSARR_RESOLUTION_PATH")
	if path == "" {
		return
	}
	s, err := NewStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	e := seedHealthResolution(t, s)
	item, _ := s.GetEntryItem(e.GetFolder())
	s.healthRetirementForTest = func(phase string) {
		if phase == os.Getenv("TESSARR_RESOLUTION_PHASE") {
			os.Exit(84) // No deferred Close or final sync.
		}
	}
	if _, err := s.RetireEntryItemFile(item.Name, "bad.mkv", item.Files["bad.mkv"]); err != nil {
		t.Fatal(err)
	}
	t.Fatal("expected crash boundary")
}

func TestHealthCanonicalRenameBackFencesStaleProbe(t *testing.T) {
	s := newMainLifecycleTestStorage(t)
	e := seedHealthResolution(t, s)
	oldName := e.GetFolder()
	stale, _ := s.GetEntryHealth(oldName)
	oldRevision := stale.DirtyRevision
	for _, name := range []string{"renamed", oldName} {
		current, err := s.Get(e.InfoHash)
		if err != nil {
			t.Fatal(err)
		}
		current.Name, current.OriginalFilename = name, name
		if err := s.AddOrUpdateDurable(current); err != nil {
			t.Fatal(err)
		}
		// Resolve using the previous canonical key, as a persisted queue does.
		item, err := s.ResolveHealthEntry(oldName, config.Get().FolderNaming)
		if err != nil || item.Name != current.GetFolder() {
			t.Fatalf("rename resolution: %+v %v", item, err)
		}
	}
	if err := s.RecordFailedRead(oldName, e.Protocol, "read_failure:timeout", FailedReadRange{InfoHash: e.InfoHash, FileName: "good.mkv", Offset: 70, Length: 1}); err != nil {
		t.Fatal(err)
	}
	stale.Status, stale.Dirty, stale.FailedReads = HealthHealthy, false, nil
	if err := s.SaveEntryHealth(stale); err != nil {
		t.Fatal(err)
	}
	h, _ := s.GetEntryHealth(oldName)
	if !h.Dirty || len(h.FailedReads) != 3 || h.DirtyRevision <= oldRevision {
		t.Fatalf("rename-back lost evidence: %+v", h)
	}
}

func TestHealthOwnedAliasCrashRecovery(t *testing.T) {
	for _, phase := range []string{"target", "source", "alias"} {
		t.Run(phase, func(t *testing.T) {
			path := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestHealthOwnedAliasCrashHelper$")
			cmd.Env = append(os.Environ(), "TESSARR_OWNED_ALIAS_PATH="+path, "TESSARR_OWNED_ALIAS_PHASE="+phase)
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
			item, err := s.ResolveHealthEntry(healthResolutionEntry().GetFolder(), config.Get().FolderNaming)
			if err != nil || item.Name != "final" {
				t.Fatalf("alias recovery: %+v %v", item, err)
			}
			h, err := s.GetEntryHealth(item.Name)
			if err != nil || !h.Dirty || len(h.FailedReads) != 2 || h.OwnerInfoHash != "health-owner" {
				t.Fatalf("recovered evidence: %+v %v", h, err)
			}
			before := h.DirtyRevision
			if _, err := s.ResolveHealthEntry(healthResolutionEntry().GetFolder(), config.Get().FolderNaming); err != nil {
				t.Fatal(err)
			}
			h, _ = s.GetEntryHealth(item.Name)
			if h.DirtyRevision != before {
				t.Fatal("alias replay duplicated evidence")
			}
		})
	}
}

func TestHealthOwnedAliasCrashHelper(t *testing.T) {
	path := os.Getenv("TESSARR_OWNED_ALIAS_PATH")
	if path == "" {
		return
	}
	s, err := NewStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	e := seedHealthResolution(t, s)
	oldName := e.GetFolder()
	for _, name := range []string{"middle", "final"} {
		current, err := s.Get(e.InfoHash)
		if err != nil {
			t.Fatal(err)
		}
		current.Name, current.OriginalFilename = name, name
		if err := s.AddOrUpdateDurable(current); err != nil {
			t.Fatal(err)
		}
		if name == "final" {
			s.healthAliasPhaseForTest = func(phase string) {
				if phase == os.Getenv("TESSARR_OWNED_ALIAS_PHASE") {
					os.Exit(84)
				}
			}
		}
		if _, err := s.ResolveHealthEntry(oldName, config.Get().FolderNaming); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("expected crash boundary")
}
