package storage

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
)

func crashHealthHandoff(t *testing.T, path, mode, phase string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestHealthHandoffCrashHelper$")
	cmd.Env = append(os.Environ(), "TESSARR_HANDOFF_PATH="+path, "TESSARR_HANDOFF_MODE="+mode, "TESSARR_HANDOFF_PHASE="+phase)
	out, err := cmd.CombinedOutput()
	var exited *exec.ExitError
	if !errors.As(err, &exited) || exited.ExitCode() != 84 {
		t.Fatalf("crash helper: %v %s", err, out)
	}
}

func TestHealthHandoffRenameSurvivesProducerCrash(t *testing.T) {
	for _, mode := range []string{"ranges", "legacy"} {
		for _, phase := range []string{"prepared", "index", "main"} {
			t.Run(mode+"/"+phase, func(t *testing.T) {
				path := t.TempDir()
				crashHealthHandoff(t, path, mode, phase)
				s, err := NewStorage(path)
				if err != nil {
					t.Fatal(err)
				}
				defer s.Close()
				e, err := s.Get("health-owner")
				if err != nil {
					t.Fatal(err)
				}
				want := "release"
				if phase == "main" {
					want = "renamed"
				}
				if e.GetFolder() != want {
					t.Fatalf("recovery used derived index instead of main owner: %+v", e)
				}
				if _, err := s.ResolveHealthEntry(e.GetFolder(), config.Get().FolderNaming); err != nil {
					t.Fatal(err)
				}
				h, err := s.GetEntryHealth(e.GetFolder())
				if err != nil || !h.Dirty || h.OwnerInfoHash != e.InfoHash {
					t.Fatalf("lost warning ownership: %+v %v", h, err)
				}
				if mode == "ranges" && len(h.FailedReads) != 2 || mode == "legacy" && (!h.FailedReadsIncomplete || len(h.FailedReads) != 0) {
					t.Fatalf("lost read evidence: %+v", h)
				}
				before := h.DirtyRevision
				if _, err := s.ResolveHealthEntry(e.GetFolder(), config.Get().FolderNaming); err != nil {
					t.Fatal(err)
				}
				h, _ = s.GetEntryHealth(e.GetFolder())
				if h.DirtyRevision != before {
					t.Fatal("repeated resolution duplicated a handoff")
				}
			})
		}
	}
}

func TestHealthHandoffReusedCanonicalCrashKeepsOwnersSeparate(t *testing.T) {
	for _, phase := range []string{"target", "source", "alias"} {
		t.Run(phase, func(t *testing.T) {
			path := t.TempDir()
			crashHealthHandoff(t, path, "reuse", phase)
			s, err := NewStorage(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			key, err := s.RecordFailedReadAt("release", config.ProtocolNZB, "read_failure:timeout", FailedReadRange{InfoHash: "new-health-owner", FileName: "bad.mkv", Offset: 40, Length: 20})
			if err != nil || key != "release" {
				t.Fatalf("recovery intake: %q %v", key, err)
			}
			old, err := s.GetEntryHealth("renamed")
			if err != nil || !old.Dirty || len(old.FailedReads) != 2 || old.OwnerInfoHash != "health-owner" {
				t.Fatalf("lost previous owner's evidence: %+v %v", old, err)
			}
			h, err := s.GetEntryHealth("release")
			if err != nil || !h.Dirty || len(h.FailedReads) != 1 || h.FailedReads[0].InfoHash != "new-health-owner" || h.OwnerInfoHash != "new-health-owner" {
				t.Fatalf("mixed or lost current owner's evidence: %+v %v", h, err)
			}
			// A stale old-owner save cannot contaminate the reused key even if
			// it carries a larger revision than the new owner's current work.
			if err := s.SaveEntryHealth(&EntryHealth{EntryName: "release", OwnerInfoHash: "health-owner", DirtyRevision: h.DirtyRevision + 10, Status: HealthHealthy}); !errors.Is(err, ErrHealthIdentity) {
				t.Fatalf("stale owner save accepted: %v", err)
			}
		})
	}
}

func TestHealthHandoffReplacementCrashKeepsHistoricalReceipt(t *testing.T) {
	for _, phase := range []string{"intent", "index", "ack"} {
		t.Run(phase, func(t *testing.T) {
			path := t.TempDir()
			crashHealthHandoff(t, path, "replacement", phase)
			s, err := NewStorage(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			item, err := s.GetEntryItem("release")
			if err != nil || !item.Files["bad.mkv"].Deleted || item.Files["good.mkv"].Deleted {
				t.Fatalf("wrong replacement recovered: %+v %v", item, err)
			}
			h, err := s.GetEntryHealth("release")
			if err != nil || !h.Dirty || !h.FileRetirements["bad.mkv"].Confirmed || h.FileRetirements["bad.mkv"].InfoHash != "new-health-owner" {
				t.Fatalf("lost current receipt: %+v %v", h, err)
			}
			if len(h.FileRetirementHistory["bad.mkv"]) != 1 || !h.FileRetirementHistory["bad.mkv"][0].Confirmed || h.FileRetirementHistory["bad.mkv"][0].InfoHash != "health-owner" {
				t.Fatalf("lost old confirmed receipt: %+v", h.FileRetirementHistory)
			}
			old := h.FileRetirementHistory["bad.mkv"][0]
			key, err := s.RecordFailedReadAt("release", config.ProtocolNZB, "read_failure:timeout", FailedReadRange{InfoHash: old.InfoHash, FileID: old.FileID, FileName: "bad.mkv", Offset: 50, Length: 1})
			if err != nil || key != "" {
				t.Fatalf("historical retired read dirtied replacement: %q %v", key, err)
			}
			if _, err := s.RetireEntryItemFile("release", "bad.mkv", &File{InfoHash: old.InfoHash, ID: old.FileID, Size: old.Size}); !errors.Is(err, ErrHealthRetirementIdentity) {
				t.Fatalf("old authorization accepted against replacement: %v", err)
			}
			if len(h.FailedReads) != 1 || h.FailedReads[0].FileName != "good.mkv" {
				t.Fatalf("unrelated evidence changed: %+v", h.FailedReads)
			}
		})
	}
}

func TestHealthHandoffCrashHelper(t *testing.T) {
	path := os.Getenv("TESSARR_HANDOFF_PATH")
	if path == "" {
		return
	}
	s, err := NewStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	e := seedHealthResolution(t, s)
	mode, phase := os.Getenv("TESSARR_HANDOFF_MODE"), os.Getenv("TESSARR_HANDOFF_PHASE")
	switch mode {
	case "legacy", "ranges":
		if mode == "legacy" {
			// Replace the initial bound fixture with an actual pre-upgrade row.
			if err := s.repairState.Delete("release"); err != nil {
				t.Fatal(err)
			}
			if err := s.SaveEntryHealth(&EntryHealth{EntryName: "release", Dirty: true, DirtyReason: "read_failure:timeout", DirtyRevision: 7}); err != nil {
				t.Fatal(err)
			}
		}
		s.healthRenameForTest = func(at string) {
			if at == phase {
				if err := s.entryItems.Sync(); err != nil {
					t.Fatal(err)
				}
				if err := s.entries.Sync(); err != nil {
					t.Fatal(err)
				}
				os.Exit(84)
			}
		}
		current, _ := s.Get(e.InfoHash)
		current.Name, current.OriginalFilename = "renamed", "renamed"
		if err := s.AddOrUpdateDurable(current); err != nil {
			t.Fatal(err)
		}
	case "reuse":
		current, _ := s.Get(e.InfoHash)
		current.Name, current.OriginalFilename = "renamed", "renamed"
		if err := s.AddOrUpdateDurable(current); err != nil {
			t.Fatal(err)
		}
		other := entryItemTransactionEntry("new-health-owner", "release", "bad.mkv")
		other.OriginalFilename, other.Protocol = "release", config.ProtocolNZB
		other.Files["bad.mkv"].Size = 100
		if err := s.AddOrUpdateDurable(other); err != nil {
			t.Fatal(err)
		}
		s.healthAliasPhaseForTest = func(at string) {
			if at == phase {
				os.Exit(84)
			}
		}
		if _, err := s.RecordFailedReadAt("release", other.Protocol, "read_failure:timeout", FailedReadRange{InfoHash: other.InfoHash, FileName: "bad.mkv", Offset: 40, Length: 20}); err != nil {
			t.Fatal(err)
		}
	case "replacement":
		item, _ := s.GetEntryItem("release")
		oldFile := *item.Files["bad.mkv"]
		if _, err := s.RetireEntryItemFile("release", "bad.mkv", &oldFile); err != nil {
			t.Fatal(err)
		}
		other := entryItemTransactionEntry("new-health-owner", "release", "bad.mkv")
		other.OriginalFilename, other.Protocol = "release", config.ProtocolNZB
		other.Files["bad.mkv"].Size = 120
		other.Files["bad.mkv"].AddedOn = time.Now().Add(time.Second)
		if err := s.AddOrUpdateDurable(other); err != nil {
			t.Fatal(err)
		}
		if key, err := s.RecordFailedReadAt("release", e.Protocol, "read_failure:timeout", FailedReadRange{InfoHash: e.InfoHash, FileID: oldFile.ID, FileName: "bad.mkv", Offset: 50, Length: 1}); err != nil || key != "" {
			t.Fatalf("retired read dirtied visible replacement: %q %v", key, err)
		}
		s.healthRetirementForTest = func(at string) {
			if at == phase {
				os.Exit(84)
			}
		}
		item, _ = s.GetEntryItem("release")
		if _, err := s.RetireEntryItemFile("release", "bad.mkv", item.Files["bad.mkv"]); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unknown handoff crash mode %q", mode)
	}
	t.Fatal("expected crash boundary without Close")
}

func TestHealthHandoffRuntimeOnlyRenamePreservesLegacyWarning(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestHealthHandoffRuntimeOnlyRenameHelper$", "-test.v")
	cmd.Env = append(os.Environ(), "TESSARR_HANDOFF_RUNTIME_ONLY=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("runtime-only rename: %v %s", err, out)
	}
}

func TestHealthHandoffRuntimeOnlyRenameHelper(t *testing.T) {
	if os.Getenv("TESSARR_HANDOFF_RUNTIME_ONLY") != "1" {
		return
	}
	defer logger.Close()
	root := t.TempDir()
	if err := config.SetConfigPath(root); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(map[string]any{"folder_naming": "original", "debrids": []map[string]any{{"name": "realdebrid", "api_key": "fixture"}}})
	if err := os.WriteFile(filepath.Join(root, "config.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	config.Reset()
	config.Get()
	s := newMainLifecycleTestStorage(t)
	e := healthResolutionEntry()
	e.Name, e.OriginalFilename = "Runtime.Before", "Canonical.Fixed"
	if err := s.AddOrUpdateDurable(e); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveEntryHealth(&EntryHealth{EntryName: e.Name, Dirty: true, DirtyReason: "read_failure:timeout", DirtyRevision: 7}); err != nil {
		t.Fatal(err)
	}
	current, _ := s.Get(e.InfoHash)
	current.Name = "Runtime.After"
	if err := s.AddOrUpdateDurable(current); err != nil {
		t.Fatal(err)
	}
	if current.GetFolder() != e.GetFolder() {
		t.Fatal("fixture changed canonical identity")
	}
	if _, err := s.ResolveHealthEntry(current.GetFolder(), config.Get().FolderNaming); err != nil {
		t.Fatal(err)
	}
	h, err := s.GetEntryHealth(current.GetFolder())
	if err != nil || !h.Dirty || !h.FailedReadsIncomplete || h.OwnerInfoHash != e.InfoHash {
		t.Fatalf("runtime rename lost unscoped legacy warning: %+v %v", h, err)
	}
}
