package storage

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func TestFailedReadEvidenceSurvivesStaleSaveAndDeduplicates(t *testing.T) {
	s := newMainLifecycleTestStorage(t)
	f := FailedReadRange{InfoHash: "owner", FileName: "video.mkv", Offset: 1024, Length: 4096}
	if err := s.RecordFailedRead("release", config.ProtocolNZB, "read_failure:timeout", f); err != nil {
		t.Fatal(err)
	}
	stale, _ := s.GetEntryHealth("release")
	if err := s.RecordFailedRead("release", config.ProtocolNZB, "read_failure:timeout", f); err != nil {
		t.Fatal(err)
	}
	newRange := f
	newRange.Offset = 8192
	if err := s.RecordFailedRead("release", config.ProtocolNZB, "read_failure:content_missing", newRange); err != nil {
		t.Fatal(err)
	}
	stale.Dirty = false
	stale.FailedReads = nil
	stale.ProbeProgress = &HealthProbeProgress{Verified: map[string]string{"video.mkv": "old"}}
	if err := s.SaveEntryHealth(stale); err != nil {
		t.Fatal(err)
	}
	h, _ := s.GetEntryHealth("release")
	if !h.Dirty || h.DirtyRevision != 3 || len(h.FailedReads) != 2 || h.ProbeProgress != nil {
		t.Fatalf("lost evidence: %+v", h)
	}
}

func TestFailedReadEvidenceOverflowAndInvalidRangeStayIncomplete(t *testing.T) {
	s := newMainLifecycleTestStorage(t)
	for i := 0; i <= maxFailedReadRanges; i++ {
		if err := s.RecordFailedRead("release", config.ProtocolNZB, "read_failure:timeout", FailedReadRange{InfoHash: "hash", FileName: "file", Offset: int64(i), Length: 1}); err != nil {
			t.Fatal(err)
		}
	}
	h, _ := s.GetEntryHealth("release")
	if len(h.FailedReads) != maxFailedReadRanges || !h.FailedReadsIncomplete {
		t.Fatalf("unbounded/lost evidence: %+v", h)
	}
	for _, f := range []FailedReadRange{{}, {InfoHash: "h", FileName: "f", Offset: -1, Length: 1}, {InfoHash: "h", FileName: "f", Offset: 1<<63 - 1, Length: 1}} {
		if err := s.RecordFailedRead("invalid", config.ProtocolNZB, "read_failure:timeout", f); err != nil {
			t.Fatal(err)
		}
	}
	h, _ = s.GetEntryHealth("invalid")
	if !h.FailedReadsIncomplete || len(h.FailedReads) != 0 {
		t.Fatalf("invalid evidence accepted: %+v", h)
	}
}

func TestHealthAliasCrashReplayAndForwarding(t *testing.T) {
	for _, phase := range []string{"range", "handoff", "alias", "progress"} {
		t.Run(phase, func(t *testing.T) {
			path := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestHealthEvidenceCrashHelper$")
			cmd.Env = append(os.Environ(), "TESSARR_HEALTH_CRASH_PATH="+path, "TESSARR_HEALTH_CRASH_PHASE="+phase)
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
			if phase == "range" {
				h, err := s.GetEntryHealth("alias")
				if err != nil || !h.Dirty || len(h.FailedReads) != 1 {
					t.Fatalf("range lost: %+v %v", h, err)
				}
				return
			}
			if phase == "progress" {
				h, err := s.GetEntryHealth("alias")
				if err != nil || h.ProbeProgress == nil || h.ProbeProgress.Verified["file"] != "target" {
					t.Fatalf("progress lost: %+v %v", h, err)
				}
				return
			}
			before, _ := s.GetEntryHealth("canonical")
			if err := s.BindHealthAlias("alias", "canonical"); err != nil {
				t.Fatal(err)
			}
			h, _ := s.GetEntryHealth("alias")
			if h.EntryName != "canonical" || !h.Dirty || len(h.FailedReads) != 1 || h.DirtyRevision != before.DirtyRevision {
				t.Fatalf("handoff replay lost/duplicated evidence: %+v", h)
			}
			if err := s.RecordFailedRead("alias", config.ProtocolNZB, "read_failure:timeout", FailedReadRange{InfoHash: "hash", FileName: "file", Offset: 20, Length: 1}); err != nil {
				t.Fatal(err)
			}
			h, _ = s.GetEntryHealth("canonical")
			if h.DirtyRevision != before.DirtyRevision+1 || len(h.FailedReads) != 2 {
				t.Fatalf("forward lost: %+v", h)
			}
			count := 0
			_ = s.ForEachEntryHealth(func(*EntryHealth) error { count++; return nil })
			if count != 1 {
				t.Fatalf("alias double counted: %d", count)
			}
		})
	}
}

func TestHealthEvidenceCrashHelper(t *testing.T) {
	path := os.Getenv("TESSARR_HEALTH_CRASH_PATH")
	if path == "" {
		return
	}
	s, err := NewStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordFailedRead("alias", config.ProtocolNZB, "read_failure:timeout", FailedReadRange{InfoHash: "hash", FileName: "file", Offset: 10, Length: 1}); err != nil {
		t.Fatal(err)
	}
	switch os.Getenv("TESSARR_HEALTH_CRASH_PHASE") {
	case "range":
	case "handoff":
		s.healthAliasSyncedForTest = func() { os.Exit(84) }
		if err := s.BindHealthAlias("alias", "canonical"); err != nil {
			t.Fatal(err)
		}
	case "alias":
		if err := s.BindHealthAlias("alias", "canonical"); err != nil {
			t.Fatal(err)
		}
	case "progress":
		h, _ := s.GetEntryHealth("alias")
		h.ProbeProgress = &HealthProbeProgress{Revision: h.DirtyRevision, Fingerprint: "files", Verified: map[string]string{"file": "target"}}
		if err := s.SaveEntryHealth(h); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal(fmt.Errorf("unknown crash phase"))
	}
	os.Exit(84) // No Close/final sync may hide a missing barrier.
}
