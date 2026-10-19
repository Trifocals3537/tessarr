package storage

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// os.Exit deliberately bypasses Close and all deferred syncs. This models a
// process crash, not a power-loss/filesystem simulator. Failed updates must
// leave the durable queue unchanged; after a successful revocation barrier,
// even reverted evidence must not revive the proof on reopen.
func TestHandoffCrashBarrier(t *testing.T) {
	if path := os.Getenv("HANDOFF_CRASH_DB"); path != "" {
		s, err := NewStorage(path)
		if err != nil {
			t.Fatal(err)
		}
		e := handoffTestEntry()
		if err = s.AddQueue(e); err != nil {
			t.Fatal(err)
		}
		if err = s.queue.Sync(); err != nil {
			t.Fatal(err)
		}
		if err = s.AuthorizeUncachedHandoff(e); err != nil {
			t.Fatal(err)
		}
		fail := errors.New("crash-test sync failure")
		s.handoffSyncForTest = func() error { return fail }
		for _, progress := range []float64{.25, .5, 0} {
			e.Progress = progress
			if err = s.UpdateQueueExisting(e); !errors.Is(err, fail) {
				t.Fatalf("failed update bypassed barrier: %v", err)
			}
		}
		if err = s.AuthorizeUncachedHandoff(e); !errors.Is(err, fail) {
			t.Fatal(err)
		}
		if os.Getenv("HANDOFF_CRASH_PHASE") == "after" {
			s.handoffSyncForTest = nil
			e.Progress = .75
			if err = s.UpdateQueueExisting(e); err != nil {
				t.Fatal(err)
			}
			e.Progress = 0
			if err = s.UpdateQueueExisting(e); err != nil {
				t.Fatal(err)
			}
			if err = s.queue.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		os.Exit(0)
	}
	for _, phase := range []string{"before", "after"} {
		t.Run(phase, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			cmd := exec.Command(os.Args[0], "-test.run=^TestHandoffCrashBarrier$")
			cmd.Env = append(os.Environ(), "HANDOFF_CRASH_DB="+path, "HANDOFF_CRASH_PHASE="+phase)
			if output, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("child: %v\n%s", err, output)
			}
			s, err := NewStorage(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			e, err := s.GetQueued("handoff")
			if err != nil {
				t.Fatal(err)
			}
			if e.Progress != 0 {
				t.Fatalf("failed evidence write became durable: %v", e.Progress)
			}
			if phase == "after" {
				intent, err := s.PrepareQBitQueuedDeletion(e.InfoHash, true)
				if err != nil {
					t.Fatal(err)
				}
				if intent.PlacementCleanupPending {
					t.Fatal("crash resurrected revoked proof")
				}
			}
		})
	}
}
