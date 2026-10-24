package storage

import (
	"errors"
	"github.com/Trifocals3537/tessarr/internal/workflow"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func cleanupFixture(t *testing.T, dir string) (*Storage, *QueueDeletionIntent) {
	t.Helper()
	s, err := NewStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	e := &Entry{InfoHash: "cleanup-work", Name: "release", State: EntryStateDownloading}
	if err := s.AddQueue(e); err != nil {
		t.Fatal(err)
	}
	i, err := s.PrepareQueuedDeletionPreservingFiles(e.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	return s, i
}
func TestCleanupClaimsFenceEveryLegacyMutation(t *testing.T) {
	s, i := cleanupFixture(t, filepath.Join(t.TempDir(), "db"))
	defer s.Close()
	i, err := s.ClaimCleanup(i.InfoHash, i.QueueIncarnation, "owner", nil, time.Now(), false)
	if err != nil {
		t.Fatal(err)
	}
	for _, call := range []func() error{
		func() error { return s.StartQueuedDeletionCleanup(i.InfoHash, i.QueueIncarnation) },
		func() error { return s.MarkQueuedDeletionPlacementsClean(i.InfoHash, i.QueueIncarnation) },
		func() error { return s.RetireQueuedDeletionRow(i.InfoHash, i.QueueIncarnation) },
		func() error { return s.CompleteQueuedDeletion(i.InfoHash, i.QueueIncarnation) },
	} {
		if err := call(); !errors.Is(err, workflow.ErrClaim) {
			t.Fatalf("unclaimed mutation accepted: %v", err)
		}
	}
	if err := s.StartCleanup(i); err != nil {
		t.Fatal(err)
	}
	if err := s.RetireCleanup(i); err != nil {
		t.Fatal(err)
	}
	if err := s.ReleaseCleanup(i.InfoHash, i.QueueIncarnation, i.Work.Token, "manual_review", true, time.Now()); err != nil {
		t.Fatal(err)
	}
	rows, _ := s.QueuedDeletionIntents()
	work := rows[0].Work
	if err := s.ResolveCleanup(i.InfoHash, "replacement", work.Revision, "request", "reconcile", time.Now()); !errors.Is(err, ErrQueuedDeletionIdentityMismatch) {
		t.Fatal("replacement incarnation accepted")
	}
	if err := s.ResolveCleanup(i.InfoHash, i.QueueIncarnation, work.Revision-1, "request", "reconcile", time.Now()); !errors.Is(err, workflow.ErrClaim) {
		t.Fatal("stale revision accepted")
	}
	if err := s.ResolveCleanup(i.InfoHash, i.QueueIncarnation, work.Revision, "request", "reconcile", time.Now()); err != nil {
		t.Fatal(err)
	}
	next, err := s.ClaimCleanup(i.InfoHash, i.QueueIncarnation, "owner", nil, time.Now(), false)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	if err := s.CompleteCleanup(i, func() error { called = true; return nil }); !errors.Is(err, workflow.ErrClaim) || called {
		t.Fatalf("stale cleanup ran: %v %v", err, called)
	}
	if err := s.CompleteCleanup(next, func() error { called = true; return nil }); err != nil {
		t.Fatal(err)
	}
	receipt, err := s.CleanupReceipts(100)
	if err != nil || len(receipt) != 1 || receipt[0].IntentPresent || len(receipt[0].Work.Decisions) != 1 {
		t.Fatalf("audit lost: %+v %v", receipt, err)
	}
}

func TestCleanupWorkflowCrashHelper(t *testing.T) {
	root := os.Getenv("TESSARR_CLEANUP_CRASH_ROOT")
	if root == "" {
		return
	}
	s, i := cleanupFixture(t, root)
	i, err := s.ClaimCleanup(i.InfoHash, i.QueueIncarnation, "dead-owner", nil, time.Now(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StartCleanup(i); err != nil {
		t.Fatal(err)
	}
	if err := s.RetireCleanup(i); err != nil {
		t.Fatal(err)
	}
	if err := s.CleanupEffectStarted(i.InfoHash, i.QueueIncarnation, i.Work.Token, time.Now()); err != nil {
		t.Fatal(err)
	}
	if phase := os.Getenv("TESSARR_CLEANUP_CRASH_PHASE"); phase == "decision" {
		if err := s.ReleaseCleanup(i.InfoHash, i.QueueIncarnation, i.Work.Token, "review", true, time.Now()); err != nil {
			t.Fatal(err)
		}
		rows, _ := s.QueuedDeletionIntents()
		if err := s.ResolveCleanup(i.InfoHash, i.QueueIncarnation, rows[0].Work.Revision, "durable-decision", "reconcile", time.Now()); err != nil {
			t.Fatal(err)
		}
	} else if phase == "receipt" {
		s.cleanupReceiptForTest = func() { os.Exit(86) }
		if err := s.CompleteCleanup(i, nil); err != nil {
			t.Fatal(err)
		}
	}
	os.Exit(86)
}
func TestCleanupClaimSurvivesProcessDeathAndFencesOldWorker(t *testing.T) {
	root := filepath.Join(t.TempDir(), "db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestCleanupWorkflowCrashHelper$")
	cmd.Env = append(os.Environ(), "TESSARR_CLEANUP_CRASH_ROOT="+root)
	output, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 86 {
		t.Fatalf("child: %v %s", err, output)
	}
	s, err := NewStorage(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	rows, err := s.QueuedDeletionIntents()
	if err != nil || len(rows) != 1 {
		t.Fatal(err)
	}
	old := rows[0]
	if _, err := s.ClaimCleanup(old.InfoHash, old.QueueIncarnation, "new-owner", nil, time.Now().Add(time.Hour), false); !errors.Is(err, workflow.ErrClaim) {
		t.Fatal("time alone stole claim")
	}
	next, err := s.ClaimCleanup(old.InfoHash, old.QueueIncarnation, "new-owner", nil, time.Now(), true)
	if err != nil {
		t.Fatal(err)
	}
	if !next.Work.EffectStarted || next.Work.Attempt != 2 {
		t.Fatal("crash lost durable attempt")
	}
	if err := s.CompleteCleanup(old, nil); !errors.Is(err, workflow.ErrClaim) {
		t.Fatal("old process outcome accepted")
	}
	if err := s.CompleteCleanup(next, nil); err != nil {
		t.Fatal(err)
	}
}

func TestCleanupDecisionAndCompletionReceiptSurviveCrash(t *testing.T) {
	for _, phase := range []string{"decision", "receipt"} {
		t.Run(phase, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "db")
			cmd := exec.Command(os.Args[0], "-test.run=^TestCleanupWorkflowCrashHelper$")
			cmd.Env = append(os.Environ(), "TESSARR_CLEANUP_CRASH_ROOT="+root, "TESSARR_CLEANUP_CRASH_PHASE="+phase)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 86 {
				t.Fatalf("child: %v %s", err, output)
			}
			s, err := NewStorage(root)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			rows, err := s.QueuedDeletionIntents()
			if err != nil || len(rows) != 1 {
				t.Fatal("lost cleanup intent", err)
			}
			i := rows[0]
			if phase == "decision" && (len(i.Work.Decisions) != 1 || i.Work.Mode != "reconcile") {
				t.Fatal("operator decision lost")
			}
			if phase == "receipt" {
				receipts, err := s.CleanupReceipts(100)
				if err != nil || len(receipts) != 1 || !receipts[0].IntentPresent {
					t.Fatal("incomplete receipt asserted completion")
				}
			}
			i, err = s.ClaimCleanup(i.InfoHash, i.QueueIncarnation, "restarted-owner", nil, time.Now(), true)
			if err != nil {
				t.Fatal(err)
			}
			if err := s.CompleteCleanup(i, nil); err != nil {
				t.Fatal(err)
			}
			if phase == "decision" {
				if err := s.ResolveCleanup(i.InfoHash, i.QueueIncarnation, 0, "durable-decision", "reconcile", time.Now()); err != nil {
					t.Fatal("completed decision retry not idempotent", err)
				}
			}
			receipts, err := s.CleanupReceipts(100)
			if err != nil || len(receipts) != 1 || receipts[0].IntentPresent {
				t.Fatal("completion receipt was not reconciled")
			}
		})
	}
}
