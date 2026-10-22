package storage

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func submissionStorageFixture(t *testing.T) (*Storage, *Entry) {
	t.Helper()
	s, err := NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	entry := &Entry{InfoHash: "submission", State: EntryStateDownloading}
	if err := s.AddQueue(entry); err != nil {
		t.Fatal(err)
	}
	return s, entry
}

func TestProviderSubmissionFencesAttemptsDeletionAndReplacement(t *testing.T) {
	s, entry := submissionStorageFixture(t)
	intent, created, err := s.BeginProviderSubmission(entry, "first", "account-scope", false)
	if err != nil || !created {
		t.Fatalf("begin: %+v %t %v", intent, created, err)
	}
	reused, created, err := s.BeginProviderSubmission(entry, "other", "other-scope", true)
	if err != nil || created || reused.Attempt != intent.Attempt || reused.Provider != "first" || reused.DownloadUncached {
		t.Fatalf("pending operation was replaced: %+v %t %v", reused, created, err)
	}
	if err := s.RecordProviderSubmissionID(entry, "stale-attempt", "id", false); !errors.Is(err, ErrProviderSubmissionConflict) {
		t.Fatal(err)
	}
	if err := s.RecordProviderSubmissionID(entry, intent.Attempt, "id", false); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordProviderSubmissionID(entry, intent.Attempt, "different-id", false); !errors.Is(err, ErrProviderSubmissionConflict) {
		t.Fatal("changed remote identity", err)
	}
	if _, err := s.PrepareQBitQueuedDeletion(entry.InfoHash, false); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordProviderSubmissionID(entry, intent.Attempt, "id", false); !errors.Is(err, ErrQueuedEntryDeleting) {
		t.Fatal("deletion accepted mutation", err)
	}
}

func TestUnresolvedSubmissionCannotAttachToReplacementIncarnation(t *testing.T) {
	s, entry := submissionStorageFixture(t)
	intent, _, err := s.BeginProviderSubmission(entry, "first", "account", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteQueued(entry.InfoHash, nil); err != nil {
		t.Fatal(err)
	}
	replacement := &Entry{InfoHash: entry.InfoHash, State: EntryStateDownloading}
	if err := s.AddQueue(replacement); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.BeginProviderSubmission(replacement, "second", "account", false); !errors.Is(err, ErrProviderSubmissionConflict) {
		t.Fatal("unresolved old create was discarded", err)
	}
	if err := s.RecordProviderSubmissionID(entry, intent.Attempt, "late", false); !errors.Is(err, ErrProviderSubmissionConflict) {
		t.Fatal("late response attached to replacement", err)
	}
}

func TestRecoveredHashMatchNeverGrantsRollbackAuthorization(t *testing.T) {
	s, entry := submissionStorageFixture(t)
	intent, _, err := s.BeginProviderSubmission(entry, "provider", "account", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordProviderSubmissionID(entry, intent.Attempt, "matched", true); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordProviderSubmissionID(entry, intent.Attempt, "matched", false); !errors.Is(err, ErrProviderSubmissionConflict) {
		t.Fatal("recovered match acquired acknowledgement authority", err)
	}
	if err := s.SetProviderSubmissionPhase(entry, intent.Attempt, SubmissionCleanup); !errors.Is(err, ErrProviderSubmissionConflict) {
		t.Fatal("hash match granted deletion", err)
	}
	if err := s.SetProviderSubmissionPhase(entry, intent.Attempt, SubmissionRejected); !errors.Is(err, ErrProviderSubmissionConflict) {
		t.Fatal("bound ID was treated as a refusal", err)
	}
}

func TestProviderSubmissionSyncFailureCannotAuthorizeCreate(t *testing.T) {
	s, entry := submissionStorageFixture(t)
	failure := errors.New("intent sync failed")
	s.submissionSyncForTest = func() error { return failure }
	if _, created, err := s.BeginProviderSubmission(entry, "provider", "account", false); !errors.Is(err, failure) || created {
		t.Fatalf("unsynced intent authorized create: created=%t err=%v", created, err)
	}
	s.submissionSyncForTest = nil
	if _, created, err := s.BeginProviderSubmission(entry, "provider", "account", false); err != nil || created {
		t.Fatalf("uncertain intent resubmitted: created=%t err=%v", created, err)
	}
}

func TestProviderBindingRecoversQueueBeforeFinalMarkerFailure(t *testing.T) {
	s, entry := submissionStorageFixture(t)
	intent, _, err := s.BeginProviderSubmission(entry, "provider", "account", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordProviderSubmissionID(entry, intent.Attempt, "accepted", false); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash/failure before the bound marker is appended, after the
	// queue mapping was already synced. Keep the durable pending marker intact.
	remote := &debridTypes.Torrent{Id: "accepted", InfoHash: entry.InfoHash, Debrid: "provider", Status: debridTypes.TorrentStatusDownloading}
	if _, err := entry.UpdateTorrentProviderState(remote); err != nil {
		t.Fatal(err)
	}
	entry.ActiveProvider = "provider"
	if err := s.UpdateQueue(entry); err != nil {
		t.Fatal(err)
	}
	if err := s.SyncQueue(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecoverProviderSubmission(entry); err != nil {
		t.Fatal(err)
	}
	recovered, err := s.GetProviderSubmission(entry)
	if err != nil || recovered.Phase != SubmissionBound {
		t.Fatalf("mapping not finalized: %+v %v", recovered, err)
	}
}

func TestSubmissionRecoveryRestoresOnlyUnresolvedFailures(t *testing.T) {
	s, entry := submissionStorageFixture(t)
	if _, _, err := s.BeginProviderSubmission(entry, "provider", "account", false); err != nil {
		t.Fatal(err)
	}
	entry.MarkAsError(errors.New("provider response lost"))
	if err := s.UpdateQueue(entry); err != nil {
		t.Fatal(err)
	}
	if changed, err := s.RecoverProviderSubmission(entry); err != nil || !changed {
		t.Fatalf("failed pending work not restored: %t %v", changed, err)
	}
	current, err := s.GetQueued(entry.InfoHash)
	if err != nil || current.State != EntryStateDownloading || current.Status != debridTypes.TorrentStatusQueued {
		t.Fatalf("recovered state: %+v %v", current, err)
	}
	intent, err := s.GetProviderSubmission(current)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SetProviderSubmissionPhase(current, intent.Attempt, SubmissionRejected); err != nil {
		t.Fatal(err)
	}
	current.MarkAsError(errors.New("definite refusal"))
	if err := s.UpdateQueue(current); err != nil {
		t.Fatal(err)
	}
	if changed, err := s.RecoverProviderSubmission(current); err != nil || changed {
		t.Fatalf("terminal refusal was replayed: %t %v", changed, err)
	}
}

func TestLegacySubmissionIdentityAdoptionFencesOtherLegacySnapshots(t *testing.T) {
	s, entry := submissionStorageFixture(t)
	entry.QueueIncarnation = ""
	if err := s.writeQueueRaw(entry, true); err != nil {
		t.Fatal(err)
	}
	adopted, err := s.EnsureProviderSubmissionIdentity(entry)
	if err != nil || adopted.QueueIncarnation == "" {
		t.Fatalf("legacy row not adopted: %+v %v", adopted, err)
	}
	if _, err := s.EnsureProviderSubmissionIdentity(entry); !errors.Is(err, ErrProviderSubmissionConflict) {
		t.Fatal("stale legacy worker acquired new identity", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewStorage(s.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	current, err := reopened.GetQueued(entry.InfoHash)
	if err != nil || current.QueueIncarnation != adopted.QueueIncarnation {
		t.Fatalf("legacy adoption lost on restart: %+v %v", current, err)
	}
}

func TestHashRecoveredPlacementCannotAuthorizeUncachedArrCleanup(t *testing.T) {
	s, entry := submissionStorageFixture(t)
	intent, _, err := s.BeginProviderSubmission(entry, "provider", "account", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordProviderSubmissionID(entry, intent.Attempt, "matched", true); err != nil {
		t.Fatal(err)
	}
	current, err := s.BindProviderSubmission(entry, intent.Attempt, &debridTypes.Torrent{Id: "matched", InfoHash: entry.InfoHash, Debrid: "provider", Status: debridTypes.TorrentStatusDownloaded})
	if err != nil {
		t.Fatal(err)
	}
	current.Protocol, current.Category, current.ClientEndpoint = config.ProtocolTorrent, "sonarr", "http://fixture.invalid:8080"
	current.State, current.HandoffReason = EntryStateStalledDL, "terminal"
	if err := s.UpdateQueue(current); err != nil {
		t.Fatal(err)
	}
	if !FailedUncachedHandoff(current) {
		t.Fatal("fixture did not reach destructive handoff eligibility")
	}
	if err := s.AuthorizeUncachedHandoff(current); !errors.Is(err, ErrProviderSubmissionConflict) {
		t.Fatal("hash match granted Arr cleanup ownership", err)
	}
}

func TestFailedBoundMarkerPreservesDurableProviderMapping(t *testing.T) {
	s, entry := submissionStorageFixture(t)
	intent, _, err := s.BeginProviderSubmission(entry, "provider", "account", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordProviderSubmissionID(entry, intent.Attempt, "acknowledged", false); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("bound marker fsync failed")
	s.submissionSyncForTest = func() error { return failure }
	if _, err := s.BindProviderSubmission(entry, intent.Attempt, &debridTypes.Torrent{Id: "acknowledged", InfoHash: entry.InfoHash, Debrid: "provider"}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	s.submissionSyncForTest = nil
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewStorage(s.dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	current, err := reopened.GetQueued(entry.InfoHash)
	if err != nil || current.GetActiveProvider() == nil || current.GetActiveProvider().ID != "acknowledged" || current.Status != debridTypes.TorrentStatusQueued {
		t.Fatalf("binding lost or published ahead of worker: %+v %v", current, err)
	}
}
