package storage

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"google.golang.org/protobuf/proto"
)

func TestPlacementAccountSurvivesQueueRetirementAndRestart(t *testing.T) {
	root := filepath.Join(t.TempDir(), "db")
	s, err := NewStorage(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	entry := &Entry{InfoHash: "parent", State: EntryStateDownloading}
	if err := s.AddQueue(entry); err != nil {
		t.Fatal(err)
	}
	intent, _, err := s.BeginProviderSubmission(entry, "provider", "original-account", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordProviderSubmissionID(entry, intent.Attempt, "42", false); err != nil {
		t.Fatal(err)
	}
	remote := &debridTypes.Torrent{Id: "42", InfoHash: entry.InfoHash, Debrid: "provider"}
	if _, err := s.BindProviderSubmission(entry, intent.Attempt, remote); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteQueued(entry.InfoHash, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewStorage(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CheckProviderPlacementAccount("provider", "42", "original-account"); err != nil {
		t.Fatal(err)
	}
	if err := s.CheckProviderPlacementAccount("provider", "42", "replacement"); !errors.Is(err, ErrProviderPlacementAccount) {
		t.Fatalf("retired placement lost account fence: %v", err)
	}
	if err := s.CheckProviderPlacementAccount("other", "42", "replacement"); err != nil {
		t.Fatalf("fence crossed provider boundary: %v", err)
	}
	if err := s.CheckProviderPlacementAccount("provider", "43", "replacement"); err != nil {
		t.Fatalf("fence crossed ID boundary: %v", err)
	}
}

func TestPlacementAccountRecoversEarlierJournalEvidence(t *testing.T) {
	root := filepath.Join(t.TempDir(), "db")
	s, err := NewStorage(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	// Simulate the previous binary: the journal survives, but its parent queue
	// row and the newer placement-account index do not exist.
	intent := &ProviderSubmission{Version: 1, InfoHash: "retired-parent", QueueIncarnation: "prior-incarnation", Attempt: "prior-attempt", Provider: "provider", Account: "original", Phase: SubmissionBound, ProviderID: "42"}
	if err := s.saveSubmission(intent); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewStorage(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.CheckProviderPlacementAccount("provider", "42", "replacement"); !errors.Is(err, ErrProviderPlacementAccount) {
		t.Fatalf("earlier journal did not protect copied placement: %v", err)
	}
	before := s.DiskSize()
	if err := s.recoverProviderPlacementAccounts(); err != nil {
		t.Fatal(err)
	}
	if s.DiskSize() != before {
		t.Fatal("idempotent account recovery appended another record")
	}
}

func TestPlacementAccountRejectsConflictingAcknowledgements(t *testing.T) {
	s, entry := submissionStorageFixture(t)
	intent, _, err := s.BeginProviderSubmission(entry, "provider", "original", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordProviderSubmissionID(entry, intent.Attempt, "42", false); err != nil {
		t.Fatal(err)
	}
	other := &Entry{InfoHash: "other-hash", State: EntryStateDownloading}
	if err := s.AddQueue(other); err != nil {
		t.Fatal(err)
	}
	replacement, _, err := s.BeginProviderSubmission(other, "provider", "replacement", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.RecordProviderSubmissionID(other, replacement.Attempt, "42", false); !errors.Is(err, ErrProviderSubmissionConflict) {
		t.Fatalf("rebound old placement to new account: %v", err)
	}
	if err := s.CheckProviderPlacementAccount("provider", "42", "original"); err != nil {
		t.Fatalf("rejected acknowledgement changed original binding: %v", err)
	}
}

func TestConflictingHistoricalPlacementAccountsRemainBlocked(t *testing.T) {
	s, _ := submissionStorageFixture(t)
	for _, account := range []string{"first", "second"} {
		intent := &ProviderSubmission{Version: 1, InfoHash: account, QueueIncarnation: "incarnation", Attempt: "attempt", Provider: "provider", Account: account, Phase: SubmissionBound, ProviderID: "42"}
		if err := s.saveSubmission(intent); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.recoverProviderPlacementAccounts(); err != nil {
		t.Fatal(err)
	}
	for _, account := range []string{"first", "second"} {
		if err := s.CheckProviderPlacementAccount("provider", "42", account); !errors.Is(err, ErrProviderPlacementAccount) {
			t.Fatalf("conflicting evidence granted account %q: %v", account, err)
		}
	}
}

func TestCorruptPlacementAccountCannotAuthorizeAccess(t *testing.T) {
	s, _ := submissionStorageFixture(t)
	for _, value := range []string{`invalid`, `{"version":1,"provider":"other","id":"42","account":"original"}`} {
		if err := s.storageState.Put(placementAccountKey("provider", "42"), []byte(value), nil); err != nil {
			t.Fatal(err)
		}
		if err := s.CheckProviderPlacementAccount("provider", "42", "original"); err == nil {
			t.Fatal("corrupt account evidence authorized a provider operation")
		}
	}
}

func TestAdmissionUncachedPolicySurvivesDurableQueueRoundTrip(t *testing.T) {
	for _, policy := range []UncachedPolicy{"", UncachedPolicyInherit, UncachedPolicyAllow, UncachedPolicyDeny} {
		t.Run(string(policy), func(t *testing.T) {
			s, entry := submissionStorageFixture(t)
			entry.AdmissionUncachedPolicy = policy
			entry.DownloadUncached = true // Effective attempt and request remain independent.
			if err := s.UpdateQueue(entry); err != nil {
				t.Fatal(err)
			}
			if err := s.SyncQueue(); err != nil {
				t.Fatal(err)
			}
			current, err := s.GetQueued(entry.InfoHash)
			if err != nil || current.AdmissionUncachedPolicy != policy || !current.DownloadUncached {
				t.Fatalf("policy round trip: %+v %v", current, err)
			}
			encoded, err := proto.Marshal(EntryToProto(current))
			if err != nil {
				t.Fatal(err)
			}
			var decoded EntryProto
			if err := proto.Unmarshal(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			if ProtoToEntry(&decoded).AdmissionUncachedPolicy != policy {
				t.Fatal("wire format discarded request policy")
			}
			public, err := json.Marshal(current)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := json.Unmarshal(public, &fields); err != nil {
				t.Fatal(err)
			}
			if _, exists := fields["admission_uncached_policy"]; exists {
				t.Fatal("internal request metadata leaked into polling API")
			}
		})
	}
}
