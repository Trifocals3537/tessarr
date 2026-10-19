package storage

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func handoffTestEntry() *Entry {
	return &Entry{InfoHash: "handoff", Protocol: config.ProtocolTorrent, State: EntryStateStalledDL, DownloadUncached: true, HandoffReason: "terminal", Category: "sonarr", ClientEndpoint: "host:8282", ActiveProvider: "fake", Providers: map[string]*ProviderEntry{"fake": {Provider: "fake", ID: "active"}}}
}

func TestUncachedAuthorizationRevokedByOwnerAndPlacementAcrossReopen(t *testing.T) {
	for _, kind := range []string{"owner", "endpoint", "placement"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "db")
			s, err := NewStorage(path)
			if err != nil {
				t.Fatal(err)
			}
			e := handoffTestEntry()
			if err = s.AddQueue(e); err != nil {
				t.Fatal(err)
			}
			if err = s.AuthorizeUncachedHandoff(e); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "owner":
				e.Category = "radarr"
			case "endpoint":
				e.ClientEndpoint = "other:8282"
			case "placement":
				e.Providers["fake"].ID = "other"
			}
			if err = s.UpdateQueueExisting(e); err != nil {
				t.Fatal(err)
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = NewStorage(path)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			e.Category = "sonarr"
			e.ClientEndpoint = "host:8282"
			e.Providers["fake"].ID = "active"
			if err = s.UpdateQueueExisting(e); err != nil {
				t.Fatal(err)
			}
			intent, err := s.PrepareQBitQueuedDeletion(e.InfoHash, true)
			if err != nil {
				t.Fatal(err)
			}
			if intent.PlacementCleanupPending {
				t.Fatal("reverted evidence resurrected revoked proof")
			}
		})
	}
}

func TestUncachedHandoffAuthorizationCannotCrossQueueIncarnation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	s, err := NewStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	old := handoffTestEntry()
	if err = s.AddQueue(old); err != nil {
		t.Fatal(err)
	}
	if err = s.AuthorizeUncachedHandoff(old); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteQueued(old.InfoHash, nil); err != nil {
		t.Fatal(err)
	}
	fresh := handoffTestEntry()
	if err = s.AddQueue(fresh); err != nil {
		t.Fatal(err)
	}
	if fresh.QueueIncarnation == old.QueueIncarnation {
		t.Fatal("incarnation reused")
	}
	if err = s.AuthorizeUncachedHandoff(old); err == nil {
		t.Fatal("old incarnation granted proof")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	intent, err := s.PrepareQBitQueuedDeletion(fresh.InfoHash, true)
	if err != nil {
		t.Fatal(err)
	}
	if intent.PlacementCleanupPending {
		t.Fatal("fresh same-hash queue inherited old proof")
	}
}

func TestUncachedAuthorizationRevokedByProgress(t *testing.T) {
	s, err := NewStorage(filepath.Join(t.TempDir(), "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e := handoffTestEntry()
	if err = s.AddQueue(e); err != nil {
		t.Fatal(err)
	}
	if err = s.AuthorizeUncachedHandoff(e); err != nil {
		t.Fatal(err)
	}
	e.Progress = .25
	if err = s.UpdateQueueExisting(e); err != nil {
		t.Fatal(err)
	}
	e.Progress = 0
	if err = s.UpdateQueueExisting(e); err != nil {
		t.Fatal(err)
	}
	intent, err := s.PrepareQBitQueuedDeletion(e.InfoHash, true)
	if err != nil {
		t.Fatal(err)
	}
	if intent.PlacementCleanupPending {
		t.Fatal("reverted progress resurrected stale authorization")
	}
}

func TestQBitDeletionScopeSurvivesReopenWithoutExpansion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "db")
	s, err := NewStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	e := handoffTestEntry()
	if err = s.AddQueue(e); err != nil {
		t.Fatal(err)
	}
	if err = s.AuthorizeUncachedHandoff(e); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := s.PrepareQBitQueuedDeletion(e.InfoHash, true)
	if err != nil {
		t.Fatal(err)
	}
	if !intent.PlacementCleanupPending {
		t.Fatal("durable authorization lost")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	main := *e
	main.Providers = map[string]*ProviderEntry{"fake": {Provider: "fake", ID: "library"}}
	intent, err = s.PrepareQueuedDeletion(e.InfoHash, true, &main)
	if err != nil {
		t.Fatal(err)
	}
	if intent.PlacementScope != QueuePlacementScopeTransferOnly || len(intent.PlacementEntries) != 1 || intent.PlacementEntries[0].GetActiveProvider().ID != "active" {
		t.Fatalf("scope expanded: %+v", intent)
	}
}

func TestMalformedTransferDeletionIntentFailsClosed(t *testing.T) {
	for _, kind := range []string{"scope", "multiple-providers", "wrong-placement", "no-placement"} {
		t.Run(kind, func(t *testing.T) {
			s, err := NewStorage(filepath.Join(t.TempDir(), "db"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			e := handoffTestEntry()
			if err = s.AddQueue(e); err != nil {
				t.Fatal(err)
			}
			if err = s.AuthorizeUncachedHandoff(e); err != nil {
				t.Fatal(err)
			}
			if _, err = s.PrepareQBitQueuedDeletion(e.InfoHash, true); err != nil {
				t.Fatal(err)
			}
			tomb, _, err := s.loadQueueDeletionTombstone(e.InfoHash)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "scope":
				tomb.PlacementScope = "unrecognized"
			case "multiple-providers":
				e.Providers["other"] = &ProviderEntry{Provider: "other", ID: "library"}
				tomb.PlacementSnapshots[0], err = marshalQueueDeletionSnapshot(e.InfoHash, e)
			case "wrong-placement":
				e.Providers["fake"].ID = "library"
				tomb.PlacementSnapshots[0], err = marshalQueueDeletionSnapshot(e.InfoHash, e)
			case "no-placement":
				tomb.PlacementSnapshots = nil
			}
			if err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(tomb)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = decodeQueueDeletionTombstone(e.InfoHash, data); err == nil {
				t.Fatal("malformed intent accepted")
			}
		})
	}
}
