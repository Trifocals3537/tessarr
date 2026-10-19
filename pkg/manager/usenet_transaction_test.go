package manager

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func queueTransactionEntry() *storage.Entry {
	return &storage.Entry{
		InfoHash:       "11111111-1111-1111-1111-111111111111",
		Protocol:       config.ProtocolNZB,
		ActiveProvider: "usenet",
		Providers: map[string]*storage.ProviderEntry{
			"usenet": {
				Provider: "usenet",
				ID:       "11111111-1111-1111-1111-111111111111",
			},
		},
	}
}

func TestInspectFailedQueueAddPreservesVisibleMatchingRecord(t *testing.T) {
	expected := queueTransactionEntry()
	preserve, err := inspectFailedQueueAdd(expected, func(string) (*storage.Entry, error) {
		copy := *expected
		return &copy, nil
	})
	if !preserve {
		t.Fatal("matching visible record was not preserved")
	}
	if !errors.Is(err, ErrQueueAddAmbiguous) {
		t.Fatalf("error = %v, want ErrQueueAddAmbiguous", err)
	}
}

func TestInspectFailedQueueAddPreservesIndeterminateLookup(t *testing.T) {
	expected := queueTransactionEntry()
	preserve, err := inspectFailedQueueAdd(expected, func(string) (*storage.Entry, error) {
		return nil, errors.New("transient queue read failure")
	})
	if !preserve {
		t.Fatal("indeterminate queue lookup was not preserved")
	}
	if !errors.Is(err, ErrQueueAddAmbiguous) {
		t.Fatalf("error = %v, want ErrQueueAddAmbiguous", err)
	}
}

func TestInspectFailedQueueAddRollsBackOnlyConfirmedAbsence(t *testing.T) {
	expected := queueTransactionEntry()
	preserve, err := inspectFailedQueueAdd(expected, func(string) (*storage.Entry, error) {
		return nil, fmt.Errorf("lookup: %w", storage.ErrQueuedEntryNotFound)
	})
	if preserve {
		t.Fatal("authoritative queue miss was preserved")
	}
	if err != nil {
		t.Fatalf("error = %v, want nil", err)
	}
}

func TestInspectFailedQueueAddPreservesMismatchedRecord(t *testing.T) {
	expected := queueTransactionEntry()
	preserve, err := inspectFailedQueueAdd(expected, func(string) (*storage.Entry, error) {
		return &storage.Entry{
			InfoHash:       expected.InfoHash,
			Protocol:       config.ProtocolTorrent,
			ActiveProvider: "realdebrid",
		}, nil
	})
	if !preserve {
		t.Fatal("mismatched record was not preserved for recovery")
	}
	if !errors.Is(err, ErrQueueAddAmbiguous) {
		t.Fatalf("error = %v, want ErrQueueAddAmbiguous", err)
	}
}
