package storage

import (
	"testing"
	"time"
)

func TestObserveTransferSeparatesPollsFromProgress(t *testing.T) {
	started := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	entry := &Entry{Progress: 0.25}

	entry.ObserveTransfer(0.25, started)
	if entry.LastObservedAt == nil || !entry.LastObservedAt.Equal(started) {
		t.Fatalf("LastObservedAt = %v, want %v", entry.LastObservedAt, started)
	}
	if entry.LastProgressAt == nil || !entry.LastProgressAt.Equal(started) {
		t.Fatalf("LastProgressAt = %v, want initial observation %v", entry.LastProgressAt, started)
	}

	unchanged := started.Add(10 * time.Minute)
	entry.ObserveTransfer(0.25, unchanged)
	if !entry.LastObservedAt.Equal(unchanged) {
		t.Fatalf("LastObservedAt = %v, want %v", entry.LastObservedAt, unchanged)
	}
	if !entry.LastProgressAt.Equal(started) {
		t.Fatalf("unchanged poll moved LastProgressAt to %v", entry.LastProgressAt)
	}

	advanced := unchanged.Add(time.Minute)
	entry.ObserveTransfer(0.30, advanced)
	if !entry.LastProgressAt.Equal(advanced) {
		t.Fatalf("progress change LastProgressAt = %v, want %v", entry.LastProgressAt, advanced)
	}
}

func TestProgressObservationProtoRoundTrip(t *testing.T) {
	observed := time.Date(2026, 9, 19, 12, 10, 0, 0, time.UTC)
	progress := observed.Add(-time.Minute)
	entry := &Entry{InfoHash: "hash", LastObservedAt: &observed, LastProgressAt: &progress}

	roundTrip := ProtoToEntry(EntryToProto(entry))
	if roundTrip.LastObservedAt == nil || !roundTrip.LastObservedAt.Equal(observed) {
		t.Fatalf("LastObservedAt = %v, want %v", roundTrip.LastObservedAt, observed)
	}
	if roundTrip.LastProgressAt == nil || !roundTrip.LastProgressAt.Equal(progress) {
		t.Fatalf("LastProgressAt = %v, want %v", roundTrip.LastProgressAt, progress)
	}
}

func TestUncachedHandoffEvidenceProtoRoundTrip(t *testing.T) {
	entry := &Entry{InfoHash: "hash", ClientEndpoint: "tessarr.example:8282", TerminalChecks: 2, HandoffReason: "terminal"}
	roundTrip := ProtoToEntry(EntryToProto(entry))
	if roundTrip.ClientEndpoint != entry.ClientEndpoint || roundTrip.TerminalChecks != 2 || roundTrip.HandoffReason != "terminal" {
		t.Fatalf("handoff evidence lost: %+v", roundTrip)
	}
}
