package storage

import (
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func TestEntryHealthSavePreservesNewerDirtyRevision(t *testing.T) {
	store, err := NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	const reason = "read_failure:timeout"
	if err := store.MarkEntryDirtyChecked("alpha", config.ProtocolTorrent, reason); err != nil {
		t.Fatal(err)
	}
	first, err := store.GetEntryHealth("alpha")
	if err != nil {
		t.Fatal(err)
	}
	if first.DirtyRevision != 1 {
		t.Fatalf("first dirty revision = %d, want 1", first.DirtyRevision)
	}
	if err := store.MarkEntryDirtyChecked("alpha", config.ProtocolTorrent, reason); err != nil {
		t.Fatal(err)
	}
	duplicate, err := store.GetEntryHealth("alpha")
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.DirtyRevision != 2 {
		t.Fatalf("second dirty revision = %d, want 2", duplicate.DirtyRevision)
	}

	// Simulate a probe that loaded revision 1 before the second failure and
	// attempts to clear the dirty flag after revision 2 was persisted.
	first.Dirty = false
	first.DirtyReason = ""
	if err := store.SaveEntryHealth(first); err != nil {
		t.Fatal(err)
	}
	merged, err := store.GetEntryHealth("alpha")
	if err != nil {
		t.Fatal(err)
	}
	if !merged.Dirty || merged.DirtyReason != reason || merged.DirtyRevision != 2 {
		t.Fatalf("merged health = %+v, want newer dirty revision preserved", merged)
	}

	if err := store.MarkEntryDirtyChecked("alpha", config.ProtocolNZB, "read_failure:content_missing"); err != nil {
		t.Fatal(err)
	}
	changed, err := store.GetEntryHealth("alpha")
	if err != nil {
		t.Fatal(err)
	}
	if changed.Protocol != config.ProtocolNZB || changed.DirtyRevision != 3 {
		t.Fatalf("changed health = %+v, want NZB protocol at dirty revision 3", changed)
	}
}
