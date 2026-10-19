package manager

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/Trifocals3537/tessarr/pkg/usenet"
)

func writeWatchedNZBReconciliationState(
	t *testing.T,
	root string,
	identity watchedNZBIdentity,
) (*storage.Entry, *storage.NZB) {
	t.Helper()
	stagedPath := filepath.Join(root, identity.ID+".queued")
	sourcePath := filepath.Join(root, identity.ID+".nzb")
	content := []byte("<nzb>fixture</nzb>")
	if err := os.WriteFile(stagedPath, content, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sourcePath, content, 0o600); err != nil {
		t.Fatal(err)
	}
	entry := testWatchedNZBEntry(identity)
	entry.Magnet = stagedPath
	metadata := &storage.NZB{
		ID:       identity.ID,
		Name:     entry.Name,
		Category: watchedNZBCategory,
		Path:     sourcePath,
	}
	return entry, metadata
}

func missingWatchedNZBEntry(string) (*storage.Entry, error) {
	return nil, errWatchedNZBTestMissing
}

func missingWatchedNZBMetadata(string) (*storage.NZB, error) {
	return nil, errWatchedNZBTestMissing
}

func TestReconcileWatchedNZBStateRecognizesStrictDurableDuplicate(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	root := t.TempDir()
	entry, metadata := writeWatchedNZBReconciliationState(t, root, identity)

	state, err := reconcileWatchedNZBState(
		identity,
		root,
		1024,
		func(string) (*storage.Entry, error) { return entry, nil },
		func(string) (*storage.Entry, error) { return entry, nil },
		func(string) (*storage.NZB, error) { return metadata, nil },
		watchedNZBTestNotFound,
		watchedNZBTestNotFound,
		watchedNZBTestNotFound,
	)
	if err != nil {
		t.Fatal(err)
	}
	if state != watchedNZBReconciliationDurable {
		t.Fatalf("reconciliation state = %d, want durable", state)
	}
}

func TestReconcileWatchedNZBStateRecognizesCrashWindows(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	t.Run("nothing persisted", func(t *testing.T) {
		state, err := reconcileWatchedNZBState(
			identity,
			t.TempDir(),
			1024,
			missingWatchedNZBEntry,
			missingWatchedNZBEntry,
			missingWatchedNZBMetadata,
			watchedNZBTestNotFound,
			watchedNZBTestNotFound,
			watchedNZBTestNotFound,
		)
		if err != nil || state != watchedNZBReconciliationAbsent {
			t.Fatalf("empty state = %d, err %v", state, err)
		}
	})

	t.Run("staged before parse", func(t *testing.T) {
		root := t.TempDir()
		if err := os.WriteFile(
			filepath.Join(root, identity.ID+".queued"),
			[]byte("<nzb>fixture</nzb>"),
			0o600,
		); err != nil {
			t.Fatal(err)
		}
		state, err := reconcileWatchedNZBState(
			identity,
			root,
			1024,
			missingWatchedNZBEntry,
			missingWatchedNZBEntry,
			missingWatchedNZBMetadata,
			watchedNZBTestNotFound,
			watchedNZBTestNotFound,
			watchedNZBTestNotFound,
		)
		if err != nil || state != watchedNZBReconciliationResumable {
			t.Fatalf("staged state = %d, err %v", state, err)
		}
	})

	t.Run("parsed before queue", func(t *testing.T) {
		root := t.TempDir()
		_, metadata := writeWatchedNZBReconciliationState(t, root, identity)
		state, err := reconcileWatchedNZBState(
			identity,
			root,
			1024,
			missingWatchedNZBEntry,
			missingWatchedNZBEntry,
			func(string) (*storage.NZB, error) { return metadata, nil },
			watchedNZBTestNotFound,
			watchedNZBTestNotFound,
			watchedNZBTestNotFound,
		)
		if err != nil || state != watchedNZBReconciliationResumable {
			t.Fatalf("partial metadata state = %d, err %v", state, err)
		}
	})

	t.Run("completed metadata before post-submit reconcile", func(t *testing.T) {
		root := t.TempDir()
		entry, metadata := writeWatchedNZBReconciliationState(t, root, identity)
		metadata.Status = usenet.NZBStatusCompleted
		if err := os.Remove(metadata.Path); err != nil {
			t.Fatal(err)
		}
		metadata.Path = ""
		state, err := reconcileWatchedNZBState(
			identity,
			root,
			1024,
			func(string) (*storage.Entry, error) { return entry, nil },
			missingWatchedNZBEntry,
			func(string) (*storage.NZB, error) { return metadata, nil },
			watchedNZBTestNotFound,
			watchedNZBTestNotFound,
			watchedNZBTestNotFound,
		)
		if err != nil || state != watchedNZBReconciliationDurable {
			t.Fatalf("completed queue state = %d, err %v", state, err)
		}
	})

	t.Run("completed main after queue and source cleanup", func(t *testing.T) {
		root := t.TempDir()
		entry, metadata := writeWatchedNZBReconciliationState(t, root, identity)
		metadata.Status = usenet.NZBStatusCompleted
		if err := os.Remove(entry.Magnet); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(metadata.Path); err != nil {
			t.Fatal(err)
		}
		entry.Magnet = ""
		metadata.Path = ""
		state, err := reconcileWatchedNZBState(
			identity,
			root,
			1024,
			missingWatchedNZBEntry,
			func(string) (*storage.Entry, error) { return entry, nil },
			func(string) (*storage.NZB, error) { return metadata, nil },
			watchedNZBTestNotFound,
			watchedNZBTestNotFound,
			watchedNZBTestNotFound,
		)
		if err != nil || state != watchedNZBReconciliationDurable {
			t.Fatalf("completed main-only state = %d, err %v", state, err)
		}
	})

	t.Run("nonterminal main-only fails closed", func(t *testing.T) {
		root := t.TempDir()
		entry, metadata := writeWatchedNZBReconciliationState(t, root, identity)
		if err := os.Remove(entry.Magnet); err != nil {
			t.Fatal(err)
		}
		if err := os.Remove(metadata.Path); err != nil {
			t.Fatal(err)
		}
		entry.Magnet = ""
		metadata.Path = ""
		state, err := reconcileWatchedNZBState(
			identity,
			root,
			1024,
			missingWatchedNZBEntry,
			func(string) (*storage.Entry, error) { return entry, nil },
			func(string) (*storage.NZB, error) { return metadata, nil },
			watchedNZBTestNotFound,
			watchedNZBTestNotFound,
			watchedNZBTestNotFound,
		)
		if state != watchedNZBReconciliationAbsent ||
			!errors.Is(err, errWatchedNZBStateAmbiguous) {
			t.Fatalf("nonterminal main-only state = %d, err %v", state, err)
		}
	})

	t.Run("completed main with cross-entry retained path fails closed", func(t *testing.T) {
		root := t.TempDir()
		entry, metadata := writeWatchedNZBReconciliationState(t, root, identity)
		metadata.Status = usenet.NZBStatusCompleted
		entry.Magnet = filepath.Join(
			root,
			"22222222-2222-4222-8222-222222222222.queued",
		)
		state, err := reconcileWatchedNZBState(
			identity,
			root,
			1024,
			missingWatchedNZBEntry,
			func(string) (*storage.Entry, error) { return entry, nil },
			func(string) (*storage.NZB, error) { return metadata, nil },
			watchedNZBTestNotFound,
			watchedNZBTestNotFound,
			watchedNZBTestNotFound,
		)
		if state != watchedNZBReconciliationAbsent ||
			!errors.Is(err, errWatchedNZBStateAmbiguous) {
			t.Fatalf("cross-entry completed main state = %d, err %v", state, err)
		}
	})
}

func TestReconcileWatchedNZBStateFailsClosedOnCorruptDurableState(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	tests := []struct {
		name   string
		mutate func(string, *storage.Entry, *storage.NZB)
	}{
		{
			name: "tampered staged bytes",
			mutate: func(_ string, entry *storage.Entry, _ *storage.NZB) {
				if err := os.WriteFile(entry.Magnet, []byte("tampered"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "cross-entry staged path",
			mutate: func(root string, entry *storage.Entry, _ *storage.NZB) {
				other := filepath.Join(root, "22222222-2222-4222-8222-222222222222.queued")
				if err := os.WriteFile(other, []byte("<nzb>fixture</nzb>"), 0o600); err != nil {
					t.Fatal(err)
				}
				entry.Magnet = other
			},
		},
		{
			name: "tampered managed source",
			mutate: func(_ string, _ *storage.Entry, metadata *storage.NZB) {
				if err := os.WriteFile(metadata.Path, []byte("tampered"), 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name: "wrong category",
			mutate: func(_ string, entry *storage.Entry, _ *storage.NZB) {
				entry.Category = "sonarr"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			entry, metadata := writeWatchedNZBReconciliationState(t, root, identity)
			test.mutate(root, entry, metadata)
			state, err := reconcileWatchedNZBState(
				identity,
				root,
				1024,
				func(string) (*storage.Entry, error) { return entry, nil },
				missingWatchedNZBEntry,
				func(string) (*storage.NZB, error) { return metadata, nil },
				watchedNZBTestNotFound,
				watchedNZBTestNotFound,
				watchedNZBTestNotFound,
			)
			if state != watchedNZBReconciliationAbsent ||
				!errors.Is(err, errWatchedNZBStateAmbiguous) {
				t.Fatalf("corrupt state = %d, err %v", state, err)
			}
		})
	}
}

func TestReconcileWatchedNZBStateTreatsOnlyTypedMissAsAbsent(t *testing.T) {
	identity := testWatchedNZBIdentity(t)
	state, err := reconcileWatchedNZBState(
		identity,
		t.TempDir(),
		1024,
		func(string) (*storage.Entry, error) {
			return nil, errors.New("queue unavailable")
		},
		missingWatchedNZBEntry,
		missingWatchedNZBMetadata,
		watchedNZBTestNotFound,
		watchedNZBTestNotFound,
		watchedNZBTestNotFound,
	)
	if state != watchedNZBReconciliationAbsent ||
		!errors.Is(err, errWatchedNZBStateAmbiguous) {
		t.Fatalf("untyped lookup state = %d, err %v", state, err)
	}
}
