package hybrid

import (
	"errors"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestSyncedExistingWriteRetainsPublishedRowOnSyncFailure(t *testing.T) {
	for _, interval := range []time.Duration{-1, 0} {
		t.Run(interval.String(), func(t *testing.T) {
			store, err := New(Config{DataPath: filepath.Join(t.TempDir(), "publication.db"), SyncInterval: interval})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			oldMeta := &EntryMeta{Category: "active", Provider: "original", Status: "downloading"}
			newMeta := &EntryMeta{Category: "done", Provider: "replacement", Status: "pausedUP"}
			if err := store.Put("key", []byte("incomplete"), oldMeta); err != nil {
				t.Fatal(err)
			}
			if err := store.Sync(); err != nil {
				t.Fatal(err)
			}
			// Prime the cache as a polling request would before completion.
			if _, err := store.Get("key"); err != nil {
				t.Fatal(err)
			}
			failure := errors.New("injected completion sync failure")
			store.syncForTest = func() error { return failure }
			if err := store.PutExistingSynced("key", []byte("complete"), newMeta); !errors.Is(err, failure) {
				t.Fatalf("completion error = %v", err)
			}
			if err := store.DurabilityError(); !errors.Is(err, ErrDurability) {
				t.Fatalf("durability failure not reported: %v", err)
			}
			assertPublishedRow := func() {
				t.Helper()
				if value, err := store.Get("key"); err != nil || string(value) != "incomplete" {
					t.Fatalf("Get published failed completion: %q %v", value, err)
				}
				if meta, err := store.GetMeta("key"); err != nil || meta.Status != oldMeta.Status || meta.Category != oldMeta.Category || meta.Provider != oldMeta.Provider {
					t.Fatalf("GetMeta published failed completion: %+v %v", meta, err)
				}
				seen := 0
				if err := store.ForEach(func(key string, value []byte) error {
					seen++
					if key != "key" || string(value) != "incomplete" {
						t.Fatalf("scan published failed completion: %s %q", key, value)
					}
					return nil
				}); err != nil || seen != 1 {
					t.Fatalf("scan: seen=%d error=%v", seen, err)
				}
				if !slices.Equal(store.FilterByCategory("active"), []string{"key"}) || len(store.FilterByCategory("done")) != 0 || !slices.Equal(store.FilterByProvider("original"), []string{"key"}) || len(store.FilterByProvider("replacement")) != 0 {
					t.Fatal("secondary indexes published failed completion")
				}
			}
			assertPublishedRow()
			if err := store.Put("other", []byte("blocked"), nil); !errors.Is(err, failure) {
				t.Fatalf("write accepted while durability is unavailable: %v", err)
			}
			store.syncForTest = nil
			if err := store.Sync(); err != nil {
				t.Fatal(err)
			}
			// A periodic sync recovering health must not publish a failed outcome.
			assertPublishedRow()
			if err := store.PutExistingSynced("key", []byte("complete"), newMeta); err != nil {
				t.Fatal(err)
			}
			if value, err := store.Get("key"); err != nil || string(value) != "complete" {
				t.Fatalf("successful retry not published: %q %v", value, err)
			}
			if meta, err := store.GetMeta("key"); err != nil || meta.Status != newMeta.Status {
				t.Fatalf("successful metadata not published: %+v %v", meta, err)
			}
			if err := store.PutExistingSynced("missing", []byte("complete"), newMeta); !IsNotFound(err) || store.Exists("missing") {
				t.Fatalf("synced update recreated a missing row: %v", err)
			}
			if err := store.Compact(); err != nil {
				t.Fatal(err)
			}
			if value, err := store.Get("key"); err != nil || string(value) != "complete" {
				t.Fatalf("compaction changed published completion: %q %v", value, err)
			}
		})
	}
}

func TestSyncedExistingWriteSerializesReadersWithSync(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			store, err := New(Config{DataPath: filepath.Join(t.TempDir(), "concurrent.db"), SyncInterval: -1})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			if err := store.Put("key", []byte("incomplete"), nil); err != nil {
				t.Fatal(err)
			}
			// Keep a hot value to exercise cached reads as well as disk scans.
			if _, err := store.Get("key"); err != nil {
				t.Fatal(err)
			}
			started, release := make(chan struct{}), make(chan struct{})
			var once, startedOnce sync.Once
			releaseSync := func() { once.Do(func() { close(release) }) }
			t.Cleanup(releaseSync)
			failure := errors.New("sync failed")
			store.syncForTest = func() error {
				startedOnce.Do(func() { close(started) })
				<-release
				if fail {
					return failure
				}
				return store.log.Sync()
			}
			written := make(chan error, 1)
			go func() { written <- store.PutExistingSynced("key", []byte("complete"), nil) }()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("completion did not reach sync")
			}
			read := make(chan string, 1)
			go func() {
				value, err := store.Get("key")
				if err != nil {
					read <- err.Error()
					return
				}
				read <- string(value)
			}()
			select {
			case value := <-read:
				t.Fatalf("reader escaped sync barrier: %q", value)
			case <-time.After(20 * time.Millisecond):
			}
			releaseSync()
			if err := <-written; (fail && !errors.Is(err, failure)) || (!fail && err != nil) {
				t.Fatal(err)
			}
			want := map[bool]string{false: "complete", true: "incomplete"}[fail]
			if value := <-read; value != want {
				t.Fatalf("reader got %q, want %q", value, want)
			}
			store.syncForTest = nil
		})
	}
}

func TestFailedSyncedWriteRemainsUncertainUntilRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "uncertain.db")
	store, err := New(Config{DataPath: path, SyncInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Put("key", []byte("incomplete"), nil); err != nil {
		t.Fatal(err)
	}
	store.syncForTest = func() error { return errors.New("uncertain sync") }
	if err := store.PutExistingSynced("key", []byte("complete"), nil); err == nil {
		t.Fatal("sync failure not returned")
	}
	store.syncForTest = nil
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	recovered, err := New(Config{DataPath: path, SyncInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	// The failed call never promised rollback; its complete append survived.
	if value, err := recovered.Get("key"); err != nil || string(value) != "complete" {
		t.Fatalf("restart did not reconcile surviving append: %q %v", value, err)
	}
}
