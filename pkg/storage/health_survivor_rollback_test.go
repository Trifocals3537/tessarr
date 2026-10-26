package storage

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func TestHealthSurvivorDoesNotArchiveRollingBackDeletion(t *testing.T) {
	for _, method := range []string{"cleanup", "snapshot"} {
		for _, phase := range []string{"main", "fence"} {
			t.Run(method+"/"+phase, func(t *testing.T) {
				s := newMainLifecycleTestStorage(t)
				a, _ := seedSharedHealthSurvivor(t, s)
				a, _ = s.Get(a.InfoHash)
				paused := make(chan struct{})
				resume := make(chan struct{})
				done := make(chan struct{})
				var deleteErr error
				var resumeOnce sync.Once
				unblock := func() { resumeOnce.Do(func() { close(resume) }) }
				injected := errors.New("retirement sync failed")
				s.mainRetirementSyncForTest = func(current string) error {
					if current == phase {
						close(paused)
						<-resume
						return injected
					}
					return nil
				}
				go func() {
					defer close(done)
					if method == "cleanup" {
						deleteErr = s.DeleteWithCleanup(a.InfoHash, nil)
					} else {
						deleteErr = s.DeleteSnapshot(a)
					}
				}()
				defer func() { unblock(); <-done }()
				select {
				case <-paused:
				case <-done:
					t.Fatalf("deletion returned before the sync boundary: %v", deleteErr)
				case <-time.After(10 * time.Second):
					t.Fatal("deletion did not reach the sync boundary")
				}
				if _, err := s.getMainRaw(a.InfoHash); !IsEntryNotFound(err) {
					t.Fatalf("main row was not temporarily absent: %v", err)
				}
				intent, found, err := s.loadMainEntryTombstone(a.InfoHash)
				want := mainEntryTombstoneDeleting
				if phase == "fence" {
					want = mainEntryTombstoneRetired
				}
				if err != nil || !found || intent.Phase != want {
					t.Fatalf("retirement fixture: %+v %v", intent, err)
				}
				resolved := make(chan error, 1)
				go func() {
					_, err := s.ResolveHealthEntry("release", config.Get().FolderNaming)
					resolved <- err
				}()
				select {
				case err := <-resolved:
					if !errors.Is(err, ErrHealthIdentity) {
						t.Fatalf("in-flight retirement released its health owner: %v", err)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("health resolution waited on the deleting lifecycle lock")
				}
				if len(survivorArchives(t, s)) != 0 {
					t.Fatal("rollback-capable deletion archived live-owner proof")
				}
				unblock()
				<-done
				if !errors.Is(deleteErr, injected) {
					t.Fatalf("deletion failed to exercise rollback: %v", deleteErr)
				}
				s.mainRetirementSyncForTest = nil
				if _, err := s.Get(a.InfoHash); err != nil {
					t.Fatalf("rollback did not restore the parent: %v", err)
				}
				h, err := s.GetEntryHealth("release")
				if err != nil || h.Archived || len(h.FailedReads) != 3 || len(h.FileRetirements) != 1 {
					t.Fatalf("rollback lost active proof: %+v %v", h, err)
				}
				// A later committed deletion must still release the survivor.
				if err := s.Delete(a.InfoHash); err != nil {
					t.Fatal(err)
				}
				assertSharedHealthSurvivor(t, s)
			})
		}
	}
}

func TestHealthSurvivorDirectoryContentionDefersWithoutDeadlock(t *testing.T) {
	s := newMainLifecycleTestStorage(t)
	a, _ := seedSharedHealthSurvivor(t, s)
	if err := s.Delete(a.InfoHash); err != nil {
		t.Fatal(err)
	}
	s.mainEntries.mu.Lock()
	resolved := make(chan error, 1)
	go func() {
		_, err := s.ResolveHealthEntry("release", config.Get().FolderNaming)
		resolved <- err
	}()
	select {
	case err := <-resolved:
		s.mainEntries.mu.Unlock()
		if !errors.Is(err, ErrHealthIdentity) {
			t.Fatalf("contended lifecycle directory released health owner: %v", err)
		}
	case <-time.After(10 * time.Second):
		s.mainEntries.mu.Unlock()
		<-resolved
		t.Fatal("health resolution blocked on the lifecycle directory")
	}
	assertSharedHealthSurvivor(t, s)
}
