package manager

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

const retiredRecheckAlias = "retired-recheck-alias"

func useRecheckOriginalNaming(t *testing.T) {
	t.Helper()
	previous := config.GetMainPath()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "config.json"), []byte(`{"folder_naming":"original"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := config.SetConfigPath(root); err != nil {
		t.Fatal(err)
	}
	config.Reset()
	config.Get()
	t.Cleanup(func() { _ = config.SetConfigPath(previous); config.Reset() })
}

// Retire a real probe's owner while its provider check is blocked, then reuse
// its old alias. History must retain the old run without owning the new work.
func newArchivedRecheckOwner(t *testing.T, runtimeName bool) (*Repair, string, *storage.Entry, *storage.EntryHealth) {
	t.Helper()
	useRecheckOriginalNaming(t)
	r, a, _ := newEvidenceRepair(t, 1)
	r.manager.arr = arr.NewStorage()
	path := t.TempDir()
	s, err := storage.NewStorage(path)
	if err != nil {
		t.Fatal(err)
	}
	r.manager.storage = s
	t.Cleanup(func() { r.runWG.Wait(); _ = r.manager.storage.Close() })
	a.MainGeneration = 0 // This fixture uses a distinct storage lifecycle.
	if err := s.AddOrUpdateDurable(a); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveHealthEntry(a.GetFolder(), config.Get().FolderNaming); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkEntryDirtyChecked(retiredRecheckAlias, a.Protocol, "entry_item_changed"); err != nil {
		t.Fatal(err)
	}
	if err := s.BindHealthAlias(retiredRecheckAlias, a.GetFolder()); err != nil {
		t.Fatal(err)
	}
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	r.manager.clients.Store("primary", &healthEvidenceClient{check: func(_ context.Context, filename string) error {
		if filename == "video-000.mkv" {
			close(started)
			<-release
		}
		return nil
	}})
	var oldFinal *storage.EntryHealth
	go func() {
		defer close(done)
		oldFinal = r.probeEntry(context.Background(), "retired-owner-running", &candidate{name: a.GetFolder()}, newHealCache(), RepairRunOptions{}, false)
	}()
	defer func() { unblock(); <-done }()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("old probe did not reach the provider check")
	}
	b := streamFailoverEntry("primary")
	b.InfoHash, b.Name, b.OriginalFilename = "recheck-survivor", a.Name, a.OriginalFilename
	b.Files = map[string]*storage.File{"survivor.mkv": {InfoHash: b.InfoHash, Name: "survivor.mkv", Size: 1024}}
	b.Providers["primary"].Files = map[string]*storage.ProviderFile{"survivor.mkv": {Link: "survivor.mkv"}}
	if err := s.AddOrUpdateDurable(b); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(a.InfoHash); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ResolveHealthEntry(b.GetFolder(), config.Get().FolderNaming); err != nil {
		t.Fatal(err)
	}
	unblock()
	<-done
	if oldFinal != nil {
		t.Fatalf("retired probe published a stale result: %+v", oldFinal)
	}
	archive, err := s.GetEntryHealth(retiredRecheckAlias)
	if err != nil || !archive.Archived || archive.ActiveRunID != "retired-owner-running" {
		t.Fatalf("missing real in-flight run in archive: %+v %v", archive, err)
	}
	c := streamFailoverEntry("primary")
	c.InfoHash, c.Name, c.OriginalFilename = "recheck-current-owner", retiredRecheckAlias, retiredRecheckAlias
	if runtimeName {
		c.OriginalFilename += "-canonical"
	}
	c.Files = map[string]*storage.File{"replacement.mkv": {InfoHash: c.InfoHash, Name: "replacement.mkv", Size: 1024}}
	c.Providers["primary"].Files = map[string]*storage.ProviderFile{"replacement.mkv": {Link: "replacement.mkv"}}
	if err := s.AddOrUpdateDurable(c); err != nil {
		t.Fatal(err)
	}
	if runtimeName && c.GetFolder() == retiredRecheckAlias {
		t.Fatal("runtime alias fixture must have a distinct canonical key")
	}
	return r, path, c, archive
}

func TestHealthRecheckReusedArchivedAliasUsesCurrentOwner(t *testing.T) {
	for _, runtimeName := range []bool{false, true} {
		for _, fix := range []bool{false, true} {
			for _, stage := range []string{"live", "reconcile", "restart"} {
				t.Run(fmt.Sprintf("runtime=%t/fix=%t/%s", runtimeName, fix, stage), func(t *testing.T) {
					r, path, current, archive := newArchivedRecheckOwner(t, runtimeName)
					before, _ := json.Marshal(archive)
					if stage == "restart" {
						if err := r.manager.storage.Close(); err != nil {
							t.Fatal(err)
						}
						s, err := storage.NewStorage(path)
						if err != nil {
							t.Fatal(err)
						}
						r.manager.storage = s
					}
					if stage != "live" {
						r.reconcileOrphans()
					}
					ack, err := r.RecheckEntry(context.Background(), retiredRecheckAlias, fix)
					if err != nil {
						t.Fatalf("history blocked current owner's manual action: %v", err)
					}
					r.runWG.Wait()
					if ack.Archived || ack.EntryName != current.GetFolder() || ack.OwnerInfoHash != current.InfoHash || ack.ActiveRunID != "recheck-"+current.GetFolder() {
						t.Fatalf("acknowledgement belongs to historical owner: %+v", ack)
					}
					h, err := r.manager.storage.GetEntryHealth(current.GetFolder())
					if err != nil || h.Archived || h.OwnerInfoHash != current.InfoHash || h.Status != storage.HealthHealthy || h.ActiveRunID != "" {
						t.Fatalf("current probe did not finish: %+v %v", h, err)
					}
					old, err := r.manager.storage.GetEntryHealth(archive.EntryName)
					after, _ := json.Marshal(old)
					if err != nil || !bytes.Equal(before, after) {
						t.Fatalf("manual action altered immutable history: %+v %v", old, err)
					}
					if _, err := r.manager.storage.Get("stream-failover-hash"); !storage.IsEntryNotFound(err) {
						t.Fatalf("manual action undid parent retirement: %v", err)
					}
				})
			}
		}
	}
}

func TestHealthRecheckStillRejectsCurrentOwnerActiveRun(t *testing.T) {
	r, _, e, _ := newArchivedRecheckOwner(t, true)
	item, err := r.manager.storage.GetEntryItem(e.GetFolder())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.manager.storage.ResolveHealthEntry(item.Name, config.Get().FolderNaming); err != nil {
		t.Fatal(err)
	}
	if e.Name == item.Name {
		t.Fatal("current-owner fixture must include a distinct runtime alias")
	}
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	var calls atomic.Int32
	r.manager.clients.Store("primary", &healthEvidenceClient{check: func(context.Context, string) error {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return nil
	}})
	go func() {
		defer close(done)
		r.probeEntry(context.Background(), "current-owner-running", &candidate{name: item.Name}, newHealCache(), RepairRunOptions{}, false)
	}()
	defer func() { unblock(); <-done; r.runWG.Wait() }()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("current probe did not reach the provider check")
	}
	old, err := r.manager.storage.GetEntryHealth(e.Name)
	if err != nil || !old.Archived || old.ActiveRunID != "retired-owner-running" {
		t.Fatalf("runtime alias must still target historical work: %+v %v", old, err)
	}
	for _, name := range []string{item.Name, e.Name} {
		for _, fix := range []bool{false, true} {
			if _, err := r.RecheckEntry(context.Background(), name, fix); err == nil || !strings.Contains(err.Error(), "current-owner-running") {
				t.Fatalf("live probe was not protected for %q fix=%t: %v", name, fix, err)
			}
		}
	}
	h, err := r.manager.storage.GetEntryHealth(item.Name)
	if err != nil || h.OwnerInfoHash != e.InfoHash || h.ActiveRunID != "current-owner-running" || calls.Load() != 1 {
		t.Fatalf("active run ownership changed: %+v calls=%d err=%v", h, calls.Load(), err)
	}
}
