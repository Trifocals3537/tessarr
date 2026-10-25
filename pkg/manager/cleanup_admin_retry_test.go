package manager

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

type offlineCleanupProvider struct{ *cleanupProvider }

func (p *offlineCleanupProvider) DeleteTorrent(string) error {
	return errors.New("provider unavailable")
}

func prepareAdministrativeRetry(t *testing.T, m *Manager, preserve bool) (*storage.Entry, string) {
	t.Helper()
	e := &storage.Entry{InfoHash: "admin-work", Name: "release", Protocol: config.ProtocolTorrent, SavePath: m.config.DownloadFolder, State: storage.EntryStateDownloading, Providers: map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "remote-id"}}}
	output, _, err := claimTorrentEntryDirectory(m.config.DownloadFolder, e, torrentLegacyProof{})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, "payload"), []byte("preserve policy"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := m.queue.Add(e); err != nil {
		t.Fatal(err)
	}
	if err := m.storage.AddOrUpdate(e); err != nil {
		t.Fatal(err)
	}
	if preserve {
		if _, err := m.storage.PrepareQBitQueuedDeletion(e.InfoHash, false); err != nil {
			t.Fatal(err)
		}
	}
	m.clients.Store("fake", &offlineCleanupProvider{&cleanupProvider{root: filepath.Dir(m.config.DownloadFolder), key: "account-one"}})
	if err := m.DeleteEntry(e.InfoHash, true); err == nil {
		t.Fatal("initial administrative failure not exercised")
	}
	m.retryCleanupWorkflows()
	return e, output
}

func TestCleanupAdministrativeRetryPreservesClientFilesAndScope(t *testing.T) {
	root := t.TempDir()
	m := cleanupManager(t, root)
	e, output := prepareAdministrativeRetry(t, m, true)
	rows, err := m.storage.QueuedDeletionIntents()
	if err != nil || len(rows) != 1 || !rows[0].PreserveFiles || rows[0].PlacementScope != storage.QueuePlacementScopeTransferOnly {
		t.Fatal("immutable client scope changed", err)
	}
	m.clients.Store("fake", &cleanupProvider{root: root, key: "account-one"})
	if err := m.DeleteEntry(e.InfoHash, true); err != nil {
		t.Fatal(err)
	}
	if b, err := os.ReadFile(filepath.Join(output, "payload")); err != nil || string(b) != "preserve policy" {
		t.Fatal("preserving client cleanup removed files", err)
	}
	if cleanupDeleteCount(root) != 1 {
		t.Fatal("separate administrative placement was not cleaned exactly once")
	}
	rows, err = m.storage.QueuedDeletionIntents()
	if err != nil || len(rows) != 0 {
		t.Fatal("queue cleanup not completed", err)
	}
}

func TestCleanupAdministrativeRetryRejectsChangedAccount(t *testing.T) {
	root := t.TempDir()
	m := cleanupManager(t, root)
	e, output := prepareAdministrativeRetry(t, m, false)
	m.clients.Store("fake", &cleanupProvider{root: root, key: "different-account"})
	if err := m.DeleteEntry(e.InfoHash, true); err == nil {
		t.Fatal("account change accepted")
	}
	if cleanupDeleteCount(root) != 0 {
		t.Fatal("provider mutation crossed account fence")
	}
	if _, err := os.Stat(filepath.Join(output, "payload")); err != nil {
		t.Fatal("files removed despite account change", err)
	}
	admins, err := m.storage.MainDeletionPlacementIntents()
	if err != nil || len(admins) != 1 {
		t.Fatal("administrative obligation lost", err)
	}
}

func TestCleanupAdministrativeCrashHelper(t *testing.T) {
	root := os.Getenv("TESSARR_ADMIN_CLEANUP_CRASH_ROOT")
	if root == "" {
		return
	}
	m := cleanupManager(t, root)
	e, _ := prepareAdministrativeRetry(t, m, false)
	m.clients.Store("fake", &cleanupProvider{root: root, key: "account-one"})
	m.cleanupPhaseForTest = func(stage string) {
		if stage == os.Getenv("TESSARR_ADMIN_CLEANUP_CRASH_PHASE") {
			os.Exit(86)
		}
	}
	if err := m.DeleteEntry(e.InfoHash, true); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash boundary not reached")
}

func TestCleanupAdministrativeRetryRecoversCrashWithoutReplayingDelete(t *testing.T) {
	for _, phase := range []string{"administrative_provider_done", "administrative_checkpoint"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestCleanupAdministrativeCrashHelper$")
			cmd.Env = append(os.Environ(), "TESSARR_ADMIN_CLEANUP_CRASH_ROOT="+root, "TESSARR_ADMIN_CLEANUP_CRASH_PHASE="+phase)
			out, err := cmd.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 86 {
				t.Fatalf("child: %v %s", err, out)
			}
			m := cleanupManager(t, root)
			residual, fatal := m.recoverQueuedDeletions()
			if residual != nil || fatal != nil {
				t.Fatalf("recovery: %v %v", residual, fatal)
			}
			if cleanupDeleteCount(root) != 1 {
				t.Fatal("startup replayed uncertain administrative DELETE")
			}
			rows, err := m.storage.QueuedDeletionIntents()
			if err != nil || len(rows) != 0 {
				t.Fatal("queue cleanup remains", err)
			}
			admins, err := m.storage.MainDeletionPlacementIntents()
			if err != nil || len(admins) != 0 {
				t.Fatal("admin cleanup remains", err)
			}
			if _, err := os.Stat(filepath.Join(root, "staging/release")); !os.IsNotExist(err) {
				t.Fatal("owned staging remains", err)
			}
			receipts, err := m.storage.CleanupReceipts(100)
			if err != nil || len(receipts) != 1 || receipts[0].IntentPresent || len(receipts[0].Work.Decisions) != 1 {
				t.Fatal("operator/admin decision checkpoint lost", err)
			}
		})
	}
}
