package manager

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

type lateCleanupAccountProvider struct {
	common.Client
	key    string
	delete func(string) error
}

func (p *lateCleanupAccountProvider) Config() config.Debrid {
	return config.Debrid{Name: "late", APIKey: p.key}
}
func (p *lateCleanupAccountProvider) DeleteTorrent(id string) error { return p.delete(id) }

// Adopt ordinary queue cleanup before the independently authorized main
// placement becomes an administrative cleanup obligation.
func prepareLateCleanupAccount(t *testing.T, m *Manager, placementBound bool) *storage.Entry {
	t.Helper()
	queued := &storage.Entry{InfoHash: "late-account-work", Name: "release", Protocol: config.ProtocolTorrent, SavePath: m.config.DownloadFolder, State: storage.EntryStateDownloading}
	main := *queued
	main.Providers = map[string]*storage.ProviderEntry{"late": {Provider: "late", ID: "same-id"}}
	if err := m.storage.AddOrUpdate(&main); err != nil {
		t.Fatal(err)
	}
	if placementBound {
		witness := &storage.Entry{InfoHash: "account-witness", State: storage.EntryStateDownloading}
		if err := m.storage.AddQueue(witness); err != nil {
			t.Fatal(err)
		}
		account := submissionAccount((&lateCleanupAccountProvider{key: "old-account"}).Config())
		i, _, err := m.storage.BeginProviderSubmission(witness, "late", account, false)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.storage.RecordProviderSubmissionID(witness, i.Attempt, "same-id", false); err != nil {
			t.Fatal(err)
		}
	}
	path := queued.DownloadPath()
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "payload"), []byte("unowned staging must remain"), 0600); err != nil {
		t.Fatal(err)
	}
	i := cleanupIntent(t, m, queued, false)
	if err := m.runCleanupIntent(i, false); err == nil {
		t.Fatal("unowned staging did not block initial queue cleanup")
	}
	rows, err := m.storage.QueuedDeletionIntents()
	if err != nil || len(rows) != 1 || rows[0].Work.Accounts["late"] != "" {
		t.Fatal("late provider was already bound", err)
	}
	return queued
}

func TestCleanupLateAdministrativeAccountBindingBlocksReplacement(t *testing.T) {
	for _, placementBound := range []bool{false, true} {
		name := "legacy"
		if placementBound {
			name = "durable-placement-fence"
		}
		t.Run(name, func(t *testing.T) {
			m := cleanupManager(t, t.TempDir())
			e := prepareLateCleanupAccount(t, m, placementBound)
			offline := errors.New("provider unavailable")
			old := &lateCleanupAccountProvider{key: "old-account"}
			expected := submissionAccount(old.Config())
			oldCalls := 0
			old.delete = func(id string) error {
				oldCalls++
				rows, err := m.storage.QueuedDeletionIntents()
				if err != nil || len(rows) != 1 || rows[0].Work.Accounts["late"] != expected {
					t.Error("DELETE began before the account binding checkpoint", err)
				}
				if id != "same-id" {
					t.Error("saved administrative scope changed")
				}
				return offline
			}
			m.clients.Store("late", old)
			if err := m.DeleteEntry(e.InfoHash, true); !errors.Is(err, offline) {
				t.Fatal("initial explicit retry did not reach its provider", err)
			}
			if oldCalls != 1 {
				t.Fatal("initial DELETE count", oldCalls)
			}
			newCalls := 0
			m.clients.Store("late", &lateCleanupAccountProvider{key: "different-account", delete: func(string) error { newCalls++; return nil }})
			if err := m.DeleteEntry(e.InfoHash, true); err == nil {
				t.Fatal("replacement account accepted")
			}
			if newCalls != 0 {
				t.Fatal("administrative retry crossed account boundary")
			}
			rows, err := m.storage.QueuedDeletionIntents()
			if err != nil || len(rows) != 1 || rows[0].Work.Accounts["late"] != expected || rows[0].Work.Reason != "provider_account_changed" {
				t.Fatal("saved binding was lost or replaced", err)
			}
			if err := m.workflowAvailable(); err != nil {
				t.Fatal("account mismatch incorrectly suspended all admissions", err)
			}
			if b, err := os.ReadFile(filepath.Join(e.DownloadPath(), "payload")); err != nil || string(b) != "unowned staging must remain" {
				t.Fatal("blocked retry changed staging", err)
			}
			m.clients.Store("late", old)
			if err := m.DeleteEntry(e.InfoHash, true); !errors.Is(err, offline) || oldCalls != 2 {
				t.Fatal("original bound account could not retry", err, oldCalls)
			}
		})
	}
}

func TestCleanupLateAdministrativeAccountSurvivesRestart(t *testing.T) {
	root := t.TempDir()
	m := cleanupManager(t, root)
	e := prepareLateCleanupAccount(t, m, false)
	old := &lateCleanupAccountProvider{key: "old-account", delete: func(string) error { return errors.New("provider unavailable") }}
	m.clients.Store("late", old)
	if err := m.DeleteEntry(e.InfoHash, true); err == nil {
		t.Fatal("expected retained administrative failure")
	}
	if err := m.storage.Close(); err != nil {
		t.Fatal(err)
	}
	m = cleanupManager(t, root)
	newCalls := 0
	m.clients.Store("late", &lateCleanupAccountProvider{key: "different-account", delete: func(string) error { newCalls++; return nil }})
	requestCleanupRetry(t, m, "reconcile")
	m.retryCleanupWorkflows()
	if err := m.DeleteEntry(e.InfoHash, true); err == nil {
		t.Fatal("restart rebound administrative account")
	}
	rows, err := m.storage.QueuedDeletionIntents()
	if err != nil || len(rows) != 1 || rows[0].Work.Accounts["late"] != submissionAccount(old.Config()) || newCalls != 0 {
		t.Fatal("restart/operator retry crossed saved account fence", err)
	}
}

func TestCleanupOperatorResolutionCannotBindLateAdministrativeAccount(t *testing.T) {
	m := cleanupManager(t, t.TempDir())
	e := prepareLateCleanupAccount(t, m, false)
	paused := errors.New("pause before provider effect")
	if err := m.storage.DeleteWithCleanup(e.InfoHash, func(main *storage.Entry) error {
		if err := m.storage.PrepareMainDeletionPlacements(main); err != nil {
			return err
		}
		return paused
	}); !errors.Is(err, paused) {
		t.Fatal(err)
	}
	calls := 0
	m.clients.Store("late", &lateCleanupAccountProvider{key: "old-account", delete: func(string) error { calls++; return nil }})
	for _, action := range []string{"retry", "reconcile"} {
		requestCleanupRetry(t, m, action)
		m.retryCleanupWorkflows()
		rows, err := m.storage.QueuedDeletionIntents()
		if err != nil || len(rows) != 1 || rows[0].Work.Accounts["late"] != "" || calls != 0 {
			t.Fatal("operator action granted new account or DELETE authority", action, err)
		}
	}
	admins, err := m.storage.MainDeletionPlacementIntents()
	if err != nil || len(admins) != 1 {
		t.Fatal("operator action discarded unbound administrative scope", err)
	}
}

func TestCleanupLateAccountChangeAfterCheckpointPreventsDispatch(t *testing.T) {
	m := cleanupManager(t, t.TempDir())
	e := prepareLateCleanupAccount(t, m, false)
	calls := 0
	old := &lateCleanupAccountProvider{key: "old-account", delete: func(string) error { calls++; return nil }}
	m.clients.Store("late", old)
	m.cleanupPhaseForTest = func(stage string) {
		if stage == "administrative_accounts_bound" {
			m.clients.Store("late", &lateCleanupAccountProvider{key: "different-account", delete: func(string) error { calls++; return nil }})
		}
	}
	if err := m.DeleteEntry(e.InfoHash, true); err == nil {
		t.Fatal("changed account reached dispatch")
	}
	rows, err := m.storage.QueuedDeletionIntents()
	if err != nil || len(rows) != 1 || rows[0].Work.Accounts["late"] != submissionAccount(old.Config()) || calls != 0 {
		t.Fatal("post-checkpoint account change crossed the fence", err, calls)
	}
}

func TestCleanupLateAccountCrashHelper(t *testing.T) {
	root := os.Getenv("TESSARR_LATE_ACCOUNT_CRASH_ROOT")
	if root == "" {
		return
	}
	m := cleanupManager(t, root)
	e := prepareLateCleanupAccount(t, m, false)
	m.clients.Store("late", &lateCleanupAccountProvider{key: "old-account", delete: func(string) error {
		f, err := os.Create(filepath.Join(root, "old-account-delete"))
		if err != nil {
			return err
		}
		defer f.Close()
		return f.Sync()
	}})
	m.cleanupPhaseForTest = func(stage string) {
		if stage == os.Getenv("TESSARR_LATE_ACCOUNT_CRASH_PHASE") {
			os.Exit(86)
		}
	}
	if err := m.DeleteEntry(e.InfoHash, true); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash boundary was not reached")
}

func TestCleanupLateAccountCheckpointSurvivesProcessDeath(t *testing.T) {
	for _, phase := range []string{"administrative_accounts_bound", "administrative_provider_done"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestCleanupLateAccountCrashHelper$")
			cmd.Env = append(os.Environ(), "TESSARR_LATE_ACCOUNT_CRASH_ROOT="+root, "TESSARR_LATE_ACCOUNT_CRASH_PHASE="+phase)
			output, err := cmd.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 86 {
				t.Fatalf("child: %v %s", err, output)
			}
			_, deleteErr := os.Stat(filepath.Join(root, "old-account-delete"))
			if phase == "administrative_accounts_bound" && !os.IsNotExist(deleteErr) {
				t.Fatal("provider DELETE preceded binding crash checkpoint")
			}
			if phase == "administrative_provider_done" && deleteErr != nil {
				t.Fatal("provider effect boundary was not exercised", deleteErr)
			}
			m := cleanupManager(t, root)
			rows, err := m.storage.QueuedDeletionIntents()
			expected := submissionAccount((&lateCleanupAccountProvider{key: "old-account"}).Config())
			if err != nil || len(rows) != 1 || rows[0].Work.Accounts["late"] != expected {
				t.Fatal("process death lost the durable late-account binding", err)
			}
			newCalls := 0
			m.clients.Store("late", &lateCleanupAccountProvider{key: "different-account", delete: func(string) error { newCalls++; return nil }})
			_, fatal := m.recoverQueuedDeletions()
			if fatal != nil {
				t.Fatal(fatal)
			}
			if err := m.DeleteEntry("late-account-work", true); err == nil {
				t.Fatal("explicit retry after crash changed accounts")
			}
			if newCalls != 0 {
				t.Fatal("crash recovery issued DELETE in the replacement account")
			}
			admins, err := m.storage.MainDeletionPlacementIntents()
			if err != nil || len(admins) != 1 {
				t.Fatal("account mismatch discarded the administrative obligation", err)
			}
		})
	}
}
