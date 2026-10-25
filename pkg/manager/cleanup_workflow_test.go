package manager

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/workflow"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

type cleanupProvider struct {
	debrid.Client
	root, key string
}

func (p *cleanupProvider) Config() config.Debrid { return config.Debrid{Name: "fake", APIKey: p.key} }
func (p *cleanupProvider) DeleteTorrent(id string) error {
	n := cleanupDeleteCount(p.root) + 1
	if err := os.WriteFile(filepath.Join(p.root, "delete-count"), []byte(strconv.Itoa(n)), 0600); err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(p.root, "provider-absent"), os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
func (p *cleanupProvider) GetTorrent(id string) (*debridTypes.Torrent, error) {
	if _, err := os.Stat(filepath.Join(p.root, "provider-absent")); err == nil {
		return nil, customerror.TorrentNotFoundError
	}
	return &debridTypes.Torrent{Id: id, InfoHash: "cleanup-work"}, nil
}
func (p *cleanupProvider) TorrentPresenceContext(ctx context.Context, id string) (debrid.TorrentPresence, error) {
	_, err := p.GetTorrent(id)
	if errors.Is(err, customerror.TorrentNotFoundError) {
		return debrid.TorrentAbsent, nil
	}
	if err != nil {
		return debrid.TorrentPresenceUnknown, err
	}
	return debrid.TorrentPresent, nil
}
func cleanupDeleteCount(root string) int {
	b, _ := os.ReadFile(filepath.Join(root, "delete-count"))
	n, _ := strconv.Atoi(string(b))
	return n
}
func cleanupManager(t *testing.T, root string) *Manager {
	t.Helper()
	old := config.GetMainPath()
	if err := config.SetConfigPath(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { config.SetConfigPath(old) })
	s, err := storage.NewStorage(filepath.Join(root, "db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	config.Get().DownloadFolder = filepath.Join(root, "staging")
	os.MkdirAll(config.Get().DownloadFolder, 0755)
	m := &Manager{storage: s, queue: newQueue(s, ""), clients: xsync.NewMap[string, debrid.Client](), logger: zerolog.Nop(), config: config.Get(), ctx: context.Background()}
	m.clients.Store("fake", &cleanupProvider{root: root, key: "account-one"})
	return m
}
func cleanupIntent(t *testing.T, m *Manager, e *storage.Entry, placements bool) *storage.QueueDeletionIntent {
	t.Helper()
	if err := m.storage.AddQueue(e); err != nil {
		t.Fatal(err)
	}
	i, err := m.storage.PrepareQueuedDeletion(e.InfoHash, placements)
	if err != nil {
		t.Fatal(err)
	}
	return i
}
func requestCleanupRetry(t *testing.T, m *Manager, action string) {
	t.Helper()
	rows, err := m.storage.QueuedDeletionIntents()
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range rows {
		if i.Work != nil {
			if err := m.ResolveCleanupWorkflow(i.InfoHash, i.QueueIncarnation, i.Work.Revision, uuid.NewString(), action); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestCleanupManagerCrashHelper(t *testing.T) {
	root := os.Getenv("TESSARR_CLEANUP_MANAGER_ROOT")
	if root == "" {
		return
	}
	m := cleanupManager(t, root)
	e := &storage.Entry{InfoHash: "cleanup-work", Protocol: config.ProtocolTorrent, Name: "release", SavePath: m.config.DownloadFolder, State: storage.EntryStateDownloading, Providers: map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "remote-id"}}}
	output, _, err := claimTorrentEntryDirectory(m.config.DownloadFolder, e, torrentLegacyProof{})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(output, "payload"), []byte("keep until authorized cleanup"), 0600); err != nil {
		t.Fatal(err)
	}
	i := cleanupIntent(t, m, e, true)
	m.cleanupPhaseForTest = func(stage string) {
		if stage == os.Getenv("TESSARR_CLEANUP_MANAGER_PHASE") {
			os.Exit(86)
		}
	}
	if err := m.runCleanupIntent(i, false); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash boundary not reached")
}
func TestCleanupManagerRecoversCrashBoundaries(t *testing.T) {
	for _, phase := range []string{"claimed", "provider_intent", "provider_done", "local_done"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestCleanupManagerCrashHelper$")
			cmd.Env = append(os.Environ(), "TESSARR_CLEANUP_MANAGER_ROOT="+root, "TESSARR_CLEANUP_MANAGER_PHASE="+phase)
			b, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 86 {
				t.Fatalf("child: %v %s", err, b)
			}
			m := cleanupManager(t, root)
			residual, fatal := m.recoverQueuedDeletions()
			if fatal != nil {
				t.Fatal(fatal)
			}
			if phase == "provider_intent" {
				if residual == nil || cleanupDeleteCount(root) != 0 {
					t.Fatal("uncertain DELETE replayed")
				}
				rows, _ := m.storage.QueuedDeletionIntents()
				if rows[0].Work.State != workflow.Blocked {
					t.Fatal("uncertainty was not visible")
				}
				if _, err := os.Stat(filepath.Join(root, "staging/release/payload")); err != nil {
					t.Fatal("files removed with uncertain provider effect")
				}
				requestCleanupRetry(t, m, "retry")
				m.retryCleanupWorkflows()
				if cleanupDeleteCount(root) != 0 {
					t.Fatal("operator retry blindly repeated uncertain DELETE")
				}
				// A separately verified provider removal is reconciled by an exact lookup.
				os.WriteFile(filepath.Join(root, "provider-absent"), nil, 0600)
				requestCleanupRetry(t, m, "retry")
				m.retryCleanupWorkflows()
			} else if residual != nil {
				t.Fatal(residual)
			}
			rows, err := m.storage.QueuedDeletionIntents()
			if err != nil || len(rows) != 0 {
				t.Fatalf("retained work: %d %v", len(rows), err)
			}
			want := 1
			if phase == "provider_intent" {
				want = 0
			}
			if cleanupDeleteCount(root) != want {
				t.Fatal("provider mutation duplicated")
			}
			if _, err := os.Stat(filepath.Join(root, "staging/release")); !os.IsNotExist(err) {
				t.Fatal("owned output not cleaned")
			}
			receipts, err := m.storage.CleanupReceipts(100)
			if err != nil || len(receipts) != 1 || receipts[0].IntentPresent {
				t.Fatal("completion receipt missing")
			}
		})
	}
}

func TestCleanupOperatorReconcilesOnlyVerifiedLegacyLeftovers(t *testing.T) {
	for _, kind := range []string{"absent-trailing-space", "empty-owned-nzb", "foreign-owner", "nonempty-nzb", "portable-alias", "torrent-quarantine"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			m := cleanupManager(t, root)
			e := &storage.Entry{InfoHash: uuid.NewString(), Protocol: config.ProtocolTorrent, Name: "Season Pack ", SavePath: m.config.DownloadFolder, State: storage.EntryStateDownloading}
			var sentinel string
			if kind == "portable-alias" {
				sentinel = filepath.Join(m.config.DownloadFolder, "Season Pack")
				os.MkdirAll(sentinel, 0755)
				os.WriteFile(filepath.Join(sentinel, "keep"), []byte("library"), 0600)
			}
			if kind == "torrent-quarantine" {
				sentinel = filepath.Join(m.config.DownloadFolder, torrentQuarantinePrefixForEntry(e.InfoHash, e.OutputComponent())+"test")
				os.MkdirAll(sentinel, 0755)
				os.WriteFile(filepath.Join(sentinel, "keep"), []byte("library"), 0600)
			}
			if kind == "empty-owned-nzb" || kind == "foreign-owner" || kind == "nonempty-nzb" {
				e.Protocol = config.ProtocolNZB
				e.Name = "Movie.1080p.DDP.5.1"
				e.OutputName = e.Name + ".x264-group"
				sentinel = e.DownloadPath()
				os.MkdirAll(sentinel, 0755)
				owner := e.InfoHash
				if kind == "foreign-owner" {
					owner = uuid.NewString()
				}
				os.WriteFile(filepath.Join(sentinel, legacyUsenetOwnerMarkerName), []byte(owner+"\n"), 0600)
				if kind == "nonempty-nzb" {
					os.WriteFile(filepath.Join(sentinel, "keep"), []byte("library"), 0600)
				}
			}
			metadata := filepath.Join(root, "usenet/meta")
			os.MkdirAll(metadata, 0755)
			os.WriteFile(filepath.Join(metadata, e.InfoHash+".meta"), []byte("preserve metadata"), 0600)
			i := cleanupIntent(t, m, e, false)
			if err := m.runCleanupIntent(i, false); err == nil {
				t.Fatal("legacy path did not remain blocked before operator review")
			}
			requestCleanupRetry(t, m, "reconcile")
			m.retryCleanupWorkflows()
			rows, err := m.storage.QueuedDeletionIntents()
			if err != nil {
				t.Fatal(err)
			}
			safe := kind == "absent-trailing-space" || kind == "empty-owned-nzb"
			if safe && len(rows) != 0 {
				t.Fatal("verified leftovers not reconciled")
			}
			if !safe && (len(rows) != 1 || rows[0].Work.State != workflow.Blocked) {
				t.Fatal("unsafe resolution was not blocked")
			}
			if !safe {
				if _, err := os.Stat(sentinel); err != nil {
					t.Fatal("foreign/nonempty output removed")
				}
			}
			b, _ := os.ReadFile(filepath.Join(metadata, e.InfoHash+".meta"))
			if string(b) != "preserve metadata" {
				t.Fatal("Usenet metadata changed")
			}
		})
	}
}

func TestCleanupAccountChangeAndPersistedBackoff(t *testing.T) {
	root := t.TempDir()
	m := cleanupManager(t, root)
	e := &storage.Entry{InfoHash: "cleanup-work", Protocol: config.ProtocolTorrent, Name: "release", SavePath: m.config.DownloadFolder, State: storage.EntryStateDownloading, Providers: map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "remote-id"}}}
	i := cleanupIntent(t, m, e, true)
	i, err := m.storage.ClaimCleanup(i.InfoHash, i.QueueIncarnation, m.queue.cleanupIdentity(), m.cleanupAccounts(i), time.Now(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storage.ReleaseCleanup(i.InfoHash, i.QueueIncarnation, i.Work.Token, "temporary", false, time.Now()); err != nil {
		t.Fatal(err)
	}
	m.retryCleanupWorkflows()
	if cleanupDeleteCount(root) != 0 {
		t.Fatal("deadline was not honored")
	}
	provider := &cleanupProvider{root: root, key: "different-account"}
	m.clients.Store("fake", provider)
	requestCleanupRetry(t, m, "retry")
	m.retryCleanupWorkflows()
	rows, _ := m.storage.QueuedDeletionIntents()
	if len(rows) != 1 || rows[0].Work.Reason != "provider_account_changed" || cleanupDeleteCount(root) != 0 {
		t.Fatal("account fence bypassed")
	}
}

type panickingCleanupProvider struct{ *cleanupProvider }

func (p *panickingCleanupProvider) DeleteTorrent(string) error { panic("secret-provider-response") }
func TestCleanupPanicRetainsRecoverableWorkWithoutPersistingPayload(t *testing.T) {
	root := t.TempDir()
	m := cleanupManager(t, root)
	m.clients.Store("fake", &panickingCleanupProvider{&cleanupProvider{root: root, key: "account-one"}})
	e := &storage.Entry{InfoHash: "cleanup-work", Name: "release", SavePath: m.config.DownloadFolder, State: storage.EntryStateDownloading, Providers: map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "remote-id"}}}
	i := cleanupIntent(t, m, e, true)
	if err := m.runCleanupIntent(i, false); err == nil {
		t.Fatal("panic reported success")
	}
	rows, err := m.storage.QueuedDeletionIntents()
	if err != nil || len(rows) != 1 || rows[0].Work.State != workflow.RetryWait || !rows[0].Work.EffectStarted || rows[0].Work.Reason != "worker_panicked" {
		t.Fatal("panic lost recovery ownership")
	}
}

func TestCleanupBackgroundRunnerCompletesDueWorkWithoutRestart(t *testing.T) {
	root := t.TempDir()
	m := cleanupManager(t, root)
	ctx, cancel := context.WithCancel(context.Background())
	m.ctx = ctx
	t.Cleanup(func() { cancel(); m.background.Wait() })
	e := &storage.Entry{InfoHash: "background-cleanup", Name: "release", Protocol: config.ProtocolTorrent, SavePath: m.config.DownloadFolder, State: storage.EntryStateDownloading}
	cleanupIntent(t, m, e, false)
	m.startCleanupWorkflows()
	deadline := time.After(8 * time.Second)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline:
			t.Fatal("cleanup did not run without restart")
		case <-ticker.C:
			rows, err := m.storage.QueuedDeletionIntents()
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) == 0 {
				return
			}
		}
	}
}

func TestCleanupCancellationAfterDurableCompletionIsSuccessful(t *testing.T) {
	m := cleanupManager(t, t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.ctx = ctx
	m.cleanupPhaseForTest = func(stage string) {
		if stage == "local_done" {
			cancel()
		}
	}
	e := &storage.Entry{InfoHash: "completed-before-shutdown", Name: "release", Protocol: config.ProtocolTorrent, SavePath: m.config.DownloadFolder, State: storage.EntryStateDownloading}
	i := cleanupIntent(t, m, e, false)
	if err := m.runCleanupIntent(i, false); err != nil {
		t.Fatalf("durable completion was reported as interrupted: %v", err)
	}
	if ctx.Err() == nil {
		t.Fatal("shutdown boundary was not exercised")
	}
	if err := m.workflowAvailable(); err != nil {
		t.Fatalf("completed cleanup suspended admissions: %v", err)
	}
	rows, err := m.storage.QueuedDeletionIntents()
	if err != nil || len(rows) != 0 {
		t.Fatalf("completed cleanup retained an intent: %v", err)
	}
	receipts, err := m.storage.CleanupReceipts(100)
	if err != nil || len(receipts) != 1 || receipts[0].IntentPresent {
		t.Fatalf("durable completion receipt missing: %v", err)
	}
}
