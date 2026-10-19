package manager

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

const (
	migrationSourceProvider = "torbox-primary"
	migrationTargetProvider = "torbox-secondary"
	migrationSourceID       = "17"
	migrationTargetID       = "99"
)

type migrationOwnershipHarness struct {
	manager            *Manager
	entry              *storage.Entry
	dbPath             string
	sourceDeletes      chan string
	targetDeletes      chan string
	sourceDeleteHook   func(string) error
	sourceErr          error
	targetSubmitErr    error
	targetCheckErr     error
	targetCheckHook    func() error
	targetGetErr       error
	targetGetHook      func() error
	targetRemoteID     string
	targetRemoteStatus debridTypes.TorrentStatus
	submitCalls        atomic.Int32
	targetGetCalls     atomic.Int32
}

func newMigrationOwnershipHarness(t *testing.T) *migrationOwnershipHarness {
	t.Helper()

	testRoot := t.TempDir()
	config.SetConfigPath(testRoot)
	dbPath := filepath.Join(testRoot, "db")
	store, err := storage.NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	harness := &migrationOwnershipHarness{
		sourceDeletes:      make(chan string, 4),
		targetDeletes:      make(chan string, 4),
		dbPath:             dbPath,
		targetRemoteID:     migrationTargetID,
		targetRemoteStatus: debridTypes.TorrentStatusDownloaded,
	}

	sourceClient := &routingTestClient{
		cfg: config.Debrid{Provider: "torbox", Name: migrationSourceProvider},
		delete: func(id string) error {
			harness.sourceDeletes <- id
			if harness.sourceDeleteHook != nil {
				if err := harness.sourceDeleteHook(id); err != nil {
					return err
				}
			}
			return harness.sourceErr
		},
	}
	targetClient := &routingTestClient{
		cfg: config.Debrid{Provider: "torbox", Name: migrationTargetProvider},
		submit: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			harness.submitCalls.Add(1)
			torrent.Id = migrationTargetID
			torrent.Debrid = migrationTargetProvider
			torrent.Status = debridTypes.TorrentStatusDownloaded
			torrent.Files = map[string]debridTypes.File{
				"movie.mkv": {
					TorrentId: migrationTargetID,
					Id:        "target-file",
					Name:      "movie.mkv",
					Path:      "movie.mkv",
					Link:      "https://target.invalid/movie.mkv",
					Size:      1024,
				},
			}
			return torrent, harness.targetSubmitErr
		},
		check: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			if harness.targetCheckHook != nil {
				if err := harness.targetCheckHook(); err != nil {
					return torrent, err
				}
			}
			return torrent, harness.targetCheckErr
		},
		get: func(_ string) (*debridTypes.Torrent, error) {
			harness.targetGetCalls.Add(1)
			if harness.targetGetHook != nil {
				if err := harness.targetGetHook(); err != nil {
					return nil, err
				}
			}
			if harness.targetGetErr != nil {
				return nil, harness.targetGetErr
			}
			return &debridTypes.Torrent{
				Id:     harness.targetRemoteID,
				Debrid: migrationTargetProvider,
				Status: harness.targetRemoteStatus,
			}, nil
		},
		delete: func(id string) error {
			harness.targetDeletes <- id
			return nil
		},
	}

	clients := xsync.NewMap[string, debrid.Client]()
	clients.Store(migrationSourceProvider, sourceClient)
	clients.Store(migrationTargetProvider, targetClient)
	cfg := &config.Config{Debrids: []config.Debrid{
		{Provider: "torbox", Name: migrationSourceProvider},
		{Provider: "torbox", Name: migrationTargetProvider},
	}}
	manager := &Manager{
		storage:       store,
		clients:       clients,
		config:        cfg,
		logger:        zerolog.Nop(),
		migrationJobs: xsync.NewMap[string, *storage.SwitcherJob](),
	}
	manager.entry = NewEntryCache(manager)
	manager.fixer = &Fixer{manager: manager}
	harness.manager = manager
	t.Cleanup(func() {
		harness.manager.stopAcceptingBackgroundWork()
		harness.manager.background.Wait()
		if err := harness.manager.storage.Close(); err != nil {
			t.Errorf("close migration test storage: %v", err)
		}
	})

	infoHash := "0123456789abcdef0123456789abcdef01234567"
	entry := &storage.Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       infoHash,
		Name:           "movie.mkv",
		Magnet:         "magnet:?xt=urn:btih:" + infoHash + "&dn=movie.mkv",
		ActiveProvider: migrationSourceProvider,
		Providers: map[string]*storage.ProviderEntry{
			migrationSourceProvider: {
				Provider: migrationSourceProvider,
				ID:       migrationSourceID,
				Status:   debridTypes.TorrentStatusDownloaded,
				Files: map[string]*storage.ProviderFile{
					"movie.mkv": {
						Id:   "source-file",
						Path: "movie.mkv",
						Link: "https://source.invalid/movie.mkv",
					},
				},
			},
		},
		Files: map[string]*storage.File{
			"movie.mkv": {
				Name:     "movie.mkv",
				Path:     "movie.mkv",
				Size:     1024,
				InfoHash: infoHash,
			},
		},
	}
	if err := store.AddOrUpdateDurable(entry); err != nil {
		t.Fatal(err)
	}
	harness.entry = entry
	return harness
}

func (h *migrationOwnershipHarness) execute(keepOld bool) *storage.SwitcherJob {
	job := &storage.SwitcherJob{
		ID:             "migration-test",
		InfoHash:       h.entry.InfoHash,
		SourceProvider: migrationSourceProvider,
		TargetProvider: migrationTargetProvider,
		Status:         storage.SwitcherStatusPending,
		CreatedAt:      time.Now(),
		KeepOld:        keepOld,
	}
	h.manager.executeMigration(job, h.entry)
	return job
}

func assertNoProviderDelete(t *testing.T, deletes <-chan string, provider string) {
	t.Helper()
	select {
	case id := <-deletes:
		t.Fatalf("unexpected delete on %s for torrent ID %q", provider, id)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestMigrationCleanupRetryDelayIsBounded(t *testing.T) {
	tests := []struct {
		failedAttempts int
		want           time.Duration
	}{
		{failedAttempts: -1, want: time.Minute},
		{failedAttempts: 0, want: time.Minute},
		{failedAttempts: 1, want: 2 * time.Minute},
		{failedAttempts: 5, want: 32 * time.Minute},
		{failedAttempts: 6, want: time.Hour},
		{failedAttempts: 100, want: time.Hour},
	}
	for _, test := range tests {
		if got := migrationCleanupRetryDelay(test.failedAttempts); got != test.want {
			t.Fatalf(
				"migrationCleanupRetryDelay(%d) = %s, want %s",
				test.failedAttempts,
				got,
				test.want,
			)
		}
	}
}

func TestExecuteMigrationRejectsSupersededStartingSnapshot(t *testing.T) {
	harness := newMigrationOwnershipHarness(t)
	const laterProvider = "later-provider"
	current, err := harness.manager.GetEntry(harness.entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	current.Providers[laterProvider] = &storage.ProviderEntry{
		Provider: laterProvider,
		ID:       "later-id",
		Status:   debridTypes.TorrentStatusDownloaded,
	}
	if err := current.ActivatePlacement(laterProvider); err != nil {
		t.Fatal(err)
	}
	if err := harness.manager.AddOrUpdate(current, nil); err != nil {
		t.Fatal(err)
	}

	job := harness.execute(false)
	if job.Status != storage.SwitcherStatusFailed ||
		!strings.Contains(job.Error, "migration was superseded") {
		t.Fatalf("superseded migration job = status %q, error %q", job.Status, job.Error)
	}
	if calls := harness.submitCalls.Load(); calls != 0 {
		t.Fatalf("target submissions from stale migration = %d, want 0", calls)
	}
	assertNoProviderDelete(t, harness.sourceDeletes, migrationSourceProvider)
	assertNoProviderDelete(t, harness.targetDeletes, migrationTargetProvider)
}

func TestExecuteMigrationDeletesSourceOnlyAcrossSameProviderAccounts(t *testing.T) {
	harness := newMigrationOwnershipHarness(t)

	job := harness.execute(false)
	if job.Status != storage.SwitcherStatusCompleted || job.Error != "" {
		t.Fatalf("migration job = status %q, error %q", job.Status, job.Error)
	}
	select {
	case id := <-harness.sourceDeletes:
		if id != migrationSourceID {
			t.Fatalf("source deletion ID = %q, want %q", id, migrationSourceID)
		}
	default:
		t.Fatal("source placement was not deleted")
	}
	assertNoProviderDelete(t, harness.targetDeletes, migrationTargetProvider)

	durable, err := harness.manager.GetEntry(harness.entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if durable.ActiveProvider != migrationTargetProvider {
		t.Fatalf("active provider = %q, want %q", durable.ActiveProvider, migrationTargetProvider)
	}
	if _, exists := durable.Providers[migrationSourceProvider]; exists {
		t.Fatal("durable source placement remains after successful cleanup")
	}
	if target := durable.Providers[migrationTargetProvider]; target == nil || target.ID != migrationTargetID {
		t.Fatalf("durable target placement = %#v", target)
	}
}

func TestExecuteMigrationKeepOldRetainsBothPlacementsWithoutDeletion(t *testing.T) {
	harness := newMigrationOwnershipHarness(t)

	job := harness.execute(true)
	if job.Status != storage.SwitcherStatusCompleted || job.Error != "" {
		t.Fatalf("migration job = status %q, error %q", job.Status, job.Error)
	}
	assertNoProviderDelete(t, harness.sourceDeletes, migrationSourceProvider)
	assertNoProviderDelete(t, harness.targetDeletes, migrationTargetProvider)

	durable, err := harness.manager.GetEntry(harness.entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if durable.ActiveProvider != migrationTargetProvider {
		t.Fatalf("active provider = %q, want %q", durable.ActiveProvider, migrationTargetProvider)
	}
	if durable.Providers[migrationSourceProvider] == nil || durable.Providers[migrationTargetProvider] == nil {
		t.Fatalf("keep-old placements = %#v", durable.Providers)
	}
}

func TestExecuteMigrationCleanupFailureRetainsSourceForRetry(t *testing.T) {
	harness := newMigrationOwnershipHarness(t)
	cleanupErr := errors.New("source provider unavailable")
	harness.sourceErr = cleanupErr
	now := time.Date(2026, time.August, 24, 12, 0, 0, 0, time.UTC)
	harness.manager.migrationCleanupNow = func() time.Time { return now }

	job := harness.execute(false)
	if job.Status != storage.SwitcherStatusFailed {
		t.Fatalf("migration status = %q, want failed", job.Status)
	}
	if !strings.Contains(job.Error, cleanupErr.Error()) {
		t.Fatalf("migration error = %q, want %q", job.Error, cleanupErr)
	}
	select {
	case id := <-harness.sourceDeletes:
		if id != migrationSourceID {
			t.Fatalf("failed source cleanup ID = %q, want %q", id, migrationSourceID)
		}
	default:
		t.Fatal("source cleanup was not attempted")
	}
	assertNoProviderDelete(t, harness.targetDeletes, migrationTargetProvider)

	durable, err := harness.manager.GetEntry(harness.entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if durable.ActiveProvider != migrationTargetProvider ||
		durable.Providers[migrationSourceProvider] == nil ||
		durable.Providers[migrationTargetProvider] == nil {
		t.Fatalf("recoverable placements after cleanup failure = %#v, active %q", durable.Providers, durable.ActiveProvider)
	}
	intents, err := harness.manager.storage.MigrationCleanups()
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 1 || intents[0].Attempts != 1 ||
		!intents[0].NextAttemptAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("durable cleanup retry = %#v", intents)
	}
	stats, err := harness.manager.GetStats()
	if err != nil {
		t.Fatal(err)
	}
	if got := stats["pending_migration_cleanups"]; got != 1 {
		t.Fatalf("pending migration cleanup stat = %#v, want 1", got)
	}

	// The sweep must honor durable backoff rather than hammering the provider.
	harness.sourceErr = nil
	now = now.Add(59 * time.Second)
	if err := harness.manager.processMigrationCleanups(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertNoProviderDelete(t, harness.sourceDeletes, migrationSourceProvider)

	// Once due, only the cleanup phase runs. The durable target is not submitted
	// again, and successful local persistence clears the intent.
	now = now.Add(time.Second)
	if err := harness.manager.processMigrationCleanups(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls := harness.submitCalls.Load(); calls != 1 {
		t.Fatalf("target submissions = %d, want 1 across migration and cleanup retry", calls)
	}
	select {
	case id := <-harness.sourceDeletes:
		if id != migrationSourceID {
			t.Fatalf("retried source cleanup ID = %q, want %q", id, migrationSourceID)
		}
	default:
		t.Fatal("source cleanup was not retried")
	}
	assertNoProviderDelete(t, harness.targetDeletes, migrationTargetProvider)

	durable, err = harness.manager.GetEntry(harness.entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := durable.Providers[migrationSourceProvider]; exists {
		t.Fatal("source placement remains after successful cleanup retry")
	}
	if got := harness.manager.storage.MigrationCleanupCount(); got != 0 {
		t.Fatalf("completed migration cleanup intents = %d, want 0", got)
	}
}

func TestMigrationCleanupRecoversAcrossManagerRestart(t *testing.T) {
	harness := newMigrationOwnershipHarness(t)
	now := time.Date(2026, time.August, 24, 14, 0, 0, 0, time.UTC)
	harness.manager.migrationCleanupNow = func() time.Time { return now }
	harness.sourceErr = errors.New("provider offline during migration")

	job := harness.execute(false)
	if job.Status != storage.SwitcherStatusFailed {
		t.Fatalf("migration status = %q, want failed pending cleanup", job.Status)
	}
	select {
	case <-harness.sourceDeletes:
	default:
		t.Fatal("initial source cleanup was not attempted")
	}
	if got := harness.manager.storage.MigrationCleanupCount(); got != 1 {
		t.Fatalf("pending cleanups before restart = %d, want 1", got)
	}

	previous := harness.manager
	previous.stopAcceptingBackgroundWork()
	previous.background.Wait()
	if err := previous.storage.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := storage.NewStorage(harness.dbPath)
	if err != nil {
		t.Fatal(err)
	}
	restarted := &Manager{
		storage:       reopened,
		clients:       previous.clients,
		config:        previous.config,
		logger:        zerolog.Nop(),
		migrationJobs: xsync.NewMap[string, *storage.SwitcherJob](),
	}
	restarted.entry = NewEntryCache(restarted)
	restarted.fixer = &Fixer{manager: restarted}
	restarted.migrationCleanupNow = func() time.Time { return now }
	harness.manager = restarted

	intents, err := restarted.storage.MigrationCleanups()
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 1 || intents[0].Attempts != 1 {
		t.Fatalf("restarted cleanup intents = %#v", intents)
	}
	harness.sourceErr = nil
	now = now.Add(time.Minute)
	if err := restarted.processMigrationCleanups(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-harness.sourceDeletes:
		if id != migrationSourceID {
			t.Fatalf("restarted source cleanup ID = %q", id)
		}
	default:
		t.Fatal("restarted manager did not recover source cleanup")
	}
	if calls := harness.submitCalls.Load(); calls != 1 {
		t.Fatalf("target submissions across restart = %d, want 1", calls)
	}
	durable, err := restarted.GetEntry(harness.entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if durable.Providers[migrationSourceProvider] != nil ||
		durable.Providers[migrationTargetProvider] == nil {
		t.Fatalf("restarted migration placements = %#v", durable.Providers)
	}
	if got := restarted.storage.MigrationCleanupCount(); got != 0 {
		t.Fatalf("pending cleanups after restart recovery = %d", got)
	}
}

func TestMigrationCleanupNeverDeletesChangedSourceIdentity(t *testing.T) {
	harness := newMigrationOwnershipHarness(t)
	now := time.Date(2026, time.August, 24, 15, 0, 0, 0, time.UTC)
	harness.manager.migrationCleanupNow = func() time.Time { return now }
	harness.sourceErr = errors.New("initial cleanup failure")

	if job := harness.execute(false); job.Status != storage.SwitcherStatusFailed {
		t.Fatalf("migration status = %q, want failed pending cleanup", job.Status)
	}
	select {
	case <-harness.sourceDeletes:
	default:
		t.Fatal("initial source cleanup was not attempted")
	}
	current, err := harness.manager.GetEntry(harness.entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	current.Providers[migrationSourceProvider].ID = "replacement-source-id"
	if err := harness.manager.AddOrUpdate(current, nil); err != nil {
		t.Fatal(err)
	}

	harness.sourceErr = nil
	now = now.Add(time.Minute)
	if err := harness.manager.processMigrationCleanups(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertNoProviderDelete(t, harness.sourceDeletes, migrationSourceProvider)
	if got := harness.manager.storage.MigrationCleanupCount(); got != 0 {
		t.Fatalf("stale cleanup intent count = %d, want 0", got)
	}
	durable, err := harness.manager.GetEntry(harness.entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if source := durable.Providers[migrationSourceProvider]; source == nil ||
		source.ID != "replacement-source-id" {
		t.Fatalf("replacement source was mutated: %#v", source)
	}
}

func TestMigrationCleanupNeverDeletesSourceAfterMigrationIsSuperseded(t *testing.T) {
	harness := newMigrationOwnershipHarness(t)
	now := time.Date(2026, time.August, 24, 15, 30, 0, 0, time.UTC)
	harness.manager.migrationCleanupNow = func() time.Time { return now }
	harness.sourceErr = errors.New("initial cleanup failure")

	if job := harness.execute(false); job.Status != storage.SwitcherStatusFailed {
		t.Fatalf("migration status = %q, want failed pending cleanup", job.Status)
	}
	select {
	case <-harness.sourceDeletes:
	default:
		t.Fatal("initial source cleanup was not attempted")
	}
	current, err := harness.manager.GetEntry(harness.entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if err := current.ActivatePlacement(migrationSourceProvider); err != nil {
		t.Fatal(err)
	}
	if err := harness.manager.AddOrUpdate(current, nil); err != nil {
		t.Fatal(err)
	}

	harness.sourceErr = nil
	now = now.Add(time.Minute)
	if err := harness.manager.processMigrationCleanups(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertNoProviderDelete(t, harness.sourceDeletes, migrationSourceProvider)
	if got := harness.manager.storage.MigrationCleanupCount(); got != 0 {
		t.Fatalf("superseded cleanup intent count = %d, want 0", got)
	}
	durable, err := harness.manager.GetEntry(harness.entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if durable.ActiveProvider != migrationSourceProvider ||
		durable.Providers[migrationSourceProvider] == nil {
		t.Fatalf(
			"superseding activation was mutated: active %q, providers %#v",
			durable.ActiveProvider,
			durable.Providers,
		)
	}
}

func TestMigrationCleanupRevalidatesAfterLiveTargetCall(t *testing.T) {
	harness := newMigrationOwnershipHarness(t)
	harness.targetGetHook = func() error {
		harness.targetGetHook = nil
		current, err := harness.manager.GetEntry(harness.entry.InfoHash)
		if err != nil {
			return err
		}
		if err := current.ActivatePlacement(migrationSourceProvider); err != nil {
			return err
		}
		return harness.manager.AddOrUpdate(current, nil)
	}

	job := harness.execute(false)
	if job.Status != storage.SwitcherStatusCompleted || job.Error != "" {
		t.Fatalf("superseded live-check job = status %q, error %q", job.Status, job.Error)
	}
	assertNoProviderDelete(t, harness.sourceDeletes, migrationSourceProvider)
	if got := harness.manager.storage.MigrationCleanupCount(); got != 0 {
		t.Fatalf("live-check superseded intent count = %d, want 0", got)
	}
	durable, err := harness.manager.GetEntry(harness.entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if durable.ActiveProvider != migrationSourceProvider ||
		durable.Providers[migrationSourceProvider] == nil {
		t.Fatalf(
			"live-check supersession was mutated: active %q, providers %#v",
			durable.ActiveProvider,
			durable.Providers,
		)
	}
}

func TestMigrationCleanupFailsClosedWhenTargetIdentityChanges(t *testing.T) {
	harness := newMigrationOwnershipHarness(t)
	now := time.Date(2026, time.August, 24, 16, 0, 0, 0, time.UTC)
	harness.manager.migrationCleanupNow = func() time.Time { return now }
	harness.sourceErr = errors.New("initial cleanup failure")

	if job := harness.execute(false); job.Status != storage.SwitcherStatusFailed {
		t.Fatalf("migration status = %q, want failed pending cleanup", job.Status)
	}
	select {
	case <-harness.sourceDeletes:
	default:
		t.Fatal("initial source cleanup was not attempted")
	}
	current, err := harness.manager.GetEntry(harness.entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	current.Providers[migrationTargetProvider].ID = "different-target-id"
	if err := harness.manager.AddOrUpdate(current, nil); err != nil {
		t.Fatal(err)
	}

	harness.sourceErr = nil
	now = now.Add(time.Minute)
	err = harness.manager.processMigrationCleanups(context.Background())
	if err == nil || !strings.Contains(err.Error(), "target identity") {
		t.Fatalf("target-mismatch sweep error = %v", err)
	}
	assertNoProviderDelete(t, harness.sourceDeletes, migrationSourceProvider)
	intents, err := harness.manager.storage.MigrationCleanups()
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 1 || intents[0].Attempts != 2 ||
		!intents[0].NextAttemptAt.Equal(now.Add(2*time.Minute)) {
		t.Fatalf("target-mismatch retry state = %#v", intents)
	}
}

func TestMigrationCleanupRequiresLiveDownloadedTarget(t *testing.T) {
	harness := newMigrationOwnershipHarness(t)
	now := time.Date(2026, time.August, 24, 16, 30, 0, 0, time.UTC)
	harness.manager.migrationCleanupNow = func() time.Time { return now }
	harness.sourceErr = errors.New("initial cleanup failure")

	if job := harness.execute(false); job.Status != storage.SwitcherStatusFailed {
		t.Fatalf("migration status = %q, want failed pending cleanup", job.Status)
	}
	select {
	case <-harness.sourceDeletes:
	default:
		t.Fatal("initial source cleanup was not attempted")
	}
	if calls := harness.targetGetCalls.Load(); calls != 1 {
		t.Fatalf("initial live target validations = %d, want 1", calls)
	}

	harness.sourceErr = nil
	harness.targetGetErr = customerror.TorrentNotFoundError
	now = now.Add(time.Minute)
	err := harness.manager.processMigrationCleanups(context.Background())
	if err == nil || !strings.Contains(err.Error(), "validate live target") {
		t.Fatalf("missing-live-target sweep error = %v", err)
	}
	assertNoProviderDelete(t, harness.sourceDeletes, migrationSourceProvider)
	if calls := harness.targetGetCalls.Load(); calls != 2 {
		t.Fatalf("live target validations after retry = %d, want 2", calls)
	}
	intents, err := harness.manager.storage.MigrationCleanups()
	if err != nil {
		t.Fatal(err)
	}
	if len(intents) != 1 || intents[0].Attempts != 2 {
		t.Fatalf("missing-live-target retry state = %#v", intents)
	}
}

func TestMigrationCleanupShutdownDoesNotConsumeRetryBackoff(t *testing.T) {
	harness := newMigrationOwnershipHarness(t)
	success, err := harness.manager.fixer.MoveTorrent(
		harness.entry,
		migrationTargetProvider,
		false,
	)
	if err != nil || !success {
		t.Fatalf("prepare target placement = %v, %v", success, err)
	}
	intent, err := harness.manager.prepareMigrationCleanup(&storage.SwitcherJob{
		ID:             "shutdown-cleanup",
		InfoHash:       harness.entry.InfoHash,
		SourceProvider: migrationSourceProvider,
		TargetProvider: migrationTargetProvider,
	})
	if err != nil || intent == nil {
		t.Fatalf("prepare shutdown cleanup intent = %#v, %v", intent, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	harness.targetGetHook = func() error {
		harness.targetGetHook = nil
		cancel()
		return nil
	}
	err = harness.manager.runMigrationCleanup(ctx, intent.ID)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown cleanup error = %v, want context canceled", err)
	}
	assertNoProviderDelete(t, harness.sourceDeletes, migrationSourceProvider)
	recovered, err := harness.manager.storage.GetMigrationCleanup(intent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Attempts != 0 || !recovered.NextAttemptAt.IsZero() {
		t.Fatalf("shutdown consumed retry backoff: %#v", recovered)
	}
}

func TestMigrationCleanupRecoversAfterProviderDeleteWinsStorageRace(t *testing.T) {
	harness := newMigrationOwnershipHarness(t)
	now := time.Date(2026, time.August, 24, 17, 0, 0, 0, time.UTC)
	harness.manager.migrationCleanupNow = func() time.Time { return now }
	harness.sourceDeleteHook = func(_ string) error {
		harness.sourceDeleteHook = nil
		current, err := harness.manager.GetEntry(harness.entry.InfoHash)
		if err != nil {
			return err
		}
		current.Category = "concurrent-update-preserved"
		if err := harness.manager.AddOrUpdate(current, nil); err != nil {
			return err
		}
		// A second idempotent delete should observe the provider object as gone.
		harness.sourceErr = customerror.TorrentNotFoundError
		return nil
	}

	job := harness.execute(false)
	if job.Status != storage.SwitcherStatusFailed ||
		!strings.Contains(job.Error, storage.ErrStaleEntryGeneration.Error()) {
		t.Fatalf("racing migration job = status %q, error %q", job.Status, job.Error)
	}
	select {
	case id := <-harness.sourceDeletes:
		if id != migrationSourceID {
			t.Fatalf("racing source cleanup ID = %q", id)
		}
	default:
		t.Fatal("racing source cleanup was not attempted")
	}
	if got := harness.manager.storage.MigrationCleanupCount(); got != 1 {
		t.Fatalf("cleanup intent after storage race = %d, want 1", got)
	}

	now = now.Add(time.Minute)
	if err := harness.manager.processMigrationCleanups(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-harness.sourceDeletes:
		if id != migrationSourceID {
			t.Fatalf("idempotent source cleanup ID = %q", id)
		}
	default:
		t.Fatal("source cleanup was not retried after storage race")
	}
	durable, err := harness.manager.GetEntry(harness.entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if durable.Category != "concurrent-update-preserved" {
		t.Fatalf("concurrent metadata update = %q", durable.Category)
	}
	if durable.Providers[migrationSourceProvider] != nil {
		t.Fatal("source placement remains after idempotent cleanup retry")
	}
	if got := harness.manager.storage.MigrationCleanupCount(); got != 0 {
		t.Fatalf("cleanup intent after idempotent recovery = %d", got)
	}
}

func TestExecuteMigrationNeverDeletesSourceBeforeTargetIsDurable(t *testing.T) {
	harness := newMigrationOwnershipHarness(t)

	// Advance the generation after target status returns but before its local
	// activation is committed. The migration's fresh starting snapshot must
	// still fail closed at the target durability checkpoint.
	harness.targetCheckHook = func() error {
		harness.targetCheckHook = nil
		current, err := harness.manager.GetEntry(harness.entry.InfoHash)
		if err != nil {
			return err
		}
		current.Category = "generation-advanced"
		return harness.manager.AddOrUpdate(current, nil)
	}

	job := harness.execute(false)
	if job.Status != storage.SwitcherStatusFailed {
		t.Fatalf("migration status = %q, want failed", job.Status)
	}
	if !strings.Contains(job.Error, "persist target placement") {
		t.Fatalf("migration error = %q, want target durability failure", job.Error)
	}
	assertNoProviderDelete(t, harness.sourceDeletes, migrationSourceProvider)
	assertNoProviderDelete(t, harness.targetDeletes, migrationTargetProvider)

	durable, err := harness.manager.GetEntry(harness.entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if durable.ActiveProvider != migrationSourceProvider || durable.Providers[migrationTargetProvider] != nil {
		t.Fatalf("durable state crossed a failed target checkpoint: active %q, placements %#v", durable.ActiveProvider, durable.Providers)
	}
}

func TestExecuteMigrationRollsBackOnlyFailedTargetSubmission(t *testing.T) {
	harness := newMigrationOwnershipHarness(t)
	harness.targetCheckErr = errors.New("target status unavailable")

	job := harness.execute(false)
	if job.Status != storage.SwitcherStatusFailed || !strings.Contains(job.Error, harness.targetCheckErr.Error()) {
		t.Fatalf("migration job = status %q, error %q", job.Status, job.Error)
	}
	select {
	case id := <-harness.targetDeletes:
		if id != migrationTargetID {
			t.Fatalf("target rollback ID = %q, want %q", id, migrationTargetID)
		}
	default:
		t.Fatal("failed target submission was not rolled back synchronously")
	}
	assertNoProviderDelete(t, harness.sourceDeletes, migrationSourceProvider)

	durable, err := harness.manager.GetEntry(harness.entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if durable.ActiveProvider != migrationSourceProvider || durable.Providers[migrationTargetProvider] != nil {
		t.Fatalf("failed target changed durable state: active %q, placements %#v", durable.ActiveProvider, durable.Providers)
	}
}

func TestExecuteMigrationRollsBackTargetReturnedWithSubmitError(t *testing.T) {
	harness := newMigrationOwnershipHarness(t)
	harness.targetSubmitErr = errors.New("target submit response failed")

	job := harness.execute(false)
	if job.Status != storage.SwitcherStatusFailed || !strings.Contains(job.Error, harness.targetSubmitErr.Error()) {
		t.Fatalf("migration job = status %q, error %q", job.Status, job.Error)
	}
	select {
	case id := <-harness.targetDeletes:
		if id != migrationTargetID {
			t.Fatalf("target rollback ID = %q, want %q", id, migrationTargetID)
		}
	default:
		t.Fatal("target returned with a submit error was not rolled back synchronously")
	}
	assertNoProviderDelete(t, harness.sourceDeletes, migrationSourceProvider)

	durable, err := harness.manager.GetEntry(harness.entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if durable.ActiveProvider != migrationSourceProvider || durable.Providers[migrationTargetProvider] != nil {
		t.Fatalf("failed submit changed durable state: active %q, placements %#v", durable.ActiveProvider, durable.Providers)
	}
}
