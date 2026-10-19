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
	"github.com/Trifocals3537/tessarr/pkg/debrid/account"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/go-co-op/gocron/v2"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

const testBackgroundWaitTimeout = 200 * time.Millisecond

func TestManagerStopCancelsAndWaitsForBackgroundTasks(t *testing.T) {
	m := &Manager{}
	m.resetLifecycle()

	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	m.startBackground("test task", func() {
		close(started)
		<-m.ctx.Done()
		close(canceled)
		<-release
	})

	<-started
	result := make(chan error, 1)
	go func() {
		result <- m.Stop()
	}()

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("manager did not cancel its background context")
	}

	select {
	case err := <-result:
		t.Fatalf("Manager.Stop() returned before background cleanup completed: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Manager.Stop() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Manager.Stop() did not return after background cleanup completed")
	}
}

func TestRestoreActiveDownloadJobsHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// A pre-canceled restoration must return before touching an uninitialized
	// queue or any persisted entry state.
	(&Manager{}).restoreActiveDownloadJobs(ctx)
}

func TestInitialWorkersAreTrackedAndCancelAroundLegacyProviderCalls(t *testing.T) {
	testRoot := t.TempDir()
	config.SetConfigPath(testRoot)
	strg, err := storage.NewStorage(filepath.Join(testRoot, "db"))
	if err != nil {
		t.Fatalf("NewStorage() error = %v", err)
	}

	release := make(chan struct{})
	provider := newBlockingLifecycleProvider(release)
	m := &Manager{
		storage:               strg,
		queue:                 newQueue(strg, ""),
		clients:               xsync.NewMap[string, debrid.Client](),
		processingEntries:     xsync.NewMap[string, struct{}](),
		backgroundWaitTimeout: testBackgroundWaitTimeout,
		logger:                zerolog.Nop(),
	}
	m.clients.Store("blocked", provider)
	m.resetLifecycle()
	m.runInitialCalls(m.ctx)

	waitForSignal(t, provider.refreshStarted, "initial download-link refresh")
	waitForSignal(t, provider.accountsStarted, "initial account sync")

	// The real startup workers must be members of the manager barrier while
	// their legacy provider calls are still blocked.
	if err := m.waitForBackground(); err == nil {
		t.Fatal("waitForBackground() returned while initial workers were still running")
	}

	if err := m.Stop(); err != nil {
		t.Fatalf("Manager.Stop() error = %v, want nil", err)
	}

	// Stop is allowed to return without the legacy API calls themselves:
	// those detached calls capture only the provider and cannot touch manager
	// storage after cancellation.
	close(release)
	waitForSignal(t, provider.refreshDone, "detached download-link refresh")
	waitForSignal(t, provider.accountsDone, "detached account sync")
}

func TestInitialProviderSyncCancelsAroundLegacyGetTorrents(t *testing.T) {
	release := make(chan struct{})
	provider := newBlockingLifecycleProvider(release)
	m := &Manager{
		clients:               xsync.NewMap[string, debrid.Client](),
		backgroundWaitTimeout: testBackgroundWaitTimeout,
		logger:                zerolog.Nop(),
	}
	m.clients.Store("blocked", provider)
	m.resetLifecycle()
	m.startBackground("initial provider sync", func() {
		m.syncTorrents(m.ctx)
	})

	waitForSignal(t, provider.torrentsStarted, "initial torrent sync")
	if err := m.waitForBackground(); err == nil {
		t.Fatal("waitForBackground() returned while initial provider sync was running")
	}
	if err := m.Stop(); err != nil {
		t.Fatalf("Manager.Stop() error = %v, want nil", err)
	}

	close(release)
	waitForSignal(t, provider.torrentsDone, "detached GetTorrents call")
}

func TestManagerStopTimeoutPreservesStorageAndMount(t *testing.T) {
	testRoot := t.TempDir()
	config.SetConfigPath(testRoot)
	strg, err := storage.NewStorage(filepath.Join(testRoot, "db"))
	if err != nil {
		t.Fatalf("NewStorage() error = %v", err)
	}
	mount := &lifecycleMount{}
	m := &Manager{
		storage:               strg,
		mountManager:          mount,
		backgroundWaitTimeout: testBackgroundWaitTimeout,
		logger:                zerolog.Nop(),
	}
	m.resetLifecycle()

	release := make(chan struct{})
	m.startBackground("uncooperative storage user", func() {
		<-release
	})

	err = m.Stop()
	if err == nil || !strings.Contains(err.Error(), "background work did not stop") {
		t.Fatalf("Manager.Stop() error = %v, want background timeout", err)
	}
	if mount.stopped.Load() {
		t.Fatal("Manager.Stop() stopped mount after background timeout")
	}
	if _, err := strg.Count(); err != nil {
		t.Fatalf("storage was closed after background timeout: %v", err)
	}

	close(release)
	if err := m.Stop(); err != nil {
		t.Fatalf("second Manager.Stop() error = %v, want nil", err)
	}
	if !mount.stopped.Load() {
		t.Fatal("Manager.Stop() did not stop mount after background work completed")
	}
}

func TestManagerResetAbortsWhenBackgroundWorkCannotStop(t *testing.T) {
	testRoot := t.TempDir()
	config.SetConfigPath(testRoot)
	strg, err := storage.NewStorage(filepath.Join(testRoot, "db"))
	if err != nil {
		t.Fatalf("NewStorage() error = %v", err)
	}
	mount := &lifecycleMount{}
	m := &Manager{
		storage:               strg,
		mountManager:          mount,
		backgroundWaitTimeout: testBackgroundWaitTimeout,
		logger:                zerolog.Nop(),
	}
	m.resetLifecycle()

	release := make(chan struct{})
	m.startBackground("uncooperative reset task", func() {
		<-release
	})

	err = m.Reset()
	if err == nil || !strings.Contains(err.Error(), "failed to stop manager during reset") {
		t.Fatalf("Manager.Reset() error = %v, want shutdown failure", err)
	}
	if mount.stopped.Load() {
		t.Fatal("Manager.Reset() stopped mount after background timeout")
	}
	if _, err := strg.Count(); err != nil {
		t.Fatalf("storage was replaced or closed after failed reset: %v", err)
	}

	close(release)
	if err := m.Stop(); err != nil {
		t.Fatalf("Manager.Stop() cleanup error = %v", err)
	}
}

func TestManagerStopRepairTimeoutPreservesStorageAndMount(t *testing.T) {
	testRoot := t.TempDir()
	config.SetConfigPath(testRoot)
	strg, err := storage.NewStorage(filepath.Join(testRoot, "db"))
	if err != nil {
		t.Fatalf("NewStorage() error = %v", err)
	}
	mount := &lifecycleMount{}
	m := &Manager{
		storage:      strg,
		mountManager: mount,
		logger:       zerolog.Nop(),
	}
	m.resetLifecycle()

	release := make(chan struct{})
	started := make(chan struct{})
	repair := &Repair{
		manager:     m,
		logger:      zerolog.Nop(),
		stopTimeout: 25 * time.Millisecond,
	}
	if err := repair.reserveRun(); err != nil {
		t.Fatalf("reserveRun() error = %v", err)
	}
	repair.runReserved(func() {
		close(started)
		<-release
	})
	m.repair = repair
	waitForSignal(t, started, "blocking repair work")

	err = m.Stop()
	if err == nil || !strings.Contains(err.Error(), "repair work did not stop") {
		t.Fatalf("Manager.Stop() error = %v, want repair timeout", err)
	}
	if mount.stopped.Load() {
		t.Fatal("Manager.Stop() stopped mount after repair timeout")
	}
	if _, err := strg.Count(); err != nil {
		t.Fatalf("storage was closed after repair timeout: %v", err)
	}
	if err := repair.reserveRun(); err == nil || !strings.Contains(err.Error(), "repair service is stopping") {
		if err == nil {
			repair.releaseRun()
		}
		t.Fatalf("reserveRun() error = %v, want repair-stopping error", err)
	}

	close(release)
	if err := m.Stop(); err != nil {
		t.Fatalf("second Manager.Stop() error = %v, want nil", err)
	}
	if !mount.stopped.Load() {
		t.Fatal("Manager.Stop() did not stop mount after repair work completed")
	}
}

func TestManagerStopSchedulerTimeoutPreservesStorageAndMount(t *testing.T) {
	testRoot := t.TempDir()
	config.SetConfigPath(testRoot)
	strg, err := storage.NewStorage(filepath.Join(testRoot, "db"))
	if err != nil {
		t.Fatalf("NewStorage() error = %v", err)
	}
	defer strg.Close()

	scheduler, err := gocron.NewScheduler(gocron.WithStopTimeout(25 * time.Millisecond))
	if err != nil {
		t.Fatalf("NewScheduler() error = %v", err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	if _, err := scheduler.NewJob(
		gocron.DurationJob(time.Hour),
		gocron.NewTask(func() {
			close(started)
			<-release
			close(done)
		}),
		gocron.WithStartAt(gocron.WithStartImmediately()),
	); err != nil {
		t.Fatalf("NewJob() error = %v", err)
	}
	scheduler.Start()
	waitForSignal(t, started, "blocking scheduler job")

	mount := &lifecycleMount{}
	m := &Manager{
		storage:      strg,
		mountManager: mount,
		scheduler:    scheduler,
		logger:       zerolog.Nop(),
	}
	m.resetLifecycle()

	err = m.Stop()
	if err == nil || !strings.Contains(err.Error(), "failed to shutdown scheduler") {
		t.Fatalf("Manager.Stop() error = %v, want scheduler timeout", err)
	}
	if mount.stopped.Load() {
		t.Fatal("Manager.Stop() stopped mount after scheduler timeout")
	}
	if _, err := strg.Count(); err != nil {
		t.Fatalf("storage was closed after scheduler timeout: %v", err)
	}

	close(release)
	waitForSignal(t, done, "scheduler job release")
}

func TestDeleteEntryUsesSynchronousCleanupAfterBackgroundShutdownStarts(t *testing.T) {
	testRoot := t.TempDir()
	config.SetConfigPath(testRoot)
	strg, err := storage.NewStorage(filepath.Join(testRoot, "db"))
	if err != nil {
		t.Fatalf("NewStorage() error = %v", err)
	}
	defer strg.Close()

	entry := &storage.Entry{
		Protocol:  config.ProtocolTorrent,
		InfoHash:  "delete-after-stop",
		Name:      "delete-after-stop",
		Providers: map[string]*storage.ProviderEntry{},
		Files:     map[string]*storage.File{},
	}
	if err := strg.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}

	m := &Manager{
		storage: strg,
		logger:  zerolog.Nop(),
	}
	m.resetLifecycle()
	m.stopAcceptingBackgroundWork()

	err = m.DeleteEntry(entry.InfoHash, true)
	if err != nil {
		t.Fatalf("DeleteEntry() error = %v", err)
	}
	exists, err := strg.Exists(entry.InfoHash)
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if exists {
		t.Fatal("DeleteEntry() retained storage entry after synchronous cleanup")
	}
}

func TestSwitchTorrentRejectsMigrationAfterShutdownStarts(t *testing.T) {
	testRoot := t.TempDir()
	config.SetConfigPath(testRoot)
	strg, err := storage.NewStorage(filepath.Join(testRoot, "db"))
	if err != nil {
		t.Fatalf("NewStorage() error = %v", err)
	}
	defer strg.Close()

	entry := &storage.Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       "switch-after-stop",
		Name:           "switch-after-stop",
		ActiveProvider: "source",
		Providers:      map[string]*storage.ProviderEntry{},
		Files:          map[string]*storage.File{},
	}
	if err := strg.AddOrUpdate(entry); err != nil {
		t.Fatalf("AddOrUpdate() error = %v", err)
	}

	m := &Manager{
		storage:       strg,
		migrationJobs: xsync.NewMap[string, *storage.SwitcherJob](),
		logger:        zerolog.Nop(),
	}
	m.resetLifecycle()
	m.stopAcceptingBackgroundWork()

	job, err := m.SwitchTorrent(context.Background(), entry.InfoHash, "target", false, false)
	if err == nil || !strings.Contains(err.Error(), "manager is stopping") {
		t.Fatalf("SwitchTorrent() error = %v, want manager-stopping error", err)
	}
	if job != nil {
		t.Fatalf("SwitchTorrent() job = %#v, want nil", job)
	}
	jobCount := 0
	m.migrationJobs.Range(func(_ string, _ *storage.SwitcherJob) bool {
		jobCount++
		return true
	})
	if jobCount != 0 {
		t.Fatalf("migration job count = %d, want 0 after rejected start", jobCount)
	}
}

func waitForSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

type lifecycleMount struct {
	stopped atomic.Bool
}

func (m *lifecycleMount) Start(context.Context) error { return nil }
func (m *lifecycleMount) Stop() error {
	m.stopped.Store(true)
	return nil
}
func (m *lifecycleMount) Stats() map[string]any  { return nil }
func (m *lifecycleMount) IsReady() bool          { return true }
func (m *lifecycleMount) Type() string           { return "test" }
func (m *lifecycleMount) Refresh([]string) error { return nil }

type blockingLifecycleProvider struct {
	release         <-chan struct{}
	refreshStarted  chan struct{}
	refreshDone     chan struct{}
	accountsStarted chan struct{}
	accountsDone    chan struct{}
	torrentsStarted chan struct{}
	torrentsDone    chan struct{}
}

func newBlockingLifecycleProvider(release <-chan struct{}) *blockingLifecycleProvider {
	return &blockingLifecycleProvider{
		release:         release,
		refreshStarted:  make(chan struct{}),
		refreshDone:     make(chan struct{}),
		accountsStarted: make(chan struct{}),
		accountsDone:    make(chan struct{}),
		torrentsStarted: make(chan struct{}),
		torrentsDone:    make(chan struct{}),
	}
}

func (p *blockingLifecycleProvider) SubmitMagnet(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
	return nil, errors.ErrUnsupported
}
func (p *blockingLifecycleProvider) CheckStatus(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
	return nil, errors.ErrUnsupported
}
func (p *blockingLifecycleProvider) GetDownloadLink(string, *debridTypes.File) (debridTypes.DownloadLink, error) {
	return debridTypes.DownloadLink{}, errors.ErrUnsupported
}
func (p *blockingLifecycleProvider) DeleteTorrent(string) error { return errors.ErrUnsupported }
func (p *blockingLifecycleProvider) IsAvailable([]string) map[string]bool {
	return nil
}
func (p *blockingLifecycleProvider) UpdateTorrent(*debridTypes.Torrent) error {
	return errors.ErrUnsupported
}
func (p *blockingLifecycleProvider) GetTorrent(string) (*debridTypes.Torrent, error) {
	return nil, errors.ErrUnsupported
}
func (p *blockingLifecycleProvider) GetTorrents() ([]*debridTypes.Torrent, error) {
	close(p.torrentsStarted)
	<-p.release
	close(p.torrentsDone)
	return nil, nil
}
func (p *blockingLifecycleProvider) Config() config.Debrid {
	return config.Debrid{Name: "blocked"}
}
func (p *blockingLifecycleProvider) Logger() zerolog.Logger { return zerolog.Nop() }
func (p *blockingLifecycleProvider) RefreshDownloadLinks() error {
	close(p.refreshStarted)
	<-p.release
	close(p.refreshDone)
	return nil
}
func (p *blockingLifecycleProvider) CheckFile(context.Context, string, string) error {
	return errors.ErrUnsupported
}
func (p *blockingLifecycleProvider) AccountManager() *account.Manager { return nil }
func (p *blockingLifecycleProvider) GetProfile() (*debridTypes.Profile, error) {
	return nil, errors.ErrUnsupported
}
func (p *blockingLifecycleProvider) GetAvailableSlots() (int, error) {
	return 0, errors.ErrUnsupported
}
func (p *blockingLifecycleProvider) SyncAccounts() {
	close(p.accountsStarted)
	<-p.release
	close(p.accountsDone)
}
func (p *blockingLifecycleProvider) DeleteLink(debridTypes.DownloadLink) error {
	return errors.ErrUnsupported
}
func (p *blockingLifecycleProvider) SpeedTest(context.Context) debridTypes.SpeedTestResult {
	return debridTypes.SpeedTestResult{}
}
func (p *blockingLifecycleProvider) SupportsCheck() bool { return false }
