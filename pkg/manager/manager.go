package manager

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trifocals3537/tessarr/internal/cdntraffic"
	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/internal/providertraffic"
	"github.com/Trifocals3537/tessarr/internal/request"
	"github.com/Trifocals3537/tessarr/internal/tlsconfig"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/internal/workflow"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/manager/link"
	"github.com/Trifocals3537/tessarr/pkg/notifications"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/Trifocals3537/tessarr/pkg/usenet"
	"github.com/Trifocals3537/tessarr/pkg/version"
	"github.com/go-co-op/gocron/v2"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"golang.org/x/sync/singleflight"
)

// Manager handles unified torrent management - replaces wire.Store completely
type Manager struct {
	cleanupPhaseForTest func(string)
	workflowFault       atomic.Pointer[workflowFault]
	storage             *storage.Storage
	migrator            *Migrator
	repair              *Repair
	clients             *xsync.Map[string, debrid.Client]
	arr                 *arr.Storage
	logger              zerolog.Logger
	ready               chan struct{}
	readyOnce           sync.Once
	lifecycle           atomic.Pointer[lifecycleSnapshot]
	streamClient        *http.Client
	streamWait          func(context.Context, time.Duration) error
	cdnTraffic          *cdntraffic.Governor
	providerTraffic     *providertraffic.Controller

	// Migration jobs tracking
	migrationJobs             *xsync.Map[string, *storage.SwitcherJob]
	refreshInterval           time.Duration
	uncachedStallTimeout      time.Duration
	stallCandidatesMu         sync.Mutex
	stallCandidates           map[string]stallCandidate
	uncachedFreshChecks       atomic.Uint64
	uncachedTerminalConfirmed atomic.Uint64
	uncachedStallConfirmed    atomic.Uint64
	uncachedHandoffAccepted   atomic.Uint64
	uncachedHandoffErrors     atomic.Uint64
	terminalReadFailures      atomic.Uint64
	readFailureLogMu          sync.Mutex
	readFailureLogLast        map[string]time.Time

	config *config.Config

	// Processing workers
	scheduler    gocron.Scheduler
	cetScheduler gocron.Scheduler
	queue        *Queue

	// downloading
	refreshSG          singleflight.Group
	migrationCleanupSG singleflight.Group
	linkService        downloadLinkService

	// migrationCleanupNow is overridden only by focused retry/backoff tests.
	// Production always falls back to time.Now.
	migrationCleanupNow func() time.Time
	migrationLocksMu    sync.Mutex
	migrationLocks      map[string]*migrationEntryLock

	// Coalesced logging for provider-rediscovery skips: entries deleted on the
	// provider side but still present in provider lists are skipped on every
	// sync cycle, and would otherwise re-log the same guard notice per cycle.
	// The map is bounded by the number of tombstoned entries.
	rediscoveryPendingMu      sync.Mutex
	rediscoveryPendingLast    map[string]time.Time
	rediscoveryPendingNowFunc func() time.Time

	// repair
	fixer  *Fixer
	ctx    context.Context
	cancel context.CancelFunc
	strm   *Strm

	background            sync.WaitGroup
	backgroundMu          sync.Mutex
	backgroundStopping    bool
	backgroundWaitTimeout time.Duration
	initializationErr     error

	customFolders *CustomFolders
	mountManager  MountManager
	cacheWarmMu   sync.Mutex
	cacheWarmSem  chan struct{}

	startTime     time.Time
	usenetTimeout time.Duration

	rootInfo   *FileInfo
	entry      *EntryCache
	downloader *Downloader
	usenet     *usenet.Usenet

	// Debrid speed test results storage
	debridSpeedTestResults *xsync.Map[string, debridTypes.SpeedTestResult]

	// Active streams tracking
	activeStreams         *xsync.Map[string, *ActiveStream]
	activeStreamSequence  atomic.Uint64
	streamRequestSequence atomic.Uint64

	// Successful pre-byte failovers are remembered briefly so range-heavy
	// playback does not retry a known-bad primary on every seek. Counters are
	// exposed through the secret-free stats snapshot.
	streamProviderPreferences *xsync.Map[string, streamProviderPreference]
	streamProviderWeather     *streamProviderWeather
	streamFileCircuits        *streamFileCircuitBreaker
	streamFailoverAttempts    atomic.Uint64
	streamFailoverSuccesses   atomic.Uint64
	streamFailoverExhausted   atomic.Uint64
	streamPreferredHits       atomic.Uint64
	streamProviderDeferrals   atomic.Uint64
	streamProviderDegraded    atomic.Uint64
	streamProviderRecoveries  atomic.Uint64
	streamHandoffAttempts     atomic.Uint64
	streamHandoffSuccesses    atomic.Uint64
	streamFileCircuitOpens    atomic.Uint64
	streamFileCircuitDefers   atomic.Uint64
	streamFileCircuitRecovers atomic.Uint64
	streamSessionMetrics      streamSessionMetrics

	// Provider-scoped content-policy cooldowns prevent recurring Arr grabs
	// from re-hitting a provider while preserving fallback to other providers.
	submissionRejections        *submissionRejectionCache
	submissionContentRejections atomic.Uint64
	submissionRejectionHits     atomic.Uint64

	// In-flight queue-processor dispatches, keyed by InfoHash, to prevent
	// duplicate goroutines from processing the same entry when the scheduler
	// re-fires before the previous pass has updated the queue row.
	processingEntries *xsync.Map[string, struct{}]

	// Unified active-download queue for torrent and NZB imports.
	jobQueue       *JobQueue
	entryLifecycle *entryLifecycle
	nzbSyncMu      sync.Mutex

	// Notifications service
	Notifications *notifications.Service
}

type downloadLinkService interface {
	GetLink(context.Context, *storage.Entry, string) (debridTypes.DownloadLink, error)
	Refresh(context.Context, *storage.Entry, string, debridTypes.DownloadLink) (debridTypes.DownloadLink, error)
	Clear()
}

func newStreamHTTPClient(governor *cdntraffic.Governor) *http.Client {
	// Optimized transport for high-performance streaming with HTTP/2
	// multiplexing and verified TLS.
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	transport := &http.Transport{
		TLSClientConfig: tlsconfig.Harden(&tls.Config{
			MinVersion:         tls.VersionTLS12,
			ClientSessionCache: tls.NewLRUClientSessionCache(200),
		}),
		TLSHandshakeTimeout:    20 * time.Second,
		ResponseHeaderTimeout:  30 * time.Second,
		MaxIdleConns:           1000,
		MaxIdleConnsPerHost:    500,
		MaxConnsPerHost:        500,
		IdleConnTimeout:        120 * time.Second,
		DisableCompression:     false,
		DialContext:            dialer.DialContext,
		Proxy:                  http.ProxyFromEnvironment,
		MaxResponseHeaderBytes: 1 << 20,
		WriteBufferSize:        32 << 10,
		ReadBufferSize:         32 << 10,
		ForceAttemptHTTP2:      true,
	}

	if governor == nil {
		governor = cdntraffic.New(cdntraffic.Options{})
	}

	return &http.Client{
		Timeout:       0,
		Transport:     cdntraffic.NewTransport(transport, governor),
		CheckRedirect: request.NoRefererRedirectPolicy,
	}
}

// New creates a new Manager instance
func New() *Manager {
	cfg := config.Get()
	_logger := logger.New("manager")

	strg, err := storage.NewStorage(filepath.Join(config.GetMainPath(), "db"))
	if err != nil {
		panic(fmt.Errorf("failed to create manager storage: %w", err))
	}

	usenetTimeout, err := utils.ParseDuration(cfg.Usenet.ProcessingTimeout)
	if err != nil {
		usenetTimeout = 10 * time.Minute
	}
	entryLifecycle := newEntryLifecycle()
	providerTraffic := providertraffic.New(providertraffic.Options{})
	cdnGovernor := cdntraffic.New(cdntraffic.Options{Traffic: providerTraffic})

	instance := &Manager{
		storage:                   strg,
		clients:                   xsync.NewMap[string, debrid.Client](),
		logger:                    _logger,
		migrationJobs:             xsync.NewMap[string, *storage.SwitcherJob](),
		config:                    cfg,
		arr:                       arr.NewStorage(),
		queue:                     newQueue(strg, cfg.RemoveStalledAfter, entryLifecycle),
		ready:                     make(chan struct{}),
		streamClient:              newStreamHTTPClient(cdnGovernor),
		streamWait:                waitForStreamRetry,
		cdnTraffic:                cdnGovernor,
		providerTraffic:           providerTraffic,
		usenetTimeout:             usenetTimeout,
		debridSpeedTestResults:    xsync.NewMap[string, debridTypes.SpeedTestResult](),
		activeStreams:             xsync.NewMap[string, *ActiveStream](),
		streamProviderPreferences: xsync.NewMap[string, streamProviderPreference](),
		streamProviderWeather:     newStreamProviderWeather(),
		streamFileCircuits:        newStreamFileCircuitBreaker(),
		processingEntries:         xsync.NewMap[string, struct{}](),
		submissionRejections:      newSubmissionRejectionCache(defaultSubmissionRejectionTTL, defaultSubmissionRejectionCapacity),
		entryLifecycle:            entryLifecycle,
	}

	instance.resetLifecycle()
	instance.init()

	// Create migrator
	return instance
}

func (m *Manager) resetLifecycle() {
	m.workflowFault.Store(nil)
	m.ctx, m.cancel = context.WithCancel(context.Background())
	m.setLifecyclePhase(LifecyclePhaseInitializing, "manager is initializing")
	m.backgroundMu.Lock()
	m.backgroundStopping = false
	m.backgroundMu.Unlock()
}

// startBackground registers manager-owned work before starting it. Stop closes
// this registration gate before waiting, which makes Add and Wait ordering safe
// and prevents late scheduler callbacks from escaping the shutdown barrier.
func (m *Manager) startBackground(name string, work func()) bool {
	m.backgroundMu.Lock()
	if m.backgroundStopping {
		m.backgroundMu.Unlock()
		return false
	}
	m.background.Add(1)
	m.backgroundMu.Unlock()

	go func() {
		defer m.background.Done()
		defer func() {
			if recovered := recover(); recovered != nil {
				m.logger.Error().
					Str("task", name).
					Interface("panic", recovered).
					Bytes("stack", debug.Stack()).
					Msg("Recovered from panic in background manager task")
			}
		}()
		work()
	}()
	return true
}

// startEntryBackground registers scheduler work with the per-entry lifecycle
// before the goroutine is admitted. Explicit deletion can therefore cancel and
// drain it, while a stale scheduler snapshot cannot start against a replacement
// row with the same key.
func (m *Manager) startEntryBackground(parent context.Context, name string, entry *storage.Entry, work func(context.Context) error) bool {
	if entry == nil || work == nil {
		return false
	}
	lease, err := m.entryLifecycle.startWork(parent, entry.InfoHash, entry.QueueGeneration)
	if err != nil {
		m.logger.Debug().
			Err(err).
			Str("task", name).
			Str("entry", entry.InfoHash).
			Msg("Skipping stale or deleting scheduled entry")
		return false
	}

	if !m.startBackground(name, func() {
		var workErr error
		func() {
			defer lease.Close()
			outcome := workflow.Run(lease.Context(), work)
			workErr = outcome.Err
			if outcome.Kind == workflow.Panicked {
				m.logger.Error().Str("task", name).Bytes("stack", outcome.Stack).Msg("Scheduled workflow panicked")
			}
			if workErr != nil && lease.Context().Err() == nil && outcome.Kind != workflow.Canceled && !errors.Is(workErr, errDeleteQueueEntryOnJobFinish) {
				_ = m.commitJobOutcome(&Job{Entry: entry}, storage.QueueOutcome{Kind: storage.QueueOutcomeFailed, Err: workErr})
			}
		}()

		if errors.Is(workErr, errDeleteQueueEntryOnJobFinish) {
			if err := m.queue.Delete(entry.InfoHash, nil); err != nil {
				m.logger.Error().
					Err(err).
					Str("entry", entry.InfoHash).
					Msg("Failed to delete completed queue entry after scheduled work drained")
			}
			return
		}
		if workErr != nil && !errors.Is(workErr, context.Canceled) {
			m.logger.Error().
				Err(workErr).
				Str("task", name).
				Str("entry", entry.InfoHash).
				Msg("Scheduled entry processing failed")
		}
	}) {
		lease.Close()
		return false
	}
	return true
}

func (m *Manager) stopAcceptingBackgroundWork() {
	m.backgroundMu.Lock()
	m.backgroundStopping = true
	m.backgroundMu.Unlock()
}

const (
	defaultBackgroundWaitTimeout   = 30 * time.Second
	defaultNotificationStopTimeout = 5 * time.Second
)

func (m *Manager) waitForBackground() error {
	timeout := m.backgroundWaitTimeout
	if timeout <= 0 {
		timeout = defaultBackgroundWaitTimeout
	}

	done := make(chan struct{})
	go func() {
		m.background.Wait()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return fmt.Errorf("manager background work did not stop within %s", timeout)
	}
}

const managerSchedulerTag = "tessarr-manager"

func newManagerScheduler(location *time.Location, tag string) (gocron.Scheduler, error) {
	options := []gocron.SchedulerOption{
		gocron.WithGlobalJobOptions(
			gocron.WithTags(tag),
			// Periodic reconciliation is state-based and safe to retry on the
			// next tick. Rescheduling a tick that arrives while the previous run
			// is active prevents slow providers or storage from creating an
			// unbounded backlog of duplicate work.
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
		),
	}
	if location != nil {
		options = append(options, gocron.WithLocation(location))
	}
	return gocron.NewScheduler(options...)
}

func (m *Manager) init() {
	m.initializationErr = nil
	cfg := config.Get()
	scheduler, err := newManagerScheduler(time.Local, managerSchedulerTag)
	if err != nil {
		scheduler, _ = newManagerScheduler(nil, managerSchedulerTag)
	}

	// Create CET scheduler for time-specific jobs
	cetLocation, err := time.LoadLocation("CET")
	if err != nil {
		cetLocation = time.UTC
	}
	cetScheduler, err := newManagerScheduler(cetLocation, "tessarr-cet")
	if err != nil {
		cetScheduler, _ = newManagerScheduler(nil, "tessarr-cet")
	}

	m.config = cfg

	// Recreate queue with new config
	if m.entryLifecycle == nil {
		m.entryLifecycle = newEntryLifecycle()
	}
	m.queue = newQueue(m.storage, cfg.RemoveStalledAfter, m.entryLifecycle)

	// Clear debrid clients so they get recreated with new config
	m.clients = xsync.NewMap[string, debrid.Client]()

	// Reset ready channel and syncTorrents.Once for the next start
	m.ready = make(chan struct{})
	m.readyOnce = sync.Once{}

	m.scheduler = scheduler
	m.cetScheduler = cetScheduler
	m.migrator = NewMigrator(m.storage)
	m.downloader = NewDownloadManager(m)

	// Initialize HTTP pool for streaming
	// Note: We can't create a single pool for all files because the LinkRefresh callback
	// needs torrent+filename context. Instead, manager.Stream will create a pool per request
	// and cache it. This is actually better because different files may have different
	// download links from different CDNs.

	refreshInterval, err := utils.ParseDuration(cfg.RefreshInterval)
	if err != nil {
		refreshInterval = 15 * time.Minute
	}
	m.refreshInterval = refreshInterval

	uncachedStallTimeout, err := parseUncachedStallTimeout(cfg.UncachedStallTimeout)
	if err != nil {
		m.logger.Warn().
			Err(err).
			Str("configured_timeout", cfg.UncachedStallTimeout).
			Msg("Uncached transfer watchdog disabled")
		uncachedStallTimeout = 0
	}
	m.uncachedStallTimeout = uncachedStallTimeout

	// initialize debrid clients
	m.initDebridClients()

	// Initialize usenet client
	m.initUsenet()

	// Initialize link service
	m.initLinkService()

	// Init custom folders
	m.initCustomFolders()

	// Initialize fixer
	m.fixer = NewFixer(m)
	m.strm = NewStrm(m)

	// Set mount paths
	m.setMountPaths()

	m.initEntryCache()

	// Initialize notifications service
	m.Notifications = notifications.New(&m.config.Notifications, m.logger)

	// Initialize repair service. It registers with the scheduler in StartWorker.
	m.repair = NewRepair(m)

	// Adopt legacy NZB ownership before workers, restore, or new intake can
	// touch queue entries. On failure Start surfaces the stored error and no
	// JobQueue or restore goroutine is created.
	m.initializeActiveDownloads(m.adoptLegacyUsenetOwnership)
}

func (m *Manager) initUsenet() {
	usenetClient, err := usenet.New()
	if err != nil {
		m.logger.Warn().Msg("Usenet client not configured")
		m.usenet = nil
		return
	}
	m.usenet = usenetClient
}

// initLinkService initializes the link service
func (m *Manager) initLinkService() {
	m.linkService = link.New(
		m.clients,
		m.refreshTorrent,
		m.ReinsertEntry,
		func(entry *storage.Entry) error { return m.AddOrUpdate(entry, nil) },
		m.streamClient,
		m.config.Retries,
		logger.New("link"),
		m.cdnProviderType,
	)
}

func (m *Manager) initJobQueue() {
	m.jobQueue = NewJobQueueWithCapacity(
		m.ctx,
		m.config.MaxActiveDownloads,
		m.config.JobQueueCapacity,
		m.processJob,
		m.entryLifecycle,
	)
	m.queue.removePendingJobs = m.jobQueue.DeleteJobs
	m.jobQueue.afterFunc = func(job *Job) {
		if job == nil || job.Entry == nil {
			return
		}
		if err := m.queue.Delete(job.Entry.InfoHash, nil); err != nil {
			m.logger.Error().
				Err(err).
				Str("entry", job.Entry.InfoHash).
				Msg("Failed to delete completed queue entry after job drain")
		}
	}
	// Restore persisted active/queued downloads in the background. With large
	// queues this re-parses thousands of NZBs over the network, and running it
	// inline blocked manager construction — and therefore the HTTP server —
	// for 60-90 minutes on big libraries, during which every arr reported
	// "download client unavailable". Backgrounding lets the API serve and the
	// worker pool drain immediately while the restore catches up.
	m.startBackground("restore active downloads", func() {
		m.restoreActiveDownloadJobs(m.ctx)
	})
}

func (m *Manager) initializeActiveDownloads(adopt func() error) {
	if adopt == nil {
		m.initializationErr = fmt.Errorf("legacy NZB ownership adoption is unavailable")
		m.jobQueue = nil
		return
	}
	if err := adopt(); err != nil {
		m.initializationErr = fmt.Errorf("initialize legacy NZB ownership: %w", err)
		m.jobQueue = nil
		return
	}
	residual, fatal := m.recoverQueuedDeletions()
	if fatal != nil {
		m.initializationErr = fmt.Errorf(
			"recover interrupted queue deletions: %w",
			fatal,
		)
		m.jobQueue = nil
		return
	}
	if residual != nil {
		m.logger.Error().
			Err(residual).
			Msg("Interrupted queue deletions retained cleanup tombstones")
	}
	if err := m.recoverInterruptedDownloads(m.ctx); err != nil {
		m.initializationErr = fmt.Errorf("recover interrupted downloads: %w", err)
		m.jobQueue = nil
		return
	}
	m.initJobQueue()
}

func (m *Manager) processJob(ctx context.Context, job *Job) {
	if job == nil {
		return
	}
	if err := m.workflowAvailable(); err != nil {
		// The worker will release this admission on return. Keep recovery
		// latched even if a later sync succeeds: queued rows are reconstructed
		// at startup, not by the normal provider-status scheduler.
		m.recordWorkflowFault(err)
		return
	}
	outcome := workflow.Run(ctx, func(workCtx context.Context) error {
		if job.Entry != nil && job.Request == nil && job.DebridTorrent == nil && job.NZBMeta == nil && !job.ResumeExisting {
			m.waitForDownloadCompletion(workCtx, job.Entry)
			return nil
		}
		switch job.Type {
		case JobTypeTorrent:
			return m.processTorrentJob(workCtx, job)
		case JobTypeNZB:
			return m.processNZBJob(workCtx, job)
		default:
			return fmt.Errorf("unknown job type: %s", job.Type)
		}
	})
	if outcome.Kind == workflow.Panicked {
		m.logger.Error().Str("job_id", job.ID).Bytes("stack", outcome.Stack).Msg("Download workflow panicked")
	}
	if outcome.Kind == workflow.Canceled {
		return
	}
	if err := outcome.Err; err != nil {
		if errors.Is(err, errDeleteQueueEntryOnJobFinish) {
			job.DeleteOnFinish = true
			return
		}
		if ctx.Err() != nil {
			return
		}
		// A persistence fault may already have committed part of the provider
		// checkpoint. Preserve that recoverable row instead of recording a new
		// terminal failure through a different, still-healthy store.
		if m.workflowAvailable() != nil {
			return
		}
		if isTooManyActiveDownloads(err) && !errors.Is(err, ErrProviderSubmissionPending) {
			if commitErr := m.commitJobOutcome(job, storage.QueueOutcome{Kind: storage.QueueOutcomeDeferred}); commitErr != nil {
				return
			}
			if retryErr := m.jobQueue.Retry(job, 30*time.Second); retryErr != nil {
				m.logger.Error().
					Err(retryErr).
					Str("job_id", job.ID).
					Msg("Failed to schedule active-download retry")
				if job.Entry != nil {
					_ = m.commitJobOutcome(job, storage.QueueOutcome{Kind: storage.QueueOutcomeFailed, Err: retryErr})
				}
			}
			return
		}
		m.logger.Error().Err(err).Str("job_id", job.ID).Str("type", string(job.Type)).Msg("Active download failed")
		if job.Entry != nil {
			_ = m.commitJobOutcome(job, storage.QueueOutcome{Kind: storage.QueueOutcomeFailed, Err: err})
		}
		return
	}

	m.waitForDownloadCompletion(ctx, job.Entry)
}

func (m *Manager) waitForDownloadCompletion(ctx context.Context, entry *storage.Entry) {
	if entry == nil {
		return
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		current, err := m.queue.GetTorrent(entry.InfoHash)
		if err != nil || current.State != storage.EntryStateDownloading {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (m *Manager) migrate(ctx context.Context) {
	// Check if migration has already been done
	status, err := m.migrator.GetStatus()
	if err == nil && !status.Running && status.Completed > 0 {
		m.logger.Info().
			Int("completed", status.Completed).
			Int("errors", status.Errors).
			Msg("Migration already completed previously")
		return
	}

	// GetReader migration stats to see if there are cache files
	stats, err := m.migrator.GetStats()
	if err != nil {
		m.logger.Warn().Err(err).Msg("Failed to get migration stats")
		return
	}

	cacheFiles, ok := stats["cache_files"].(int)
	if !ok || cacheFiles == 0 {
		return
	}

	cacheTorrents, ok := stats["cache_torrents"].(int)
	if !ok {
		cacheTorrents = 0
	}

	m.logger.Info().
		Int("cache_files", cacheFiles).
		Int("unique_torrents", cacheTorrents).
		Msg("Found cache files, starting automatic migration...")

	// Start migration with backup
	if err := m.migrator.Start(ctx); err != nil {
		m.logger.Error().Err(err).Msg("Failed to start automatic migration")
		return
	}

	m.logger.Info().Msg("Automatic migration started successfully")
}

// Start starts the manager and all its components
func (m *Manager) Start(ctx context.Context) error {
	m.setLifecyclePhase(LifecyclePhaseStarting, "manager services are starting")
	if m.initializationErr != nil {
		m.setLifecyclePhase(LifecyclePhaseFailed, "manager initialization failed")
		return m.initializationErr
	}
	if m.jobQueue == nil {
		m.setLifecyclePhase(LifecyclePhaseFailed, "active download queue is unavailable")
		return fmt.Errorf("active download queue is not initialized")
	}
	m.startTime = time.Now()
	m.logger.Info().
		Str("version", version.GetInfo().String()).
		Str("mount_type", string(m.config.Mount.Type)).
		Str("notifications", fmt.Sprintf("%v", m.Notifications.IsEnabled())).
		Str("mount_path", m.config.Mount.MountPath).
		Msg("Starting manager")

	// Tie manager-owned startup work to the service lifetime. Stop also cancels
	// this context, so restarts and direct shutdowns use the same path.
	m.startBackground("service cancellation", func() {
		select {
		case <-ctx.Done():
			if m.cancel != nil {
				m.cancel()
			}
		case <-m.ctx.Done():
		}
	})

	// run the migration process
	m.migrate(m.ctx)

	m.startBackground("initial provider sync", func() {
		m.syncTorrents(m.ctx)
		if m.ctx.Err() != nil {
			return
		}
		// Sync NZBs
		if err := m.syncNZBs(m.ctx); err != nil && m.ctx.Err() == nil {
			m.logger.Error().Err(err).Msg("Failed to perform initial NZB syncTorrents")
		}
		if fixNZB := os.Getenv("TESSARR_FIX_NZB_SIZES"); fixNZB == "1" {
			m.logger.Info().Msg("Starting NZB file size correction as requested by environment variable")
			m.fixNZBFileSizes(m.ctx)
		}
		m.strm.SweepAsync("startup")
	})

	// Start workers
	if err := m.StartWorker(m.ctx); err != nil {
		m.setLifecyclePhase(LifecyclePhaseFailed, "manager workers failed to start")
		return fmt.Errorf("failed to start manager worker: %w", err)
	}

	m.startCleanupWorkflows()

	// Start the mount manager if set
	// This also start thr mounting process
	if m.mountManager != nil {
		m.setLifecyclePhase(LifecyclePhaseMounting, "media mount is starting")
		if err := m.mountManager.Start(ctx); err != nil {
			m.setLifecyclePhase(LifecyclePhaseFailed, "media mount failed to start")
			return fmt.Errorf("failed to start mount manager: %w", err)
		}
		if err := waitForMountReady(ctx, m.mountManager, defaultMountReadyTimeout, defaultMountReadyPollInterval); err != nil {
			m.setLifecyclePhase(LifecyclePhaseFailed, "media mount did not become ready")
			return fmt.Errorf("media mount readiness: %w", err)
		}
	}

	m.setLifecyclePhase(LifecyclePhaseReady, "media data path is ready")
	// Publish readiness only after the configured mount has proved ready.
	m.readyOnce.Do(func() {
		close(m.ready)
	})

	return nil
}

// Stop stops the manager and cleans up all resources
func (m *Manager) Stop() error {
	m.setLifecyclePhase(LifecyclePhaseStopping, "manager is stopping")
	m.logger.Info().Msg("Stopping manager")
	var shutdownErr error

	// Close the background registration gate before cancellation. Any work
	// accepted before this point is included in the barrier; later scheduler
	// callbacks are rejected.
	m.stopAcceptingBackgroundWork()

	if m.cancel != nil {
		m.cancel()
	}

	if m.migrator != nil {
		if err := m.migrator.Stop(); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("failed to stop migration: %w", err))
		}
	}

	// Stop schedulers
	if m.scheduler != nil {
		if err := m.scheduler.Shutdown(); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("failed to shutdown scheduler: %w", err))
		} else {
			// Shutdown consumes the scheduler's terminal result. Clear the
			// handle so a retry after a later drain timeout remains safe.
			m.scheduler = nil
		}
	}
	if m.cetScheduler != nil {
		if err := m.cetScheduler.Shutdown(); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("failed to shutdown CET scheduler: %w", err))
		} else {
			m.cetScheduler = nil
		}
	}

	if m.jobQueue != nil {
		m.logger.Info().Msg("Closing active download queue")
		m.jobQueue.Close()
	}

	if m.repair != nil {
		if err := m.repair.Stop(); err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("failed to stop repair service: %w", err))
		}
	}

	// All manager-owned startup work must finish before mount, usenet, or
	// storage resources are closed. A provider API may not expose a
	// context-aware method; in that case return a bounded error and preserve
	// those resources so the unfinished task can never run against closed
	// state.
	if err := m.waitForBackground(); err != nil {
		shutdownErr = errors.Join(shutdownErr, err)
	}
	if shutdownErr != nil {
		m.logger.Error().Err(shutdownErr).Msg("Manager shutdown paused with resources still open")
		return shutdownErr
	}

	if m.Notifications != nil {
		notificationCtx, notificationCancel := context.WithTimeout(
			context.Background(),
			defaultNotificationStopTimeout,
		)
		if err := m.Notifications.Stop(notificationCtx); err != nil {
			// Notification events contain detached snapshots and do not own mount,
			// usenet, or storage resources. A slow webhook must not prevent those
			// critical resources from closing.
			m.logger.Warn().Err(err).Msg("Notification delivery did not drain before shutdown")
		}
		notificationCancel()
	}

	if m.mountManager != nil {
		m.logger.Info().Msg("Stopping mount manager")
		if err := m.mountManager.Stop(); err != nil {
			return fmt.Errorf("failed to stop mount manager: %w", err)
		}
	}

	// Close usenet connection manager if active
	if m.usenet != nil {
		m.logger.Info().Msg("Closing usenet connections")
		if err := m.usenet.Close(); err != nil {
			return fmt.Errorf("failed to close usenet: %w", err)
		}
	}

	// Close storage
	if m.storage != nil {
		m.logger.Info().Msg("Closing storage database")
		if err := m.storage.Close(); err != nil {
			return fmt.Errorf("failed to close storage: %w", err)
		}
	}

	m.logger.Info().Msg("Manager stopped successfully")
	m.setLifecyclePhase(LifecyclePhaseStopped, "manager is stopped")
	return nil
}

// Reset resets the manager with the new configuration
// This is called after config changes (e.g., setup wizard) to apply new settings
func (m *Manager) Reset() error {
	m.logger.Info().Msg("Resetting manager with new configuration")

	// Stop resources before resetting
	if err := m.Stop(); err != nil {
		return fmt.Errorf("failed to stop manager during reset: %w", err)
	}

	// Reopen storage database (it was closed by Stop)
	strg, err := storage.NewStorage(filepath.Join(config.GetMainPath(), "db"))
	if err != nil {
		return fmt.Errorf("failed to reopen storage after reset: %w", err)
	}
	m.storage = strg

	m.resetLifecycle()

	// Reload configuration
	m.init()
	if m.initializationErr != nil {
		return fmt.Errorf("manager reset initialization failed: %w", m.initializationErr)
	}
	m.logger.Info().Msg("Manager reset complete")
	return nil
}

func (m *Manager) GetStats() (map[string]any, error) {
	count, err := m.storage.Count()
	if err != nil {
		return nil, err
	}

	diskSize := m.storage.DiskSize()
	activeJobs := 0
	completedJobs := 0
	failedJobs := 0
	m.migrationJobs.Range(func(_ string, job *storage.SwitcherJob) bool {
		switch job.Status {
		case storage.SwitcherStatusPending, storage.SwitcherStatusInProgress:
			activeJobs++
		case storage.SwitcherStatusCompleted:
			completedJobs++
		case storage.SwitcherStatusFailed, storage.SwitcherStatusCancelled:
			failedJobs++
		}
		return true
	})

	return map[string]any{
		"total_torrents":             count,
		"storage_stats":              map[string]any{"total_size": diskSize},
		"active_jobs":                activeJobs,
		"completed_jobs":             completedJobs,
		"failed_jobs":                failedJobs,
		"pending_migration_cleanups": m.storage.MigrationCleanupCount(),
		"uncached_handoff": map[string]uint64{
			"fresh_checks":       m.uncachedFreshChecks.Load(),
			"terminal_confirmed": m.uncachedTerminalConfirmed.Load(),
			"stall_confirmed":    m.uncachedStallConfirmed.Load(),
			"arr_accepted":       m.uncachedHandoffAccepted.Load(),
			"handoff_errors":     m.uncachedHandoffErrors.Load(),
		},
		"terminal_read_failures": m.terminalReadFailures.Load(),
	}, nil
}

func (m *Manager) IsReady() chan struct{} {
	return m.ready
}

func (m *Manager) Uptime() time.Duration {
	return time.Since(m.startTime)
}

func (m *Manager) StartTime() time.Time {
	return m.startTime
}

// CRUD operations

func (m *Manager) GetEntryItem(torrentName string) (*storage.EntryItem, error) {
	return m.storage.GetEntryItem(torrentName)
}

func (m *Manager) GetEntryByName(torrentName, filename string) (*storage.Entry, error) {
	// First get entry
	entry, err := m.storage.GetEntryItem(torrentName)
	if err != nil {
		return nil, err
	}

	// Find the file in the entry
	file, err := entry.GetFile(filename)
	if err != nil {
		return nil, err
	}
	return m.GetEntry(file.InfoHash)
}

func (m *Manager) AddOrUpdate(entry *storage.Entry, callback func(t *storage.Entry)) error {
	entry.UpdatedAt = time.Now()
	if err := m.storage.AddOrUpdateDurable(entry); err != nil {
		return err
	}
	if entry.IsComplete && m.strm != nil {
		m.strm.SyncEntryAsync(entry.InfoHash)
	}
	if callback != nil {
		m.startBackground("entry update callback", func() {
			callback(entry)
		})
	}
	return nil
}

// GetEntry gets a torrent by name
func (m *Manager) GetEntry(infohash string) (*storage.Entry, error) {
	return m.storage.Get(infohash)
}

func (m *Manager) EntryExists(infohash string) (bool, error) {
	return m.storage.Exists(infohash)
}

func (m *Manager) GetTorrents(filter func(*storage.Entry) bool) ([]*storage.Entry, error) {
	// Use streaming to avoid loading all torrents into memory at once
	var torrents []*storage.Entry
	err := m.storage.ForEach(func(t *storage.Entry) error {
		if filter == nil || filter(t) {
			torrents = append(torrents, t)
		}
		return nil
	})
	return torrents, err
}

func (m *Manager) GetTorrentsCount() (int, error) {
	return m.storage.Count()
}

// DeleteEntry deletes a torrent by infohash
func (m *Manager) DeleteEntry(infohash string, removePlacements bool) error {
	err := m.deleteMainEntryWithCleanup(infohash, func(mainEntry *storage.Entry) error {
		if removePlacements {
			if err := m.storage.PrepareMainDeletionPlacements(mainEntry); err != nil {
				return err
			}
		}
		// A completed entry is written to main storage before its post-download
		// action finishes. The main-entry tombstone is already installed here;
		// queue deletion then drains that worker before provider or main-row
		// cleanup can complete.
		if m.queue != nil {
			if removePlacements {
				return m.deleteAdministrativeQueueAndMainPlacements(infohash, mainEntry)
			}
			_, err := m.queue.deleteWithResultAndSnapshots(infohash, nil, mainEntry)
			return err
		}
		if removePlacements {
			return m.finishAdministrativeMainPlacements(mainEntry)
		}
		return nil
	})
	if removePlacements && storage.IsEntryNotFound(err) && m.queue != nil {
		if handled, retryErr := m.retryAdoptedAdministrativeCleanup(infohash); handled {
			return retryErr
		}
	}
	return err
}

func (m *Manager) deleteAdministrativeQueueAndMainPlacements(infohash string, mainEntry *storage.Entry) error {
	if handled, err := m.retryAdoptedAdministrativeCleanup(infohash); handled {
		return err
	}
	mainFinished := false
	_, err := m.queue.deleteWithPreparation(infohash, func(entry *storage.Entry) error { return m.RemoveTorrentPlacements(entry) }, true, func() (*storage.QueueDeletionIntent, error) {
		return m.queue.storage.PrepareQueuedDeletion(infohash, true, mainEntry)
	}, func() error {
		if err := m.finishAdministrativeMainPlacements(mainEntry); err != nil {
			return err
		}
		mainFinished = true
		return nil
	})
	if err != nil {
		return err
	}
	if !mainFinished {
		return m.finishAdministrativeMainPlacements(mainEntry)
	}
	return nil
}

func (m *Manager) finishAdministrativeMainPlacements(entry *storage.Entry) error {
	if err := m.RemoveTorrentPlacements(entry); err != nil {
		return err
	}
	return m.storage.CompleteMainDeletionPlacements(entry.InfoHash)
}

func (m *Manager) deleteMainEntryWithCleanup(
	infohash string,
	cleanup func(*storage.Entry) error,
) error {
	var deleted *storage.Entry
	if err := m.storage.DeleteWithCleanup(infohash, func(entry *storage.Entry) error {
		deleted = entry
		if cleanup == nil {
			return nil
		}
		return cleanup(entry)
	}); err != nil {
		return err
	}
	if deleted != nil && m.strm != nil {
		m.strm.RemoveEntryAsync(deleted)
	}
	if m.entry != nil {
		m.RefreshEntries(true)
	}
	return nil
}

// DeleteEntryForQueueCleanup removes the union of main-storage and queued
// placements without deleting main storage. Queue.Delete callers use it before
// filesystem cleanup; main deletion, when requested, must happen only after
// queue filesystem cleanup succeeds.
func (m *Manager) DeleteEntryForQueueCleanup(queued *storage.Entry) error {
	if queued == nil {
		return fmt.Errorf("queued entry is nil")
	}
	mainEntry, err := m.GetEntry(queued.InfoHash)
	if err != nil {
		if storage.IsEntryNotFound(err) {
			return m.RemoveTorrentPlacements(queued)
		}
		return fmt.Errorf("load main entry for queue cleanup: %w", err)
	}
	if err := m.RemoveTorrentPlacements(mainEntry, queued); err != nil {
		return err
	}
	return nil
}

// DeleteQBitQueueEntry retires only the client queue and owned staging data.
// Provider cancellation requires a durable, Arr-validated failed-transfer proof.
func (m *Manager) DeleteQBitQueueEntry(infohash string, deleteFiles bool) error {
	var cleanup func(*storage.Entry) error
	if deleteFiles {
		cleanup = func(e *storage.Entry) error { return m.RemoveTorrentPlacements(e) }
	}
	_, err := m.queue.deleteWithPreparation(infohash, cleanup, deleteFiles, func() (*storage.QueueDeletionIntent, error) {
		return m.queue.storage.PrepareQBitQueuedDeletion(infohash, deleteFiles)
	})
	return err
}

// DeleteQueueEntry is the API-facing queue deletion path. It preserves legacy
// behavior when removePlacements is false. When true, provider cleanup happens
// before queue files, and an associated main row is deleted only after queue
// filesystem cleanup succeeds.
func (m *Manager) DeleteQueueEntry(infohash string, removePlacements bool) error {
	if !removePlacements {
		_, err := m.queue.deleteWithResult(infohash, nil)
		return err
	}

	err := m.deleteMainEntryWithCleanup(infohash, func(mainEntry *storage.Entry) error {
		if err := m.storage.PrepareMainDeletionPlacements(mainEntry); err != nil {
			return err
		}
		return m.deleteAdministrativeQueueAndMainPlacements(infohash, mainEntry)
	})
	if err == nil {
		return nil
	}
	if !storage.IsEntryNotFound(err) {
		return err
	}

	// An authoritative main miss still permits queue-only deletion. Any
	// indeterminate main lookup error returned above fails closed.
	if handled, err := m.retryAdoptedAdministrativeCleanup(infohash); handled {
		return err
	}
	_, queueErr := m.queue.deleteWithResult(infohash, func(queued *storage.Entry) error {
		return m.RemoveTorrentPlacements(queued)
	})
	return queueErr
}

func (m *Manager) DeleteQueueEntries(infohashes []string, removePlacements bool) error {
	var errs []error
	for _, infohash := range infohashes {
		if err := m.DeleteQueueEntry(infohash, removePlacements); err != nil {
			errs = append(errs, fmt.Errorf("delete queue entry %s: %w", infohash, err))
		}
	}
	return errors.Join(errs...)
}

func (m *Manager) DeleteTorrents(infohashes []string, removeFromDebrid bool) error {
	for _, infohash := range infohashes {
		if err := m.DeleteEntry(infohash, removeFromDebrid); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) GetMigrationJob(jobID string) (*storage.SwitcherJob, error) {
	job, exists := m.migrationJobs.Load(jobID)
	if !exists {
		return nil, fmt.Errorf("migration job not found: %s", jobID)
	}
	return job, nil
}

// SubmitJob submits an import to the unified active-download queue.
func (m *Manager) SubmitJob(job *Job) error {
	if err := m.workflowAvailable(); err != nil {
		return err
	}
	if m.jobQueue == nil {
		return fmt.Errorf("active download queue not initialized")
	}
	if err := m.prepareJobSubmission(job); err != nil {
		return err
	}
	return m.jobQueue.Submit(job)
}

func (m *Manager) submitRestoredJob(ctx context.Context, job *Job) error {
	if err := m.workflowAvailable(); err != nil {
		return err
	}
	if m.jobQueue == nil {
		return fmt.Errorf("%w: active download queue not initialized", ErrJobQueueClosed)
	}
	if err := m.prepareJobSubmission(job); err != nil {
		return err
	}
	return m.jobQueue.submitWait(ctx, job)
}

func (m *Manager) reserveJob(
	ctx context.Context,
	jobID string,
) (*jobReservation, error) {
	if err := m.workflowAvailable(); err != nil {
		return nil, err
	}
	if m.jobQueue == nil {
		return nil, fmt.Errorf("%w: active download queue not initialized", ErrJobQueueClosed)
	}
	reservation, err := m.jobQueue.reserveContext(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if m.queue == nil {
		reservation.release()
		return nil, fmt.Errorf("active download storage is unavailable")
	}

	_, lookupErr := m.queue.GetTorrent(jobID)
	switch {
	case lookupErr == nil:
		reservation.release()
		return nil, &DuplicateJobError{Key: normalizeQueueEntryKey(jobID)}
	case errors.Is(lookupErr, storage.ErrQueuedEntryDeleting):
		reservation.release()
		return nil, lookupErr
	case storage.IsQueuedEntryNotFound(lookupErr):
		return reservation, nil
	default:
		reservation.release()
		return nil, fmt.Errorf("check durable queue admission for %s: %w", jobID, lookupErr)
	}
}

func (m *Manager) submitReservedJob(reservation *jobReservation, job *Job) error {
	if m.jobQueue == nil {
		return fmt.Errorf("%w: active download queue not initialized", ErrJobQueueClosed)
	}
	if err := m.prepareJobSubmission(job); err != nil {
		return err
	}
	return m.jobQueue.submitReserved(reservation, job)
}

func (m *Manager) prepareJobSubmission(job *Job) error {
	if job != nil && job.Entry != nil {
		if job.Entry.QueueGeneration == 0 {
			if m.entryLifecycle == nil {
				return fmt.Errorf("active download lifecycle not initialized")
			}
			if err := m.entryLifecycle.bindEntry(job.Entry); err != nil {
				return err
			}
		}
		job.Generation = job.Entry.QueueGeneration
	}
	return nil
}
