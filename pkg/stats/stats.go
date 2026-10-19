package stats

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/internal/utils"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/rs/zerolog"
)

// Collector owns the cached stats snapshot and the HTTP handler.
type Collector struct {
	mgr    *manager.Manager
	logger zerolog.Logger

	mu       sync.RWMutex
	snapshot *Snapshot

	// Cached debrid profiles with TTL
	profileMu      sync.RWMutex
	profileCache   map[string]*debridTypes.Profile
	profileFetched time.Time
	profileTTL     time.Duration

	refreshGate chan struct{}
	refreshReq  chan struct{}
	collectFn   func(context.Context) (*Snapshot, error)

	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	done        chan struct{}
}

const (
	refreshInterval = 5 * time.Second
	refreshTimeout  = 10 * time.Second
)

// New creates an idle Collector. Start owns all background work.
func New(mgr *manager.Manager) *Collector {
	c := &Collector{
		mgr:          mgr,
		logger:       logger.New("stats"),
		profileCache: make(map[string]*debridTypes.Profile),
		profileTTL:   60 * time.Second,
		refreshGate:  make(chan struct{}, 1),
		refreshReq:   make(chan struct{}, 1),
		snapshot:     &Snapshot{},
	}
	c.collectFn = c.collect
	return c
}

// RequestRefresh asks the background loop to refresh soon without delaying the
// caller. Repeated requests are coalesced.
func (c *Collector) RequestRefresh() {
	select {
	case c.refreshReq <- struct{}{}:
	default:
	}
}

// Start begins the background refresh loop. Call from server startup.
func (c *Collector) Start(ctx context.Context) {
	c.lifecycleMu.Lock()
	if c.cancel != nil {
		c.lifecycleMu.Unlock()
		return
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	c.cancel = cancel
	c.done = done
	c.lifecycleMu.Unlock()

	go func() {
		defer close(done)
		c.loop(runCtx)
	}()
}

// Stop cancels the background loop and waits for its in-flight refresh.
func (c *Collector) Stop(ctx context.Context) error {
	c.lifecycleMu.Lock()
	cancel := c.cancel
	done := c.done
	c.lifecycleMu.Unlock()

	if cancel == nil || done == nil {
		return nil
	}
	cancel()

	select {
	case <-done:
		c.lifecycleMu.Lock()
		if c.done == done {
			c.cancel = nil
			c.done = nil
		}
		c.lifecycleMu.Unlock()
		return nil
	case <-ctx.Done():
		return fmt.Errorf("stop stats collector: %w", ctx.Err())
	}
}

// Snapshot returns the latest cached snapshot (zero-alloc per call).
func (c *Collector) Snapshot() *Snapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snapshot
}

// Refresh rebuilds and stores a fresh snapshot immediately. Only one refresh
// runs at a time, and every refresh has a bounded lifetime.
func (c *Collector) Refresh(ctx context.Context) (*Snapshot, error) {
	if ctx == nil {
		return c.Snapshot(), fmt.Errorf("stats refresh context is required")
	}
	refreshCtx, cancel := context.WithTimeout(ctx, refreshTimeout)
	defer cancel()

	select {
	case c.refreshGate <- struct{}{}:
		defer func() { <-c.refreshGate }()
	case <-refreshCtx.Done():
		return c.Snapshot(), refreshCtx.Err()
	}

	snap, err := c.collectFn(refreshCtx)
	if err != nil {
		return c.Snapshot(), err
	}
	c.mu.Lock()
	c.snapshot = snap
	c.mu.Unlock()
	return snap, nil
}

// Handler returns an http.HandlerFunc that serves the cached snapshot as JSON.
func (c *Collector) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		snap := c.Snapshot()
		utils.JSONResponse(w, snap, http.StatusOK)
	}
}

// loop refreshes the snapshot on a timer.
func (c *Collector) loop(ctx context.Context) {
	c.refreshAndLog(ctx)

	ticker := time.NewTicker(refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.refreshReq:
			c.refreshAndLog(ctx)
		case <-ticker.C:
			c.refreshAndLog(ctx)
		}
	}
}

func (c *Collector) refreshAndLog(ctx context.Context) {
	if _, err := c.Refresh(ctx); err != nil && ctx.Err() == nil {
		c.logger.Warn().Err(err).Msg("Failed to refresh statistics")
	}
}

// collect builds a full Snapshot from all subsystems.
func (c *Collector) collect(ctx context.Context) (*Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	uptime := c.mgr.Uptime()
	startTime := c.mgr.StartTime()
	cfg := config.Get()

	snap := &Snapshot{}

	// --- System ---
	mb := func(b uint64) string { return fmt.Sprintf("%.2fMB", float64(b)/1024/1024) }
	snap.System = SystemStats{
		// This is Go runtime memory. Direct mmap allocations are excluded.
		MemoryUsed:     mb(memStats.Sys - memStats.HeapReleased),
		HeapAllocMB:    mb(memStats.HeapAlloc),
		HeapInuseMB:    mb(memStats.HeapInuse),
		HeapReleasedMB: mb(memStats.HeapReleased),
		SysMB:          mb(memStats.Sys),
		GCCycles:       memStats.NumGC,
		Goroutines:     runtime.NumGoroutine(),
		NumCPU:         runtime.NumCPU(),
		OS:             runtime.GOOS,
		Arch:           runtime.GOARCH,
		GoVersion:      runtime.Version(),
		UptimeSeconds:  int64(uptime.Seconds()),
		Uptime:         uptime.String(),
		StartTime:      startTime.Format("2006-01-02 15:04:05"),
	}
	if rss, err := processRSS(); err == nil {
		snap.System.ProcessRSSMB = mb(rss)
	}

	// --- Debrids ---
	debrids, err := c.collectDebrids(ctx, cfg)
	if err != nil {
		return nil, err
	}
	snap.Debrids = debrids

	// --- Mount ---
	mountStats, err := c.collectMount(ctx, cfg)
	if err != nil {
		return nil, err
	}
	snap.Mount = mountStats

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// --- Usenet ---
	if c.mgr.HasUsenet() {
		snap.Usenet = c.mgr.UsenetStats()
	}

	// --- Active Streams ---
	streams := c.mgr.GetActiveStreams()
	snap.ActiveStreams = ActiveStreamStats{
		Count:   len(streams),
		Streams: streams,
	}
	snap.CDNTraffic = c.mgr.CDNTrafficStats()
	snap.StreamFailover = c.mgr.StreamFailoverStats()
	snap.StreamSessions = c.mgr.StreamSessionStats()
	snap.TorrentAdmission = c.mgr.TorrentAdmissionStats()

	// --- Storage ---
	torrentCount, err := c.mgr.GetTorrentsCount()
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to get torrents count")
	}
	snap.Storage = StorageStats{
		DBSize:       c.mgr.Storage().DiskSize(),
		TotalEntries: torrentCount,
	}

	// --- Queue ---
	if queue := c.mgr.JobQueue(); queue != nil {
		snap.Queue = QueueStats{
			Pending: queue.Len(),
			Active:  queue.ActiveCount(),
		}
	}

	// --- Arrs ---
	arrs := c.mgr.Arr().GetAll()
	arrNames := make([]string, 0, len(arrs))
	for _, a := range arrs {
		arrNames = append(arrNames, a.Name)
	}
	snap.Arrs = ArrStats{
		Count: len(arrs),
		Names: arrNames,
	}

	// --- Repair ---
	if svc := c.mgr.Repair(); svc != nil {
		st := svc.Status()
		health := make(map[string]int, len(st.HealthCounts))
		for k, v := range st.HealthCounts {
			health[string(k)] = v
		}
		snap.Repair = RepairStats{
			Enabled: st.Enabled,
			Active:  st.ActiveRun != nil,
			Health:  health,
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return snap, nil
}

// collectDebrids gathers debrid stats with cached profiles.
func (c *Collector) collectDebrids(ctx context.Context, cfg *config.Config) ([]debridTypes.Stats, error) {
	torrentCount, err := c.mgr.GetTorrentsCount()
	if err != nil {
		c.logger.Error().Err(err).Msg("Failed to get torrents count for debrid stats")
		torrentCount = 0
	}

	profiles, err := c.getProfiles(ctx)
	if err != nil {
		return nil, err
	}

	result := make([]debridTypes.Stats, 0)
	c.mgr.Clients().Range(func(debridName string, client debrid.Client) bool {
		if client == nil {
			return true
		}

		ds := debridTypes.Stats{}
		ls := debridTypes.LibraryStats{}

		profile := cloneProfile(profiles[debridName])
		if profile == nil {
			profile = &debridTypes.Profile{Name: debridName}
		}
		profile.Name = debridName
		ds.Profile = profile

		ls.Total = torrentCount
		ls.ActiveLinks = c.mgr.GetTotalActiveDownloadLinks()
		ds.Library = ls
		ds.Accounts = client.AccountManager().Stats()

		if speedResult, ok := c.mgr.GetDebridSpeedTestResult(debridName); ok {
			ds.SpeedTestResult = &speedResult
		}

		result = append(result, ds)
		return true
	})

	// Order by config
	ordered := make([]debridTypes.Stats, 0, len(result))
	for _, debridCfg := range cfg.Debrids {
		for _, ds := range result {
			if ds.Profile.Name == debridCfg.Name {
				ordered = append(ordered, ds)
				break
			}
		}
	}
	return ordered, nil
}

// getProfiles returns cached debrid profiles, refreshing if stale.
func (c *Collector) getProfiles(ctx context.Context) (map[string]*debridTypes.Profile, error) {
	c.profileMu.RLock()
	if !c.profileFetched.IsZero() && time.Since(c.profileFetched) < c.profileTTL {
		profiles := cloneProfiles(c.profileCache)
		c.profileMu.RUnlock()
		return profiles, nil
	}
	c.profileMu.RUnlock()

	// Fetch independent providers concurrently so one slow service does not
	// delay every other account behind it.
	type namedClient struct {
		name   string
		client debrid.Client
	}
	clients := make([]namedClient, 0)
	c.mgr.Clients().Range(func(name string, client debrid.Client) bool {
		if client != nil {
			clients = append(clients, namedClient{name: name, client: client})
		}
		return true
	})
	type profileResult struct {
		name    string
		profile *debridTypes.Profile
		err     error
	}
	results := make(chan profileResult, len(clients))
	for _, item := range clients {
		go func() {
			profile, err := getProfile(ctx, item.client)
			results <- profileResult{name: item.name, profile: profile, err: err}
		}()
	}

	fresh := make(map[string]*debridTypes.Profile)
	for range clients {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case result := <-results:
			if result.err == nil {
				fresh[result.name] = cloneProfile(result.profile)
				continue
			}
			c.logger.Error().Err(result.err).Str("debrid", result.name).Msg("Failed to get debrid profile")
			// Use stale cache entry if available
			c.profileMu.RLock()
			if cached, ok := c.profileCache[result.name]; ok {
				fresh[result.name] = cloneProfile(cached)
			}
			c.profileMu.RUnlock()
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.profileMu.Lock()
	c.profileCache = cloneProfiles(fresh)
	c.profileFetched = time.Now()
	c.profileMu.Unlock()

	return fresh, nil
}

// collectMount gathers mount stats.
func (c *Collector) collectMount(ctx context.Context, cfg *config.Config) (MountStats, error) {
	mountMgr := c.mgr.MountManager()
	enabled := cfg.Mount.Type != config.MountTypeNone

	if mountMgr == nil || !mountMgr.IsReady() {
		return MountStats{
			Ready:   false,
			Enabled: enabled,
		}, nil
	}

	var mountStats map[string]any
	if contextual, ok := mountMgr.(interface {
		StatsContext(context.Context) map[string]any
	}); ok {
		mountStats = contextual.StatsContext(ctx)
	} else {
		mountStats = mountMgr.Stats()
	}
	if err := ctx.Err(); err != nil {
		return MountStats{}, err
	}
	if mountStats == nil {
		return MountStats{
			Ready:   true,
			Enabled: enabled,
			Type:    mountMgr.Type(),
			Error:   "failed to get mount stats",
		}, nil
	}

	return MountStats{
		Ready:   true,
		Enabled: enabled,
		Type:    mountMgr.Type(),
		Detail:  mountStats,
	}, nil
}

type contextualProfileClient interface {
	GetProfileContext(context.Context) (*debridTypes.Profile, error)
}

func getProfile(ctx context.Context, client debrid.Client) (*debridTypes.Profile, error) {
	if contextual, ok := client.(contextualProfileClient); ok {
		return contextual.GetProfileContext(ctx)
	}
	return client.GetProfile()
}

func cloneProfile(profile *debridTypes.Profile) *debridTypes.Profile {
	if profile == nil {
		return nil
	}
	cloned := *profile
	return &cloned
}

func cloneProfiles(profiles map[string]*debridTypes.Profile) map[string]*debridTypes.Profile {
	cloned := make(map[string]*debridTypes.Profile, len(profiles))
	for name, profile := range profiles {
		cloned[name] = cloneProfile(profile)
	}
	return cloned
}
