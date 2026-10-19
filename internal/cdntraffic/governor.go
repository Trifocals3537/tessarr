package cdntraffic

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/internal/providertraffic"
)

const (
	defaultConcurrentRequests = 8
	defaultTorBoxRequests     = 4
	defaultTorBoxHostRequests = 16
	defaultMinimumRequests    = 1
	defaultRecoveryInterval   = 30 * time.Second
	defaultThrottleBackoff    = time.Second
	defaultMaximumBackoff     = 5 * time.Minute
	defaultInteractiveBurst   = 8
	maxTrafficStates          = 1024
)

// Options controls the governor. Zero values select production defaults.
// The fields are exported primarily so deterministic tests can use short
// recovery windows; Tessarr intentionally does not expose user settings yet.
type Options struct {
	DefaultLimit        int
	TorBoxLimit         int // per stable download link
	TorBoxHostLimit     int
	TorBoxResolverLimit int
	MinimumLimit        int
	RecoveryInterval    time.Duration
	DefaultBackoff      time.Duration
	MaximumBackoff      time.Duration
	MaxInteractiveBurst int
	Traffic             *providertraffic.Controller
}

type waiter struct {
	priority Priority
	ready    chan struct{}
	granted  bool
}

type trafficState struct {
	provider     string
	providerType string
	account      string
	host         string
	hosts        map[string]struct{}
	report       bool

	limit    int
	maxLimit int
	minLimit int

	activeInteractive  int
	activeBackground   int
	waitingInteractive []*waiter
	waitingBackground  []*waiter
	interactiveBurst   int

	blockedUntil  time.Time
	recoverAfter  time.Time
	wakeScheduled bool

	logicalActiveInteractive   int
	logicalActiveBackground    int
	logicalWaitingInteractive  int
	logicalWaitingBackground   int
	logicalAdmittedInteractive uint64
	logicalAdmittedBackground  uint64
	logicalWaitNanos           uint64

	throttles uint64
	lastUsed  time.Time
}

type stateSpec struct {
	key          string
	provider     string
	providerType string
	account      string
	host         string
	maxLimit     int
	report       bool
}

// ProviderStats is a secret-free aggregate for one configured provider.
type ProviderStats struct {
	Provider             string     `json:"provider"`
	ProviderType         string     `json:"provider_type,omitempty"`
	Accounts             int        `json:"accounts"`
	Hosts                []string   `json:"hosts,omitempty"`
	Active               int        `json:"active"`
	ActiveInteractive    int        `json:"active_interactive"`
	ActiveBackground     int        `json:"active_background"`
	WaitingInteractive   int        `json:"waiting_interactive"`
	WaitingBackground    int        `json:"waiting_background"`
	CurrentLimit         int        `json:"current_limit"`
	MaximumLimit         int        `json:"maximum_limit"`
	AdmittedInteractive  uint64     `json:"admitted_interactive"`
	AdmittedBackground   uint64     `json:"admitted_background"`
	Throttles            uint64     `json:"throttles"`
	CumulativeWaitMillis uint64     `json:"cumulative_wait_ms"`
	BlockedUntil         *time.Time `json:"blocked_until,omitempty"`
}

// Stats is a point-in-time view of CDN admission and throttling.
type Stats struct {
	Active             int             `json:"active"`
	WaitingInteractive int             `json:"waiting_interactive"`
	WaitingBackground  int             `json:"waiting_background"`
	Throttles          uint64          `json:"throttles"`
	Providers          []ProviderStats `json:"providers"`
}

// Governor coordinates logical CDN requests across every shared HTTP client.
// Its mutex is touched only at request admission/release, never while bytes are
// copied, so it does not sit on the streaming hot path.
type Governor struct {
	mu      sync.Mutex
	states  map[string]*trafficState
	options Options
	now     func() time.Time
	traffic *providertraffic.Controller
}

// Permit owns every constraint slot for one logical HTTP request.
type Permit struct {
	once       sync.Once
	governor   *Governor
	states     []permitState
	logicalKey string
	priority   Priority
}

type permitState struct {
	key      string
	priority Priority
}

// Release returns the request slot exactly once.
func (p *Permit) Release() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		p.governor.releasePermit(p.logicalKey, p.priority, p.states)
	})
}

// New creates a provider-aware adaptive CDN governor.
func New(options Options) *Governor {
	torboxCapabilities := providertraffic.For("torbox")
	if options.DefaultLimit <= 0 {
		options.DefaultLimit = defaultConcurrentRequests
	}
	if options.TorBoxLimit <= 0 {
		options.TorBoxLimit = torboxCapabilities.CDNLinkConcurrency
	}
	if options.TorBoxHostLimit <= 0 {
		options.TorBoxHostLimit = torboxCapabilities.CDNHostConcurrency
	}
	if options.TorBoxResolverLimit <= 0 {
		options.TorBoxResolverLimit = torboxCapabilities.ResolverConcurrency
	}
	// Defensive fallbacks keep custom capability tables from producing a
	// zero-capacity governor.
	if options.TorBoxLimit <= 0 {
		options.TorBoxLimit = defaultTorBoxRequests
	}
	if options.TorBoxHostLimit <= 0 {
		options.TorBoxHostLimit = defaultTorBoxHostRequests
	}
	if options.TorBoxResolverLimit <= 0 {
		options.TorBoxResolverLimit = defaultTorBoxRequests
	}
	if options.MinimumLimit <= 0 {
		options.MinimumLimit = defaultMinimumRequests
	}
	if options.RecoveryInterval <= 0 {
		options.RecoveryInterval = defaultRecoveryInterval
	}
	if options.DefaultBackoff <= 0 {
		options.DefaultBackoff = defaultThrottleBackoff
	}
	if options.MaximumBackoff <= 0 {
		options.MaximumBackoff = defaultMaximumBackoff
	}
	if options.MaxInteractiveBurst <= 0 {
		options.MaxInteractiveBurst = defaultInteractiveBurst
	}
	traffic := options.Traffic
	if traffic == nil {
		traffic = providertraffic.New(providertraffic.Options{})
	}
	return &Governor{
		states:  make(map[string]*trafficState),
		options: options,
		now:     time.Now,
		traffic: traffic,
	}
}

// Acquire waits for a provider/account slot. Interactive requests retain one
// slot ahead of bulk work whenever the current limit is at least two.
func (g *Governor) Acquire(ctx context.Context, identity Identity, requestHost string, priority Priority) (*Permit, error) {
	return g.acquire(ctx, identity, strings.ToLower(strings.TrimSpace(requestHost)), providertraffic.OperationNone, "", priority)
}

// AcquireRequest applies URL-aware provider capabilities. TorBox link
// resolution spends API budget without occupying a CDN slot; the redirected
// media request is constrained independently by CDN host and stable link.
func (g *Governor) AcquireRequest(ctx context.Context, identity Identity, requestURL *url.URL, priority Priority) (*Permit, error) {
	host := ""
	if requestURL != nil {
		host = strings.ToLower(strings.TrimSpace(requestURL.Hostname()))
	}
	operation := providertraffic.ClassifyURL(identity.ProviderType, requestURL)
	endpoint := providertraffic.EndpointKey(identity.ProviderType, http.MethodGet, requestURL)
	return g.acquire(ctx, identity, host, operation, endpoint, priority)
}

func (g *Governor) acquire(
	ctx context.Context,
	identity Identity,
	requestHost string,
	operation providertraffic.Operation,
	endpoint string,
	priority Priority,
) (*Permit, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	specs := g.stateSpecs(identity, requestHost, operation)
	logicalKey, queuedAt := g.beginLogical(specs, priority)
	if operation == providertraffic.OperationResolveLink {
		if err := g.traffic.WaitEndpoint(ctx, providerIdentity(identity), operation, endpoint); err != nil {
			g.cancelLogicalAndRelease(logicalKey, priority, nil)
			return nil, err
		}
	}

	permits := make([]permitState, 0, len(specs))
	for _, spec := range specs {
		permit, err := g.acquireState(ctx, spec, priority)
		if err != nil {
			g.cancelLogicalAndRelease(logicalKey, priority, permits)
			return nil, err
		}
		permits = append(permits, permit)
	}
	if err := ctx.Err(); err != nil {
		g.cancelLogicalAndRelease(logicalKey, priority, permits)
		return nil, err
	}
	g.admitLogical(logicalKey, priority, queuedAt)
	return &Permit{
		governor:   g,
		states:     permits,
		logicalKey: logicalKey,
		priority:   priority,
	}, nil
}

func (g *Governor) beginLogical(specs []stateSpec, priority Priority) (string, time.Time) {
	queuedAt := g.now()
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, spec := range specs {
		if !spec.report {
			continue
		}
		key, state := g.stateLocked(spec)
		if priority == PriorityInteractive {
			state.logicalWaitingInteractive++
		} else {
			state.logicalWaitingBackground++
		}
		return key, queuedAt
	}
	return "", queuedAt
}

func (g *Governor) admitLogical(key string, priority Priority, queuedAt time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	state := g.states[key]
	if state == nil {
		return
	}
	waited := g.now().Sub(queuedAt)
	if waited > 0 {
		state.logicalWaitNanos += uint64(waited)
	}
	if priority == PriorityInteractive {
		if state.logicalWaitingInteractive > 0 {
			state.logicalWaitingInteractive--
		}
		state.logicalActiveInteractive++
		state.logicalAdmittedInteractive++
	} else {
		if state.logicalWaitingBackground > 0 {
			state.logicalWaitingBackground--
		}
		state.logicalActiveBackground++
		state.logicalAdmittedBackground++
	}
}

func (g *Governor) cancelLogicalAndRelease(key string, priority Priority, permits []permitState) {
	g.mu.Lock()
	if state := g.states[key]; state != nil {
		if priority == PriorityInteractive {
			if state.logicalWaitingInteractive > 0 {
				state.logicalWaitingInteractive--
			}
		} else if state.logicalWaitingBackground > 0 {
			state.logicalWaitingBackground--
		}
	}
	for i := len(permits) - 1; i >= 0; i-- {
		permit := permits[i]
		if state := g.states[permit.key]; state != nil {
			g.releaseLocked(permit.key, state, permit.priority)
		}
	}
	g.mu.Unlock()
}

func (g *Governor) acquireState(ctx context.Context, spec stateSpec, priority Priority) (permitState, error) {
	g.mu.Lock()
	key, state := g.stateLocked(spec)
	w := &waiter{priority: priority, ready: make(chan struct{})}
	if priority == PriorityInteractive {
		state.waitingInteractive = append(state.waitingInteractive, w)
	} else {
		state.waitingBackground = append(state.waitingBackground, w)
	}
	g.dispatchLocked(key, state)
	g.mu.Unlock()

	select {
	case <-w.ready:
		if err := ctx.Err(); err != nil {
			g.release(key, priority)
			return permitState{}, err
		}
		return permitState{key: key, priority: priority}, nil
	case <-ctx.Done():
		g.mu.Lock()
		if w.granted {
			g.releaseLocked(key, state, priority)
		} else {
			g.removeWaiterLocked(state, w)
		}
		g.mu.Unlock()
		return permitState{}, ctx.Err()
	}
}

// Observe adjusts admission after a completed response header. Only 429
// reduces concurrency; successful responses restore one slot at a time.
func (g *Governor) Observe(identity Identity, requestHost string, statusCode int, header http.Header) {
	g.observe(identity, strings.ToLower(strings.TrimSpace(requestHost)), providertraffic.OperationNone, statusCode, header)
}

// ObserveRequest is the URL-aware counterpart to Observe.
func (g *Governor) ObserveRequest(identity Identity, requestURL *url.URL, statusCode int, header http.Header) {
	host := ""
	if requestURL != nil {
		host = strings.ToLower(strings.TrimSpace(requestURL.Hostname()))
	}
	operation := providertraffic.ClassifyURL(identity.ProviderType, requestURL)
	g.observe(identity, host, operation, statusCode, header)
}

func (g *Governor) observe(
	identity Identity,
	requestHost string,
	operation providertraffic.Operation,
	statusCode int,
	header http.Header,
) {
	g.mu.Lock()
	for _, spec := range g.stateSpecs(identity, requestHost, operation) {
		key, state := g.stateLocked(spec)
		g.observeStateLocked(key, state, statusCode, header)
	}
	g.mu.Unlock()
	if operation == providertraffic.OperationResolveLink {
		g.traffic.Observe(providerIdentity(identity), operation, statusCode, header)
	}
}

func (g *Governor) observeStateLocked(key string, state *trafficState, statusCode int, header http.Header) {
	now := g.now()
	if statusCode == http.StatusTooManyRequests {
		if state.report {
			state.throttles++
		}
		if state.limit > state.minLimit {
			state.limit = max(state.minLimit, (state.limit+1)/2)
		}
		backoff := retryAfter(header, now)
		if backoff <= 0 {
			backoff = g.options.DefaultBackoff
		}
		if backoff > g.options.MaximumBackoff {
			backoff = g.options.MaximumBackoff
		}
		blockedUntil := now.Add(backoff)
		if blockedUntil.After(state.blockedUntil) {
			state.blockedUntil = blockedUntil
		}
		state.recoverAfter = state.blockedUntil.Add(g.options.RecoveryInterval)
		g.scheduleWakeLocked(key, state, now)
	} else if statusCode >= http.StatusOK && statusCode < http.StatusBadRequest &&
		state.limit < state.maxLimit && !now.Before(state.recoverAfter) {
		state.limit++
		state.recoverAfter = now.Add(g.options.RecoveryInterval)
	}
	g.dispatchLocked(key, state)
}

func (g *Governor) release(key string, priority Priority) {
	g.mu.Lock()
	state := g.states[key]
	if state != nil {
		g.releaseLocked(key, state, priority)
	}
	g.mu.Unlock()
}

func (g *Governor) releasePermit(logicalKey string, priority Priority, permits []permitState) {
	g.mu.Lock()
	if state := g.states[logicalKey]; state != nil {
		if priority == PriorityInteractive {
			if state.logicalActiveInteractive > 0 {
				state.logicalActiveInteractive--
			}
		} else if state.logicalActiveBackground > 0 {
			state.logicalActiveBackground--
		}
	}
	for i := len(permits) - 1; i >= 0; i-- {
		permit := permits[i]
		if state := g.states[permit.key]; state != nil {
			g.releaseLocked(permit.key, state, permit.priority)
		}
	}
	g.mu.Unlock()
}

func (g *Governor) releaseLocked(key string, state *trafficState, priority Priority) {
	if priority == PriorityInteractive {
		if state.activeInteractive > 0 {
			state.activeInteractive--
		}
	} else if state.activeBackground > 0 {
		state.activeBackground--
	}
	g.dispatchLocked(key, state)
}

func (g *Governor) dispatchLocked(key string, state *trafficState) {
	now := g.now()
	if now.Before(state.blockedUntil) {
		g.scheduleWakeLocked(key, state, now)
		return
	}
	state.blockedUntil = time.Time{}

	for state.activeInteractive+state.activeBackground < state.limit {
		var w *waiter
		if state.activeBackground >= backgroundCapacity(state.limit) {
			if len(state.waitingInteractive) == 0 {
				return
			}
			w = state.waitingInteractive[0]
			state.waitingInteractive = state.waitingInteractive[1:]
			state.interactiveBurst++
		} else {
			w = g.nextWaiterLocked(state)
		}
		if w == nil {
			return
		}
		w.granted = true
		if w.priority == PriorityInteractive {
			state.activeInteractive++
		} else {
			state.activeBackground++
		}
		close(w.ready)
	}
}

func (g *Governor) nextWaiterLocked(state *trafficState) *waiter {
	if len(state.waitingInteractive) > 0 && len(state.waitingBackground) == 0 {
		// Playback that ran while no bulk work was waiting does not consume the
		// fairness budget of a future background request.
		state.interactiveBurst = 0
		w := state.waitingInteractive[0]
		state.waitingInteractive = state.waitingInteractive[1:]
		return w
	}
	if len(state.waitingInteractive) > 0 && state.interactiveBurst < g.options.MaxInteractiveBurst {
		w := state.waitingInteractive[0]
		state.waitingInteractive = state.waitingInteractive[1:]
		state.interactiveBurst++
		return w
	}
	if len(state.waitingBackground) > 0 {
		w := state.waitingBackground[0]
		state.waitingBackground = state.waitingBackground[1:]
		state.interactiveBurst = 0
		return w
	}
	if len(state.waitingInteractive) > 0 {
		w := state.waitingInteractive[0]
		state.waitingInteractive = state.waitingInteractive[1:]
		state.interactiveBurst++
		return w
	}
	return nil
}

func (g *Governor) removeWaiterLocked(state *trafficState, target *waiter) {
	queue := &state.waitingBackground
	if target.priority == PriorityInteractive {
		queue = &state.waitingInteractive
	}
	for i, candidate := range *queue {
		if candidate == target {
			copy((*queue)[i:], (*queue)[i+1:])
			*queue = (*queue)[:len(*queue)-1]
			return
		}
	}
}

func (g *Governor) scheduleWakeLocked(key string, state *trafficState, now time.Time) {
	if state.wakeScheduled || !now.Before(state.blockedUntil) {
		return
	}
	state.wakeScheduled = true
	delay := state.blockedUntil.Sub(now)
	time.AfterFunc(delay, func() {
		g.mu.Lock()
		current := g.states[key]
		if current == state {
			state.wakeScheduled = false
			g.dispatchLocked(key, state)
		}
		g.mu.Unlock()
	})
}

func (g *Governor) stateLocked(spec stateSpec) (string, *trafficState) {
	now := g.now()
	state := g.states[spec.key]
	maxLimit := max(1, spec.maxLimit)
	if state == nil {
		if len(g.states) >= maxTrafficStates {
			g.pruneOldestIdleStateLocked()
		}
		minimum := min(g.options.MinimumLimit, maxLimit)
		state = &trafficState{
			provider:     spec.provider,
			providerType: spec.providerType,
			account:      spec.account,
			host:         spec.host,
			hosts:        make(map[string]struct{}),
			report:       spec.report,
			limit:        maxLimit,
			maxLimit:     maxLimit,
			minLimit:     minimum,
			lastUsed:     now,
		}
		if spec.host != "" {
			state.hosts[spec.host] = struct{}{}
		}
		g.states[spec.key] = state
		return spec.key, state
	}
	if state.provider == "" && spec.provider != "" {
		state.provider = spec.provider
	}
	if state.providerType == "" && spec.providerType != "" {
		state.providerType = spec.providerType
		state.maxLimit = maxLimit
		state.minLimit = min(g.options.MinimumLimit, maxLimit)
		state.limit = min(state.limit, state.maxLimit)
	}
	if state.account == "" && spec.account != "" {
		state.account = spec.account
	}
	if spec.report {
		state.report = true
	}
	if spec.host != "" {
		state.host = spec.host
		if state.hosts == nil {
			state.hosts = make(map[string]struct{})
		}
		state.hosts[spec.host] = struct{}{}
	}
	state.lastUsed = now
	return spec.key, state
}

func (g *Governor) pruneOldestIdleStateLocked() {
	var oldestKey string
	var oldest time.Time
	for key, state := range g.states {
		if state.activeInteractive != 0 || state.activeBackground != 0 ||
			len(state.waitingInteractive) != 0 || len(state.waitingBackground) != 0 ||
			state.logicalActiveInteractive != 0 || state.logicalActiveBackground != 0 ||
			state.logicalWaitingInteractive != 0 || state.logicalWaitingBackground != 0 ||
			state.wakeScheduled {
			continue
		}
		if oldestKey == "" || state.lastUsed.Before(oldest) {
			oldestKey = key
			oldest = state.lastUsed
		}
	}
	if oldestKey != "" {
		delete(g.states, oldestKey)
	}
}

func (g *Governor) stateSpecs(
	identity Identity,
	requestHost string,
	operation providertraffic.Operation,
) []stateSpec {
	provider := strings.TrimSpace(identity.Provider)
	providerType := strings.ToLower(strings.TrimSpace(identity.ProviderType))
	if provider == "" {
		provider = providerType
	}
	host := strings.ToLower(strings.TrimSpace(requestHost))
	account := accountKey(identity.AccountToken)

	if !strings.EqualFold(providerType, "torbox") {
		key := "account\x00" + strings.ToLower(provider) + "\x00" + account
		if provider == "" {
			key = "host\x00" + host
			provider = host
		}
		return []stateSpec{{
			key:          key,
			provider:     provider,
			providerType: providerType,
			account:      account,
			host:         host,
			maxLimit:     g.options.DefaultLimit,
			report:       true,
		}}
	}

	if operation == providertraffic.OperationResolveLink {
		return []stateSpec{{
			key:          "resolver\x00" + strings.ToLower(provider) + "\x00" + account,
			provider:     provider,
			providerType: providerType,
			account:      account,
			host:         host,
			maxLimit:     g.options.TorBoxResolverLimit,
			report:       true,
		}}
	}

	linkKey := strings.TrimSpace(identity.LinkKey)
	primaryKey := "fallback\x00" + strings.ToLower(provider) + "\x00" + account + "\x00" + host
	if linkKey != "" {
		primaryKey = "link\x00" + strings.ToLower(provider) + "\x00" + account + "\x00" + shortHash(linkKey)
	}
	specs := []stateSpec{{
		key:          primaryKey,
		provider:     provider,
		providerType: providerType,
		account:      account,
		host:         host,
		maxLimit:     g.options.TorBoxLimit,
		report:       true,
	}}
	if host != "" {
		specs = append(specs, stateSpec{
			key:          "cdn-host\x00" + providerType + "\x00" + host,
			providerType: providerType,
			host:         host,
			maxLimit:     g.options.TorBoxHostLimit,
		})
	}
	return specs
}

func providerIdentity(identity Identity) providertraffic.Identity {
	return providertraffic.Identity{
		ProviderType: identity.ProviderType,
		AccountToken: identity.AccountToken,
	}
}

func accountKey(token string) string {
	account := "shared"
	if token != "" {
		account = shortHash(token)
	}
	return account
}

func shortHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:8])
}

func backgroundCapacity(limit int) int {
	if limit <= 1 {
		return 1
	}
	return limit - 1
}

func retryAfter(header http.Header, now time.Time) time.Duration {
	if header == nil {
		return 0
	}
	value := strings.TrimSpace(header.Get("Retry-After"))
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	when, err := http.ParseTime(value)
	if err != nil || !when.After(now) {
		return 0
	}
	return when.Sub(now)
}

// Snapshot returns secret-free provider aggregates in deterministic order.
func (g *Governor) Snapshot() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()

	type aggregate struct {
		stats    ProviderStats
		hosts    map[string]struct{}
		accounts map[string][2]int
	}
	aggregates := make(map[string]*aggregate)
	result := Stats{}
	for _, state := range g.states {
		// Composite host constraints are internal implementation detail. Every
		// logical request has exactly one reporting state, so active/waiting
		// request counts remain truthful instead of being double-counted.
		if !state.report {
			continue
		}
		provider := state.provider
		if provider == "" {
			provider = state.host
		}
		key := strings.ToLower(provider) + "\x00" + state.providerType
		a := aggregates[key]
		if a == nil {
			a = &aggregate{
				stats:    ProviderStats{Provider: provider, ProviderType: state.providerType},
				hosts:    make(map[string]struct{}),
				accounts: make(map[string][2]int),
			}
			aggregates[key] = a
		}
		account := state.account
		if account == "" {
			account = "shared"
		}
		limits, exists := a.accounts[account]
		if !exists || state.limit < limits[0] {
			limits[0] = state.limit
		}
		if !exists || state.maxLimit < limits[1] {
			limits[1] = state.maxLimit
		}
		a.accounts[account] = limits
		for host := range state.hosts {
			if host != "" {
				a.hosts[host] = struct{}{}
			}
		}
		a.stats.ActiveInteractive += state.logicalActiveInteractive
		a.stats.ActiveBackground += state.logicalActiveBackground
		a.stats.WaitingInteractive += state.logicalWaitingInteractive
		a.stats.WaitingBackground += state.logicalWaitingBackground
		a.stats.AdmittedInteractive += state.logicalAdmittedInteractive
		a.stats.AdmittedBackground += state.logicalAdmittedBackground
		a.stats.Throttles += state.throttles
		a.stats.CumulativeWaitMillis += state.logicalWaitNanos / uint64(time.Millisecond)
		if !state.blockedUntil.IsZero() &&
			(a.stats.BlockedUntil == nil || state.blockedUntil.After(*a.stats.BlockedUntil)) {
			blockedUntil := state.blockedUntil
			a.stats.BlockedUntil = &blockedUntil
		}
	}
	for _, a := range aggregates {
		a.stats.Accounts = len(a.accounts)
		for _, limits := range a.accounts {
			a.stats.CurrentLimit += limits[0]
			a.stats.MaximumLimit += limits[1]
		}
		a.stats.Active = a.stats.ActiveInteractive + a.stats.ActiveBackground
		for host := range a.hosts {
			a.stats.Hosts = append(a.stats.Hosts, host)
		}
		sort.Strings(a.stats.Hosts)
		result.Active += a.stats.Active
		result.WaitingInteractive += a.stats.WaitingInteractive
		result.WaitingBackground += a.stats.WaitingBackground
		result.Throttles += a.stats.Throttles
		result.Providers = append(result.Providers, a.stats)
	}
	sort.Slice(result.Providers, func(i, j int) bool {
		if result.Providers[i].Provider == result.Providers[j].Provider {
			return result.Providers[i].ProviderType < result.Providers[j].ProviderType
		}
		return result.Providers[i].Provider < result.Providers[j].Provider
	})
	return result
}
