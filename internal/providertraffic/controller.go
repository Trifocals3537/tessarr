package providertraffic

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultThrottleBackoff = 5 * time.Minute
	defaultMaximumBackoff  = 15 * time.Minute
	maxAccountStates       = 1024
)

// Identity names one provider account. AccountToken is used only to derive an
// internal hash and is never retained in observable state.
type Identity struct {
	ProviderType string
	AccountToken string
}

// Options controls a Controller. Zero values select production defaults; the
// capability hook exists so focused tests can use short deterministic budgets.
type Options struct {
	Capabilities   func(string) Capabilities
	DefaultBackoff time.Duration
	MaximumBackoff time.Duration
}

type rateLimiter struct {
	mu           sync.Mutex
	requests     int
	period       time.Duration
	refillPerSec float64
	burst        float64
	tokens       float64
	updated      time.Time
	blockedUntil time.Time
	events       []time.Time
	eventHead    int
}

func newRateLimiter(budget RateBudget) *rateLimiter {
	if !budget.valid() {
		return nil
	}
	burst := budget.Burst
	if burst <= 0 {
		burst = 1
	}
	burst = min(burst, budget.Requests)
	now := time.Now()
	return &rateLimiter{
		requests:     budget.Requests,
		period:       budget.Period,
		refillPerSec: float64(budget.Requests) / budget.Period.Seconds(),
		burst:        float64(burst),
		tokens:       float64(burst),
		updated:      now,
	}
}

// wait takes one token from a configured burst bucket and an exact
// rolling-window allowance. The rolling window prevents either a cold process
// or token refill from exceeding the documented period ceiling.
func (l *rateLimiter) wait(ctx context.Context) error {
	if l == nil || l.refillPerSec <= 0 {
		return nil
	}
	for {
		l.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(l.updated).Seconds()
		if elapsed > 0 {
			l.tokens = min(l.burst, l.tokens+(elapsed*l.refillPerSec))
			l.updated = now
		}
		windowStart := now.Add(-l.period)
		for l.eventHead < len(l.events) && !l.events[l.eventHead].After(windowStart) {
			l.eventHead++
		}

		wait := time.Duration(0)
		if now.Before(l.blockedUntil) {
			wait = l.blockedUntil.Sub(now)
		}
		if activeEvents := len(l.events) - l.eventHead; activeEvents >= l.requests {
			windowWait := l.events[l.eventHead].Add(l.period).Sub(now)
			if windowWait > wait {
				wait = windowWait
			}
		}
		if l.tokens < 1 {
			tokenWait := time.Duration(((1 - l.tokens) / l.refillPerSec) * float64(time.Second))
			if tokenWait > wait {
				wait = tokenWait
			}
		}
		if wait <= 0 {
			l.tokens--
			if l.eventHead > 0 && (l.eventHead >= len(l.events)/2 || len(l.events) >= 2*l.requests) {
				copy(l.events, l.events[l.eventHead:])
				l.events = l.events[:len(l.events)-l.eventHead]
				l.eventHead = 0
			}
			l.events = append(l.events, now)
			l.mu.Unlock()
			return nil
		}
		l.mu.Unlock()
		if err := waitContext(ctx, wait); err != nil {
			return err
		}
	}
}

func (l *rateLimiter) backoff(until time.Time) {
	if l == nil || until.IsZero() {
		return
	}
	l.mu.Lock()
	if until.After(l.blockedUntil) {
		l.blockedUntil = until
	}
	l.mu.Unlock()
}

type accountState struct {
	accountAPI   *rateLimiter
	endpoints    map[string]*rateLimiter
	operations   map[Operation]*rateLimiter
	blockedUntil time.Time
	lastUsed     time.Time
	inUse        int
}

// Controller coordinates rate budgets and account-wide Retry-After backoff
// across provider API clients and link resolution in the streaming client.
type Controller struct {
	mu           sync.Mutex
	states       map[string]*accountState
	capabilities func(string) Capabilities
	defaultWait  time.Duration
	maximumWait  time.Duration
}

func New(options Options) *Controller {
	capabilityLookup := options.Capabilities
	if capabilityLookup == nil {
		capabilityLookup = For
	}
	if options.DefaultBackoff <= 0 {
		options.DefaultBackoff = defaultThrottleBackoff
	}
	if options.MaximumBackoff <= 0 {
		options.MaximumBackoff = defaultMaximumBackoff
	}
	return &Controller{
		states:       make(map[string]*accountState),
		capabilities: capabilityLookup,
		defaultWait:  options.DefaultBackoff,
		maximumWait:  options.MaximumBackoff,
	}
}

// Wait admits one API request under a conservative shared endpoint key and any
// narrower operation budget. URL-aware callers should use WaitEndpoint.
func (c *Controller) Wait(ctx context.Context, identity Identity, operation Operation) error {
	return c.WaitEndpoint(ctx, identity, operation, "")
}

// WaitEndpoint is Wait with an explicit query-free endpoint key. Empty keys
// retain a conservative shared bucket for callers without URL information.
func (c *Controller) WaitEndpoint(
	ctx context.Context,
	identity Identity,
	operation Operation,
	endpoint string,
) error {
	if c == nil || operation == OperationNone {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	capabilities := c.capabilities(identity.ProviderType)
	operationBudget := capabilities.budgetFor(operation)
	if !capabilities.AccountAPIBudget.valid() &&
		!capabilities.APIBudget.valid() &&
		!operationBudget.valid() {
		return nil
	}

	key := identityKey(identity)
	c.mu.Lock()
	state := c.stateLocked(key)
	blockedUntil := state.blockedUntil
	state.lastUsed = time.Now()
	state.inUse++
	c.mu.Unlock()
	defer c.releaseState(key, state)

	if err := waitUntil(ctx, blockedUntil); err != nil {
		return err
	}
	if operationBudget.valid() {
		c.mu.Lock()
		limiter := state.operations[operation]
		if limiter == nil {
			limiter = newRateLimiter(operationBudget)
			limiter.backoff(state.blockedUntil)
			state.operations[operation] = limiter
		}
		c.mu.Unlock()
		// Spend the scarce endpoint token first. Requests parked behind an
		// hourly creation budget must not reserve the general API budget and
		// delay unrelated list/status/link calls that can still succeed.
		if err := limiter.wait(ctx); err != nil {
			return err
		}
	}
	if capabilities.AccountAPIBudget.valid() {
		c.mu.Lock()
		limiter := state.accountAPI
		if limiter == nil {
			limiter = newRateLimiter(capabilities.AccountAPIBudget)
			limiter.backoff(state.blockedUntil)
			state.accountAPI = limiter
		}
		c.mu.Unlock()
		if err := limiter.wait(ctx); err != nil {
			return err
		}
	}
	if !capabilities.APIBudget.valid() {
		return nil
	}

	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		endpoint = "shared"
	}
	c.mu.Lock()
	limiter := state.endpoints[endpoint]
	if limiter == nil {
		limiter = newRateLimiter(capabilities.APIBudget)
		limiter.backoff(state.blockedUntil)
		state.endpoints[endpoint] = limiter
	}
	c.mu.Unlock()
	return limiter.wait(ctx)
}

func (c *Controller) releaseState(key string, state *accountState) {
	c.mu.Lock()
	if c.states[key] == state {
		if state.inUse > 0 {
			state.inUse--
		}
		state.lastUsed = time.Now()
	}
	c.mu.Unlock()
}

// Observe parks every queued request for an account after a 429. This avoids
// spending more requests merely to rediscover the same provider lockout.
func (c *Controller) Observe(identity Identity, operation Operation, statusCode int, header http.Header) {
	if c == nil || operation == OperationNone || statusCode != http.StatusTooManyRequests {
		return
	}
	capabilities := c.capabilities(identity.ProviderType)
	if !capabilities.AccountAPIBudget.valid() &&
		!capabilities.APIBudget.valid() &&
		!capabilities.budgetFor(operation).valid() {
		return
	}

	wait := retryAfter(header, time.Now())
	if wait <= 0 {
		wait = c.defaultWait
	}
	if wait > c.maximumWait {
		wait = c.maximumWait
	}
	until := time.Now().Add(wait)

	c.mu.Lock()
	state := c.stateLocked(identityKey(identity))
	if until.After(state.blockedUntil) {
		state.blockedUntil = until
	}
	state.lastUsed = time.Now()
	if state.accountAPI != nil {
		state.accountAPI.backoff(state.blockedUntil)
	}
	for _, limiter := range state.endpoints {
		limiter.backoff(state.blockedUntil)
	}
	for _, limiter := range state.operations {
		limiter.backoff(state.blockedUntil)
	}
	c.mu.Unlock()
}

func (c *Controller) stateLocked(key string) *accountState {
	if state := c.states[key]; state != nil {
		return state
	}
	if len(c.states) >= maxAccountStates {
		c.pruneOldestLocked()
	}
	state := &accountState{
		endpoints:  make(map[string]*rateLimiter),
		operations: make(map[Operation]*rateLimiter),
		lastUsed:   time.Now(),
	}
	c.states[key] = state
	return state
}

func (c *Controller) pruneOldestLocked() {
	var oldestKey string
	var oldest time.Time
	now := time.Now()
	for key, state := range c.states {
		if state.inUse != 0 || now.Before(state.blockedUntil) {
			continue
		}
		if oldestKey == "" || state.lastUsed.Before(oldest) {
			oldestKey = key
			oldest = state.lastUsed
		}
	}
	if oldestKey != "" {
		delete(c.states, oldestKey)
	}
}

func identityKey(identity Identity) string {
	provider := strings.ToLower(strings.TrimSpace(identity.ProviderType))
	account := "shared"
	if identity.AccountToken != "" {
		digest := sha256.Sum256([]byte(identity.AccountToken))
		account = hex.EncodeToString(digest[:8])
	}
	return provider + "\x00" + account
}

func waitUntil(ctx context.Context, until time.Time) error {
	if until.IsZero() {
		return nil
	}
	return waitContext(ctx, time.Until(until))
}

func waitContext(ctx context.Context, wait time.Duration) error {
	if wait <= 0 {
		return nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
		if seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
		return 0
	}
	when, err := http.ParseTime(value)
	if err != nil || !when.After(now) {
		return 0
	}
	return when.Sub(now)
}
