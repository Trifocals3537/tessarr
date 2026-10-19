package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/manager/link"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

const (
	streamFileCircuitBaseCooldown = 30 * time.Second
	streamFileCircuitMaxCooldown  = 5 * time.Minute
	maxStreamFileCircuits         = 8192
)

type streamFileCircuitState struct {
	failures      int
	retryAt       time.Time
	lastSeen      time.Time
	probeInFlight bool
}

type streamFileCircuitBreaker struct {
	mu        sync.Mutex
	now       func() time.Time
	baseDelay time.Duration
	maxDelay  time.Duration
	capacity  int
	states    map[string]*streamFileCircuitState
}

type streamFileCircuitResult struct {
	Class      string
	Failures   int
	Cooldown   time.Duration
	NewlyOpen  bool
	RetryAfter time.Duration
}

type streamFileCircuitOpenError struct {
	retryAfter time.Duration
}

func (e streamFileCircuitOpenError) Error() string {
	if e.retryAfter <= 0 {
		return "file provider circuit open"
	}
	return fmt.Sprintf("file provider circuit open; retry after %s", e.retryAfter.Round(time.Second))
}

func newStreamFileCircuitBreaker() *streamFileCircuitBreaker {
	return &streamFileCircuitBreaker{
		now:       time.Now,
		baseDelay: streamFileCircuitBaseCooldown,
		maxDelay:  streamFileCircuitMaxCooldown,
		capacity:  maxStreamFileCircuits,
		states:    make(map[string]*streamFileCircuitState),
	}
}

func streamFileCircuitKey(entry *storage.Entry, filename, provider string) string {
	if entry == nil {
		return ""
	}
	hash := strings.ToLower(strings.TrimSpace(entry.InfoHash))
	filename = strings.ToLower(strings.TrimSpace(filename))
	provider = strings.ToLower(strings.TrimSpace(provider))
	if hash == "" || filename == "" || provider == "" {
		return ""
	}
	return hash + "\x00" + filename + "\x00" + provider
}

func streamFileCircuitDelay(base, maximum time.Duration, failures int) time.Duration {
	if base <= 0 || failures <= 0 {
		return 0
	}
	delay := base
	for attempt := 1; attempt < failures && delay < maximum; attempt++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if maximum > 0 && delay > maximum {
		return maximum
	}
	return delay
}

func classifyStreamFileCircuitFailure(err error) (string, bool) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "", false
	}
	if linkErr := link.GetLinkError(err); linkErr != nil && linkErr.Code == link.CodeLinkRefreshCooldown {
		return "local_cooldown", false
	}
	class, _ := classifyStreamProviderFailure(err)
	if class == "" || class == "internal" || class == "local_cooldown" {
		return class, false
	}
	return class, true
}

func (b *streamFileCircuitBreaker) recordFailure(key, class string) streamFileCircuitResult {
	result := streamFileCircuitResult{Class: class}
	if b == nil || key == "" || class == "" {
		return result
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	state := b.states[key]
	if state == nil {
		b.makeRoom(now)
		state = &streamFileCircuitState{}
		b.states[key] = state
	} else if now.Before(state.retryAt) {
		// Several requests can already be in flight when the first one opens
		// the circuit. Coalesce that burst into one failure episode instead of
		// multiplying the backoff for the same outage.
		state.lastSeen = now
		result.Failures = state.failures
		result.Cooldown = state.retryAt.Sub(now)
		result.RetryAfter = result.Cooldown
		return result
	}
	state.failures++
	state.lastSeen = now
	state.probeInFlight = false
	delay := streamFileCircuitDelay(b.baseDelay, b.maxDelay, state.failures)
	state.retryAt = now.Add(delay)
	result.Failures = state.failures
	result.Cooldown = delay
	result.RetryAfter = delay
	result.NewlyOpen = true
	return result
}

func (b *streamFileCircuitBreaker) beginAttempt(key string) (probe, allowed bool, retryAfter time.Duration) {
	if b == nil || key == "" {
		return false, true, 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	state := b.states[key]
	if state == nil {
		return false, true, 0
	}
	now := b.now()
	state.lastSeen = now
	if now.Before(state.retryAt) {
		return false, false, state.retryAt.Sub(now)
	}
	if state.probeInFlight {
		return false, false, 0
	}
	state.probeInFlight = true
	return true, true, 0
}

func (b *streamFileCircuitBreaker) releaseProbe(key string) {
	if b == nil || key == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if state := b.states[key]; state != nil {
		state.probeInFlight = false
	}
}

func (b *streamFileCircuitBreaker) recordSuccess(key string) bool {
	if b == nil || key == "" {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	_, existed := b.states[key]
	delete(b.states, key)
	return existed
}

func (b *streamFileCircuitBreaker) blocked(key string) bool {
	if b == nil || key == "" {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	state := b.states[key]
	if state == nil {
		return false
	}
	return b.now().Before(state.retryAt) || state.probeInFlight
}

func (b *streamFileCircuitBreaker) openCount() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	count := 0
	for _, state := range b.states {
		if now.Before(state.retryAt) || state.probeInFlight {
			count++
		}
	}
	return count
}

func (b *streamFileCircuitBreaker) makeRoom(now time.Time) {
	if b.capacity <= 0 || len(b.states) < b.capacity {
		return
	}
	var oldestKey string
	var oldestTime time.Time
	for key, state := range b.states {
		if !state.probeInFlight && !now.Before(state.retryAt) {
			delete(b.states, key)
			if len(b.states) < b.capacity {
				return
			}
			continue
		}
		if oldestKey == "" || state.lastSeen.Before(oldestTime) {
			oldestKey = key
			oldestTime = state.lastSeen
		}
	}
	if oldestKey != "" {
		delete(b.states, oldestKey)
	}
}

func (m *Manager) recordStreamFileCircuitFailure(entry *storage.Entry, filename, provider string, err error) streamFileCircuitResult {
	class, eligible := classifyStreamFileCircuitFailure(err)
	result := streamFileCircuitResult{Class: class}
	if !eligible || m == nil || m.streamFileCircuits == nil {
		return result
	}
	key := streamFileCircuitKey(entry, filename, provider)
	result = m.streamFileCircuits.recordFailure(key, class)
	if result.NewlyOpen {
		m.streamFileCircuitOpens.Add(1)
		m.logger.Info().
			Str("event", "stream.file_circuit_opened").
			Str("outcome", "cooldown").
			Str("hash", entry.InfoHash).
			Str("file", filename).
			Str("provider", provider).
			Str("failure_class", class).
			Int("failures", result.Failures).
			Dur("cooldown", result.Cooldown).
			Msg("Stream file/provider circuit opened")
	}
	return result
}

func (m *Manager) beginStreamFileCircuitAttempt(entry *storage.Entry, filename, provider string) (key string, probe, allowed bool, retryAfter time.Duration) {
	key = streamFileCircuitKey(entry, filename, provider)
	if m == nil || m.streamFileCircuits == nil {
		return key, false, true, 0
	}
	probe, allowed, retryAfter = m.streamFileCircuits.beginAttempt(key)
	return key, probe, allowed, retryAfter
}

func (m *Manager) markStreamFileCircuitSuccess(entry *storage.Entry, filename, provider string) {
	if m == nil || m.streamFileCircuits == nil {
		return
	}
	if m.streamFileCircuits.recordSuccess(streamFileCircuitKey(entry, filename, provider)) {
		m.streamFileCircuitRecovers.Add(1)
		m.logger.Info().
			Str("event", "stream.file_circuit_recovered").
			Str("outcome", "healthy").
			Str("hash", entry.InfoHash).
			Str("file", filename).
			Str("provider", provider).
			Msg("Stream file/provider circuit recovered")
	}
}

func (m *Manager) orderStreamCandidatesByFileCircuit(entry *storage.Entry, filename string, candidates []streamCandidate) []streamCandidate {
	if m == nil || m.streamFileCircuits == nil || len(candidates) < 2 {
		return candidates
	}
	healthy := make([]streamCandidate, 0, len(candidates))
	blocked := make([]streamCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if m.streamFileCircuits.blocked(streamFileCircuitKey(entry, filename, candidate.provider)) {
			blocked = append(blocked, candidate)
		} else {
			healthy = append(healthy, candidate)
		}
	}
	if len(healthy) == 0 || len(blocked) == 0 {
		return candidates
	}
	ordered := append(healthy, blocked...)
	for index := range candidates {
		if strings.EqualFold(candidates[index].provider, ordered[index].provider) {
			continue
		}
		m.streamFileCircuitDefers.Add(1)
		break
	}
	return ordered
}
