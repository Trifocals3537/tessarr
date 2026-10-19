package manager

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/manager/link"
)

const (
	streamProviderFailureThreshold = 3
	streamProviderFailureWindow    = 30 * time.Second
	streamProviderCooldown         = 30 * time.Second
	maxStreamProviderWeather       = 64
)

type streamProviderCondition struct {
	lastSeen      time.Time
	failures      map[string]time.Time
	degradedUntil time.Time
	probeInFlight bool
}

// streamProviderWeather tracks only provider-wide, pre-commit failures. It is
// intentionally independent from the provider traffic governor: this state
// changes candidate order, while the governor continues to own concurrency and
// Retry-After pacing.
type streamProviderWeather struct {
	mu             sync.Mutex
	now            func() time.Time
	threshold      int
	failureWindow  time.Duration
	cooldown       time.Duration
	maxProviders   int
	providerStates map[string]*streamProviderCondition
}

type streamProviderFailureResult struct {
	Class         string
	DistinctFiles int
	Cooldown      time.Duration
	NewlyDegraded bool
}

func newStreamProviderWeather() *streamProviderWeather {
	return &streamProviderWeather{
		now:            time.Now,
		threshold:      streamProviderFailureThreshold,
		failureWindow:  streamProviderFailureWindow,
		cooldown:       streamProviderCooldown,
		maxProviders:   maxStreamProviderWeather,
		providerStates: make(map[string]*streamProviderCondition),
	}
}

func (m *Manager) recordStreamProviderFailure(provider, fileKey string, err error) streamProviderFailureResult {
	class, providerWide := classifyStreamProviderFailure(err)
	result := streamProviderFailureResult{Class: class}
	if !providerWide || m == nil || m.streamProviderWeather == nil {
		return result
	}

	result = m.streamProviderWeather.recordFailure(provider, fileKey, class)
	if result.NewlyDegraded {
		m.streamProviderDegraded.Add(1)
		m.logger.Warn().
			Str("event", "stream.provider_degraded").
			Str("outcome", "cooldown").
			Str("provider", provider).
			Str("failure_class", result.Class).
			Int("distinct_files", result.DistinctFiles).
			Dur("cooldown", result.Cooldown).
			Msg("Stream provider is temporarily degraded")
	}
	return result
}

func (w *streamProviderWeather) recordFailure(provider, fileKey, class string) streamProviderFailureResult {
	result := streamProviderFailureResult{Class: class}
	provider = strings.TrimSpace(provider)
	fileKey = strings.TrimSpace(fileKey)
	if w == nil || provider == "" || fileKey == "" {
		return result
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	key := strings.ToLower(provider)
	state := w.providerStates[key]
	if state == nil {
		w.makeRoom(now)
		state = &streamProviderCondition{
			failures: make(map[string]time.Time, w.threshold),
		}
		w.providerStates[key] = state
	}
	state.lastSeen = now

	// A failed trial after a cooldown immediately reopens the provider. Requiring
	// another three files here would expose multiple clients to an outage that
	// was already established during the previous episode.
	if !state.degradedUntil.IsZero() && !now.Before(state.degradedUntil) {
		state.degradedUntil = now.Add(w.cooldown)
		state.probeInFlight = false
		clear(state.failures)
		result.NewlyDegraded = true
		result.DistinctFiles = 1
		result.Cooldown = w.cooldown
		return result
	}
	if now.Before(state.degradedUntil) {
		result.Cooldown = state.degradedUntil.Sub(now)
		return result
	}

	cutoff := now.Add(-w.failureWindow)
	for key, observedAt := range state.failures {
		if observedAt.Before(cutoff) {
			delete(state.failures, key)
		}
	}
	state.failures[fileKey] = now
	result.DistinctFiles = len(state.failures)
	if result.DistinctFiles < w.threshold {
		return result
	}

	state.degradedUntil = now.Add(w.cooldown)
	state.probeInFlight = false
	clear(state.failures)
	result.NewlyDegraded = true
	result.Cooldown = w.cooldown
	return result
}

func (w *streamProviderWeather) recordSuccess(provider string) bool {
	provider = strings.TrimSpace(provider)
	if w == nil || provider == "" {
		return false
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	key := strings.ToLower(provider)
	state := w.providerStates[key]
	if state == nil {
		return false
	}
	recovered := !state.degradedUntil.IsZero()
	delete(w.providerStates, key)
	return recovered
}

func (w *streamProviderWeather) isDegraded(provider string) bool {
	provider = strings.TrimSpace(provider)
	if w == nil || provider == "" {
		return false
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	state := w.providerStates[strings.ToLower(provider)]
	return state != nil && w.now().Before(state.degradedUntil)
}

func (w *streamProviderWeather) candidateDegraded(provider string) bool {
	provider = strings.TrimSpace(provider)
	if w == nil || provider == "" {
		return false
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	state := w.providerStates[strings.ToLower(provider)]
	if state == nil || state.degradedUntil.IsZero() {
		return false
	}
	now := w.now()
	return now.Before(state.degradedUntil) || state.probeInFlight
}

// beginAttempt atomically admits one half-open trial after cooldown. The
// reservation is taken only when the stream actually reaches this candidate,
// so a healthy earlier placement never gets displaced by a background probe.
func (w *streamProviderWeather) beginAttempt(provider string) (probe, allowed bool) {
	provider = strings.TrimSpace(provider)
	if w == nil || provider == "" {
		return false, true
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	state := w.providerStates[strings.ToLower(provider)]
	if state == nil || state.degradedUntil.IsZero() || w.now().Before(state.degradedUntil) {
		return false, true
	}
	if state.probeInFlight {
		return false, false
	}
	state.probeInFlight = true
	return true, true
}

func (w *streamProviderWeather) releaseProbe(provider string) {
	provider = strings.TrimSpace(provider)
	if w == nil || provider == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if state := w.providerStates[strings.ToLower(provider)]; state != nil {
		state.probeInFlight = false
	}
}

func (w *streamProviderWeather) makeRoom(now time.Time) {
	if len(w.providerStates) < w.maxProviders {
		return
	}
	var oldestKey string
	var oldestTime time.Time
	for key, state := range w.providerStates {
		if !now.Before(state.degradedUntil) && now.Sub(state.lastSeen) >= w.failureWindow {
			delete(w.providerStates, key)
			if len(w.providerStates) < w.maxProviders {
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
		delete(w.providerStates, oldestKey)
	}
}

func (m *Manager) orderStreamCandidatesByWeather(candidates []streamCandidate) []streamCandidate {
	if m == nil || m.streamProviderWeather == nil || len(candidates) < 2 {
		return candidates
	}

	healthy := make([]streamCandidate, 0, len(candidates))
	degraded := make([]streamCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if m.streamProviderWeather.candidateDegraded(candidate.provider) {
			degraded = append(degraded, candidate)
		} else {
			healthy = append(healthy, candidate)
		}
	}
	if len(healthy) == 0 || len(degraded) == 0 {
		return candidates
	}

	ordered := append(healthy, degraded...)
	for index := range candidates {
		if strings.EqualFold(candidates[index].provider, ordered[index].provider) {
			continue
		}
		m.streamProviderDeferrals.Add(1)
		break
	}
	return ordered
}

func classifyStreamProviderFailure(err error) (string, bool) {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "", false
	}
	if linkErr := link.GetLinkError(err); linkErr != nil {
		// Retry policy and provider-health evidence are separate: this delay
		// suppresses refresh work for one file without observing a new failure.
		if linkErr.Code == link.CodeLinkRefreshCooldown {
			return "local_cooldown", false
		}
		switch linkErr.Category {
		case link.CategoryThrottled:
			return "throttled", true
		case link.CategoryAccountIssue:
			return "account_issue", true
		case link.CategoryRetryable:
			return "upstream_status", true
		default:
			return linkErr.Category.String(), false
		}
	}
	if streamErr, ok := directStreamError(err); ok && streamErr.Retryable {
		if isConnectionError(streamErr.Err) {
			return "connection", true
		}
		return "upstream_io", true
	}
	if isConnectionError(err) {
		return "connection", true
	}
	return "internal", false
}
