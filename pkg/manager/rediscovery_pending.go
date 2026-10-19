package manager

import (
	"time"
)

// rediscoveryPendingLogInterval bounds how often the manager repeats the
// "awaiting provider absence" notice for the same entry. Provider sync would
// otherwise re-log the same guard rejection on every refresh cycle for every
// entry that legitimately cannot be rediscovered yet (deleted upstream, still
// returned by a stale provider list), which has produced thousands of
// duplicate ERROR lines per day per host.
const rediscoveryPendingLogInterval = time.Hour

// rediscoveryPendingMaxEntries is a defensive ceiling for installations that
// see an unusually large number of distinct stale provider entries inside one
// log interval. Normal entries are removed as soon as rediscovery is no longer
// blocked; the ceiling prevents malformed or perpetually changing provider
// responses from growing the process-lifetime map without bound.
const rediscoveryPendingMaxEntries = 4096

// rediscoveryPendingNow is the clock seam for the log-rate tests; production
// always uses time.Now.
func (m *Manager) rediscoveryPendingNow() time.Time {
	if m.rediscoveryPendingNowFunc != nil {
		return m.rediscoveryPendingNowFunc()
	}
	return time.Now()
}

// shouldLogRediscoveryPending decides, under the limiter's mutex, whether this
// key is due for its once-per-interval operational notice. Callers log at
// Warn when it returns true and at Debug otherwise, so repeated syncs stay
// visible in debug runs without restorming production logs.
func (m *Manager) shouldLogRediscoveryPending(key string, now time.Time) bool {
	m.rediscoveryPendingMu.Lock()
	defer m.rediscoveryPendingMu.Unlock()
	if m.rediscoveryPendingLast == nil {
		m.rediscoveryPendingLast = make(map[string]time.Time)
	}
	last, seen := m.rediscoveryPendingLast[key]
	if seen && now.Sub(last) < rediscoveryPendingLogInterval {
		return false
	}
	if !seen && len(m.rediscoveryPendingLast) >= rediscoveryPendingMaxEntries {
		// Evict the oldest notice. This scan only occurs at the defensive cap,
		// keeping the common path constant-time while bounding memory strictly.
		var oldestKey string
		var oldestTime time.Time
		for candidateKey, candidateTime := range m.rediscoveryPendingLast {
			if oldestKey == "" || candidateTime.Before(oldestTime) {
				oldestKey = candidateKey
				oldestTime = candidateTime
			}
		}
		delete(m.rediscoveryPendingLast, oldestKey)
	}
	m.rediscoveryPendingLast[key] = now
	return true
}

// clearRediscoveryPending releases limiter state once the storage guard says
// the provider/hash pair is no longer awaiting authoritative absence.
func (m *Manager) clearRediscoveryPending(provider, infoHash string) {
	key := provider + "/" + infoHash
	m.rediscoveryPendingMu.Lock()
	delete(m.rediscoveryPendingLast, key)
	m.rediscoveryPendingMu.Unlock()
}

// noteRediscoveryPending emits the coalesced notice for a provider-sync skip.
func (m *Manager) noteRediscoveryPending(provider, infoHash string) {
	key := provider + "/" + infoHash
	if m.shouldLogRediscoveryPending(key, m.rediscoveryPendingNow()) {
		m.logger.Warn().
			Str("debrid", provider).
			Str("infohash", infoHash).
			Msg("Skipping rediscovery for deleted entry: awaiting authoritative post-delete provider absence")
		return
	}
	m.logger.Debug().
		Str("debrid", provider).
		Str("infohash", infoHash).
		Msg("Still awaiting post-delete provider absence; skipping rediscovery attempt")
}
