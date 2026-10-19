package manager

import (
	"sync/atomic"
	"time"
)

// StreamSessionStats is a secret-free process-lifetime view of managed stream
// sessions. It intentionally aggregates events instead of identifying files,
// hashes, URLs, ranges, or providers.
type StreamSessionStats struct {
	SessionsOpened      uint64 `json:"sessions_opened"`
	SessionsClosed      uint64 `json:"sessions_closed"`
	ActiveSessions      uint64 `json:"active_sessions"`
	SourceOpens         uint64 `json:"source_opens"`
	ReusedReads         uint64 `json:"reused_reads"`
	RecoveryAttempts    uint64 `json:"recovery_attempts"`
	RecoveriesScheduled uint64 `json:"recoveries_scheduled"`
	RecoveriesExhausted uint64 `json:"recoveries_exhausted"`
	HTTPResumeAttempts  uint64 `json:"http_resume_attempts"`
	HTTPResumeSuccesses uint64 `json:"http_resume_successes"`
	OpenStalls          uint64 `json:"open_stalls"`
	ReadStalls          uint64 `json:"read_stalls"`
	IdleCloses          uint64 `json:"idle_closes"`
	RecoveryWaitTotalMS uint64 `json:"recovery_wait_total_ms"`
	RecoveryWaitMaxMS   uint64 `json:"recovery_wait_max_ms"`
}

type streamSessionMetrics struct {
	sessionsOpened       atomic.Uint64
	sessionsClosed       atomic.Uint64
	sourceOpens          atomic.Uint64
	reusedReads          atomic.Uint64
	recoveryAttempts     atomic.Uint64
	recoveriesScheduled  atomic.Uint64
	recoveriesExhausted  atomic.Uint64
	httpResumeAttempts   atomic.Uint64
	httpResumeSuccesses  atomic.Uint64
	openStalls           atomic.Uint64
	readStalls           atomic.Uint64
	idleCloses           atomic.Uint64
	recoveryWaitNanos    atomic.Uint64
	recoveryWaitMaxNanos atomic.Uint64
}

func (m *streamSessionMetrics) observeRecoveryWait(elapsed time.Duration) {
	if m == nil || elapsed <= 0 {
		return
	}
	nanos := uint64(elapsed)
	m.recoveryWaitNanos.Add(nanos)
	for current := m.recoveryWaitMaxNanos.Load(); nanos > current; current = m.recoveryWaitMaxNanos.Load() {
		if m.recoveryWaitMaxNanos.CompareAndSwap(current, nanos) {
			break
		}
	}
}

// StreamSessionStats returns aggregate counters for managed stream sessions.
func (m *Manager) StreamSessionStats() StreamSessionStats {
	if m == nil {
		return StreamSessionStats{}
	}
	opened := m.streamSessionMetrics.sessionsOpened.Load()
	closed := m.streamSessionMetrics.sessionsClosed.Load()
	active := uint64(0)
	if opened > closed {
		active = opened - closed
	}
	return StreamSessionStats{
		SessionsOpened:      opened,
		SessionsClosed:      closed,
		ActiveSessions:      active,
		SourceOpens:         m.streamSessionMetrics.sourceOpens.Load(),
		ReusedReads:         m.streamSessionMetrics.reusedReads.Load(),
		RecoveryAttempts:    m.streamSessionMetrics.recoveryAttempts.Load(),
		RecoveriesScheduled: m.streamSessionMetrics.recoveriesScheduled.Load(),
		RecoveriesExhausted: m.streamSessionMetrics.recoveriesExhausted.Load(),
		HTTPResumeAttempts:  m.streamSessionMetrics.httpResumeAttempts.Load(),
		HTTPResumeSuccesses: m.streamSessionMetrics.httpResumeSuccesses.Load(),
		OpenStalls:          m.streamSessionMetrics.openStalls.Load(),
		ReadStalls:          m.streamSessionMetrics.readStalls.Load(),
		IdleCloses:          m.streamSessionMetrics.idleCloses.Load(),
		RecoveryWaitTotalMS: m.streamSessionMetrics.recoveryWaitNanos.Load() / uint64(time.Millisecond),
		RecoveryWaitMaxMS:   m.streamSessionMetrics.recoveryWaitMaxNanos.Load() / uint64(time.Millisecond),
	}
}
