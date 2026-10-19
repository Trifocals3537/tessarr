package account

import (
	"fmt"
	"time"
)

const (
	recoveryBackoffBase = time.Minute
	recoveryBackoffMax  = 15 * time.Minute
	recoveryRetryMax    = 24 * time.Hour
	recoveryProbeLease  = 30 * time.Second
)

// State is the externally safe lifecycle state of a debrid account. Tokens
// and other credentials are intentionally absent from this model.
type State string

const (
	StateActive               State = "active"
	StateTemporarilySuspended State = "temporarily_suspended"
	StateRecoveryReady        State = "recovery_ready"
	StateRecoveryProbe        State = "recovery_probe"
	StatePermanentlyDisabled  State = "permanently_disabled"
)

type recoveryState struct {
	failures   int
	nextProbe  uint64
	probeID    uint64
	probeUntil time.Time
	reason     string
	retryAt    time.Time
}

// RecoveryStatus is a secret-free snapshot suitable for diagnostics and API
// statistics.
type RecoveryStatus struct {
	State      State
	Reason     string
	Failures   int
	RetryAt    time.Time
	RetryAfter time.Duration
}

// UnavailableError distinguishes a temporary all-account cooldown from a
// genuinely permanent no-account condition without coupling this package to
// the link service's error types.
type UnavailableError struct {
	Debrid     string
	RetryAfter time.Duration
	Temporary  bool
}

func (e *UnavailableError) Error() string {
	if e.Temporary {
		return fmt.Sprintf("all %s accounts are temporarily suspended; retry after %s", e.Debrid, e.RetryAfter.Round(time.Second))
	}
	return fmt.Sprintf("no active account for debrid %s", e.Debrid)
}

// TryAcquire admits ordinary active-account work freely, but allows exactly
// one half-open request after a suspension cooldown. The probe lease prevents
// a canceled or abandoned request from blocking recovery forever.
func (a *Account) TryAcquire(now time.Time) (acquired bool, probeID uint64, retryAfter time.Duration) {
	if a.Disabled.Load() {
		return false, 0, 0
	}

	a.recoveryMu.Lock()
	defer a.recoveryMu.Unlock()
	if a.Disabled.Load() {
		return false, 0, 0
	}

	if a.recovery.retryAt.IsZero() {
		return true, 0, 0
	}
	if a.recovery.probeID != 0 && !now.Before(a.recovery.probeUntil) {
		a.recovery.probeID = 0
		a.recovery.probeUntil = time.Time{}
	}
	if now.Before(a.recovery.retryAt) {
		return false, 0, a.recovery.retryAt.Sub(now)
	}
	if a.recovery.probeID != 0 {
		return false, 0, a.recovery.probeUntil.Sub(now)
	}

	a.recovery.nextProbe++
	if a.recovery.nextProbe == 0 {
		a.recovery.nextProbe++
	}
	a.recovery.probeID = a.recovery.nextProbe
	a.recovery.probeUntil = now.Add(recoveryProbeLease)
	return true, a.recovery.probeID, 0
}

// SuspendTemporary records a provider-pressure failure. A burst of late
// failures is coalesced into the current cooldown, while a failed half-open
// probe advances the bounded exponential backoff.
func (a *Account) SuspendTemporary(now time.Time, probeID uint64, requested time.Duration, reason string) (RecoveryStatus, bool) {
	if a.Disabled.Load() {
		return a.RecoveryStatus(now), false
	}

	a.recoveryMu.Lock()
	defer a.recoveryMu.Unlock()
	if a.Disabled.Load() {
		return RecoveryStatus{State: StatePermanentlyDisabled}, false
	}

	if probeID != 0 {
		if a.recovery.probeID != probeID {
			return a.recoveryStatusLocked(now), false
		}
	} else {
		// A request started before the half-open probe must not be allowed to
		// fail or replace that probe's recovery decision.
		if a.recovery.probeID != 0 {
			return a.recoveryStatusLocked(now), false
		}
		if !a.recovery.retryAt.IsZero() && now.Before(a.recovery.retryAt) {
			if requested > a.recovery.retryAt.Sub(now) {
				a.recovery.retryAt = now.Add(boundRequestedRetry(requested))
			}
			return a.recoveryStatusLocked(now), false
		}
	}

	a.recovery.failures++
	delay := recoveryBackoffDelay(a.recovery.failures)
	if requested > delay {
		delay = boundRequestedRetry(requested)
	}
	a.recovery.probeID = 0
	a.recovery.probeUntil = time.Time{}
	a.recovery.reason = reason
	a.recovery.retryAt = now.Add(delay)
	return a.recoveryStatusLocked(now), true
}

// MarkHealthy closes a matching half-open probe. Successes from ordinary or
// stale in-flight requests cannot clear a newer suspension.
func (a *Account) MarkHealthy(probeID uint64) bool {
	if probeID == 0 || a.Disabled.Load() {
		return false
	}
	a.recoveryMu.Lock()
	defer a.recoveryMu.Unlock()
	if a.Disabled.Load() {
		return false
	}
	if a.recovery.probeID != probeID {
		return false
	}
	a.recovery = recoveryState{nextProbe: a.recovery.nextProbe}
	return true
}

// ReleaseProbe makes a canceled half-open request immediately eligible for a
// replacement without treating client cancellation as a provider failure.
func (a *Account) ReleaseProbe(probeID uint64) bool {
	if probeID == 0 {
		return false
	}
	a.recoveryMu.Lock()
	defer a.recoveryMu.Unlock()
	if a.Disabled.Load() {
		return false
	}
	if a.recovery.probeID != probeID {
		return false
	}
	a.recovery.probeID = 0
	a.recovery.probeUntil = time.Time{}
	return true
}

func (a *Account) RecoveryStatus(now time.Time) RecoveryStatus {
	if a.Disabled.Load() {
		return RecoveryStatus{State: StatePermanentlyDisabled}
	}
	a.recoveryMu.Lock()
	defer a.recoveryMu.Unlock()
	if a.Disabled.Load() {
		return RecoveryStatus{State: StatePermanentlyDisabled}
	}
	return a.recoveryStatusLocked(now)
}

func (a *Account) recoveryStatusLocked(now time.Time) RecoveryStatus {
	status := RecoveryStatus{
		State:    StateActive,
		Reason:   a.recovery.reason,
		Failures: a.recovery.failures,
	}
	if a.recovery.retryAt.IsZero() {
		return status
	}
	if a.recovery.probeID != 0 && now.Before(a.recovery.probeUntil) {
		status.State = StateRecoveryProbe
		status.RetryAt = a.recovery.probeUntil
		status.RetryAfter = a.recovery.probeUntil.Sub(now)
		return status
	}
	if now.Before(a.recovery.retryAt) {
		status.State = StateTemporarilySuspended
		status.RetryAt = a.recovery.retryAt
		status.RetryAfter = a.recovery.retryAt.Sub(now)
		return status
	}
	status.State = StateRecoveryReady
	return status
}

func recoveryBackoffDelay(failures int) time.Duration {
	delay := recoveryBackoffBase
	for attempt := 1; attempt < failures && delay < recoveryBackoffMax; attempt++ {
		if delay >= recoveryBackoffMax/2 {
			return recoveryBackoffMax
		}
		delay *= 2
	}
	if delay > recoveryBackoffMax {
		return recoveryBackoffMax
	}
	return delay
}

func boundRequestedRetry(requested time.Duration) time.Duration {
	if requested < 0 {
		return 0
	}
	if requested > recoveryRetryMax {
		return recoveryRetryMax
	}
	return requested
}
