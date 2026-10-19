package server

import (
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	loginFailureWindow = 5 * time.Minute
	loginBlockDuration = 10 * time.Minute
	loginMaxFailures   = 10
	loginLimiterMaxIPs = 4096
)

type loginAttempt struct {
	windowStarted time.Time
	failures      int
	blockedUntil  time.Time
	lastSeen      time.Time
}

type loginAttemptLimiter struct {
	mu       sync.Mutex
	attempts map[string]loginAttempt
	now      func() time.Time
}

func newLoginAttemptLimiter() *loginAttemptLimiter {
	return &loginAttemptLimiter{
		attempts: make(map[string]loginAttempt),
		now:      time.Now,
	}
}

func (l *loginAttemptLimiter) retryAfter(key string) time.Duration {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	attempt, ok := l.attempts[key]
	if !ok || !now.Before(attempt.blockedUntil) {
		return 0
	}
	return attempt.blockedUntil.Sub(now)
}

func (l *loginAttemptLimiter) recordFailure(key string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	l.pruneLocked(now)
	attempt := l.attempts[key]
	if attempt.windowStarted.IsZero() || now.Sub(attempt.windowStarted) >= loginFailureWindow {
		attempt.windowStarted = now
		attempt.failures = 0
	}
	attempt.failures++
	attempt.lastSeen = now
	if attempt.failures >= loginMaxFailures {
		attempt.blockedUntil = now.Add(loginBlockDuration)
	}
	l.attempts[key] = attempt
}

func (l *loginAttemptLimiter) reset(key string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	delete(l.attempts, key)
	l.mu.Unlock()
}

func (l *loginAttemptLimiter) pruneLocked(now time.Time) {
	for key, attempt := range l.attempts {
		if !now.Before(attempt.blockedUntil) && now.Sub(attempt.lastSeen) >= loginFailureWindow {
			delete(l.attempts, key)
		}
	}
	if len(l.attempts) < loginLimiterMaxIPs {
		return
	}
	var oldestKey string
	var oldest time.Time
	for key, attempt := range l.attempts {
		if oldestKey == "" || attempt.lastSeen.Before(oldest) {
			oldestKey = key
			oldest = attempt.lastSeen
		}
	}
	if oldestKey != "" {
		delete(l.attempts, oldestKey)
	}
}

func loginClientKey(r *http.Request) string {
	if r == nil {
		return "unknown"
	}
	remote := strings.TrimSpace(r.RemoteAddr)
	if host, _, err := net.SplitHostPort(remote); err == nil {
		return host
	}
	if remote == "" {
		return "unknown"
	}
	return remote
}
