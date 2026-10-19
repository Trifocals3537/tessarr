package server

import (
	"testing"
	"time"
)

func TestLoginAttemptLimiterBlocksAndExpires(t *testing.T) {
	limiter := newLoginAttemptLimiter()
	now := time.Unix(100, 0)
	limiter.now = func() time.Time { return now }

	for range loginMaxFailures {
		limiter.recordFailure("192.0.2.1")
	}
	if retry := limiter.retryAfter("192.0.2.1"); retry != loginBlockDuration {
		t.Fatalf("retry after = %v, want %v", retry, loginBlockDuration)
	}
	if retry := limiter.retryAfter("192.0.2.2"); retry != 0 {
		t.Fatalf("unrelated client retry after = %v", retry)
	}

	now = now.Add(loginBlockDuration)
	if retry := limiter.retryAfter("192.0.2.1"); retry != 0 {
		t.Fatalf("expired block retry after = %v", retry)
	}
}

func TestLoginAttemptLimiterReset(t *testing.T) {
	limiter := newLoginAttemptLimiter()
	for range loginMaxFailures {
		limiter.recordFailure("192.0.2.1")
	}
	limiter.reset("192.0.2.1")
	if retry := limiter.retryAfter("192.0.2.1"); retry != 0 {
		t.Fatalf("reset client retry after = %v", retry)
	}
}
