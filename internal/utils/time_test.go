package utils

import (
	"sync"
	"testing"
	"time"
)

// TestCachedTimeWithoutStartTracksRealTime reproduces the CI failure shape:
// a process (for example the pkg/usenet/parser test binary) that never calls
// Start must not serve a clock frozen at construction time. NNTP connection
// deadlines are computed as utils.Now().Add(timeout); a frozen clock makes
// those deadlines already-expired once the process has outlived the timeout,
// surfacing as instant "i/o timeout" read failures on the first greeting.
func TestCachedTimeWithoutStartTracksRealTime(t *testing.T) {
	ct := NewCachedTime()

	before := ct.Now()
	if skew := time.Since(before); skew > 2*time.Second {
		t.Fatalf("fresh cache already skewed %v from real time", skew)
	}

	// Outlive the original cache snapshot without a refresher running.
	time.Sleep(1200 * time.Millisecond)

	after := ct.Now()
	if elapsed := after.Sub(before); elapsed < time.Second {
		t.Fatalf("Now() advanced only %v without Start(); the clock is frozen at construction time", elapsed)
	}
	if skew := time.Since(after); skew > 2*time.Second {
		t.Fatalf("Now() is skewed %v from real time without Start()", skew)
	}
	if unix := ct.Unix(); time.Since(time.Unix(unix, 0)) > 2*time.Second {
		t.Fatalf("Unix() is skewed from real time without Start()")
	}
	if unixNano := ct.UnixNano(); time.Since(time.Unix(0, unixNano)) > 2*time.Second {
		t.Fatalf("UnixNano() is skewed from real time without Start()")
	}
}

// TestCachedTimeServesCacheWhileRunning pins the production contract: with the
// refresher running, Now() may lag real time by at most one refresh interval
// (the hot path this cache exists for), and never freezes.
func TestCachedTimeServesCacheWhileRunning(t *testing.T) {
	ct := NewCachedTime()
	ct.Start()
	t.Cleanup(ct.Stop)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Hold the read side busy while the refresher updates underneath.
		for range 50 {
			_ = ct.Now()
			time.Sleep(10 * time.Millisecond)
		}
	}()
	wg.Wait()

	now := ct.Now()
	if skew := time.Since(now); skew > 2*time.Second {
		t.Fatalf("running cache is skewed %v from real time; refresh interval is 500ms", skew)
	}
}

// TestCachedTimeStopReturnsToRealClock proves a stopped cache stops serving a
// frozen value, so shutdown paths that still consult the clock stay accurate.
func TestCachedTimeStopReturnsToRealClock(t *testing.T) {
	ct := NewCachedTime()
	ct.Start()

	// Give the refresher at least one tick so the cache holds a live value.
	time.Sleep(700 * time.Millisecond)
	ct.Stop()

	// The run loop clears the running flag on exit; wait for that handshake.
	deadline := time.Now().Add(5 * time.Second)
	for ct.running.Load() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if ct.running.Load() {
		t.Fatal("Stop() did not clear the running flag in time")
	}

	time.Sleep(200 * time.Millisecond)
	frozenAt := ct.Now()
	if skew := time.Since(frozenAt); skew > 2*time.Second {
		t.Fatalf("after Stop(), Now() is skewed %v; a stopped cache must fall back to the real clock", skew)
	}
}
