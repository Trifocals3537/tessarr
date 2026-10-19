package cdntraffic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/providertraffic"
)

func testGovernor(limit int) *Governor {
	return New(Options{
		DefaultLimit:        limit,
		TorBoxLimit:         limit,
		MinimumLimit:        1,
		RecoveryInterval:    20 * time.Millisecond,
		DefaultBackoff:      40 * time.Millisecond,
		MaximumBackoff:      time.Second,
		MaxInteractiveBurst: 4,
	})
}

func mustAcquire(t *testing.T, governor *Governor, identity Identity, priority Priority) *Permit {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	permit, err := governor.Acquire(ctx, identity, "cdn.example", priority)
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	return permit
}

func TestGovernorReservesCapacityForInteractiveRequests(t *testing.T) {
	governor := testGovernor(3)
	identity := Identity{Provider: "debrid", ProviderType: "realdebrid", AccountToken: "secret"}

	backgroundOne := mustAcquire(t, governor, identity, PriorityBackground)
	backgroundTwo := mustAcquire(t, governor, identity, PriorityBackground)

	thirdBackground := make(chan *Permit, 1)
	go func() {
		permit, _ := governor.Acquire(context.Background(), identity, "cdn.example", PriorityBackground)
		thirdBackground <- permit
	}()
	select {
	case permit := <-thirdBackground:
		permit.Release()
		t.Fatal("third background request consumed the playback reserve")
	case <-time.After(20 * time.Millisecond):
	}

	interactive := mustAcquire(t, governor, identity, PriorityInteractive)
	interactive.Release()
	select {
	case permit := <-thirdBackground:
		permit.Release()
		t.Fatal("background request was admitted while the reserved slot was the only capacity")
	case <-time.After(20 * time.Millisecond):
	}

	backgroundOne.Release()
	select {
	case permit := <-thirdBackground:
		if permit == nil {
			t.Fatal("queued background request returned a nil permit")
		}
		permit.Release()
	case <-time.After(time.Second):
		t.Fatal("queued background request was not admitted after background capacity returned")
	}
	backgroundTwo.Release()
}

func TestGovernorQueuesInteractiveAheadOfBackground(t *testing.T) {
	governor := testGovernor(2)
	identity := Identity{Provider: "debrid", AccountToken: "secret"}
	first := mustAcquire(t, governor, identity, PriorityInteractive)
	second := mustAcquire(t, governor, identity, PriorityInteractive)

	backgroundReady := make(chan *Permit, 1)
	interactiveReady := make(chan *Permit, 1)
	go func() {
		permit, _ := governor.Acquire(context.Background(), identity, "cdn.example", PriorityBackground)
		backgroundReady <- permit
	}()
	go func() {
		permit, _ := governor.Acquire(context.Background(), identity, "cdn.example", PriorityInteractive)
		interactiveReady <- permit
	}()

	deadline := time.Now().Add(time.Second)
	for governor.Snapshot().WaitingInteractive != 1 || governor.Snapshot().WaitingBackground != 1 {
		if time.Now().After(deadline) {
			t.Fatal("requests did not enter both priority queues")
		}
		time.Sleep(time.Millisecond)
	}

	first.Release()
	var queuedInteractive *Permit
	select {
	case queuedInteractive = <-interactiveReady:
		if queuedInteractive == nil {
			t.Fatal("queued interactive request returned a nil permit")
		}
	case permit := <-backgroundReady:
		permit.Release()
		t.Fatal("background request was admitted ahead of queued playback")
	case <-time.After(time.Second):
		t.Fatal("queued interactive request was not admitted")
	}

	queuedInteractive.Release()
	second.Release()
	select {
	case permit := <-backgroundReady:
		if permit == nil {
			t.Fatal("queued background request returned a nil permit")
		}
		permit.Release()
	case <-time.After(time.Second):
		t.Fatal("queued background request starved after playback drained")
	}
}

func TestGovernorCancellationRemovesWaiter(t *testing.T) {
	governor := testGovernor(1)
	identity := Identity{Provider: "debrid"}
	active := mustAcquire(t, governor, identity, PriorityBackground)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := governor.Acquire(ctx, identity, "cdn.example", PriorityInteractive)
		result <- err
	}()

	deadline := time.Now().Add(time.Second)
	for governor.Snapshot().WaitingInteractive != 1 {
		if time.Now().After(deadline) {
			t.Fatal("interactive request did not enter the queue")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire() cancellation error = %v", err)
	}
	if got := governor.Snapshot().WaitingInteractive; got != 0 {
		t.Fatalf("waiting interactive = %d, want 0", got)
	}
	active.Release()
}

func TestGovernorBacksOffOn429AndRecoversGradually(t *testing.T) {
	governor := testGovernor(4)
	identity := Identity{Provider: "debrid", ProviderType: "realdebrid", AccountToken: "secret"}
	governor.Observe(identity, "cdn.example", http.StatusTooManyRequests, nil)

	stats := governor.Snapshot()
	if len(stats.Providers) != 1 {
		t.Fatalf("provider stats = %d, want 1", len(stats.Providers))
	}
	if stats.Providers[0].CurrentLimit != 2 {
		t.Fatalf("current limit = %d, want 2", stats.Providers[0].CurrentLimit)
	}
	if stats.Throttles != 1 || stats.Providers[0].BlockedUntil == nil || stats.Providers[0].BlockedUntil.IsZero() {
		t.Fatalf("throttle snapshot = %+v", stats)
	}

	started := time.Now()
	permit := mustAcquire(t, governor, identity, PriorityInteractive)
	if waited := time.Since(started); waited < 25*time.Millisecond {
		permit.Release()
		t.Fatalf("429 admission wait = %s, want at least 25ms", waited)
	}
	permit.Release()

	time.Sleep(25 * time.Millisecond)
	governor.Observe(identity, "cdn.example", http.StatusOK, nil)
	if got := governor.Snapshot().Providers[0].CurrentLimit; got != 3 {
		t.Fatalf("recovered limit = %d, want 3", got)
	}
}

func TestGovernorSnapshotDoesNotExposeAccountToken(t *testing.T) {
	governor := testGovernor(2)
	identity := Identity{Provider: "named-provider", ProviderType: "torbox", AccountToken: "do-not-expose"}
	permit := mustAcquire(t, governor, identity, PriorityInteractive)
	permit.Release()

	encoded, err := json.Marshal(governor.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), identity.AccountToken) {
		t.Fatalf("snapshot exposed account token: %s", encoded)
	}
	if strings.Contains(string(encoded), "blocked_until") {
		t.Fatalf("healthy snapshot included an empty backoff timestamp: %s", encoded)
	}
}

func TestGovernorUsesTorBoxSafeStartingLimitForNamedProvider(t *testing.T) {
	governor := New(Options{})
	governor.Observe(
		Identity{Provider: "my-primary", ProviderType: "torbox", AccountToken: "secret"},
		"api.torbox.app",
		http.StatusOK,
		nil,
	)
	stats := governor.Snapshot()
	if len(stats.Providers) != 1 {
		t.Fatalf("provider stats = %d, want 1", len(stats.Providers))
	}
	if got := stats.Providers[0].MaximumLimit; got != defaultTorBoxRequests {
		t.Fatalf("TorBox maximum limit = %d, want %d", got, defaultTorBoxRequests)
	}
}

func TestGovernorConcurrentAcquireReleaseStaysWithinLimit(t *testing.T) {
	governor := testGovernor(4)
	identity := Identity{Provider: "debrid", AccountToken: "secret"}
	var active atomic.Int64
	var peak atomic.Int64
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			priority := PriorityBackground
			if i%3 == 0 {
				priority = PriorityInteractive
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			permit, err := governor.Acquire(ctx, identity, "cdn.example", priority)
			if err != nil {
				t.Errorf("Acquire() error = %v", err)
				return
			}
			current := active.Add(1)
			for {
				observed := peak.Load()
				if current <= observed || peak.CompareAndSwap(observed, current) {
					break
				}
			}
			time.Sleep(time.Millisecond)
			active.Add(-1)
			permit.Release()
		}()
	}
	wg.Wait()
	if got := peak.Load(); got > 4 {
		t.Fatalf("peak concurrency = %d, want <= 4", got)
	}
	stats := governor.Snapshot()
	if stats.Active != 0 || stats.WaitingInteractive != 0 || stats.WaitingBackground != 0 {
		t.Fatalf("governor leaked admission state: %+v", stats)
	}
}

func TestGovernorMixedTrafficSoakStaysBoundedAndMakesProgress(t *testing.T) {
	const (
		limit      = 4
		workers    = 16
		iterations = 30
	)
	governor := New(Options{
		DefaultLimit:        limit,
		TorBoxLimit:         limit,
		MinimumLimit:        1,
		RecoveryInterval:    2 * time.Millisecond,
		DefaultBackoff:      3 * time.Millisecond,
		MaximumBackoff:      20 * time.Millisecond,
		MaxInteractiveBurst: 3,
	})
	identity := Identity{Provider: "torbox-primary", ProviderType: "torbox", AccountToken: "secret"}

	runCtx, cancelRun := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRun()

	var active atomic.Int64
	var peak atomic.Int64
	var observed atomic.Int64
	var completedInteractive atomic.Int64
	var completedBackground atomic.Int64
	var canceled atomic.Int64
	errCh := make(chan error, workers)
	start := make(chan struct{})

	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for iteration := range iterations {
				priority := PriorityBackground
				if (worker+iteration)%3 != 0 {
					priority = PriorityInteractive
				}

				if (worker+iteration)%13 == 0 {
					requestCtx, cancelRequest := context.WithCancel(runCtx)
					cancelRequest()
					_, err := governor.Acquire(requestCtx, identity, "cdn.example", priority)
					if !errors.Is(err, context.Canceled) {
						errCh <- fmt.Errorf("canceled Acquire() error = %v", err)
						return
					}
					canceled.Add(1)
					continue
				}

				requestCtx, cancelRequest := context.WithTimeout(runCtx, time.Second)
				permit, err := governor.Acquire(requestCtx, identity, "cdn.example", priority)
				cancelRequest()
				if err != nil {
					errCh <- fmt.Errorf("Acquire() error = %w", err)
					return
				}

				current := active.Add(1)
				for {
					previous := peak.Load()
					if current <= previous || peak.CompareAndSwap(previous, current) {
						break
					}
				}

				time.Sleep(200 * time.Microsecond)
				if observed.Add(1)%41 == 0 {
					governor.Observe(identity, "cdn.example", http.StatusTooManyRequests, nil)
				} else {
					governor.Observe(identity, "cdn.example", http.StatusOK, nil)
				}

				active.Add(-1)
				permit.Release()
				if priority == PriorityInteractive {
					completedInteractive.Add(1)
				} else {
					completedBackground.Add(1)
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if err := runCtx.Err(); err != nil {
		t.Fatalf("mixed traffic soak exceeded its deadline: %v", err)
	}
	if got := peak.Load(); got > limit {
		t.Fatalf("peak concurrency = %d, want <= %d", got, limit)
	}
	if completedInteractive.Load() == 0 || completedBackground.Load() == 0 {
		t.Fatalf(
			"completed interactive/background = %d/%d, want both to make progress",
			completedInteractive.Load(),
			completedBackground.Load(),
		)
	}
	if canceled.Load() == 0 {
		t.Fatal("mixed traffic soak did not exercise cancellation")
	}

	stats := governor.Snapshot()
	if stats.Active != 0 || stats.WaitingInteractive != 0 || stats.WaitingBackground != 0 {
		t.Fatalf("governor leaked admission state: %+v", stats)
	}
	if stats.Throttles == 0 {
		t.Fatal("mixed traffic soak did not exercise 429 throttling")
	}
	if len(stats.Providers) != 1 {
		t.Fatalf("provider stats = %d, want 1", len(stats.Providers))
	}
	provider := stats.Providers[0]
	if provider.AdmittedInteractive == 0 || provider.AdmittedBackground == 0 {
		t.Fatalf("provider admissions did not make progress: %+v", provider)
	}
}

func TestRetryAfterSupportsSecondsAndHTTPDate(t *testing.T) {
	now := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)
	seconds := make(http.Header)
	seconds.Set("Retry-After", "12")
	if got := retryAfter(seconds, now); got != 12*time.Second {
		t.Fatalf("seconds Retry-After = %s", got)
	}

	date := make(http.Header)
	date.Set("Retry-After", now.Add(45*time.Second).Format(http.TimeFormat))
	if got := retryAfter(date, now); got != 45*time.Second {
		t.Fatalf("date Retry-After = %s", got)
	}
}

func TestGovernorBoundsIdleTrafficStates(t *testing.T) {
	governor := testGovernor(2)
	for i := range maxTrafficStates + 25 {
		governor.Observe(
			Identity{Provider: fmt.Sprintf("provider-%d", i)},
			"cdn.example",
			http.StatusOK,
			nil,
		)
	}
	governor.mu.Lock()
	states := len(governor.states)
	governor.mu.Unlock()
	if states > maxTrafficStates {
		t.Fatalf("traffic states = %d, want <= %d", states, maxTrafficStates)
	}
}

func mustParseURL(t *testing.T, rawURL string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestTorBoxPerLinkLimitDoesNotSerializeIndependentFiles(t *testing.T) {
	governor := New(Options{TorBoxLimit: 4, TorBoxHostLimit: 16})
	first := Identity{
		Provider: "torbox-primary", ProviderType: "torbox",
		AccountToken: "secret", LinkKey: "torbox://1/1",
	}
	second := first
	second.LinkKey = "torbox://2/1"

	permits := make([]*Permit, 0, 5)
	for range 4 {
		permit, err := governor.Acquire(context.Background(), first, "nexus.example", PriorityInteractive)
		if err != nil {
			t.Fatal(err)
		}
		permits = append(permits, permit)
	}

	blocked := make(chan *Permit, 1)
	go func() {
		permit, _ := governor.Acquire(context.Background(), first, "nexus.example", PriorityInteractive)
		blocked <- permit
	}()
	select {
	case permit := <-blocked:
		permit.Release()
		t.Fatal("fifth request for one link exceeded the per-link limit")
	case <-time.After(20 * time.Millisecond):
	}

	otherCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	other, err := governor.Acquire(otherCtx, second, "nexus.example", PriorityInteractive)
	if err != nil {
		t.Fatalf("independent link was serialized behind the first: %v", err)
	}
	other.Release()

	permits[0].Release()
	select {
	case permit := <-blocked:
		if permit == nil {
			t.Fatal("queued same-link request returned nil permit")
		}
		permit.Release()
	case <-time.After(time.Second):
		t.Fatal("same-link request did not resume after capacity returned")
	}
	for _, permit := range permits[1:] {
		permit.Release()
	}
}

func TestTorBoxHostLimitSpansLinksAccountsAndConfiguredProviders(t *testing.T) {
	governor := New(Options{TorBoxLimit: 4, TorBoxHostLimit: 3})
	identities := []Identity{
		{Provider: "one", ProviderType: "torbox", AccountToken: "a", LinkKey: "torbox://1/1"},
		{Provider: "one", ProviderType: "torbox", AccountToken: "a", LinkKey: "torbox://2/1"},
		{Provider: "two", ProviderType: "torbox", AccountToken: "b", LinkKey: "torbox://3/1"},
		{Provider: "two", ProviderType: "torbox", AccountToken: "b", LinkKey: "torbox://4/1"},
	}
	permits := make([]*Permit, 0, 3)
	for _, identity := range identities[:3] {
		permit, err := governor.Acquire(context.Background(), identity, "nexus.example", PriorityInteractive)
		if err != nil {
			t.Fatal(err)
		}
		permits = append(permits, permit)
	}

	queued := make(chan *Permit, 1)
	go func() {
		permit, _ := governor.Acquire(context.Background(), identities[3], "nexus.example", PriorityInteractive)
		queued <- permit
	}()
	select {
	case permit := <-queued:
		permit.Release()
		t.Fatal("request exceeded the shared CDN-host limit")
	case <-time.After(20 * time.Millisecond):
	}
	stats := governor.Snapshot()
	if stats.Active != 3 || stats.WaitingInteractive != 1 {
		t.Fatalf("host queue snapshot active/waiting = %d/%d, want 3/1", stats.Active, stats.WaitingInteractive)
	}
	permits[0].Release()
	select {
	case permit := <-queued:
		permit.Release()
	case <-time.After(time.Second):
		t.Fatal("host-limited request did not resume")
	}
	for _, permit := range permits[1:] {
		permit.Release()
	}
}

func TestTorBoxResolverDoesNotConsumePerLinkCDNCapacity(t *testing.T) {
	traffic := providertraffic.New(providertraffic.Options{
		Capabilities: func(string) providertraffic.Capabilities {
			return providertraffic.Capabilities{
				APIBudget: providertraffic.RateBudget{
					Requests: 1000, Period: time.Second, Burst: 100,
				},
			}
		},
	})
	governor := New(Options{TorBoxLimit: 4, TorBoxHostLimit: 16, Traffic: traffic})
	identity := Identity{
		Provider: "torbox-primary", ProviderType: "torbox",
		AccountToken: "secret", LinkKey: "torbox://1/1",
	}
	permits := make([]*Permit, 0, 4)
	for range 4 {
		permit, err := governor.Acquire(context.Background(), identity, "nexus.example", PriorityInteractive)
		if err != nil {
			t.Fatal(err)
		}
		permits = append(permits, permit)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	resolver, err := governor.AcquireRequest(
		ctx,
		identity,
		mustParseURL(t, "https://api.torbox.app/v1/api/torrents/requestdl?token=secret"),
		PriorityInteractive,
	)
	if err != nil {
		t.Fatalf("link resolution was serialized behind CDN bytes: %v", err)
	}
	resolver.Release()
	for _, permit := range permits {
		permit.Release()
	}
}

func TestTorBoxResolverRedirectRecoversAdaptiveLimit(t *testing.T) {
	governor := testGovernor(4)
	identity := Identity{
		Provider: "torbox-primary", ProviderType: "torbox", AccountToken: "secret",
	}
	resolverURL := mustParseURL(t, "https://api.torbox.app/v1/api/torrents/requestdl?token=secret")
	governor.ObserveRequest(identity, resolverURL, http.StatusTooManyRequests, nil)
	if got := governor.Snapshot().Providers[0].CurrentLimit; got != 2 {
		t.Fatalf("throttled resolver limit = %d, want 2", got)
	}

	time.Sleep(65 * time.Millisecond)
	governor.ObserveRequest(identity, resolverURL, http.StatusFound, nil)
	if got := governor.Snapshot().Providers[0].CurrentLimit; got != 3 {
		t.Fatalf("redirect-recovered resolver limit = %d, want 3", got)
	}
}

func TestTorBoxSnapshotCountsCompositeRequestOnceAndRedactsKeys(t *testing.T) {
	governor := New(Options{TorBoxLimit: 4, TorBoxHostLimit: 16})
	identity := Identity{
		Provider: "torbox-primary", ProviderType: "torbox",
		AccountToken: "account-secret", LinkKey: "signed-link-secret",
	}
	permit, err := governor.Acquire(context.Background(), identity, "nexus.example", PriorityInteractive)
	if err != nil {
		t.Fatal(err)
	}
	stats := governor.Snapshot()
	if stats.Active != 1 || len(stats.Providers) != 1 || stats.Providers[0].Accounts != 1 {
		permit.Release()
		t.Fatalf("composite request snapshot = %+v, want one request/account", stats)
	}
	encoded, err := json.Marshal(stats)
	if err != nil {
		permit.Release()
		t.Fatal(err)
	}
	for _, secret := range []string{identity.AccountToken, identity.LinkKey} {
		if strings.Contains(string(encoded), secret) {
			permit.Release()
			t.Fatalf("snapshot exposed secret %q: %s", secret, encoded)
		}
	}
	permit.Release()
}

func BenchmarkGovernorTorBoxAcquireRelease(b *testing.B) {
	governor := New(Options{TorBoxLimit: 4, TorBoxHostLimit: 16})
	identity := Identity{
		Provider: "torbox-primary", ProviderType: "torbox",
		AccountToken: "secret", LinkKey: "torbox://1/1",
	}
	b.ReportAllocs()
	for b.Loop() {
		permit, err := governor.Acquire(
			context.Background(),
			identity,
			"nexus.example",
			PriorityInteractive,
		)
		if err != nil {
			b.Fatal(err)
		}
		permit.Release()
	}
}
