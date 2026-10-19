package nntp

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/rs/zerolog"
)

// Callbacks simulate article responses without contacting real providers. Keep
// enough fresh connections for the entire retry budget, including broken ones.
func newFailoverTestClient(t *testing.T, retries int, providers ...config.UsenetProvider) *Client {
	t.Helper()
	c := &Client{
		pools: make(map[string]*ProviderPool), providers: providers, retries: retries,
		logger: zerolog.Nop(), idleTimeout: time.Hour, staleThreshold: time.Hour,
	}
	for _, provider := range providers {
		pool := newReaperTestPool((retries+1)*len(providers) + 1)
		pool.config = provider
		for range pool.max {
			conn := newPipeConnection(t, false)
			conn.address = provider.Host
			addReaperTestEntry(pool, conn, 0)
		}
		c.pools[provider.Host] = pool
	}
	t.Cleanup(func() { assertFailoverSlotsReleased(t, c) })
	return c
}

func assertFailoverSlotsReleased(t *testing.T, c *Client) {
	t.Helper()
	for host, pool := range c.pools {
		if held := len(pool.slots); held != 0 {
			t.Errorf("%s: %d connection slots leaked", host, held)
		}
		pool.activeConns.Range(func(_, _ any) bool {
			t.Errorf("%s: connection still registered as active", host)
			return false
		})
	}
}

func TestFailoverArticleOutcomes(t *testing.T) {
	missing := classifyNNTPError(430, "article missing")
	temporary := NewTimeoutError(errors.New("temporary provider failure"))
	a := config.UsenetProvider{Host: "provider-a", Backbone: "a"}
	b := config.UsenetProvider{Host: "provider-b", Backbone: "b"}
	c := config.UsenetProvider{Host: "provider-c", Backbone: "a"}
	for _, tt := range []struct {
		name      string
		retries   int
		providers []config.UsenetProvider
		outcomes  map[string]error
		want      error
		visited   []string
	}{
		{"healthy", 1, []config.UsenetProvider{a, b}, nil, nil, []string{a.Host}},
		{"successful fallback", 0, []config.UsenetProvider{a, b}, map[string]error{a.Host: missing}, nil, []string{a.Host, b.Host}},
		{"all missing", 1, []config.UsenetProvider{a, b}, map[string]error{a.Host: missing, b.Host: missing}, missing, []string{a.Host, b.Host}},
		{"shared backbone missing", 1, []config.UsenetProvider{a, c, b}, map[string]error{a.Host: missing, b.Host: missing}, missing, []string{a.Host, b.Host}},
		{"timeout then missing without retry", 0, []config.UsenetProvider{a, b}, map[string]error{a.Host: temporary, b.Host: missing}, temporary, []string{a.Host, b.Host}},
		{"timeout then missing with retry", 1, []config.UsenetProvider{a, b}, map[string]error{a.Host: temporary, b.Host: missing}, temporary, nil},
		{"missing then timeout", 1, []config.UsenetProvider{a, b}, map[string]error{a.Host: missing, b.Host: temporary}, temporary, []string{a.Host, b.Host, b.Host}},
		{"retry switches backbone", 1, []config.UsenetProvider{a, b, c}, map[string]error{a.Host: temporary, b.Host: missing}, nil, []string{a.Host, b.Host, a.Host, c.Host}},
		{"retry preserves missing exclusion", 1, []config.UsenetProvider{a, b, {Host: "provider-c", Backbone: "c"}}, map[string]error{a.Host: missing, b.Host: temporary}, nil, []string{a.Host, b.Host, "provider-c"}},
		{"retry prefers untried provider", 2, []config.UsenetProvider{a, b, c}, map[string]error{a.Host: temporary, b.Host: temporary}, nil, []string{a.Host, b.Host, c.Host}},
		{"same backbone resolves uncertainty", 1, []config.UsenetProvider{a, c}, map[string]error{a.Host: temporary, c.Host: missing}, missing, []string{a.Host, c.Host}},
		{"unlabelled providers are independent", 1, []config.UsenetProvider{{Host: a.Host}, {Host: b.Host}}, map[string]error{a.Host: temporary, b.Host: missing}, temporary, nil},
		{"backup follows missing primary", 1, []config.UsenetProvider{a, {Host: b.Host, Backup: true}}, map[string]error{a.Host: missing}, nil, []string{a.Host, b.Host}},
		{"missing backup cannot erase primary timeout", 1, []config.UsenetProvider{a, {Host: b.Host, Backup: true}}, map[string]error{a.Host: temporary, b.Host: missing}, temporary, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := newFailoverTestClient(t, tt.retries, tt.providers...)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var visited []string
			err := client.ExecuteWithFailover(ctx, func(conn *Connection) error {
				visited = append(visited, conn.address)
				return tt.outcomes[conn.address]
			})
			if (tt.want == nil && err != nil) || (tt.want != nil && !errors.Is(err, tt.want)) {
				t.Errorf("got %v, want %v; visited=%v", err, tt.want, visited)
			}
			if IsArticleNotFoundError(err) != IsArticleNotFoundError(tt.want) {
				t.Errorf("incorrect article-missing classification: %v; visited=%v", err, visited)
			}
			if tt.visited != nil && !slices.Equal(visited, tt.visited) {
				t.Errorf("visited %v, want %v", visited, tt.visited)
			}
		})
	}
}

func TestFailoverRetryBudget(t *testing.T) {
	for _, failure := range []error{
		NewConnectionError(errors.New("disconnected")), NewTimeoutError(errors.New("timeout")),
		&Error{Type: ErrorTypeServerBusy, Code: 400},
	} {
		t.Run(failure.Error(), func(t *testing.T) {
			t.Parallel()
			client := newFailoverTestClient(t, 1, config.UsenetProvider{Host: "provider-a"}, config.UsenetProvider{Host: "provider-b"})
			calls := 0
			err := client.ExecuteWithFailover(context.Background(), func(*Connection) error { calls++; return failure })
			if !errors.Is(err, failure) || calls != 4 {
				t.Fatalf("got %v after %d calls; want %v within 2 providers * 2 attempts", err, calls, failure)
			}
		})
	}
}

func TestFailoverUnreachableProviderIsNotMissing(t *testing.T) {
	// An invalid port fails locally without DNS, network access, or a listener.
	unreachable := config.UsenetProvider{Host: "127.0.0.1", Port: -1}
	client := newFailoverTestClient(t, 1, unreachable, config.UsenetProvider{Host: "provider-b"})
	pool := client.pools[unreachable.Host]
	for _, entry := range pool.conns {
		_ = entry.conn.Close()
		releaseConnectionEntry(entry)
	}
	pool.conns = nil
	calls := 0
	err := client.ExecuteWithFailover(context.Background(), func(*Connection) error {
		calls++
		return classifyNNTPError(430, "article missing")
	})
	if err == nil || IsArticleNotFoundError(err) || calls != 1 {
		t.Fatalf("unreachable provider + missing is inconclusive; got %v after %d calls", err, calls)
	}
}

func TestFailoverPanicReleasesConnection(t *testing.T) {
	client := newFailoverTestClient(t, 1, config.UsenetProvider{Host: "provider-a"}, config.UsenetProvider{Host: "provider-b"})
	var failed *Connection
	err := client.ExecuteWithFailover(context.Background(), func(conn *Connection) error {
		if conn.address == "provider-a" {
			failed = conn
			panic("synthetic callback panic")
		}
		return nil
	})
	if err != nil || failed == nil || !failed.IsClosed() {
		t.Fatalf("panic should close connection and allow fallback; got %v", err)
	}
}

func TestFailoverReturnsRetryAcquisitionFailure(t *testing.T) {
	provider := config.UsenetProvider{Host: "127.0.0.1", Port: -1}
	client := newFailoverTestClient(t, 1, config.UsenetProvider{Host: "provider-a"}, provider)
	pool := client.pools[provider.Host]
	// Only one existing session is usable. After it times out, reconnecting
	// fails locally; that acquisition error must not become an older 430 or
	// callback timeout, nor permit the excluded provider to be queried again.
	for _, entry := range pool.conns[1:] {
		_ = entry.conn.Close()
		releaseConnectionEntry(entry)
	}
	pool.conns = pool.conns[:1]
	var visited []string
	err := client.ExecuteWithFailover(context.Background(), func(conn *Connection) error {
		visited = append(visited, conn.address)
		if conn.address == "provider-a" {
			return classifyNNTPError(430, "article missing")
		}
		return NewTimeoutError(errors.New("temporary callback timeout"))
	})
	var nntpErr *Error
	if !errors.As(err, &nntpErr) || nntpErr.Type != ErrorTypeConnection {
		t.Fatalf("got %v, want retry acquisition failure", err)
	}
	if !slices.Equal(visited, []string{"provider-a", provider.Host}) {
		t.Fatalf("unexpected extra callbacks: %v", visited)
	}
}

func TestFailoverBusyPrimaryDoesNotUseBackup(t *testing.T) {
	primary := config.UsenetProvider{Host: "provider-a"}
	client := newFailoverTestClient(t, 1, primary, config.UsenetProvider{Host: "provider-b", Backup: true})
	pool := client.pools[primary.Host]
	for range cap(pool.slots) {
		pool.slots <- struct{}{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	called := make(chan struct{}, 1)
	go func() {
		finished <- client.ExecuteWithFailover(ctx, func(*Connection) error { called <- struct{}{}; return nil })
	}()
	select {
	case <-called:
		t.Error("busy primary incorrectly caused a backup request")
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	if err := <-finished; !errors.Is(err, context.Canceled) {
		t.Errorf("got %v, want cancellation", err)
	}
	for range cap(pool.slots) {
		<-pool.slots
	}
	// The acquisition race drains asynchronously on cancellation. Wait for its
	// slot bookkeeping to settle before the test's leak check.
	deadline := time.Now().Add(time.Second)
	for len(pool.slots) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
}

func TestFailoverExclusionCloneIsIndependent(t *testing.T) {
	var original providerExclusions
	original.excludeHost("exhausted")
	original.excludeBackbone("missing")
	cloned := original.clone()
	cloned.excludeHost("temporary")
	cloned.excludeBackbone("another")
	for _, provider := range []config.UsenetProvider{{Host: "exhausted"}, {Backbone: "missing"}} {
		if !cloned.excludes(provider) || !original.excludes(provider) {
			t.Fatal("clone lost an existing exclusion")
		}
	}
	if original.excludes(config.UsenetProvider{Host: "temporary"}) || original.excludes(config.UsenetProvider{Backbone: "another"}) {
		t.Fatal("temporary retry exclusions changed the original")
	}
}

func TestFailoverDoesNotAcquireAfterLastAttempt(t *testing.T) {
	client := newFailoverTestClient(t, 0, config.UsenetProvider{Host: "provider-a"})
	pool := client.pools["provider-a"]
	initial := len(pool.conns)
	old := time.Now().Add(-time.Minute)
	for _, entry := range pool.conns {
		entry.lastUsed = old
	}
	err := client.ExecuteWithFailover(context.Background(), func(*Connection) error {
		return NewConnectionError(errors.New("disconnected"))
	})
	if err == nil || IsArticleNotFoundError(err) {
		t.Fatalf("unexpected result: %v", err)
	}
	if len(pool.conns) != initial-1 {
		t.Fatalf("consumed %d connections for one attempt", initial-len(pool.conns))
	}
	for _, entry := range pool.conns {
		if !entry.lastUsed.Equal(old) {
			t.Fatal("acquired a replacement connection after the final attempt")
		}
	}
}

func TestFailoverCancellationDuringBackoff(t *testing.T) {
	client := newFailoverTestClient(t, 1, config.UsenetProvider{Host: "provider-a"}, config.UsenetProvider{Host: "provider-b"})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	calls := 0
	// No replacement connection should be checked out while the retry sleeps.
	initial := time.Now().Add(-time.Minute)
	for _, entry := range client.pools["provider-b"].conns {
		entry.lastUsed = initial
	}
	err := client.ExecuteWithFailover(ctx, func(*Connection) error {
		calls++
		return NewTimeoutError(errors.New("provider timeout"))
	})
	if !errors.Is(err, context.DeadlineExceeded) || calls != 1 {
		t.Fatalf("got %v after %d calls; want deadline after first attempt", err, calls)
	}
	pool := client.pools["provider-b"]
	for _, entry := range pool.conns {
		if !entry.lastUsed.Equal(initial) {
			t.Fatal("replacement connection was held during backoff")
		}
	}
}

func TestFailoverNonRetryableErrors(t *testing.T) {
	for _, failure := range []error{
		&Error{Type: ErrorTypeAuthentication}, &Error{Type: ErrorTypePermissionDenied},
		NewProtocolError(500, "invalid response"), errors.New("unknown failure"), context.Canceled,
	} {
		t.Run(failure.Error(), func(t *testing.T) {
			client := newFailoverTestClient(t, 1, config.UsenetProvider{Host: "provider-a"}, config.UsenetProvider{Host: "provider-b"})
			calls := 0
			err := client.ExecuteWithFailover(context.Background(), func(*Connection) error { calls++; return failure })
			if !errors.Is(err, failure) || calls != 1 {
				t.Fatalf("got %v after %d calls; want immediate %v", err, calls, failure)
			}
		})
	}
}
