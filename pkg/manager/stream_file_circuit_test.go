package manager

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestStreamFileCircuitUsesAdaptiveCooldownAndOneHalfOpenProbe(t *testing.T) {
	now := time.Unix(10_000, 0)
	breaker := newStreamFileCircuitBreaker()
	breaker.now = func() time.Time { return now }
	key := "hash\x00video.mkv\x00primary"

	first := breaker.recordFailure(key, "upstream_io")
	if first.Failures != 1 || first.Cooldown != 30*time.Second || !first.NewlyOpen {
		t.Fatalf("first failure = %+v", first)
	}
	duplicate := breaker.recordFailure(key, "connection")
	if duplicate.Failures != 1 || duplicate.Cooldown != 30*time.Second || duplicate.NewlyOpen {
		t.Fatalf("concurrent failure was not coalesced: %+v", duplicate)
	}
	if probe, allowed, retryAfter := breaker.beginAttempt(key); probe || allowed || retryAfter != 30*time.Second {
		t.Fatalf("cooldown attempt = %v/%v/%s, want false/false/30s", probe, allowed, retryAfter)
	}

	now = now.Add(31 * time.Second)
	if probe, allowed, _ := breaker.beginAttempt(key); !probe || !allowed {
		t.Fatalf("first half-open attempt = %v/%v, want true/true", probe, allowed)
	}
	if probe, allowed, _ := breaker.beginAttempt(key); probe || allowed {
		t.Fatalf("concurrent half-open attempt = %v/%v, want false/false", probe, allowed)
	}
	second := breaker.recordFailure(key, "connection")
	if second.Failures != 2 || second.Cooldown != time.Minute {
		t.Fatalf("failed probe = %+v, want second failure and 1m cooldown", second)
	}

	now = now.Add(time.Minute + time.Second)
	if probe, allowed, _ := breaker.beginAttempt(key); !probe || !allowed {
		t.Fatalf("second half-open attempt = %v/%v, want true/true", probe, allowed)
	}
	if !breaker.recordSuccess(key) {
		t.Fatal("successful half-open probe did not clear circuit")
	}
	if probe, allowed, retryAfter := breaker.beginAttempt(key); probe || !allowed || retryAfter != 0 {
		t.Fatalf("post-recovery attempt = %v/%v/%s, want false/true/0", probe, allowed, retryAfter)
	}
}

func TestStreamFileCircuitIsolatesProviderAndFile(t *testing.T) {
	entry := streamFailoverEntry("primary", "fallback")
	manager := newStreamFailoverTestManager(nil, http.DefaultClient, "primary", "fallback")
	manager.recordStreamFileCircuitFailure(entry, "video.mkv", "primary", retryableStreamFailure())

	candidates := manager.streamCandidates(entry, "video.mkv")
	providers := []string{candidates[0].provider, candidates[1].provider}
	if !slices.Equal(providers, []string{"fallback", "primary"}) {
		t.Fatalf("failed file candidates = %v, want healthy fallback first", providers)
	}
	if got := manager.streamCandidates(entry, "other.mkv"); got[0].provider != "primary" {
		t.Fatalf("unrelated file inherited circuit: %+v", got)
	}
	if _, _, allowed, _ := manager.beginStreamFileCircuitAttempt(entry, "video.mkv", "fallback"); !allowed {
		t.Fatal("healthy provider inherited primary circuit")
	}
}

func TestStreamFileCircuitIgnoresCancellationAndLocalCooldown(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "canceled", err: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "local cooldown", err: localRefreshCooldownError()},
		{name: "internal", err: errors.New("local invariant")},
	} {
		t.Run(test.name, func(t *testing.T) {
			if class, eligible := classifyStreamFileCircuitFailure(test.err); eligible {
				t.Fatalf("classification = %q/%v, want ineligible", class, eligible)
			}
		})
	}
	if class, eligible := classifyStreamFileCircuitFailure(io.ErrUnexpectedEOF); !eligible || class == "" {
		t.Fatalf("unexpected EOF classification = %q/%v, want eligible", class, eligible)
	}
}

func TestStreamFileCircuitStateIsBounded(t *testing.T) {
	breaker := newStreamFileCircuitBreaker()
	breaker.capacity = 2
	for _, key := range []string{"first", "second", "third"} {
		breaker.recordFailure(key, "connection")
	}
	if got := len(breaker.states); got != 2 {
		t.Fatalf("circuit state count = %d, want bounded capacity 2", got)
	}
}

func TestStreamFileCircuitSkipsKnownBadProviderAfterFallback(t *testing.T) {
	var primaryHealthy atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/primary" && !primaryHealthy.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Content-Range", "bytes 0-3/4")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("data"))
	}))
	defer server.Close()

	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary":  {DownloadLink: server.URL + "/primary"},
		"fallback": {DownloadLink: server.URL + "/fallback"},
	}}
	manager := newStreamFailoverTestManager(links, server.Client(), "primary", "fallback")
	entry := streamFailoverEntry("primary", "fallback")

	stream := func() {
		t.Helper()
		var output bytes.Buffer
		if err := manager.Stream(context.Background(), entry, "video.mkv", 0, 3, &output, nil, "test"); err != nil {
			t.Fatalf("Stream() error = %v", err)
		}
		if output.String() != "data" {
			t.Fatalf("stream output = %q, want data", output.String())
		}
	}
	stream()
	manager.streamProviderPreferences.Clear()
	stream()
	if got := links.callOrder(); !slices.Equal(got, []string{"primary", "fallback", "fallback"}) {
		t.Fatalf("provider calls = %v, want failed primary once then fallback", got)
	}
	stats := manager.StreamFailoverStats()
	if stats.FileCircuitOpens != 1 || stats.FileCircuitDeferrals != 1 {
		t.Fatalf("file circuit stats = %+v, want one open and one deferral", stats)
	}

	primaryHealthy.Store(true)
	manager.streamProviderPreferences.Clear()
	now := time.Now().Add(streamFileCircuitBaseCooldown + time.Second)
	manager.streamFileCircuits.now = func() time.Time { return now }
	stream()
	if got := links.callOrder(); !slices.Equal(got, []string{"primary", "fallback", "fallback", "primary"}) {
		t.Fatalf("post-cooldown provider calls = %v, want one primary recovery probe", got)
	}
	if stats := manager.StreamFailoverStats(); stats.FileCircuitRecoveries != 1 {
		t.Fatalf("file circuit recovery stats = %+v", stats)
	}
}

func TestStreamFileCircuitAllowsSameRequestActiveRecovery(t *testing.T) {
	var primaryRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/primary" && primaryRequests.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if r.URL.Path == "/fallback" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Content-Range", "bytes 0-3/4")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("data"))
	}))
	defer server.Close()
	links := &failoverLinkService{links: map[string]debridTypes.DownloadLink{
		"primary":  {DownloadLink: server.URL + "/primary"},
		"fallback": {DownloadLink: server.URL + "/fallback"},
	}}
	manager := newStreamFailoverTestManager(links, server.Client(), "primary", "fallback")
	entry := streamFailoverEntry("primary", "fallback")
	var output bytes.Buffer
	if err := manager.Stream(context.Background(), entry, "video.mkv", 0, 3, &output, nil, "test"); err != nil {
		t.Fatal(err)
	}
	if output.String() != "data" {
		t.Fatalf("output = %q, want data", output.String())
	}
	if got := links.callOrder(); !slices.Equal(got, []string{"primary", "fallback", "primary"}) {
		t.Fatalf("provider calls = %v, want active recovery preserved", got)
	}
}
