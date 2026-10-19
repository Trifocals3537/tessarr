package manager

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/manager/link"
	"github.com/rs/zerolog"
)

func retryableStreamFailure() error {
	return StreamError{
		Err:       link.NewRetryableError(errors.New("provider unavailable"), "503"),
		Retryable: true,
	}
}

func TestProviderWeatherRequiresDistinctFiles(t *testing.T) {
	now := time.Unix(1_000, 0)
	weather := newStreamProviderWeather()
	weather.now = func() time.Time { return now }

	for range streamProviderFailureThreshold + 2 {
		result := weather.recordFailure("torbox", "same-file", "upstream_status")
		if result.NewlyDegraded {
			t.Fatal("repeated failures for one file degraded the provider")
		}
	}
	for index, file := range []string{"second-file", "third-file"} {
		result := weather.recordFailure("torbox", file, "upstream_status")
		if got, want := result.NewlyDegraded, index == 1; got != want {
			t.Fatalf("failure %q degraded = %v, want %v", file, got, want)
		}
	}
	if !weather.isDegraded("TORBOX") {
		t.Fatal("provider was not marked degraded after three distinct files")
	}
}

func TestProviderWeatherExpiresEvidenceAndReopensFailedTrial(t *testing.T) {
	now := time.Unix(2_000, 0)
	weather := newStreamProviderWeather()
	weather.now = func() time.Time { return now }

	weather.recordFailure("realdebrid", "old-file", "connection")
	now = now.Add(streamProviderFailureWindow + time.Second)
	for _, file := range []string{"new-one", "new-two"} {
		if weather.recordFailure("realdebrid", file, "connection").NewlyDegraded {
			t.Fatal("expired evidence contributed to a degradation")
		}
	}
	if result := weather.recordFailure("realdebrid", "new-three", "connection"); !result.NewlyDegraded {
		t.Fatal("three current files did not degrade provider")
	}

	now = now.Add(streamProviderCooldown + time.Second)
	if weather.isDegraded("realdebrid") {
		t.Fatal("provider remained deferred after its bounded cooldown")
	}
	if result := weather.recordFailure("realdebrid", "trial-file", "connection"); !result.NewlyDegraded {
		t.Fatal("failed post-cooldown trial did not immediately reopen provider")
	}
	if !weather.recordSuccess("realdebrid") || weather.isDegraded("realdebrid") {
		t.Fatal("successful trial did not clear degraded state")
	}
}

func TestProviderWeatherOnlyCountsProviderWideFailures(t *testing.T) {
	tests := []struct {
		name  string
		err   error
		class string
		count bool
	}{
		{name: "503", err: retryableStreamFailure(), class: "upstream_status", count: true},
		{name: "429", err: StreamError{Err: link.ErrorCodeToLinkError("429"), Retryable: true}, class: "throttled", count: true},
		{name: "local refresh cooldown", err: localRefreshCooldownError(), class: "local_cooldown"},
		{name: "wrapped local refresh cooldown", err: StreamError{Err: localRefreshCooldownError(), Retryable: true}, class: "local_cooldown"},
		{name: "account cooldown", err: link.NewLinkError(link.ErrNoActiveAccount, link.CategoryThrottled, "account_cooldown"), class: "throttled", count: true},
		{name: "account pressure", err: link.NewAccountError(link.ErrBandwidthExceeded, "bandwidth_exceeded"), class: "account_issue", count: true},
		{name: "connection", err: StreamError{Err: errors.New("connection reset by peer"), Retryable: true}, class: "connection", count: true},
		{name: "stale link", err: StreamError{Err: link.ErrorCodeToLinkError("404"), LinkError: true}, class: "refetchable"},
		{name: "range", err: StreamError{Err: link.ClassifyHTTPStatus(http.StatusRequestedRangeNotSatisfiable, nil)}, class: "permanent"},
		{name: "cancel", err: context.Canceled},
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "wrapped cancel", err: link.NewLinkError(context.Canceled, link.CategoryThrottled, "link_refresh_cooldown")},
		{name: "wrapped deadline", err: link.NewLinkError(context.DeadlineExceeded, link.CategoryThrottled, "link_refresh_cooldown")},
		{name: "internal", err: errors.New("bad state"), class: "internal"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			class, count := classifyStreamProviderFailure(test.err)
			if class != test.class || count != test.count {
				t.Fatalf("class/count = %q/%v, want %q/%v", class, count, test.class, test.count)
			}
		})
	}
}

func TestDegradedProviderMovesBehindHealthyPlacementButRemainsLastResort(t *testing.T) {
	manager := newStreamFailoverTestManager(nil, http.DefaultClient, "primary", "fallback")
	for _, file := range []string{"one", "two", "three"} {
		manager.recordStreamProviderFailure("primary", file, retryableStreamFailure())
	}

	candidates := manager.streamCandidates(streamFailoverEntry("primary", "fallback"), "video.mkv")
	providers := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		providers = append(providers, candidate.provider)
	}
	if !slices.Equal(providers, []string{"fallback", "primary"}) {
		t.Fatalf("candidate order = %v, want healthy then degraded", providers)
	}
	attempts := streamAttemptCandidates(candidates, "primary")
	if len(attempts) != 2 || attempts[1].provider != "primary" {
		t.Fatalf("attempts = %+v, degraded active provider must remain last resort", attempts)
	}
	stats := manager.StreamFailoverStats()
	if stats.ProviderDegradations != 1 || stats.ProviderDeferrals != 1 {
		t.Fatalf("weather stats = %+v, want one degradation and deferral", stats)
	}
}

func TestProviderWeatherAdmitsOnlyOneConcurrentHalfOpenProbe(t *testing.T) {
	now := time.Unix(3_000, 0)
	manager := newStreamFailoverTestManager(nil, http.DefaultClient, "primary", "fallback")
	manager.streamProviderWeather.now = func() time.Time { return now }
	for _, file := range []string{"one", "two", "three"} {
		manager.recordStreamProviderFailure("primary", file, retryableStreamFailure())
	}
	now = now.Add(streamProviderCooldown + time.Second)
	entry := streamFailoverEntry("primary", "fallback")

	first := manager.streamCandidates(entry, "video.mkv")
	if first[0].provider != "primary" {
		t.Fatalf("first post-cooldown candidates = %+v, want normal primary order", first)
	}
	if probe, allowed := manager.streamProviderWeather.beginAttempt("primary"); !probe || !allowed {
		t.Fatalf("first post-cooldown attempt = probe %v allowed %v, want true/true", probe, allowed)
	}
	second := manager.streamCandidates(entry, "video.mkv")
	if second[0].provider != "fallback" || second[1].provider != "primary" {
		t.Fatalf("concurrent candidates = %+v, want primary deferred behind fallback", second)
	}
	if probe, allowed := manager.streamProviderWeather.beginAttempt("primary"); probe || allowed {
		t.Fatalf("concurrent attempt = probe %v allowed %v, want false/false", probe, allowed)
	}

	manager.streamProviderWeather.releaseProbe("primary")
	third := manager.streamCandidates(entry, "video.mkv")
	if third[0].provider != "primary" {
		t.Fatalf("released probe candidates = %+v, want normal primary order", third)
	}
	if probe, allowed := manager.streamProviderWeather.beginAttempt("primary"); !probe || !allowed {
		t.Fatalf("released attempt = probe %v allowed %v, want true/true", probe, allowed)
	}
	manager.streamProviderWeather.releaseProbe("primary")
}

func TestConcurrentPlaybackDoesNotDuplicateHalfOpenProbe(t *testing.T) {
	var primaryRequests atomic.Int32
	var fallbackRequests atomic.Int32
	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/primary" {
			if primaryRequests.Add(1) == 1 {
				close(probeStarted)
			}
			<-releaseProbe
		} else {
			fallbackRequests.Add(1)
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
	now := time.Unix(4_000, 0)
	manager.streamProviderWeather.now = func() time.Time { return now }
	for _, file := range []string{"one", "two", "three"} {
		manager.recordStreamProviderFailure("primary", file, retryableStreamFailure())
	}
	now = now.Add(streamProviderCooldown + time.Second)
	entry := streamFailoverEntry("primary", "fallback")

	firstDone := make(chan error, 1)
	go func() {
		var output bytes.Buffer
		err := manager.Stream(context.Background(), entry, "video.mkv", 0, 3, &output, nil, "first")
		if err == nil && output.String() != "data" {
			err = errors.New("first stream returned wrong data")
		}
		firstDone <- err
	}()
	<-probeStarted

	var secondOutput bytes.Buffer
	if err := manager.Stream(context.Background(), entry, "video.mkv", 0, 3, &secondOutput, nil, "second"); err != nil {
		t.Fatalf("second Stream() error = %v", err)
	}
	if secondOutput.String() != "data" || primaryRequests.Load() != 1 || fallbackRequests.Load() != 1 {
		t.Fatalf("second output/primary/fallback = %q/%d/%d, want data/1/1",
			secondOutput.String(), primaryRequests.Load(), fallbackRequests.Load())
	}

	close(releaseProbe)
	if err := <-firstDone; err != nil {
		t.Fatalf("first Stream() error = %v", err)
	}
}

func TestProviderWeatherLogsOneWarningPerEpisodeWithoutFileIdentity(t *testing.T) {
	var output bytes.Buffer
	manager := newStreamFailoverTestManager(nil, http.DefaultClient, "primary", "fallback")
	manager.logger = zerolog.New(&output).Level(zerolog.DebugLevel)

	for _, file := range []string{"private-one", "private-two", "private-three", "private-four"} {
		manager.recordStreamProviderFailure("primary", file, retryableStreamFailure())
	}
	logs := output.String()
	if got := strings.Count(logs, `"level":"warn"`); got != 1 {
		t.Fatalf("warning count = %d, want 1; logs=%s", got, logs)
	}
	if strings.Contains(logs, "private-") {
		t.Fatalf("provider warning exposed file identity: %s", logs)
	}
	for _, want := range []string{`"event":"stream.provider_degraded"`, `"outcome":"cooldown"`} {
		if !strings.Contains(logs, want) {
			t.Fatalf("provider warning missing %s: %s", want, logs)
		}
	}
}
