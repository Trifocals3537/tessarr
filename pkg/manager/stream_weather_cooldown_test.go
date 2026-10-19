package manager

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/manager/link"
)

func localRefreshCooldownError() *link.Error {
	err := link.NewLinkError(errors.New("download link refresh deferred"), link.CategoryThrottled, "link_refresh_cooldown")
	err.RetryAfter = 30 * time.Second
	return err
}

func TestLocalRefreshCooldownDoesNotCountAsProviderEvidence(t *testing.T) {
	manager := newStreamFailoverTestManager(nil, http.DefaultClient, "primary", "fallback")
	for _, file := range []string{"real-one", "real-two"} {
		manager.recordStreamProviderFailure("primary", file, retryableStreamFailure())
	}
	var workers sync.WaitGroup
	for index := range 16 {
		workers.Go(func() {
			result := manager.recordStreamProviderFailure("primary", fmt.Sprintf("local-%d", index), localRefreshCooldownError())
			if result.Class != "local_cooldown" || result.NewlyDegraded || result.DistinctFiles != 0 || result.Cooldown != 0 {
				t.Errorf("local cooldown changed provider evidence: %+v", result)
			}
		})
	}
	workers.Wait()
	if manager.streamProviderWeather.isDegraded("primary") || manager.StreamFailoverStats().ProviderDegradations != 0 {
		t.Fatal("local cooldowns completed a provider-wide failure threshold")
	}
	candidates := manager.streamCandidates(streamFailoverEntry("primary", "fallback"), "video.mkv")
	if len(candidates) != 2 || candidates[0].provider != "primary" || manager.StreamFailoverStats().ProviderDeferrals != 0 {
		t.Fatalf("local cooldowns changed the configured candidate order: %+v", candidates)
	}
	result := manager.recordStreamProviderFailure("primary", "real-three", retryableStreamFailure())
	if !result.NewlyDegraded || result.DistinctFiles != 3 {
		t.Fatalf("local cooldowns erased genuine provider evidence: %+v", result)
	}
}

func TestLocalRefreshCooldownDoesNotReopenProviderProbe(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "429", err: link.ClassifyHTTPStatus(http.StatusTooManyRequests, nil)},
		{name: "503", err: link.ClassifyHTTPStatus(http.StatusServiceUnavailable, nil)},
		{name: "account pressure", err: link.ErrorCodeToLinkError("bandwidth_exceeded")},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Unix(5_000, 0)
			manager := newStreamFailoverTestManager(nil, http.DefaultClient, "primary", "fallback")
			weather := manager.streamProviderWeather
			weather.now = func() time.Time { return now }
			for _, file := range []string{"one", "two", "three"} {
				manager.recordStreamProviderFailure("primary", file, test.err)
			}
			if !weather.isDegraded("primary") {
				t.Fatal("genuine provider failures did not degrade provider")
			}
			now = now.Add(streamProviderCooldown + time.Second)
			if probe, allowed := weather.beginAttempt("primary"); !probe || !allowed {
				t.Fatalf("recovery probe = %v/%v, want true/true", probe, allowed)
			}
			result := manager.recordStreamProviderFailure("primary", "local-trial", localRefreshCooldownError())
			weather.releaseProbe("primary")
			if result.NewlyDegraded || weather.isDegraded("primary") || manager.StreamFailoverStats().ProviderDegradations != 1 {
				t.Fatalf("local cooldown reopened provider degradation: %+v", result)
			}
			if probe, allowed := weather.beginAttempt("primary"); !probe || !allowed {
				t.Fatalf("local cooldown prevented a subsequent probe: %v/%v", probe, allowed)
			}
			weather.releaseProbe("primary")
			if result := manager.recordStreamProviderFailure("primary", "real-trial", test.err); !result.NewlyDegraded {
				t.Fatal("genuine failed trial no longer reopens provider degradation")
			}
		})
	}
}

func TestLocalRefreshCooldownPreservesUnrelatedPlayback(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "4")
		w.Header().Set("Content-Range", "bytes 0-3/4")
		w.WriteHeader(http.StatusPartialContent)
		if r.URL.Path == "/primary" {
			_, _ = w.Write([]byte("main"))
		} else {
			_, _ = w.Write([]byte("back"))
		}
	}))
	defer server.Close()
	links := &failoverLinkService{
		links: map[string]debridTypes.DownloadLink{
			"primary":  {DownloadLink: server.URL + "/primary"},
			"fallback": {DownloadLink: server.URL + "/fallback"},
		},
		errs: map[string]error{"primary": localRefreshCooldownError()},
	}
	manager := newStreamFailoverTestManager(links, server.Client(), "primary", "fallback")
	for index := range streamProviderFailureThreshold {
		entry := streamFailoverEntry("primary", "fallback")
		entry.InfoHash = fmt.Sprintf("cooling-entry-%d", index)
		var output bytes.Buffer
		if err := manager.Stream(context.Background(), entry, "video.mkv", 0, 3, &output, nil, "test"); err != nil {
			t.Fatalf("fallback during file cooldown: %v", err)
		}
		if output.String() != "back" || entry.ActiveProvider != "primary" {
			t.Fatalf("cooldown fallback changed bytes or active provider: %q/%s", output.String(), entry.ActiveProvider)
		}
	}
	links.mu.Lock()
	delete(links.errs, "primary")
	links.mu.Unlock()
	entry := streamFailoverEntry("primary", "fallback")
	entry.InfoHash = "unrelated-healthy-entry"
	var output bytes.Buffer
	if err := manager.Stream(context.Background(), entry, "video.mkv", 0, 3, &output, nil, "test"); err != nil {
		t.Fatalf("unrelated playback: %v", err)
	}
	if output.String() != "main" {
		t.Fatalf("unrelated playback was diverted by local file cooldowns: %q", output.String())
	}
	if stats := manager.StreamFailoverStats(); stats.ProviderDegradations != 0 || stats.ProviderDeferrals != 0 {
		t.Fatalf("local file cooldowns affected provider health: %+v", stats)
	}
}
