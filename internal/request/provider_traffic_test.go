package request

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/providertraffic"
)

type providerRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn providerRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func providerTestController(budget providertraffic.RateBudget) *providertraffic.Controller {
	return providertraffic.New(providertraffic.Options{
		Capabilities: func(string) providertraffic.Capabilities {
			return providertraffic.Capabilities{APIBudget: budget}
		},
		DefaultBackoff: 35 * time.Millisecond,
		MaximumBackoff: time.Second,
	})
}

func providerTestAccountController(budget providertraffic.RateBudget) *providertraffic.Controller {
	return providertraffic.New(providertraffic.Options{
		Capabilities: func(string) providertraffic.Capabilities {
			return providertraffic.Capabilities{AccountAPIBudget: budget}
		},
	})
}

func TestProviderTrafficTransportBudgetsEveryPhysicalAttempt(t *testing.T) {
	controller := providerTestController(providertraffic.RateBudget{
		Requests: 1, Period: 35 * time.Millisecond, Burst: 1,
	})
	var calls atomic.Int64
	transport := &providerTrafficTransport{
		base: providerRoundTripFunc(func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("ok")),
			}, nil
		}),
		controller: controller,
		identity: providertraffic.Identity{
			ProviderType: "test-provider", AccountToken: "secret",
		},
	}
	request, err := http.NewRequest(http.MethodGet, "https://api.example.test/resource", nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = first.Body.Close()

	start := time.Now()
	second, err := transport.RoundTrip(request.Clone(context.Background()))
	if err != nil {
		t.Fatal(err)
	}
	_ = second.Body.Close()
	if wait := time.Since(start); wait < 25*time.Millisecond {
		t.Fatalf("second physical attempt waited %s, want provider pacing", wait)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("base calls = %d, want 2", got)
	}
}

func TestProviderTrafficTransportSharesAccountBudgetAcrossClients(t *testing.T) {
	controller := providerTestAccountController(providertraffic.RateBudget{
		Requests: 1, Period: 40 * time.Millisecond, Burst: 1,
	})
	base := providerRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       http.NoBody,
		}, nil
	})
	identity := providertraffic.Identity{ProviderType: "test-provider", AccountToken: "shared-secret"}
	mainClient := &providerTrafficTransport{base: base, controller: controller, identity: identity}
	downloadClient := &providerTrafficTransport{base: base, controller: controller, identity: identity}

	mainRequest, err := http.NewRequest(http.MethodGet, "https://api.example.test/main", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mainClient.RoundTrip(mainRequest); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	downloadRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.example.test/download", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := downloadClient.RoundTrip(downloadRequest); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second client error = %v, want shared-account deadline", err)
	}
}

func TestProviderTrafficTransportShares429Backoff(t *testing.T) {
	controller := providerTestController(providertraffic.RateBudget{
		Requests: 1000, Period: time.Second, Burst: 100,
	})
	var calls atomic.Int64
	base := providerRoundTripFunc(func(*http.Request) (*http.Response, error) {
		call := calls.Add(1)
		status := http.StatusOK
		if call == 1 {
			status = http.StatusTooManyRequests
		}
		return &http.Response{
			StatusCode: status,
			Header:     make(http.Header),
			Body:       http.NoBody,
		}, nil
	})
	identity := providertraffic.Identity{ProviderType: "test-provider", AccountToken: "secret"}
	transport := &providerTrafficTransport{base: base, controller: controller, identity: identity}
	request, err := http.NewRequest(http.MethodGet, "https://api.example.test/resource", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transport.RoundTrip(request); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = transport.RoundTrip(request.Clone(ctx))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("backed-off request error = %v, want deadline", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("backed-off request reached base transport; calls = %d", got)
	}

	other := &providerTrafficTransport{
		base: base, controller: controller,
		identity: providertraffic.Identity{ProviderType: "test-provider", AccountToken: "other-secret"},
	}
	if _, err := other.RoundTrip(request.Clone(context.Background())); err != nil {
		t.Fatalf("other account inherited backoff: %v", err)
	}
}
