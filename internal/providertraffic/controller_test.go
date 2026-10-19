package providertraffic

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testCapabilities(string) Capabilities {
	return Capabilities{
		APIBudget:                   RateBudget{Requests: 1000, Period: time.Second, Burst: 100},
		UncachedTorrentCreateBudget: RateBudget{Requests: 1, Period: 40 * time.Millisecond, Burst: 1},
	}
}

func TestControllerScopesEndpointBudgetWithoutBlockingGeneralAPI(t *testing.T) {
	controller := New(Options{Capabilities: testCapabilities})
	identity := Identity{ProviderType: "torbox", AccountToken: "secret"}
	if err := controller.Wait(context.Background(), identity, OperationCreateTorrentUncached); err != nil {
		t.Fatal(err)
	}

	blockedCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := controller.Wait(blockedCtx, identity, OperationCreateTorrentUncached); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second create error = %v, want deadline", err)
	}

	start := time.Now()
	if err := controller.Wait(context.Background(), identity, OperationAPI); err != nil {
		t.Fatal(err)
	}
	if wait := time.Since(start); wait > 15*time.Millisecond {
		t.Fatalf("general API was delayed by create budget for %s", wait)
	}
}

func TestControllerGeneralBudgetIsPerEndpoint(t *testing.T) {
	controller := New(Options{Capabilities: func(string) Capabilities {
		return Capabilities{
			APIBudget: RateBudget{Requests: 1, Period: 40 * time.Millisecond, Burst: 1},
		}
	}})
	identity := Identity{ProviderType: "torbox", AccountToken: "secret"}
	if err := controller.WaitEndpoint(context.Background(), identity, OperationAPI, "GET /first"); err != nil {
		t.Fatal(err)
	}

	blockedCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := controller.WaitEndpoint(blockedCtx, identity, OperationAPI, "GET /first"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("same endpoint error = %v, want deadline", err)
	}
	if err := controller.WaitEndpoint(context.Background(), identity, OperationAPI, "GET /second"); err != nil {
		t.Fatalf("independent endpoint inherited first endpoint's budget: %v", err)
	}
}

func TestControllerAccountBudgetIsSharedAcrossEndpoints(t *testing.T) {
	controller := New(Options{Capabilities: func(string) Capabilities {
		return Capabilities{
			AccountAPIBudget: RateBudget{Requests: 1, Period: 40 * time.Millisecond, Burst: 1},
		}
	}})
	first := Identity{ProviderType: "realdebrid", AccountToken: "first-secret"}
	second := Identity{ProviderType: "realdebrid", AccountToken: "second-secret"}
	if err := controller.WaitEndpoint(context.Background(), first, OperationAPI, "GET /first"); err != nil {
		t.Fatal(err)
	}

	blockedCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := controller.WaitEndpoint(blockedCtx, first, OperationAPI, "GET /second"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second endpoint error = %v, want shared-account deadline", err)
	}
	if err := controller.WaitEndpoint(context.Background(), second, OperationAPI, "GET /second"); err != nil {
		t.Fatalf("independent account inherited first account's budget: %v", err)
	}
}

func TestControllerBurstStillHonorsRollingWindowCeiling(t *testing.T) {
	controller := New(Options{Capabilities: func(string) Capabilities {
		return Capabilities{
			APIBudget: RateBudget{Requests: 3, Period: 40 * time.Millisecond, Burst: 3},
		}
	}})
	identity := Identity{ProviderType: "torbox", AccountToken: "secret"}
	for range 3 {
		if err := controller.WaitEndpoint(context.Background(), identity, OperationAPI, "GET /endpoint"); err != nil {
			t.Fatal(err)
		}
	}

	blockedCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := controller.WaitEndpoint(blockedCtx, identity, OperationAPI, "GET /endpoint"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fourth request error = %v, want rolling-window deadline", err)
	}

	time.Sleep(35 * time.Millisecond)
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err := controller.WaitEndpoint(ctx, identity, OperationAPI, "GET /endpoint"); err != nil {
		t.Fatalf("rolling window did not reopen: %v", err)
	}
}

func TestControllerConcurrentBurstNeverExceedsBudget(t *testing.T) {
	controller := New(Options{Capabilities: func(string) Capabilities {
		return Capabilities{
			APIBudget: RateBudget{Requests: 10, Period: 5 * time.Second, Burst: 10},
		}
	}})
	identity := Identity{ProviderType: "torbox", AccountToken: "secret"}
	start := make(chan struct{})
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if controller.WaitEndpoint(ctx, identity, OperationAPI, "GET /endpoint") == nil {
				admitted.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := admitted.Load(); got != 10 {
		t.Fatalf("concurrent admissions = %d, want exact burst of 10", got)
	}
}

func TestControllerCancellationDoesNotConsumeFutureToken(t *testing.T) {
	controller := New(Options{Capabilities: testCapabilities})
	identity := Identity{ProviderType: "torbox", AccountToken: "secret"}
	if err := controller.Wait(context.Background(), identity, OperationCreateTorrentUncached); err != nil {
		t.Fatal(err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := controller.Wait(canceled, identity, OperationCreateTorrentUncached); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait error = %v", err)
	}

	time.Sleep(45 * time.Millisecond)
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err := controller.Wait(ctx, identity, OperationCreateTorrentUncached); err != nil {
		t.Fatalf("token was lost after cancellation: %v", err)
	}
}

func TestControllerParksOneAccountAfter429(t *testing.T) {
	controller := New(Options{
		Capabilities: testCapabilities,
		// Keep the synthetic backoff comfortably beyond loaded Windows runner
		// scheduling delays. The test cancels its wait, so this does not make the
		// suite sleep for the full duration.
		DefaultBackoff: 2 * time.Second,
		MaximumBackoff: 3 * time.Second,
	})
	first := Identity{ProviderType: "torbox", AccountToken: "first-secret"}
	second := Identity{ProviderType: "torbox", AccountToken: "second-secret"}
	controller.Observe(first, OperationAPI, http.StatusTooManyRequests, nil)

	blockedCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := controller.Wait(blockedCtx, first, OperationAPI); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked account error = %v, want deadline", err)
	}
	if err := controller.Wait(context.Background(), second, OperationAPI); err != nil {
		t.Fatalf("independent account was blocked: %v", err)
	}
}

func TestControllerNeverStoresPlainAccountTokenInKey(t *testing.T) {
	controller := New(Options{Capabilities: testCapabilities})
	secret := "never-store-this-token"
	if err := controller.Wait(
		context.Background(),
		Identity{ProviderType: "torbox", AccountToken: secret},
		OperationAPI,
	); err != nil {
		t.Fatal(err)
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	for key := range controller.states {
		if strings.Contains(key, secret) {
			t.Fatalf("controller key exposed account token: %q", key)
		}
	}
}

func TestControllerBoundsIdleAccountStates(t *testing.T) {
	controller := New(Options{Capabilities: testCapabilities})
	for i := range maxAccountStates + 25 {
		if err := controller.Wait(
			context.Background(),
			Identity{ProviderType: "torbox", AccountToken: fmt.Sprintf("token-%d", i)},
			OperationAPI,
		); err != nil {
			t.Fatal(err)
		}
	}
	controller.mu.Lock()
	states := len(controller.states)
	controller.mu.Unlock()
	if states > maxAccountStates {
		t.Fatalf("controller states = %d, want <= %d", states, maxAccountStates)
	}
}
