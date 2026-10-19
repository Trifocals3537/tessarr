package request

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type retryRoundTripFunc func(*http.Request) (*http.Response, error)

func (f retryRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func disableRetryDelay(client *Client) {
	client.client.Backoff = func(
		time.Duration,
		time.Duration,
		int,
		*http.Response,
	) time.Duration {
		return 0
	}
}

func TestRetryPolicyUsesConfiguredHTTPStatuses(t *testing.T) {
	for _, test := range []struct {
		name       string
		status     int
		configured []int
		wantCalls  int32
		wantStatus int
	}{
		{
			name:       "configured rate limit retries",
			status:     http.StatusTooManyRequests,
			configured: []int{http.StatusTooManyRequests},
			wantCalls:  2,
			wantStatus: http.StatusOK,
		},
		{
			name:       "configured server error retries",
			status:     http.StatusBadGateway,
			configured: []int{http.StatusBadGateway},
			wantCalls:  2,
			wantStatus: http.StatusOK,
		},
		{
			name:       "unconfigured server error is preserved",
			status:     http.StatusBadGateway,
			configured: []int{http.StatusTooManyRequests},
			wantCalls:  1,
			wantStatus: http.StatusBadGateway,
		},
		{
			name:       "malformed request is permanent",
			status:     http.StatusBadRequest,
			configured: []int{http.StatusTooManyRequests},
			wantCalls:  1,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "authentication failure is permanent",
			status:     http.StatusUnauthorized,
			configured: []int{http.StatusTooManyRequests},
			wantCalls:  1,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "authorization failure is permanent",
			status:     http.StatusForbidden,
			configured: []int{http.StatusTooManyRequests},
			wantCalls:  1,
			wantStatus: http.StatusForbidden,
		},
		{
			name:       "missing provider entry is permanent",
			status:     http.StatusNotFound,
			configured: []int{http.StatusTooManyRequests},
			wantCalls:  1,
			wantStatus: http.StatusNotFound,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if calls.Add(1) == 1 {
					w.WriteHeader(test.status)
					_, _ = io.WriteString(w, "provider response")
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, "recovered")
			}))
			defer server.Close()

			client := New(
				WithMaxRetries(1),
				WithRetryableStatus(test.configured...),
			)
			disableRetryDelay(client)
			request, err := http.NewRequestWithContext(
				t.Context(),
				http.MethodGet,
				server.URL,
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}

			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if got := calls.Load(); got != test.wantCalls {
				t.Fatalf("calls = %d, want %d", got, test.wantCalls)
			}
			if response.StatusCode != test.wantStatus {
				t.Fatalf("status = %d, want %d", response.StatusCode, test.wantStatus)
			}
			if test.wantCalls == 1 && string(body) != "provider response" {
				t.Fatalf("provider response body = %q", body)
			}
		})
	}
}

func TestRetryPolicyRetriesTransportFailure(t *testing.T) {
	var calls atomic.Int32
	client := New(
		WithMaxRetries(1),
		WithRetryableStatus(),
	)
	disableRetryDelay(client)
	client.httpClient.Transport = retryRoundTripFunc(func(*http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("temporary transport failure")
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("ok")),
		}, nil
	})

	request, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		"https://provider.example.test/resource",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if got := calls.Load(); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}
}

func TestRetryPolicyStopsOnContextCancellation(t *testing.T) {
	var calls atomic.Int32
	client := New(WithMaxRetries(3))
	disableRetryDelay(client)
	client.httpClient.Transport = retryRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		calls.Add(1)
		<-request.Context().Done()
		return nil, request.Context().Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"https://provider.example.test/resource",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Do(request)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context cancellation", err)
	}
	if got := calls.Load(); got > 1 {
		t.Fatalf("cancelled request attempts = %d, want at most 1", got)
	}
}

func TestSingleAttemptReturnsConfiguredRetryStatus(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	client := New(
		WithSingleAttempt(),
		WithRetryableStatus(http.StatusTooManyRequests),
	)
	request, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		server.URL,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusTooManyRequests || calls.Load() != 1 {
		t.Fatalf("status = %d, calls = %d", response.StatusCode, calls.Load())
	}
}

func TestConfiguredRetryExhaustionIsBounded(t *testing.T) {
	const secret = "provider-secret-diagnostic"
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, secret)
	}))
	defer server.Close()

	client := New(
		WithMaxRetries(1),
		WithRetryableStatus(http.StatusServiceUnavailable),
	)
	disableRetryDelay(client)
	request, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		server.URL,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Do(request)
	if err == nil {
		t.Fatal("retry exhaustion unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("retry exhaustion exposed provider response body: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("calls = %d, want 2", got)
	}
}

func TestRetryAfterBackoffIsBounded(t *testing.T) {
	futureHTTPDate := time.Now().Add(2 * time.Hour).UTC().Format(http.TimeFormat)
	for _, test := range []struct {
		name       string
		status     int
		retryAfter string
		want       time.Duration
	}{
		{
			name:       "rate limit uses requested delay",
			status:     http.StatusTooManyRequests,
			retryAfter: "3",
			want:       3 * time.Second,
		},
		{
			name:       "rate limit delay is capped",
			status:     http.StatusTooManyRequests,
			retryAfter: "120",
			want:       30 * time.Second,
		},
		{
			name:       "service unavailable delay is capped",
			status:     http.StatusServiceUnavailable,
			retryAfter: "120",
			want:       30 * time.Second,
		},
		{
			name:       "HTTP date delay is capped",
			status:     http.StatusTooManyRequests,
			retryAfter: futureHTTPDate,
			want:       30 * time.Second,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := &http.Response{
				StatusCode: test.status,
				Header: http.Header{
					"Retry-After": []string{test.retryAfter},
				},
			}
			if got := retryAfterBackoff(
				time.Second,
				30*time.Second,
				0,
				response,
			); got != test.want {
				t.Fatalf("backoff = %s, want %s", got, test.want)
			}
		})
	}
}
