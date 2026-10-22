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
)

func TestMutationAttemptDoesNotRetryAmbiguousResponsesOrTransportErrors(t *testing.T) {
	for _, status := range []int{0, http.StatusTooManyRequests, http.StatusBadGateway} {
		client := New(WithMaxRetries(3), WithRetryableStatus(http.StatusTooManyRequests, http.StatusBadGateway))
		var calls atomic.Int32
		client.httpClient.Transport = retryRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls.Add(1)
			if status == 0 {
				return nil, io.ErrUnexpectedEOF
			}
			return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: req}, nil
		})
		req, err := http.NewRequestWithContext(WithMutationAttempt(context.Background()), http.MethodPost, "https://example.invalid/create?api_key=fixture-secret", strings.NewReader("input"))
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(req)
		if response != nil {
			_ = response.Body.Close()
		}
		if err != nil && strings.Contains(err.Error(), "fixture-secret") {
			t.Fatal("mutation failure exposed the provider URL token")
		}
		if calls.Load() != 1 || (status == 0 && !errors.Is(err, io.ErrUnexpectedEOF)) || (status != 0 && (err != nil || response.StatusCode != status)) {
			t.Fatalf("mutation retried or hid response: status=%d calls=%d err=%v", status, calls.Load(), err)
		}
	}
}

func TestMutationAttemptDoesNotFollowRedirectsOrChangeSharedClient(t *testing.T) {
	var targetCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/create" {
			http.Redirect(w, r, "/target", http.StatusTemporaryRedirect)
			return
		}
		targetCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	client := New()
	for _, mutation := range []bool{true, false} {
		ctx := context.Background()
		if mutation {
			ctx = WithMutationAttempt(ctx)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/create", strings.NewReader("input"))
		if err != nil {
			t.Fatal(err)
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if mutation && (res.StatusCode != http.StatusTemporaryRedirect || targetCalls.Load() != 0) {
			t.Fatal("mutation redirect replayed")
		}
		if !mutation && (res.StatusCode != http.StatusNoContent || targetCalls.Load() != 1) {
			t.Fatal("mutation changed shared client behavior")
		}
	}
}
