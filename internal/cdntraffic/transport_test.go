package cdntraffic

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestTransportHoldsPermitUntilResponseBodyCloses(t *testing.T) {
	governor := testGovernor(1)
	var calls atomic.Int64
	base := roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader("body")),
		}, nil
	})
	client := &http.Client{Transport: NewTransport(base, governor)}
	ctx := WithIdentity(context.Background(), Identity{Provider: "debrid", AccountToken: "secret"})

	firstRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://cdn.example/one", nil)
	if err != nil {
		t.Fatal(err)
	}
	first, err := client.Do(firstRequest)
	if err != nil {
		t.Fatal(err)
	}

	secondResult := make(chan *http.Response, 1)
	secondError := make(chan error, 1)
	go func() {
		secondRequest, requestErr := http.NewRequestWithContext(ctx, http.MethodGet, "https://cdn.example/two", nil)
		if requestErr != nil {
			secondError <- requestErr
			return
		}
		response, requestErr := client.Do(secondRequest)
		if requestErr != nil {
			secondError <- requestErr
			return
		}
		secondResult <- response
	}()

	time.Sleep(20 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		_ = first.Body.Close()
		t.Fatalf("base transport calls before close = %d, want 1", got)
	}
	if err := first.Body.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-secondError:
		t.Fatal(err)
	case second := <-secondResult:
		if err := second.Body.Close(); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second request was not admitted after the first body closed")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("base transport calls = %d, want 2", got)
	}
}

func TestTransportReleasesPermitBetweenRedirectHops(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/final", http.StatusFound)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	t.Cleanup(server.Close)

	base, ok := server.Client().Transport.(*http.Transport)
	if !ok {
		t.Fatalf("server transport = %T, want *http.Transport", server.Client().Transport)
	}
	client := &http.Client{Transport: NewTransport(base.Clone(), testGovernor(1))}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ctx = WithIdentity(ctx, Identity{Provider: "debrid", AccountToken: "secret"})
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/redirect", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("redirect request deadlocked between governed hops: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "ok" {
		t.Fatalf("redirect body = %q, want ok", body)
	}
}

func TestTransportSeparatesTorBoxResolverAndCDNHops(t *testing.T) {
	governor := testGovernor(4)
	var resolverCalls atomic.Int64
	var cdnCalls atomic.Int64
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Hostname() {
		case "api.torbox.app":
			resolverCalls.Add(1)
			header := make(http.Header)
			header.Set("Location", "https://nexus-001.torbox.app/signed/file")
			return &http.Response{
				StatusCode: http.StatusFound,
				Header:     header,
				Body:       http.NoBody,
				Request:    req,
			}, nil
		case "nexus-001.torbox.app":
			cdnCalls.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader("media")),
				Request:    req,
			}, nil
		default:
			t.Fatalf("unexpected request host %q", req.URL.Hostname())
			return nil, nil
		}
	})
	client := &http.Client{Transport: NewTransport(base, governor)}
	ctx := WithIdentity(context.Background(), Identity{
		Provider: "torbox-primary", ProviderType: "torbox",
		AccountToken: "secret", LinkKey: "torbox://1/1",
	})
	ctx = WithPriority(ctx, PriorityInteractive)
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"https://api.torbox.app/v1/api/torrents/requestdl?token=secret",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if string(body) != "media" || resolverCalls.Load() != 1 || cdnCalls.Load() != 1 {
		t.Fatalf("body/resolver/CDN = %q/%d/%d", body, resolverCalls.Load(), cdnCalls.Load())
	}
	stats := governor.Snapshot()
	if stats.Active != 0 || len(stats.Providers) != 1 || stats.Providers[0].AdmittedInteractive != 2 {
		t.Fatalf("redirect traffic snapshot = %+v", stats)
	}
}
