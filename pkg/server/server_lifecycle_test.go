package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestServeHTTPStopsAfterContextCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- serveHTTP(ctx, srv, listener, time.Second)
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + listener.Addr().String())
	if err != nil {
		cancel()
		t.Fatalf("HTTP request failed: %v", err)
	}
	_ = resp.Body.Close()

	cancel()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("serveHTTP() error = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serveHTTP() did not stop after context cancellation")
	}
}

func TestServeHTTPReportsListenerFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	srv := &http.Server{Handler: http.NotFoundHandler()}
	err = serveHTTP(context.Background(), srv, listener, time.Second)
	if err == nil {
		t.Fatal("serveHTTP() error = nil, want listener error")
	}
	if errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("serveHTTP() error = %v, want underlying listener failure", err)
	}
}

func TestServeHTTPForcesCloseAfterShutdownTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	srv := &http.Server{
		Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			close(requestStarted)
			<-releaseRequest
		}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- serveHTTP(ctx, srv, listener, 50*time.Millisecond)
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	requestDone := make(chan error, 1)
	go func() {
		resp, err := client.Get("http://" + listener.Addr().String())
		if resp != nil {
			_ = resp.Body.Close()
		}
		requestDone <- err
	}()

	select {
	case <-requestStarted:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("request did not reach the test server")
	}

	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("serveHTTP() error = %v, want shutdown deadline", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serveHTTP() did not force-close the stuck connection")
	}

	close(releaseRequest)
	select {
	case <-requestDone:
	case <-time.After(2 * time.Second):
		t.Fatal("test request did not finish after forced close")
	}
}
