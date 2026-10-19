package request

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestExplicitInvalidProxyFailsClosed(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	tests := []struct {
		name     string
		proxyURL string
	}{
		{name: "malformed", proxyURL: "://bad-proxy"},
		{name: "missing host", proxyURL: "http://user:secret@"},
		{name: "unsupported scheme", proxyURL: "ftp://proxy.example:21"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := New(WithMaxRetries(0), WithProxy(tt.proxyURL))
			response, err := client.Get(server.URL)
			if response != nil {
				_ = response.Body.Close()
			}
			if !errors.Is(err, ErrInvalidProxy) {
				t.Fatalf("Get error = %v, want %v", err, ErrInvalidProxy)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("proxy configuration error exposed credentials: %v", err)
			}
		})
	}
	if got := hits.Load(); got != 0 {
		t.Fatalf("provider received %d direct requests despite invalid proxy configuration", got)
	}
}

func TestExplicitProxyAppliesToCustomTransportInAnyOptionOrder(t *testing.T) {
	const proxyAddress = "http://proxy.example:8080"
	tests := []struct {
		name    string
		options func(*http.Transport) []ClientOption
	}{
		{
			name: "transport then proxy",
			options: func(transport *http.Transport) []ClientOption {
				return []ClientOption{WithTransport(transport), WithProxy(proxyAddress)}
			},
		},
		{
			name: "proxy then transport",
			options: func(transport *http.Transport) []ClientOption {
				return []ClientOption{WithProxy(proxyAddress), WithTransport(transport)}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			callerTransport := &http.Transport{MaxConnsPerHost: 7}
			client := New(tt.options(callerTransport)...)
			if client.configurationErr != nil {
				t.Fatalf("New configuration error: %v", client.configurationErr)
			}
			transport := client.httpClient.Transport.(*http.Transport)
			request, err := http.NewRequest(http.MethodGet, "https://provider.example/path", nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err := transport.Proxy(request)
			if err != nil {
				t.Fatalf("Proxy: %v", err)
			}
			if got == nil || got.String() != proxyAddress {
				t.Fatalf("Proxy = %v, want %s", got, proxyAddress)
			}
			if callerTransport.Proxy != nil {
				t.Fatal("WithTransport mutated the caller-owned transport")
			}
			if transport.MaxConnsPerHost != 7 {
				t.Fatalf("custom MaxConnsPerHost = %d, want 7", transport.MaxConnsPerHost)
			}
		})
	}
}

func TestSOCKSProxyDialHonorsRequestContext(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	release := make(chan struct{})
	done := make(chan struct{})
	accepted := make(chan struct{}, 1)
	go func() {
		defer close(done)
		connection, err := listener.Accept()
		if err != nil {
			return
		}
		accepted <- struct{}{}
		<-release
		_ = connection.Close()
	}()
	t.Cleanup(func() {
		close(release)
		_ = listener.Close()
		<-done
	})

	client := New(
		WithMaxRetries(0),
		WithTimeout(5*time.Second),
		WithProxy("socks5://"+listener.Addr().String()),
	)
	requestContext, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	request, err := http.NewRequestWithContext(
		requestContext,
		http.MethodGet,
		"http://provider.invalid/resource",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	response, err := client.Do(request)
	if response != nil {
		_ = response.Body.Close()
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Do error = %v, want %v", err, context.DeadlineExceeded)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("canceled SOCKS handshake returned after %s, want under 2s", elapsed)
	}
	select {
	case <-accepted:
	default:
		t.Fatal("request never connected to the configured SOCKS proxy")
	}
}

func TestDefaultTransportCapsActiveConnections(t *testing.T) {
	client := New()
	transport := client.httpClient.Transport.(*http.Transport)
	if transport.MaxConnsPerHost != defaultMaxConnsPerHost {
		t.Fatalf(
			"MaxConnsPerHost = %d, want %d",
			transport.MaxConnsPerHost,
			defaultMaxConnsPerHost,
		)
	}
}
