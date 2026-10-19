package request

import (
	"context"
	"crypto/tls"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
)

func TestMain(m *testing.M) {
	configDir, err := os.MkdirTemp("", "tessarr-request-test-")
	if err != nil {
		panic(err)
	}
	config.SetConfigPath(configDir)

	code := m.Run()

	config.Reset()
	config.SetConfigPath("")
	_ = os.RemoveAll(configDir)
	os.Exit(code)
}

func quietTLSServer(t *testing.T) *httptest.Server {
	t.Helper()

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

func TestClientRejectsUntrustedTLSByDefault(t *testing.T) {
	server := quietTLSServer(t)
	client := New(WithMaxRetries(0), WithTimeout(2*time.Second))

	response, err := client.Get(server.URL)
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil {
		t.Fatal("request to a server with an untrusted certificate succeeded")
	}
}

func TestWithTransportCannotDisableTLSVerification(t *testing.T) {
	server := quietTLSServer(t)
	callerTransport := http.DefaultTransport.(*http.Transport).Clone()
	callerTransport.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true,
	}

	client := New(
		WithMaxRetries(0),
		WithTimeout(2*time.Second),
		WithTransport(callerTransport),
	)

	response, err := client.Get(server.URL)
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil {
		t.Fatal("insecure caller transport disabled certificate verification")
	}
	if !callerTransport.TLSClientConfig.InsecureSkipVerify {
		t.Fatal("WithTransport mutated the caller-owned transport")
	}
}

func TestWithTransportClearsTLSBypassHooks(t *testing.T) {
	server := quietTLSServer(t)
	callerTransport := http.DefaultTransport.(*http.Transport).Clone()
	var hookCalled atomic.Bool
	callerTransport.DialTLSContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		hookCalled.Store(true)
		rawConnection, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		tlsConnection := tls.Client(rawConnection, &tls.Config{InsecureSkipVerify: true})
		if err := tlsConnection.HandshakeContext(ctx); err != nil {
			_ = rawConnection.Close()
			return nil, err
		}
		return tlsConnection, nil
	}
	//lint:ignore SA1019 The test verifies that both legacy and context-aware TLS bypass hooks are removed.
	callerTransport.DialTLS = func(network, address string) (net.Conn, error) {
		return nil, net.ErrClosed
	}

	client := New(
		WithMaxRetries(0),
		WithTimeout(2*time.Second),
		WithTransport(callerTransport),
	)

	response, err := client.Get(server.URL)
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil {
		t.Fatal("custom TLS dial hook bypassed certificate verification")
	}
	if hookCalled.Load() {
		t.Fatal("custom DialTLSContext hook was invoked")
	}
	securedTransport := client.httpClient.Transport.(*http.Transport)
	//lint:ignore SA1019 The legacy hook is part of the security invariant under test.
	if securedTransport.DialTLS != nil || securedTransport.DialTLSContext != nil {
		t.Fatal("secured transport retained a custom TLS dial hook")
	}
}

func TestWithTransportPreservesTrustedRoots(t *testing.T) {
	server := quietTLSServer(t)
	trustedTransport := server.Client().Transport.(*http.Transport).Clone()

	client := New(
		WithMaxRetries(0),
		WithTimeout(2*time.Second),
		WithTransport(trustedTransport),
	)

	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("request with an explicitly trusted certificate failed: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusNoContent)
	}
}
