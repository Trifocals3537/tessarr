package manager

import (
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/cdntraffic"
)

func baseStreamHTTPTransport(t *testing.T, client *http.Client) *http.Transport {
	t.Helper()
	governed, ok := client.Transport.(*cdntraffic.Transport)
	if !ok {
		t.Fatalf("stream transport = %T, want *cdntraffic.Transport", client.Transport)
	}
	transport, ok := governed.BaseTransport().(*http.Transport)
	if !ok {
		t.Fatalf("base stream transport = %T, want *http.Transport", governed.BaseTransport())
	}
	return transport
}

func managerTLSServer(t *testing.T) *httptest.Server {
	t.Helper()

	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	t.Cleanup(server.Close)
	return server
}

func TestStreamClientRejectsUntrustedTLSByDefault(t *testing.T) {
	server := managerTLSServer(t)
	client := newStreamHTTPClient(nil)
	client.Timeout = 2 * time.Second

	response, err := client.Get(server.URL)
	if response != nil {
		_ = response.Body.Close()
	}
	if err == nil {
		t.Fatal("stream request to a server with an untrusted certificate succeeded")
	}
}

func TestStreamClientUsesTrustedRoots(t *testing.T) {
	server := managerTLSServer(t)
	client := newStreamHTTPClient(nil)
	client.Timeout = 2 * time.Second

	transport := baseStreamHTTPTransport(t, client)
	trustedTransport := server.Client().Transport.(*http.Transport)
	transport.TLSClientConfig.RootCAs = trustedTransport.TLSClientConfig.RootCAs

	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("stream request with an explicitly trusted certificate failed: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusNoContent)
	}
}

func TestStreamClientBoundsResponseHeaderWait(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		close(started)
		<-release
	}))
	t.Cleanup(server.Close)

	client := newStreamHTTPClient(nil)
	transport := baseStreamHTTPTransport(t, client)
	if transport.ResponseHeaderTimeout != 30*time.Second {
		t.Fatalf(
			"ResponseHeaderTimeout = %s, want 30s",
			transport.ResponseHeaderTimeout,
		)
	}
	transport.ResponseHeaderTimeout = 25 * time.Millisecond

	response, err := client.Get(server.URL)
	if response != nil {
		_ = response.Body.Close()
	}
	<-started
	close(release)
	if err == nil {
		t.Fatal("stream request without response headers did not time out")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("response-header stall error = %v, want timeout", err)
	}
}
