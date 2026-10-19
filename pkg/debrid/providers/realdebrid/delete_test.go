package realdebrid

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/request"
	"github.com/rs/zerolog"
)

func TestDeleteTorrentDistinguishesNotFoundAndDrainsResponse(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var status atomic.Int32
	status.Store(http.StatusNotFound)
	var newConnections atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/torrents/delete/42" {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(int(status.Load()))
		_, _ = w.Write(make([]byte, 8<<10))
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			newConnections.Add(1)
		}
	}
	server.Start()
	defer server.Close()

	client := &RealDebrid{
		Host:   server.URL,
		client: request.New(request.WithMaxRetries(0), request.WithTimeout(2*time.Second)),
		logger: zerolog.Nop(),
	}
	if err := client.DeleteTorrent("42"); !errors.Is(err, customerror.TorrentNotFoundError) {
		t.Fatalf("404 error = %v, want typed not-found", err)
	}
	status.Store(http.StatusUnauthorized)
	if err := client.DeleteTorrent("42"); err == nil || errors.Is(err, customerror.TorrentNotFoundError) {
		t.Fatalf("401 error = %v, want non-not-found failure", err)
	}
	if newConnections.Load() != 1 {
		t.Fatalf("new connections = %d, want 1 after drained/closed response reuse", newConnections.Load())
	}
}
