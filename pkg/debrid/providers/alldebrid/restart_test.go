package alldebrid

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/request"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/rs/zerolog"
)

func TestCheckStatusRestartsCodeSevenWithBoundedCooldown(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var restartCalls atomic.Int32
	server := newAllDebridRestartServer(t, allDebridStatusNotDownloaded, &restartCalls)
	defer server.Close()

	now := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
	provider := newAllDebridRestartTestProvider(server.URL, func() time.Time { return now })
	torrent := &types.Torrent{Id: "42", Name: "Release", DownloadUncached: true}

	result, err := provider.CheckStatus(torrent)
	if err != nil || result.Status != types.TorrentStatusDownloading {
		t.Fatalf("first CheckStatus() = (%#v, %v), want deferred download", result, err)
	}
	if got := restartCalls.Load(); got != 1 {
		t.Fatalf("restart calls = %d, want 1", got)
	}

	if _, err := provider.CheckStatus(torrent); err != nil {
		t.Fatalf("cooldown CheckStatus() error = %v", err)
	}
	if got := restartCalls.Load(); got != 1 {
		t.Fatalf("restart calls during cooldown = %d, want 1", got)
	}

	now = now.Add(allDebridRestartCooldown + time.Second)
	if _, err := provider.CheckStatus(torrent); err != nil {
		t.Fatalf("second restart CheckStatus() error = %v", err)
	}
	if got := restartCalls.Load(); got != 2 {
		t.Fatalf("restart calls = %d, want bounded second attempt", got)
	}

	now = now.Add(allDebridRestartCooldown + time.Second)
	_, err = provider.CheckStatus(torrent)
	if err == nil || !strings.Contains(err.Error(), "status 7") ||
		!strings.Contains(err.Error(), "20 minutes") {
		t.Fatalf("exhausted CheckStatus() error = %v, want descriptive status-7 error", err)
	}
	if got := restartCalls.Load(); got != 2 {
		t.Fatalf("restart calls after exhaustion = %d, want 2", got)
	}
}

func TestCheckStatusDoesNotRestartCodeSevenForCachedOnlyImport(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var restartCalls atomic.Int32
	server := newAllDebridRestartServer(t, allDebridStatusNotDownloaded, &restartCalls)
	defer server.Close()
	provider := newAllDebridRestartTestProvider(server.URL, time.Now)

	_, err := provider.CheckStatus(&types.Torrent{Id: "42", Name: "Release"})
	var customErr *customerror.Error
	if !errors.As(err, &customErr) || customErr.Code != "torrent_not_cached" {
		t.Fatalf("CheckStatus() error = %v, want typed cache miss", err)
	}
	if got := restartCalls.Load(); got != 0 {
		t.Fatalf("restart calls = %d, want 0 for cached-only import", got)
	}
}

func TestCheckStatusPreservesOtherTerminalAllDebridCodes(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var restartCalls atomic.Int32
	server := newAllDebridRestartServer(t, 10, &restartCalls)
	defer server.Close()
	provider := newAllDebridRestartTestProvider(server.URL, time.Now)

	torrent := &types.Torrent{Id: "42", Name: "Release", DownloadUncached: true}
	_, err := provider.CheckStatus(torrent)
	if err == nil || !strings.Contains(err.Error(), "status 10") ||
		!strings.Contains(err.Error(), "72 hours") {
		t.Fatalf("CheckStatus() error = %v, want descriptive terminal status", err)
	}
	if !errors.Is(err, types.ErrTerminalProviderTorrent) {
		t.Fatalf("CheckStatus() error = %v, want terminal provider marker", err)
	}
	if torrent.ProviderState != "10" {
		t.Fatalf("provider state = %q, want raw AllDebrid status 10", torrent.ProviderState)
	}
	if got := restartCalls.Load(); got != 0 {
		t.Fatalf("restart calls = %d, want 0 for status 10", got)
	}
}

func TestAllDebridStatusErrorClassifiesOnlyDocumentedTerminalCodes(t *testing.T) {
	for statusCode := 5; statusCode <= 15; statusCode++ {
		err := newAllDebridStatusError("Release", statusCode)
		if !errors.Is(err, types.ErrTerminalProviderTorrent) {
			t.Fatalf("newAllDebridStatusError(%d) = %v, want terminal provider marker", statusCode, err)
		}
	}

	for _, statusCode := range []int{4, 16, 99} {
		err := newAllDebridStatusError("Release", statusCode)
		if errors.Is(err, types.ErrTerminalProviderTorrent) {
			t.Fatalf("newAllDebridStatusError(%d) = %v, undocumented status must remain retryable", statusCode, err)
		}
	}
}

func TestDownloadedMagnetClearsRestartBudget(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var restartCalls atomic.Int32
	var statusCode atomic.Int32
	statusCode.Store(allDebridStatusNotDownloaded)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/magnet/status":
			writeAllDebridStatus(t, w, int(statusCode.Load()))
		case "/magnet/restart":
			restartCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"message":"restarted"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	provider := newAllDebridRestartTestProvider(server.URL, time.Now)
	torrent := &types.Torrent{Id: "42", Name: "Release", DownloadUncached: true}

	if _, err := provider.CheckStatus(torrent); err != nil {
		t.Fatal(err)
	}
	statusCode.Store(4)
	if _, err := provider.CheckStatus(torrent); err != nil {
		t.Fatal(err)
	}
	provider.restartMu.Lock()
	remaining := len(provider.restartStates)
	provider.restartMu.Unlock()
	if remaining != 0 {
		t.Fatalf("restart states after completion = %d, want 0", remaining)
	}
	if got := restartCalls.Load(); got != 1 {
		t.Fatalf("restart calls = %d, want 1", got)
	}
}

func TestConcurrentCodeSevenChecksIssueOneRestart(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	var restartCalls atomic.Int32
	server := newAllDebridRestartServer(t, allDebridStatusNotDownloaded, &restartCalls)
	defer server.Close()
	provider := newAllDebridRestartTestProvider(server.URL, time.Now)

	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			result, err := provider.CheckStatus(&types.Torrent{
				Id:               "42",
				Name:             "Release",
				DownloadUncached: true,
			})
			if err != nil || result.Status != types.TorrentStatusDownloading {
				t.Errorf("CheckStatus() = (%#v, %v), want deferred download", result, err)
			}
		})
	}
	wg.Wait()
	if got := restartCalls.Load(); got != 1 {
		t.Fatalf("concurrent restart calls = %d, want 1", got)
	}
}

func TestMagnetRestartStateIsBoundedAndExpires(t *testing.T) {
	now := time.Date(2026, time.August, 25, 12, 0, 0, 0, time.UTC)
	provider := newAllDebridRestartTestProvider("http://example.invalid", func() time.Time { return now })
	for id := 1; id <= allDebridRestartStateLimit+1; id++ {
		if _, decision := provider.planMagnetRestart(fmt.Sprint(id)); decision != magnetRestartExecute {
			t.Fatalf("planMagnetRestart(%d) was not executable", id)
		}
	}
	provider.restartMu.Lock()
	bounded := len(provider.restartStates)
	provider.restartMu.Unlock()
	if bounded != allDebridRestartStateLimit {
		t.Fatalf("restart state count = %d, want %d", bounded, allDebridRestartStateLimit)
	}

	now = now.Add(allDebridRestartStateTTL + time.Second)
	freshID := fmt.Sprint(allDebridRestartStateLimit + 2)
	if attempt, decision := provider.planMagnetRestart(freshID); attempt != 1 || decision != magnetRestartExecute {
		t.Fatalf("post-expiry decision = (%d, %d), want fresh first attempt", attempt, decision)
	}
	provider.restartMu.Lock()
	remaining := len(provider.restartStates)
	provider.restartMu.Unlock()
	if remaining != 1 {
		t.Fatalf("restart state count after expiry = %d, want 1", remaining)
	}
}

func TestAllDebridRestartEndpointUsesDocumentedV4Route(t *testing.T) {
	if got, want := allDebridRestartEndpoint("https://api.alldebrid.com/v4.1"),
		"https://api.alldebrid.com/v4/magnet/restart"; got != want {
		t.Fatalf("restart endpoint = %q, want %q", got, want)
	}
}

func TestRestartMagnetDoesNotExposeProviderResponseDetails(t *testing.T) {
	secret := "provider-internal-detail"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(
			w,
			`{"status":"error","error":{"code":"MAGNET_INVALID_ID","message":%q}}`,
			secret,
		)
	}))
	defer server.Close()
	provider := newAllDebridRestartTestProvider(server.URL, time.Now)

	err := provider.restartMagnet("42")
	if err == nil || !strings.Contains(err.Error(), "MAGNET_INVALID_ID") {
		t.Fatalf("restartMagnet() error = %v, want provider error code", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("restartMagnet() exposed provider response detail: %v", err)
	}
}

func newAllDebridRestartServer(t *testing.T, statusCode int, restartCalls *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/magnet/status":
			if r.Method != http.MethodGet || r.URL.Query().Get("id") != "42" {
				t.Errorf("status request = %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
			}
			writeAllDebridStatus(t, w, statusCode)
		case "/magnet/restart":
			if r.Method != http.MethodPost {
				t.Errorf("restart method = %s, want POST", r.Method)
			}
			if err := r.ParseForm(); err != nil {
				t.Errorf("ParseForm() error = %v", err)
			}
			if got := r.Form.Get("id"); got != "42" {
				t.Errorf("restart id = %q, want 42", got)
			}
			restartCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"success","data":{"message":"restarted"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
}

func writeAllDebridStatus(t *testing.T, w http.ResponseWriter, statusCode int) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(
		w,
		`{"status":"success","data":{"magnets":[{"id":42,"filename":"Release","size":100,"statusCode":%d}]}}`,
		statusCode,
	)
}

func newAllDebridRestartTestProvider(host string, now func() time.Time) *AllDebrid {
	return &AllDebrid{
		Host:          host,
		client:        request.New(request.WithMaxRetries(0), request.WithTimeout(2*time.Second)),
		logger:        zerolog.Nop(),
		restartStates: make(map[string]magnetRestartState),
		restartNow:    now,
	}
}
