package manager

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

func TestParseUncachedStallTimeoutFailsClosed(t *testing.T) {
	for _, test := range []struct {
		raw     string
		want    time.Duration
		wantErr bool
	}{
		{raw: "", want: 0},
		{raw: "2h", want: 2 * time.Hour},
		{raw: "9m", wantErr: true},
		{raw: "invalid", wantErr: true},
	} {
		got, err := parseUncachedStallTimeout(test.raw)
		if (err != nil) != test.wantErr || got != test.want {
			t.Fatalf("parseUncachedStallTimeout(%q) = %s, %v; want %s, error=%t", test.raw, got, err, test.want, test.wantErr)
		}
	}
}

func TestUncachedTransferStalledRequiresDefiniteInactivity(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	progressAt := now.Add(-2 * time.Hour)
	observedAt := now
	base := storage.Entry{
		Protocol:         config.ProtocolTorrent,
		DownloadUncached: true,
		State:            storage.EntryStateDownloading,
		Status:           debridTypes.TorrentStatusDownloading,
		Progress:         0.25,
		LastProgressAt:   &progressAt,
		LastObservedAt:   &observedAt,
	}
	if !uncachedTransferStalled(&base, debridTypes.TorrentStatusDownloading, now, time.Hour) {
		t.Fatal("definitely inactive uncached transfer was not classified as stalled")
	}

	checks := []struct {
		name           string
		providerStatus debridTypes.TorrentStatus
		mutate         func(*storage.Entry)
	}{
		{name: "cached policy", providerStatus: debridTypes.TorrentStatusDownloading, mutate: func(e *storage.Entry) { e.DownloadUncached = false }},
		{name: "provider speed", providerStatus: debridTypes.TorrentStatusDownloading, mutate: func(e *storage.Entry) { e.Speed = 1 }},
		{name: "recent progress", providerStatus: debridTypes.TorrentStatusDownloading, mutate: func(e *storage.Entry) { recent := now.Add(-time.Minute); e.LastProgressAt = &recent }},
		{name: "no baseline", providerStatus: debridTypes.TorrentStatusDownloading, mutate: func(e *storage.Entry) { e.LastProgressAt = nil }},
		{name: "generic error", providerStatus: debridTypes.TorrentStatusDownloading, mutate: func(e *storage.Entry) { e.State = storage.EntryStateError }},
		{name: "completed progress", providerStatus: debridTypes.TorrentStatusDownloading, mutate: func(e *storage.Entry) { e.Progress = 1 }},
		{name: "provider completed", providerStatus: debridTypes.TorrentStatusDownloaded, mutate: func(*storage.Entry) {}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			candidate := base
			check.mutate(&candidate)
			if uncachedTransferStalled(&candidate, check.providerStatus, now, time.Hour) {
				t.Fatal("uncertain or active transfer was classified as stalled")
			}
		})
	}
}

func TestQueuedProviderPollPersistsStalledHandoffState(t *testing.T) {
	m := newQueueRecoveryTestManager(t)
	m.uncachedStallTimeout = time.Hour
	entry := interruptedQueueTestEntry("stalled-provider-transfer")
	entry.Status = debridTypes.TorrentStatusDownloading
	entry.IsDownloading = false
	entry.Progress = 0.25
	stale := time.Now().Add(-2 * time.Hour)
	entry.LastProgressAt = &stale
	if err := m.queue.Add(entry); err != nil {
		t.Fatal(err)
	}
	m.clients.Store("torbox", &routingTestClient{
		cfg: config.Debrid{Name: "torbox"},
		check: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			torrent.Debrid = "torbox"
			torrent.Name = "release.mkv"
			torrent.Status = debridTypes.TorrentStatusDownloading
			torrent.ProviderState = "downloading"
			torrent.Progress = 25
			torrent.Speed = 0
			return torrent, nil
		},
	})

	if err := m.processQueuedTorrent(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
	current, err := m.queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != storage.EntryStateDownloading {
		t.Fatalf("first confirmation state = %s, want downloading", current.State)
	}
	key := uncachedStallCandidateKey(current)
	m.stallCandidatesMu.Lock()
	m.stallCandidates[key] = stallCandidate{kind: "stalled", checkedAt: time.Now().Add(-time.Minute)}
	m.stallCandidatesMu.Unlock()
	if err := m.processQueuedTorrent(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	current, err = m.queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != storage.EntryStateStalledDL || current.Status != debridTypes.TorrentStatusError {
		t.Fatalf("stalled state = %s/%s, want stalledDL/error", current.State, current.Status)
	}
	if current.LastError == "" || current.LastProgressAt == nil || current.LastProgressAt.Unix() != stale.Unix() {
		t.Fatalf("stalled evidence was not preserved: error=%q progressAt=%v", current.LastError, current.LastProgressAt)
	}
}

func TestTerminalUncachedFailureRequiresTwoFreshConfirmations(t *testing.T) {
	m := newQueueRecoveryTestManager(t)
	entry := interruptedQueueTestEntry("terminal-provider-transfer")
	entry.Status = debridTypes.TorrentStatusDownloading
	entry.IsDownloading = false
	if err := m.queue.Add(entry); err != nil {
		t.Fatal(err)
	}
	var freshCalls atomic.Int32
	m.clients.Store("torbox", &routingTestClient{
		cfg: config.Debrid{Name: "torbox"},
		check: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			torrent.Status = debridTypes.TorrentStatusError
			torrent.ProviderState = "failed (processing)"
			return torrent, debridTypes.ErrTerminalProviderTorrent
		},
		fresh: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			freshCalls.Add(1)
			torrent.Status = debridTypes.TorrentStatusError
			torrent.ProviderState = "failed (processing)"
			return torrent, debridTypes.ErrTerminalProviderTorrent
		},
	})
	for n := 1; n <= 2; n++ {
		if err := m.processQueuedTorrent(context.Background(), entry); err != nil {
			t.Fatal(err)
		}
		current, err := m.queue.GetTorrent(entry.InfoHash)
		if err != nil {
			t.Fatal(err)
		}
		if current.TerminalChecks != n {
			t.Fatalf("checks = %d, want %d", current.TerminalChecks, n)
		}
		if n == 1 && current.State != storage.EntryStateDownloading {
			t.Fatalf("first failure state = %s", current.State)
		}
		if n == 2 && (current.State != storage.EntryStateStalledDL || current.HandoffReason != "terminal") {
			t.Fatalf("confirmed failure = %s/%s", current.State, current.HandoffReason)
		}
		entry = current
	}
	if freshCalls.Load() != 2 {
		t.Fatalf("fresh calls = %d", freshCalls.Load())
	}
}

func TestUnclassifiedUncachedProviderErrorStaysQueued(t *testing.T) {
	m := newQueueRecoveryTestManager(t)
	entry := interruptedQueueTestEntry("unclassified-provider-transfer")
	entry.Status = debridTypes.TorrentStatusDownloading
	entry.IsDownloading = false
	entry.TerminalChecks = 1
	if err := m.queue.Add(entry); err != nil {
		t.Fatal(err)
	}
	var freshCalls atomic.Int32
	var deleteCalls atomic.Int32
	m.clients.Store("torbox", &routingTestClient{
		cfg: config.Debrid{Name: "torbox"},
		check: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			torrent.Status = debridTypes.TorrentStatusError
			torrent.ProviderState = "provider-specific-error"
			return torrent, errors.New("torrent has error")
		},
		fresh: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			freshCalls.Add(1)
			torrent.Status = debridTypes.TorrentStatusError
			torrent.ProviderState = "provider-specific-error"
			return torrent, errors.New("torrent has error")
		},
		delete: func(string) error {
			deleteCalls.Add(1)
			return nil
		},
	})

	for range 2 {
		if err := m.processQueuedTorrent(context.Background(), entry); err != nil {
			t.Fatal(err)
		}
		current, err := m.queue.GetTorrent(entry.InfoHash)
		if err != nil {
			t.Fatal(err)
		}
		if current.TerminalChecks != 0 {
			t.Fatalf("checks = %d, want reset for an unclassified state", current.TerminalChecks)
		}
		if current.State != storage.EntryStateDownloading || current.HandoffReason != "" {
			t.Fatalf("unclassified failure = %s/%s, want downloading with no handoff", current.State, current.HandoffReason)
		}
		entry = current
	}
	if freshCalls.Load() != 0 {
		t.Fatalf("fresh calls = %d, want 0 without an explicit terminal classification", freshCalls.Load())
	}
	if deleteCalls.Load() != 0 {
		t.Fatalf("provider delete calls = %d, want 0 before Arr handoff", deleteCalls.Load())
	}
}

func TestTerminalSentinelNeedsProviderErrorStatus(t *testing.T) {
	m := newQueueRecoveryTestManager(t)
	entry := interruptedQueueTestEntry("terminal-sentinel-without-error-state")
	entry.Status = debridTypes.TorrentStatusDownloading
	entry.IsDownloading = false
	entry.TerminalChecks = 1
	if err := m.queue.Add(entry); err != nil {
		t.Fatal(err)
	}
	m.clients.Store("torbox", &routingTestClient{
		cfg: config.Debrid{Name: "torbox"},
		check: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			torrent.Status = debridTypes.TorrentStatusDownloading
			torrent.ProviderState = "downloading"
			return torrent, debridTypes.ErrTerminalProviderTorrent
		},
	})

	if err := m.processQueuedTorrent(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
	current, err := m.queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if current.TerminalChecks != 0 {
		t.Fatalf("checks = %d, want reset without provider error status", current.TerminalChecks)
	}
	if current.State != storage.EntryStateDownloading || current.HandoffReason != "" {
		t.Fatalf("sentinel-only failure = %s/%s, want downloading with no handoff", current.State, current.HandoffReason)
	}
}

func TestTransientFreshCheckBreaksTerminalConfirmation(t *testing.T) {
	m := newQueueRecoveryTestManager(t)
	entry := interruptedQueueTestEntry("transient-provider-error")
	entry.Status = debridTypes.TorrentStatusDownloading
	entry.IsDownloading = false
	if err := m.queue.Add(entry); err != nil {
		t.Fatal(err)
	}
	var freshCalls atomic.Int32
	m.clients.Store("torbox", &routingTestClient{
		cfg: config.Debrid{Name: "torbox"},
		check: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			torrent.Status = debridTypes.TorrentStatusError
			torrent.ProviderState = "failed"
			return torrent, debridTypes.ErrTerminalProviderTorrent
		},
		fresh: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			if freshCalls.Add(1) == 2 {
				return torrent, errors.New("temporary provider outage")
			}
			torrent.Status = debridTypes.TorrentStatusError
			torrent.ProviderState = "failed"
			return torrent, debridTypes.ErrTerminalProviderTorrent
		},
	})
	for n, want := range []int{1, 0, 1, 2} {
		if err := m.processQueuedTorrent(context.Background(), entry); err != nil {
			t.Fatal(err)
		}
		current, err := m.queue.GetTorrent(entry.InfoHash)
		if err != nil {
			t.Fatal(err)
		}
		if current.TerminalChecks != want {
			t.Fatalf("poll %d checks = %d, want %d", n+1, current.TerminalChecks, want)
		}
		entry = current
	}
}

func TestUncachedStallPolicyUsesStateNotSeederCountAlone(t *testing.T) {
	for _, test := range []struct {
		state   string
		status  debridTypes.TorrentStatus
		seeders int
		want    string
		timeout time.Duration
	}{
		{state: "stalled (no seeds)", status: debridTypes.TorrentStatusDownloading, want: "no_seeds", timeout: 10 * time.Minute},
		{state: "stalledDL", status: debridTypes.TorrentStatusDownloading, want: "no_seeds", timeout: 10 * time.Minute},
		{state: "stalled (no seeds)", status: debridTypes.TorrentStatusDownloading, seeders: 2, want: "stalled", timeout: 30 * time.Minute},
		{state: "metaDL", status: debridTypes.TorrentStatusDownloading, want: "metadata", timeout: 20 * time.Minute},
		{state: "downloading", status: debridTypes.TorrentStatusDownloading, want: "stalled", timeout: 30 * time.Minute},
		{state: "stalledDL", status: debridTypes.TorrentStatusDownloading, seeders: 2, want: "stalled", timeout: 30 * time.Minute},
		{state: "", status: debridTypes.TorrentStatusDownloading, want: "stalled", timeout: 30 * time.Minute},
		{state: "0", status: debridTypes.TorrentStatusDownloading, want: "stalled", timeout: 30 * time.Minute},
		{state: "running", status: debridTypes.TorrentStatusDownloading, want: "stalled", timeout: 30 * time.Minute},
		{state: "finished", status: debridTypes.TorrentStatusDownloading, want: "stalled", timeout: 30 * time.Minute},
		{state: "magnet_conversion", status: debridTypes.TorrentStatusDownloading, want: "stalled", timeout: 30 * time.Minute},
		{state: "paused", status: debridTypes.TorrentStatusDownloading},
		{state: "checkingDL", status: debridTypes.TorrentStatusDownloading},
		{state: "queued", status: debridTypes.TorrentStatusQueued},
		{state: "future-provider-state", status: debridTypes.TorrentStatusError},
	} {
		kind, timeout := uncachedStallPolicy(test.state, test.status, test.seeders, 30*time.Minute)
		if kind != test.want || timeout != test.timeout {
			t.Errorf("state %q, seeders %d = %q/%s, want %q/%s", test.state, test.seeders, kind, timeout, test.want, test.timeout)
		}
	}
	if kind, timeout := uncachedStallPolicy("metaDL", debridTypes.TorrentStatusDownloading, 0, 0); kind != "" || timeout != 0 {
		t.Fatalf("disabled watchdog = %q/%s", kind, timeout)
	}
}

func TestUncachedStallRequiresSpacedMatchingFreshChecks(t *testing.T) {
	m := &Manager{}
	base := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	if m.confirmUncachedStall("torrent", "no_seeds", base) ||
		m.confirmUncachedStall("torrent", "no_seeds", base.Add(10*time.Second)) {
		t.Fatal("stall confirmed without spaced checks")
	}
	if !m.confirmUncachedStall("torrent", "no_seeds", base.Add(31*time.Second)) {
		t.Fatal("matching spaced checks did not confirm stall")
	}
	if m.confirmUncachedStall("torrent", "metadata", base) ||
		m.confirmUncachedStall("torrent", "no_seeds", base.Add(time.Minute)) {
		t.Fatal("different stall kinds were combined")
	}
	m.clearUncachedStallCandidate("torrent")
	if m.confirmUncachedStall("torrent", "no_seeds", base.Add(2*time.Minute)) {
		t.Fatal("cleared candidate still confirmed")
	}
}

func TestQueuedUncachedStateSpecificStallThresholds(t *testing.T) {
	for _, test := range []struct {
		name          string
		state         string
		noProgressFor time.Duration
		wantKind      string
	}{
		{name: "no seeds after ten minutes", state: "stalled (no seeds)", noProgressFor: 11 * time.Minute, wantKind: "no_seeds"},
		{name: "metadata before twenty minutes", state: "metaDL", noProgressFor: 15 * time.Minute},
		{name: "metadata after twenty minutes", state: "metaDL", noProgressFor: 21 * time.Minute, wantKind: "metadata"},
		{name: "ordinary download before thirty minutes", state: "downloading", noProgressFor: 21 * time.Minute},
		{name: "zero seeds alone does not accelerate", state: "downloading", noProgressFor: 11 * time.Minute},
		{name: "AllDebrid active code uses generic timeout", state: "0", noProgressFor: 31 * time.Minute, wantKind: "stalled"},
		{name: "Premiumize running uses generic timeout", state: "running", noProgressFor: 31 * time.Minute, wantKind: "stalled"},
		{name: "paused is not a stall", state: "paused", noProgressFor: time.Hour},
	} {
		t.Run(test.name, func(t *testing.T) {
			m := newQueueRecoveryTestManager(t)
			m.uncachedStallTimeout = 30 * time.Minute
			entry := interruptedQueueTestEntry(test.name)
			entry.Status = debridTypes.TorrentStatusDownloading
			entry.IsDownloading = false
			entry.Progress = 0.25
			stale := time.Now().Add(-test.noProgressFor)
			entry.LastProgressAt = &stale
			if err := m.queue.Add(entry); err != nil {
				t.Fatal(err)
			}
			var freshCalls atomic.Int32
			m.clients.Store("torbox", &routingTestClient{
				cfg: config.Debrid{Name: "torbox"},
				check: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
					torrent.Debrid = "torbox"
					torrent.Name = "release.mkv"
					torrent.Status = debridTypes.TorrentStatusDownloading
					torrent.ProviderState = test.state
					torrent.Progress = 25
					return torrent, nil
				},
				fresh: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
					freshCalls.Add(1)
					return torrent, nil
				},
			})
			if err := m.processQueuedTorrent(context.Background(), entry); err != nil {
				t.Fatal(err)
			}
			current, err := m.queue.GetTorrent(entry.InfoHash)
			if err != nil {
				t.Fatal(err)
			}
			if current.State != storage.EntryStateDownloading {
				t.Fatalf("first poll state = %s, want downloading", current.State)
			}
			key := uncachedStallCandidateKey(current)
			m.stallCandidatesMu.Lock()
			candidate, found := m.stallCandidates[key]
			m.stallCandidatesMu.Unlock()
			if found != (test.wantKind != "") || (found && candidate.kind != test.wantKind) {
				t.Fatalf("candidate = %q (found=%t), want %q", candidate.kind, found, test.wantKind)
			}
			if got := freshCalls.Load(); got != int32(0) && test.wantKind == "" || got != 1 && test.wantKind != "" {
				t.Fatalf("fresh checks = %d, want one only for eligible state", got)
			}
			if test.wantKind == "" {
				return
			}
			m.stallCandidatesMu.Lock()
			m.stallCandidates[key] = stallCandidate{kind: test.wantKind, checkedAt: time.Now().Add(-time.Minute)}
			m.stallCandidatesMu.Unlock()
			if err := m.processQueuedTorrent(context.Background(), current); err != nil {
				t.Fatal(err)
			}
			current, err = m.queue.GetTorrent(entry.InfoHash)
			if err != nil {
				t.Fatal(err)
			}
			if current.State != storage.EntryStateStalledDL || current.HandoffReason != test.wantKind {
				t.Fatalf("confirmed state = %s/%s, want stalledDL/%s", current.State, current.HandoffReason, test.wantKind)
			}
		})
	}
}

func TestQueuedUncachedFreshProgressCancelsHandoff(t *testing.T) {
	m := newQueueRecoveryTestManager(t)
	m.uncachedStallTimeout = 30 * time.Minute
	entry := interruptedQueueTestEntry("recovering-no-seeds")
	entry.Status = debridTypes.TorrentStatusDownloading
	entry.IsDownloading = false
	entry.Progress = 0.25
	stale := time.Now().Add(-15 * time.Minute)
	entry.LastProgressAt = &stale
	if err := m.queue.Add(entry); err != nil {
		t.Fatal(err)
	}
	var freshCalls atomic.Int32
	m.clients.Store("torbox", &routingTestClient{
		cfg: config.Debrid{Name: "torbox"},
		check: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			torrent.Debrid = "torbox"
			torrent.Name = "release.mkv"
			torrent.Status = debridTypes.TorrentStatusDownloading
			torrent.ProviderState = "stalled (no seeds)"
			torrent.Progress = 25
			return torrent, nil
		},
		fresh: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
			if freshCalls.Add(1) == 2 {
				torrent.ProviderState = "downloading"
				torrent.Progress = 26
				torrent.Speed = 1024
			}
			return torrent, nil
		},
	})
	if err := m.processQueuedTorrent(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
	current, err := m.queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	key := uncachedStallCandidateKey(current)
	m.stallCandidatesMu.Lock()
	m.stallCandidates[key] = stallCandidate{kind: "no_seeds", checkedAt: time.Now().Add(-time.Minute)}
	m.stallCandidatesMu.Unlock()
	if err := m.processQueuedTorrent(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	current, err = m.queue.GetTorrent(entry.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != storage.EntryStateDownloading || current.Progress != 0.26 || current.Speed != 1024 {
		t.Fatalf("resumed transfer = %s, progress %v, speed %v", current.State, current.Progress, current.Speed)
	}
	if current.LastProgressAt == nil || !current.LastProgressAt.After(stale) {
		t.Fatalf("resumed progress baseline was not reset: %v", current.LastProgressAt)
	}
	m.stallCandidatesMu.Lock()
	_, pending := m.stallCandidates[key]
	m.stallCandidatesMu.Unlock()
	if pending {
		t.Fatal("recovered transfer retained a pending failure confirmation")
	}
}

func TestHandoffStalledUncachedTargetsOwningArr(t *testing.T) {
	var manager *Manager
	var deletes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/downloadclient":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"name":"Tessarr","implementation":"QBittorrent","fields":[{"name":"host","value":"tessarr.example"},{"name":"port","value":8282}]}]`))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v3/queue":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"page":1,"pageSize":200,"totalRecords":1,"records":[{"id":17,"downloadId":"ABC123","protocol":"torrent","downloadClient":"Tessarr"}]}`))
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v3/queue/bulk":
			deletes.Add(1)
			if err := manager.DeleteQBitQueueEntry("abc123", true); err != nil {
				t.Errorf("synchronous qbit callback: %v", err)
				w.WriteHeader(500)
				return
			}
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	store := newLifecycleTestStorage(t)
	queue := newLifecycleTestQueue(store, newEntryLifecycle())
	entry, _ := addLifecycleTestEntry(t, queue, "abc123")
	entry.Category = "sonarr"
	entry.State = storage.EntryStateStalledDL
	entry.DownloadUncached = true
	entry.ClientEndpoint = "tessarr.example:8282"
	entry.HandoffReason = "terminal"
	entry.ActiveProvider = "fake"
	entry.Providers = map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "active"}}
	if err := queue.Update(entry); err != nil {
		t.Fatal(err)
	}
	arrs := arr.NewStorage()
	arrs.AddOrUpdate(arr.New("sonarr", server.URL, "token", false, nil, "", "manual"))
	clients := xsync.NewMap[string, debrid.Client]()
	var cancellations atomic.Int32
	clients.Store("fake", &placementDeleteTestProvider{name: "fake", delete: func(id string) error {
		if id != "active" {
			t.Errorf("wrong placement %s", id)
		}
		cancellations.Add(1)
		return nil
	}})
	m := &Manager{storage: store, queue: queue, arr: arrs, clients: clients, logger: zerolog.Nop()}
	manager = m
	if err := m.handoffUncachedFailures(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cancellations.Load() != 1 {
		t.Fatalf("provider cancellation=%d", cancellations.Load())
	}
	if deletes.Load() != 1 {
		t.Fatalf("Arr delete requests = %d, want 1", deletes.Load())
	}
}

func TestHandoffStalledUncachedRequiresSubmittingEndpoint(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.NotFound(w, r)
	}))
	defer server.Close()
	store := newLifecycleTestStorage(t)
	queue := newLifecycleTestQueue(store, newEntryLifecycle())
	entry := &storage.Entry{InfoHash: "legacy-stall", Protocol: config.ProtocolTorrent, Category: "sonarr", State: storage.EntryStateStalledDL, DownloadUncached: true}
	if err := queue.Add(entry); err != nil {
		t.Fatal(err)
	}
	arrs := arr.NewStorage()
	arrs.AddOrUpdate(arr.New("sonarr", server.URL, "token", false, nil, "", "manual"))
	m := &Manager{queue: queue, arr: arrs, logger: zerolog.Nop()}
	if err := m.handoffUncachedFailures(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 0 {
		t.Fatalf("unowned handoff made %d Arr requests", requests.Load())
	}
}

func TestLegacyStalledCleanupPreservesWatchdogHandoff(t *testing.T) {
	store := newLifecycleTestStorage(t)
	queue := newLifecycleTestQueue(store, newEntryLifecycle())
	queue.removeStalledAfter = 10 * time.Minute
	old := time.Now().Add(-time.Hour)

	watchdog, _ := addLifecycleTestEntry(t, queue, "watchdog-stall")
	watchdog.State = storage.EntryStateStalledDL
	watchdog.Status = debridTypes.TorrentStatusError
	watchdog.DownloadUncached = true
	watchdog.AddedOn = old
	if err := queue.Update(watchdog); err != nil {
		t.Fatal(err)
	}

	if err := queue.DeleteStalled(); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.GetTorrent(watchdog.InfoHash); err != nil {
		t.Fatalf("watchdog handoff row was removed before Arr acknowledgement: %v", err)
	}
}

func TestLegacyStalledCleanupStillRemovesOrdinaryErrors(t *testing.T) {
	store := newLifecycleTestStorage(t)
	queue := newLifecycleTestQueue(store, newEntryLifecycle())
	queue.removeStalledAfter = 10 * time.Minute

	entry, _ := addLifecycleTestEntry(t, queue, "ordinary-stall")
	entry.State = storage.EntryStateError
	entry.Status = debridTypes.TorrentStatusError
	entry.AddedOn = time.Now().Add(-time.Hour)
	if err := queue.Update(entry); err != nil {
		t.Fatal(err)
	}

	if err := queue.DeleteStalled(); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.GetTorrent(entry.InfoHash); !storage.IsQueuedEntryNotFound(err) {
		t.Fatalf("ordinary stalled row was not cleaned up: %v", err)
	}
}
