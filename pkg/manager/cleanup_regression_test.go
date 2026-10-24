package manager

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/workflow"
	"github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/debrid/providers/debridlink"
	"github.com/Trifocals3537/tessarr/pkg/debrid/providers/premiumize"
	"github.com/Trifocals3537/tessarr/pkg/debrid/providers/realdebrid"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

type legacyAbsentCleanupProvider struct {
	common.Client
	calls int
}

func (p *legacyAbsentCleanupProvider) Config() config.Debrid {
	return config.Debrid{Name: "fake", APIKey: "account-one"}
}
func (p *legacyAbsentCleanupProvider) GetTorrent(string) (*debridTypes.Torrent, error) {
	p.calls++
	return nil, customerror.TorrentNotFoundError
}

type unknownPresenceCleanupProvider struct{ *legacyAbsentCleanupProvider }

func (p *unknownPresenceCleanupProvider) TorrentPresenceContext(context.Context, string) (common.TorrentPresence, error) {
	return common.TorrentPresenceUnknown, nil
}

func TestCleanupDoesNotFallBackToLegacyAbsenceOrUnknownPresence(t *testing.T) {
	for _, legacyOnly := range []bool{true, false} {
		t.Run(fmt.Sprint(legacyOnly), func(t *testing.T) {
			m := cleanupManager(t, t.TempDir())
			legacy := &legacyAbsentCleanupProvider{}
			var client common.Client = legacy
			if !legacyOnly {
				client = &unknownPresenceCleanupProvider{legacy}
			}
			m.clients.Store("fake", client)
			e := &storage.Entry{InfoHash: "unknown-presence", Name: "release", Protocol: config.ProtocolTorrent, SavePath: m.config.DownloadFolder, State: storage.EntryStateDownloading, Providers: map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "transfer"}}}
			i := cleanupIntent(t, m, e, true)
			i, err := m.storage.ClaimCleanup(i.InfoHash, i.QueueIncarnation, "old-owner", m.cleanupAccounts(i), time.Now(), false)
			if err != nil {
				t.Fatal(err)
			}
			if err := m.storage.CleanupEffectStarted(i.InfoHash, i.QueueIncarnation, i.Work.Token, time.Now()); err != nil {
				t.Fatal(err)
			}
			rows, _ := m.storage.QueuedDeletionIntents()
			if err := m.runCleanupIntent(rows[0], true); err == nil {
				t.Fatal("unverifiable absence was accepted")
			}
			rows, err = m.storage.QueuedDeletionIntents()
			if err != nil || len(rows) != 1 || rows[0].Work.Reason != "provider_absence_requires_review" {
				t.Fatal("unknown presence lost its recovery obligation", err)
			}
			if legacy.calls != 0 {
				t.Fatal("cleanup fell back to an unaudited GetTorrent absence error")
			}
		})
	}
}

func TestCleanupClaimIsReleasedWhenActiveWorkCannotDrain(t *testing.T) {
	m := cleanupManager(t, t.TempDir())
	e := &storage.Entry{InfoHash: "drain-timeout", Name: "release", Protocol: config.ProtocolTorrent, SavePath: m.config.DownloadFolder, State: storage.EntryStateDownloading}
	if err := m.queue.Add(e); err != nil {
		t.Fatal(err)
	}
	lease, err := m.queue.lifecycle.startWork(context.Background(), e.InfoHash, e.QueueGeneration)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Close()
	i, err := m.storage.PrepareQueuedDeletion(e.InfoHash, false)
	if err != nil {
		t.Fatal(err)
	}
	m.queue.deleteDrainTimeout = 10 * time.Millisecond
	if err := m.runCleanupIntent(i, false); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("drain result: %v", err)
	}
	rows, err := m.storage.QueuedDeletionIntents()
	if err != nil || len(rows) != 1 {
		t.Fatal("claim lost after drain timeout", err)
	}
	w := rows[0].Work
	if w.State != workflow.RetryWait || w.Owner != "" || w.Token != "" {
		t.Fatal("drain timeout stranded a running claim")
	}
	if err := m.workflowAvailable(); err != nil {
		t.Fatal("drain timeout incorrectly suspended admissions", err)
	}
	lease.Close()
	requestCleanupRetry(t, m, "retry")
	m.retryCleanupWorkflows()
	rows, err = m.storage.QueuedDeletionIntents()
	if err != nil || len(rows) != 0 {
		t.Fatal("released cleanup did not resume", err)
	}
}

func TestCleanupRegressionStaleCleanupSnapshotDoesNotAffectReplacement(t *testing.T) {
	m := cleanupManager(t, t.TempDir())
	e := &storage.Entry{InfoHash: "stale-scan", Name: "release", Protocol: config.ProtocolTorrent, SavePath: m.config.DownloadFolder, State: storage.EntryStateDownloading}
	snapshot := cleanupIntent(t, m, e, false)
	if err := m.queue.Delete(e.InfoHash, nil); err != nil {
		t.Fatal(err)
	}
	replacement := &storage.Entry{InfoHash: e.InfoHash, Name: "new-release", Protocol: config.ProtocolTorrent, SavePath: m.config.DownloadFolder, State: storage.EntryStateDownloading}
	if err := m.queue.Add(replacement); err != nil {
		t.Fatal(err)
	}
	lease, err := m.queue.lifecycle.startWork(context.Background(), replacement.InfoHash, replacement.QueueGeneration)
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-stop:
		case <-lease.Context().Done():
		}
		lease.Close()
	}()
	removedPending := false
	m.queue.removePendingJobs = func(string) int { removedPending = true; return 1 }
	err = m.runCleanupIntent(snapshot, false)
	t.Logf("stale attempt=%v, replacement context=%v, admission status=%v", err, lease.Context().Err(), m.workflowAvailable())
	if lease.Context().Err() != nil {
		t.Error("stale cleanup canceled the replacement worker")
	}
	if m.workflowAvailable() != nil {
		t.Error("completed stale cleanup suspended all new admissions")
	}
	if removedPending {
		t.Error("stale snapshot removed replacement pending jobs")
	}
}

func TestCleanupRegressionDebridLinkCanRecoverCompletedProviderDeletion(t *testing.T) {
	m := cleanupManager(t, t.TempDir())
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "DELETE" && r.URL.Path == "/seedbox/deleted-transfer/remove" {
			deleted = true
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != "GET" || r.URL.Path != "/seedbox/list" || r.URL.Query().Get("ids") != "deleted-transfer" {
			t.Errorf("unexpected lookup %s %s", r.Method, r.URL)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"success":true,"value":[]}`))
	}))
	defer server.Close()
	p, err := debridlink.New(config.Debrid{Name: "fixture", APIKey: "account-one"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	p.Host = server.URL
	m.clients.Store("fixture", p)
	e := &storage.Entry{InfoHash: "dl-crash-after-delete", Name: "release", Protocol: config.ProtocolTorrent, SavePath: m.config.DownloadFolder, State: storage.EntryStateDownloading, Providers: map[string]*storage.ProviderEntry{"fixture": {Provider: "fixture", ID: "deleted-transfer"}}}
	i := cleanupIntent(t, m, e, true)
	i, err = m.storage.ClaimCleanup(i.InfoHash, i.QueueIncarnation, "crashed-owner", m.cleanupAccounts(i), time.Now(), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storage.StartCleanup(i); err != nil {
		t.Fatal(err)
	}
	if err := m.storage.CleanupEffectStarted(i.InfoHash, i.QueueIncarnation, i.Work.Token, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := p.DeleteTorrent("deleted-transfer"); err != nil {
		t.Fatal(err)
	}
	if !deleted {
		t.Fatal("provider DELETE did not execute")
	}
	rows, _ := m.storage.QueuedDeletionIntents()
	recoveryErr := m.runCleanupIntent(rows[0], true)
	requestCleanupRetry(t, m, "reconcile")
	m.retryCleanupWorkflows()
	rows, err = m.storage.QueuedDeletionIntents()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("deleted=%v recovery=%v retained intents=%d", deleted, recoveryErr, len(rows))
	if len(rows) != 0 {
		t.Error("successful DebridLink deletion cannot be discharged after restart or operator reconciliation")
	}
}

func TestCleanupRegressionAdministrativeRetryRemainsReachable(t *testing.T) {
	m := cleanupManager(t, t.TempDir())
	e := &storage.Entry{InfoHash: "admin-retry", Name: "release", Protocol: config.ProtocolTorrent, SavePath: m.config.DownloadFolder, State: storage.EntryStateDownloading, Providers: map[string]*storage.ProviderEntry{"fake": {Provider: "fake", ID: "remote-id"}}}
	if err := m.queue.Add(e); err != nil {
		t.Fatal(err)
	}
	if err := m.storage.AddOrUpdate(e); err != nil {
		t.Fatal(err)
	}
	offline := errors.New("temporarily offline")
	calls := 0
	m.clients.Store("fake", &placementDeleteTestProvider{name: "fake", delete: func(string) error { calls++; return offline }})
	if err := m.DeleteEntry(e.InfoHash, true); !errors.Is(err, offline) {
		t.Fatalf("initial failure=%v", err)
	}
	m.retryCleanupWorkflows()
	requestCleanupRetry(t, m, "retry")
	m.retryCleanupWorkflows()
	if calls != 1 {
		t.Fatal("operator retry replayed an uncertain administrative DELETE")
	}
	m.clients.Store("fake", &placementDeleteTestProvider{name: "fake", delete: func(string) error { calls++; return nil }})
	retryErr := m.DeleteEntry(e.InfoHash, true)
	requestCleanupRetry(t, m, "retry")
	m.retryCleanupWorkflows()
	admins, err := m.storage.MainDeletionPlacementIntents()
	if err != nil {
		t.Fatal(err)
	}
	rows, err := m.storage.QueuedDeletionIntents()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("healthy-provider retry=%v, delete calls=%d, admin obligations=%d, queue obligations=%d", retryErr, calls, len(admins), len(rows))
	if retryErr != nil {
		t.Fatal(retryErr)
	}
	if len(rows) != 0 {
		t.Error("queue cleanup was not completed")
	}
	if _, err := m.storage.Get(e.InfoHash); !storage.IsEntryNotFound(err) {
		t.Errorf("administrative main retirement was not completed: %v", err)
	}
	if len(admins) != 0 {
		t.Error("neither original delete nor operator retry can discharge the admin obligation")
	}
}

func TestCleanupRegressionRecoveryRequiresAuthoritativeProviderAbsence(t *testing.T) {
	for _, kind := range []string{"premiumize-list-miss", "realdebrid-proxy-404"} {
		t.Run(kind, func(t *testing.T) {
			m := cleanupManager(t, t.TempDir())
			requests := []string{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests = append(requests, r.Method+" "+r.URL.Path)
				if kind == "premiumize-list-miss" {
					w.Header().Set("Content-Type", "application/json")
					w.Write([]byte(`{"status":"success","transfers":[]}`))
					return
				}
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("<html>proxy route not found</html>"))
			}))
			defer server.Close()
			cfg := config.Debrid{Name: "fixture", APIKey: "account-one"}
			var client common.Client
			if kind == "premiumize-list-miss" {
				p, err := premiumize.New(cfg, nil)
				if err != nil {
					t.Fatal(err)
				}
				p.Host = server.URL
				client = p
			} else {
				p, err := realdebrid.New(cfg, nil)
				if err != nil {
					t.Fatal(err)
				}
				p.Host = server.URL
				client = p
			}
			m.clients.Store("fixture", client)
			e := &storage.Entry{InfoHash: "uncertain-delete", Name: "release", Protocol: config.ProtocolTorrent, SavePath: m.config.DownloadFolder, State: storage.EntryStateDownloading, Providers: map[string]*storage.ProviderEntry{"fixture": {Provider: "fixture", ID: "still-existing-transfer"}}}
			output, _, err := claimTorrentEntryDirectory(m.config.DownloadFolder, e, torrentLegacyProof{})
			if err != nil {
				t.Fatal(err)
			}
			os.WriteFile(filepath.Join(output, "payload"), []byte("keep while remote deletion is uncertain"), 0600)
			i := cleanupIntent(t, m, e, true)
			i, err = m.storage.ClaimCleanup(i.InfoHash, i.QueueIncarnation, "crashed-owner", m.cleanupAccounts(i), time.Now(), false)
			if err != nil {
				t.Fatal(err)
			}
			if err := m.storage.StartCleanup(i); err != nil {
				t.Fatal(err)
			}
			if err := m.storage.CleanupEffectStarted(i.InfoHash, i.QueueIncarnation, i.Work.Token, time.Now()); err != nil {
				t.Fatal(err)
			}
			rows, _ := m.storage.QueuedDeletionIntents()
			err = m.runCleanupIntent(rows[0], true)
			rows, _ = m.storage.QueuedDeletionIntents()
			_, pathErr := os.Stat(output)
			t.Logf("recovery=%v requests=%v retained intents=%d output exists=%v", err, requests, len(rows), pathErr == nil)
			if len(rows) == 0 {
				t.Error("unverified provider absence discharged the cleanup obligation")
			}
			if pathErr != nil {
				t.Error("unverified provider absence authorized local deletion")
			}
		})
	}
}
