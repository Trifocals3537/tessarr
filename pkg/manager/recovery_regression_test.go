package manager

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/debrid/providers/alldebrid"
	"github.com/Trifocals3537/tessarr/pkg/debrid/providers/debridlink"
	"github.com/Trifocals3537/tessarr/pkg/debrid/providers/premiumize"
	"github.com/Trifocals3537/tessarr/pkg/debrid/providers/realdebrid"
	"github.com/Trifocals3537/tessarr/pkg/debrid/providers/torbox"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/notifications"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

func TestAcknowledgedRealDebridRecoveryResumesFileSelection(t *testing.T) {
	m, req := submissionTestManager(t, t.TempDir())
	var selections, reads atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/torrents/info/acknowledged":
			reads.Add(1)
			status := "waiting_files_selection"
			if selections.Load() != 0 {
				status = "downloading"
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"id": "acknowledged", "hash": req.Magnet.InfoHash, "filename": "Example", "status": status,
				"files": []map[string]any{{"id": 1, "path": "/Example.mkv", "bytes": 1000000000, "selected": 0}}})
		case r.Method == http.MethodPost && r.URL.Path == "/torrents/selectFiles/acknowledged":
			if reads.Load() < 2 {
				t.Error("selection preceded read-only identity verification")
			}
			selections.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected provider operation: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	provider, err := realdebrid.New(config.Debrid{Name: "rd", Provider: "realdebrid", APIKey: "fixture-key"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	provider.Host = server.URL
	m.clients.Store("rd", provider)
	m.config.Debrids = []config.Debrid{provider.Config()}
	entry := submissionTestEntry(t, m, req)
	intent, _, err := m.storage.BeginProviderSubmission(entry, "rd", submissionAccount(provider.Config()), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storage.RecordProviderSubmissionID(entry, intent.Attempt, "acknowledged", false); err != nil {
		t.Fatal(err)
	}
	remote, err := m.sendToDebrid(t.Context(), req, entry)
	if err != nil {
		t.Fatal(err)
	}
	if remote.Id != "acknowledged" || remote.Status != debridTypes.TorrentStatusDownloading || selections.Load() != 1 {
		t.Fatalf("active transfer did not resume selection: %+v selections=%d", remote, selections.Load())
	}
	intent, err = m.storage.GetProviderSubmission(entry)
	if err != nil || intent.Phase != storage.SubmissionBound || intent.Recovered {
		t.Fatalf("acknowledged ownership was lost: %+v %v", intent, err)
	}
}

func TestAcknowledgedRecoveryRequiresReadOnlyIdentityBeforeStatus(t *testing.T) {
	for _, scenario := range []string{"wrong hash", "missing hash", "wrong ID", "wrong provider", "lookup error", "missing transfer"} {
		t.Run(scenario, func(t *testing.T) {
			provider := &routingTestClient{cfg: config.Debrid{Name: "torbox", APIKey: "fixture-key"}}
			m, req := submissionTestManager(t, t.TempDir(), provider)
			entry := submissionTestEntry(t, m, req)
			intent, _, err := m.storage.BeginProviderSubmission(entry, "torbox", submissionAccount(provider.cfg), true)
			if err != nil {
				t.Fatal(err)
			}
			if err := m.storage.RecordProviderSubmissionID(entry, intent.Attempt, "known", false); err != nil {
				t.Fatal(err)
			}
			provider.get = func(id string) (*debridTypes.Torrent, error) {
				if id != "known" {
					t.Errorf("lookup ID=%s", id)
				}
				remote := submittedTestTorrent(entry.InfoHash, id)
				switch scenario {
				case "wrong hash":
					remote.InfoHash = strings.Repeat("b", 40)
				case "missing hash":
					remote.InfoHash = ""
				case "wrong ID":
					remote.Id = "reused"
				case "wrong provider":
					remote.Debrid = "other"
				case "lookup error":
					return nil, errors.New("unavailable")
				case "missing transfer":
					return nil, nil
				}
				return remote, nil
			}
			provider.check = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
				t.Error("unverified transfer reached mutating status processing")
				return nil, nil
			}
			provider.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
				t.Error("recovery created another transfer")
				return nil, nil
			}
			provider.delete = func(string) error { t.Error("recovery deleted unverified transfer"); return nil }
			if _, err := m.sendToDebrid(t.Context(), req, entry); !errors.Is(err, ErrProviderSubmissionPending) {
				t.Fatalf("expected pending recovery, got %v", err)
			}
		})
	}
}

func TestAcknowledgedRecoveryUsesActiveTransferLookupForEveryProvider(t *testing.T) {
	for _, tc := range []struct{ name, path, payload string }{
		{"realdebrid", "/torrents/info/42", `{"id":"42","hash":"HASH","filename":"Example","status":"downloading","files":[]}`},
		{"alldebrid", "/magnet/status", `{"status":"success","data":{"magnets":[{"id":42,"hash":"HASH","filename":"Example","size":100,"statusCode":1}]}}`},
		{"debridlink", "/seedbox/list", `{"success":true,"value":[{"id":"42","hashString":"HASH","name":"Example","status":0,"files":[]}]}`},
		{"premiumize", "/api/transfer/list", `{"status":"success","transfers":[{"id":"42","name":"Example","src":"magnet:?xt=urn:btih:HASH","status":"running","progress":0.5}]}`},
		{"torbox", "/api/torrents/mylist", `{"success":true,"data":{"id":42,"hash":"HASH","name":"Example","download_state":"downloading","download_finished":false,"files":[]}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, req := submissionTestManager(t, t.TempDir())
			var reads atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != tc.path {
					t.Errorf("unexpected operation: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if tc.name == "alldebrid" && (r.URL.Query().Get("id") != "42" || r.URL.Query().Has("status")) {
					t.Error("used completed-library listing")
				}
				if tc.name == "debridlink" && r.URL.Query().Get("ids") != "42" {
					t.Error("lookup not scoped to saved ID")
				}
				if tc.name == "torbox" && r.URL.Query().Get("id") != "42" {
					t.Error("lookup not scoped to saved ID")
				}
				reads.Add(1)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(strings.ReplaceAll(tc.payload, "HASH", req.Magnet.InfoHash)))
			}))
			defer server.Close()
			cfg := config.Debrid{Name: tc.name, Provider: tc.name, APIKey: "fixture-key"}
			var provider debrid.Client
			var err error
			switch tc.name {
			case "realdebrid":
				p, e := realdebrid.New(cfg, nil)
				if e == nil {
					p.Host = server.URL
				}
				provider, err = p, e
			case "alldebrid":
				p, e := alldebrid.New(cfg, nil)
				if e == nil {
					p.Host = server.URL
				}
				provider, err = p, e
			case "debridlink":
				p, e := debridlink.New(cfg, nil)
				if e == nil {
					p.Host = server.URL
				}
				provider, err = p, e
			case "premiumize":
				p, e := premiumize.New(cfg, nil)
				if e == nil {
					p.Host = server.URL
				}
				provider, err = p, e
			case "torbox":
				p, e := torbox.New(cfg, nil)
				if e == nil {
					p.Host = server.URL
				}
				provider, err = p, e
			}
			if err != nil {
				t.Fatal(err)
			}
			m.clients.Store(cfg.Name, provider)
			m.config.Debrids = []config.Debrid{cfg}
			entry := submissionTestEntry(t, m, req)
			intent, _, err := m.storage.BeginProviderSubmission(entry, cfg.Name, submissionAccount(cfg), true)
			if err != nil {
				t.Fatal(err)
			}
			if err := m.storage.RecordProviderSubmissionID(entry, intent.Attempt, "42", false); err != nil {
				t.Fatal(err)
			}
			remote, err := m.sendToDebrid(t.Context(), req, entry)
			if err != nil || remote == nil || remote.Id != "42" || remote.Status != debridTypes.TorrentStatusDownloading {
				t.Fatalf("active recovery failed: %+v %v", remote, err)
			}
			if reads.Load() < 2 {
				t.Fatal("status processing omitted identity lookup")
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			before := reads.Load()
			if _, err := getProviderTorrent(ctx, provider, "42"); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled lookup: %v", err)
			}
			if reads.Load() != before {
				t.Fatal("canceled lookup reached provider")
			}
		})
	}
}

func TestCanceledSynchronousImportTransfersReservationToRecovery(t *testing.T) {
	for _, acknowledged := range []bool{false, true} {
		t.Run(map[bool]string{false: "unknown ID", true: "acknowledged ID"}[acknowledged], func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var creates atomic.Int32
			provider := &submissionTestProvider{routingTestClient: &routingTestClient{cfg: config.Debrid{Name: "torbox"}}}
			m, req := submissionTestManager(t, t.TempDir(), provider)
			m.processingEntries = xsync.NewMap[string, struct{}]()
			m.Notifications = notifications.New(&config.Get().Notifications, zerolog.Nop())
			m.downloader = &Downloader{manager: m, dest: m.config.DownloadFolder, logger: zerolog.Nop()}
			remote := submittedTestTorrent(req.Magnet.InfoHash, "accepted")
			provider.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
				creates.Add(1)
				cancel()
				if acknowledged {
					return remote.Copy(), nil
				}
				return nil, context.Canceled
			}
			provider.get = func(string) (*debridTypes.Torrent, error) { return remote.Copy(), nil }
			provider.list = func() ([]*debridTypes.Torrent, error) { return []*debridTypes.Torrent{remote.Copy()}, nil }
			provider.check = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) { return remote.Copy(), nil }
			provider.delete = func(string) error { t.Error("cancellation authorized provider deletion"); return nil }
			started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
			m.jobQueue = NewJobQueueWithCapacity(m.ctx, 1, 1, func(workCtx context.Context, job *Job) {
				defer close(done)
				close(started)
				select {
				case <-release:
				case <-workCtx.Done():
					return
				}
				if workCtx.Err() != nil {
					t.Error("recovery inherited HTTP cancellation")
					return
				}
				m.processJob(workCtx, job)
			}, m.entryLifecycle)
			t.Cleanup(func() { m.jobQueue.Close(); m.background.Wait() })
			if err := m.AddNewTorrent(ctx, req); !errors.Is(err, context.Canceled) {
				t.Fatalf("unexpected import result: %v", err)
			}
			awaitAsyncAdmissionValue(t, started, "service-owned recovery")
			if m.jobQueue.OutstandingCount() != 1 {
				t.Fatal("recovery released bounded admission")
			}
			if err := m.AddNewTorrent(t.Context(), req); !errors.Is(err, ErrJobQueueDuplicate) {
				t.Fatalf("duplicate admission was allowed: %v", err)
			}
			close(release)
			awaitAsyncAdmissionValue(t, done, "recovered completion")
			entry, err := m.queue.GetTorrent(req.Magnet.InfoHash)
			if err != nil || !entry.IsComplete || entry.LastError != "" {
				t.Fatalf("recovery did not complete: %+v %v", entry, err)
			}
			if creates.Load() != 1 {
				t.Fatalf("provider created %d transfers", creates.Load())
			}
		})
	}
}

func TestMultiSeasonChildClaimExcludesConcurrentScheduler(t *testing.T) {
	var checks atomic.Int32
	provider := &routingTestClient{cfg: config.Debrid{Name: "torbox"}, check: func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		checks.Add(1)
		return nil, errors.New("fixture status failure")
	}}
	m, req := submissionTestManager(t, t.TempDir(), provider)
	m.processingEntries = xsync.NewMap[string, struct{}]()
	m.Notifications = notifications.New(&config.Get().Notifications, zerolog.Nop())
	m.config.Mount.MountPath = t.TempDir()
	parent := newTorrentQueueEntry(req, debridTypes.TorrentStatusDownloaded)
	parent.ActiveProvider = "torbox"
	parent.Providers["torbox"] = &storage.ProviderEntry{Provider: "torbox", ID: "parent-transfer"}
	parent.Name, parent.OriginalFilename = "Example Complete Series", "Example Complete Series"
	parent.OutputName = storage.NewTorrentOutputName(parent.Name, parent.InfoHash)
	parent.Action = config.DownloadActionSymlink
	parent.Files = map[string]*storage.File{
		"Example.S01E01.mkv": {Name: "Example.S01E01.mkv", Path: "Example.S01E01.mkv", Size: 100, InfoHash: parent.InfoHash},
		"Example.S02E01.mkv": {Name: "Example.S02E01.mkv", Path: "Example.S02E01.mkv", Size: 100, InfoHash: parent.InfoHash},
	}
	if err := m.queue.Add(parent); err != nil {
		t.Fatal(err)
	}
	d := &Downloader{manager: m, dest: m.config.DownloadFolder, logger: zerolog.Nop()}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- d.download(ctx, parent) }()
	defer func() { cancel(); <-done; m.background.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		entries := m.queue.ListFilter("", config.ProtocolAll, storage.EntryStateDownloading, nil, "", true)
		if len(entries) > 1 {
			for _, child := range entries {
				if !child.IsDownloading {
					t.Fatalf("unclaimed child was published: %s", child.InfoHash)
				}
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("downloader did not publish a child")
		}
		time.Sleep(time.Millisecond)
	}
	for range 10 {
		m.processQueuedEntries(t.Context())
	}
	if m.processingEntries.Size() != 0 || checks.Load() != 0 {
		t.Fatal("scheduler processed a child already owned by the parent")
	}
}

func TestSynchronousCancellationPreservesShutdownAndDeletionFences(t *testing.T) {
	for _, cause := range []string{"shutdown", "delete", "closed queue", "failed checkpoint", "unavailable storage"} {
		t.Run(cause, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			provider := &routingTestClient{cfg: config.Debrid{Name: "torbox"}}
			root := t.TempDir()
			m, req := submissionTestManager(t, root, provider)
			var workers atomic.Int32
			m.jobQueue = NewJobQueueWithCapacity(m.ctx, 1, 1, func(context.Context, *Job) { workers.Add(1) }, m.entryLifecycle)
			t.Cleanup(m.jobQueue.Close)
			var deletion *entryDeleteLease
			provider.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
				switch cause {
				case "shutdown":
					m.cancel()
				case "delete":
					var err error
					deletion, err = m.entryLifecycle.beginDelete(req.Magnet.InfoHash)
					if err != nil {
						t.Fatal(err)
					}
				case "closed queue", "failed checkpoint":
					m.jobQueue.Close()
				case "unavailable storage":
					if err := m.storage.Close(); err != nil {
						t.Fatal(err)
					}
				}
				if cause == "failed checkpoint" {
					m.queue.outcomes = &failingOutcomeStore{err: errors.New("fixture sync failure")}
				}
				cancel()
				return nil, context.Canceled
			}
			if err := m.AddNewTorrent(ctx, req); !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
			if workers.Load() != 0 || m.jobQueue.OutstandingCount() != 0 {
				t.Fatal("shutdown/deletion/closed queue started recovery")
			}
			if deletion != nil {
				if err := deletion.Wait(t.Context()); err != nil {
					t.Fatal(err)
				}
				deletion.Finish(false)
			}
			if cause == "unavailable storage" {
				reopened, err := storage.NewStorage(filepath.Join(root, "db"))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = reopened.Close() })
				m.storage, m.queue.storage = reopened, reopened
			}
			entry, err := m.queue.GetTorrent(req.Magnet.InfoHash)
			if err != nil {
				t.Fatal(err)
			}
			if cause == "closed queue" {
				if entry.State != storage.EntryStateError || !strings.Contains(entry.LastError, ErrProviderSubmissionPending.Error()) {
					t.Fatalf("failed handoff was not durably visible: %+v", entry)
				}
				if err := m.recoverInterruptedDownloads(t.Context()); err != nil {
					t.Fatal(err)
				}
				restored, err := m.queue.GetTorrent(entry.InfoHash)
				if err != nil || restored.Status != debridTypes.TorrentStatusQueued || restored.State != storage.EntryStateDownloading {
					t.Fatalf("failed handoff cannot resume: %+v %v", restored, err)
				}
			} else if entry.State != storage.EntryStateDownloading {
				t.Fatalf("shutdown/deletion replaced recovery input: %+v", entry)
			}
			if cause == "failed checkpoint" || cause == "unavailable storage" {
				if !errors.Is(m.workflowAvailable(), ErrWorkflowUnavailable) {
					t.Fatal("failed checkpoint did not latch admission")
				}
			} else if m.workflowAvailable() != nil {
				t.Fatal("ordinary cancellation tripped storage fault")
			}
		})
	}
}

func TestMultiSeasonChildClaimsResetAfterRestart(t *testing.T) {
	root := t.TempDir()
	m, req := submissionTestManager(t, root)
	parent := newTorrentQueueEntry(req, debridTypes.TorrentStatusDownloaded)
	children := convertToMultiSeason(parent, []SeasonInfo{{InfoHash: strings.Repeat("b", 40), Name: "Example S01"}, {InfoHash: strings.Repeat("c", 40), Name: "Example S02"}})
	for _, child := range children {
		if !child.IsDownloading {
			t.Fatal("producer omitted child claim")
		}
		if err := m.queue.Add(child); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storage.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, _ := submissionTestManager(t, root)
	if err := restarted.recoverInterruptedDownloads(t.Context()); err != nil {
		t.Fatal(err)
	}
	for _, child := range children {
		current, err := restarted.queue.GetTorrent(child.InfoHash)
		if err != nil || current.IsDownloading || current.State != storage.EntryStateDownloading {
			t.Fatalf("restart stranded child: %+v %v", current, err)
		}
	}
}
