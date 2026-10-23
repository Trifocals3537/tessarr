package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/pkg/debrid/providers/realdebrid"
	"github.com/Trifocals3537/tessarr/pkg/debrid/providers/torbox"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
)

func TestRealDebridAcknowledgedCacheMissResumesFallback(t *testing.T) {
	for _, recovering := range []bool{false, true} {
		name := "fresh"
		if recovering {
			name = "recovering"
		}
		t.Run(name, func(t *testing.T) {
			var selections, deletes atomic.Int32
			m, req := submissionTestManager(t, t.TempDir())
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/torrents/addMagnet":
					_, _ = w.Write([]byte(`{"id":"acknowledged"}`))
				case r.Method == http.MethodGet && r.URL.Path == "/torrents/info/acknowledged":
					status := "waiting_files_selection"
					if selections.Load() > 0 {
						status = "downloading"
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"id": "acknowledged", "hash": req.Magnet.InfoHash, "filename": "Example", "status": status,
						"files": []map[string]any{{"id": 1, "path": "/Example.mkv", "bytes": 1000000000, "selected": 0}}})
				case r.Method == http.MethodPost && r.URL.Path == "/torrents/selectFiles/acknowledged":
					selections.Add(1)
					w.WriteHeader(http.StatusNoContent)
				case r.Method == http.MethodDelete && r.URL.Path == "/torrents/delete/acknowledged":
					deletes.Add(1)
					w.WriteHeader(http.StatusNoContent)
				default:
					t.Errorf("unexpected fixture request: %s %s", r.Method, r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			provider, err := realdebrid.New(config.Debrid{Name: "rd", Provider: "realdebrid", APIKey: "fixture-key"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			provider.Host = server.URL
			var fallback atomic.Int32
			second := &routingTestClient{cfg: config.Debrid{Name: "second"}, submit: func(remote *debridTypes.Torrent) (*debridTypes.Torrent, error) {
				fallback.Add(1)
				remote.Id, remote.Debrid, remote.Status = "fallback", "second", debridTypes.TorrentStatusDownloaded
				return remote, nil
			}}
			m.clients.Store("rd", provider)
			m.clients.Store("second", second)
			m.config.Debrids = []config.Debrid{provider.Config(), second.Config()}
			entry := submissionTestEntry(t, m, req)
			if recovering {
				intent, _, err := m.storage.BeginProviderSubmission(entry, "rd", submissionAccount(provider.Config()), false)
				if err != nil {
					t.Fatal(err)
				}
				if err := m.storage.RecordProviderSubmissionID(entry, intent.Attempt, "acknowledged", false); err != nil {
					t.Fatal(err)
				}
			}
			remote, err := m.sendToDebrid(t.Context(), req, entry)
			if err != nil || remote == nil || deletes.Load() != 1 || fallback.Load() != 1 {
				t.Fatalf("real RD cached-only recovery stranded: remote=%v err=%v selections=%d deletes=%d fallback=%d", remote, err, selections.Load(), deletes.Load(), fallback.Load())
			}
		})
	}
}

func TestAcknowledgedRefusalRecoveryPreservesCleanupFences(t *testing.T) {
	for _, scenario := range []string{"cache miss", "slot refusal", "terminal", "transport failure", "cleanup failure", "canceled", "changed ID", "changed hash"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var deletes, fallback int
			first := &routingTestClient{cfg: config.Debrid{Name: "torbox", APIKey: "fixture-key"}}
			second := &routingTestClient{cfg: config.Debrid{Name: "second"}, submit: func(remote *debridTypes.Torrent) (*debridTypes.Torrent, error) {
				fallback++
				remote.Id, remote.Debrid, remote.Status = "fallback", "second", debridTypes.TorrentStatusDownloaded
				return remote, nil
			}}
			m, req := submissionTestManager(t, t.TempDir(), first, second)
			entry := submissionTestEntry(t, m, req)
			intent, _, err := m.storage.BeginProviderSubmission(entry, "torbox", submissionAccount(first.cfg), false)
			if err != nil {
				t.Fatal(err)
			}
			if err := m.storage.RecordProviderSubmissionID(entry, intent.Attempt, "ack", false); err != nil {
				t.Fatal(err)
			}
			first.get = func(id string) (*debridTypes.Torrent, error) { return submittedTestTorrent(entry.InfoHash, id), nil }
			first.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
				t.Error("recreated the resolved cached attempt")
				return nil, errors.New("unexpected create")
			}
			first.check = func(remote *debridTypes.Torrent) (*debridTypes.Torrent, error) {
				cause := error(customerror.NewTorrentNotCachedError("Example"))
				switch scenario {
				case "slot refusal":
					cause = customerror.TooManyActiveDownloadsError
				case "terminal":
					cause = fmt.Errorf("%w: fixture terminal state", debridTypes.ErrTerminalProviderTorrent)
				case "transport failure":
					cause = errors.New("unknown response")
				case "canceled":
					cancel()
				case "changed ID":
					remote.Id = "different"
				case "changed hash":
					remote.InfoHash = strings.Repeat("b", 40)
				}
				return remote, cause
			}
			first.delete = func(string) error {
				deletes++
				if scenario == "cleanup failure" {
					return errors.New("delete response lost")
				}
				return nil
			}
			remote, err := m.sendToDebrid(ctx, req, entry)
			resolved := scenario == "cache miss" || scenario == "slot refusal" || scenario == "terminal"
			if resolved {
				if err != nil || remote == nil || fallback != 1 || deletes != 1 {
					t.Fatalf("definite refusal did not resolve: %v %v deletes=%d fallback=%d", remote, err, deletes, fallback)
				}
			} else {
				if !errors.Is(err, ErrProviderSubmissionPending) || fallback != 0 {
					t.Fatalf("uncertain refusal authorized fallback: %v fallback=%d", err, fallback)
				}
				wantDeletes := 0
				if scenario == "cleanup failure" {
					wantDeletes = 1
				}
				if deletes != wantDeletes {
					t.Fatalf("unsafe cleanup count %d, want %d", deletes, wantDeletes)
				}
				if scenario == "cleanup failure" {
					if _, err := m.sendToDebrid(t.Context(), req, entry); !errors.Is(err, ErrProviderSubmissionPending) || deletes != 1 {
						t.Fatalf("ambiguous rollback replayed: %v deletes=%d", err, deletes)
					}
				}
			}
		})
	}
}

func TestQueuedPolicyRestorePreservesTriStateAndLegacyBoolean(t *testing.T) {
	for _, policy := range []storage.UncachedPolicy{"", storage.UncachedPolicyInherit, storage.UncachedPolicyAllow, storage.UncachedPolicyDeny, "invalid"} {
		for _, effective := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/effective=%t", policy, effective), func(t *testing.T) {
				m, req := submissionTestManager(t, t.TempDir())
				entry := newTorrentQueueEntry(req, debridTypes.TorrentStatusQueued)
				entry.DownloadUncached, entry.AdmissionUncachedPolicy = effective, policy
				if err := m.queue.Add(entry); err != nil {
					t.Fatal(err)
				}
				current, err := m.queue.GetTorrent(entry.InfoHash)
				if err != nil {
					t.Fatal(err)
				}
				job, err := m.rebuildQueuedTorrentJob(current)
				if policy == "invalid" {
					if err == nil {
						t.Fatal("invalid policy silently enabled provider work")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if policy == storage.UncachedPolicyInherit {
					if job.Request.DownloadUncached != nil {
						t.Fatal("inherited policy acquired an explicit override")
					}
					return
				}
				want := effective
				if policy == storage.UncachedPolicyAllow {
					want = true
				}
				if policy == storage.UncachedPolicyDeny {
					want = false
				}
				if job.Request.DownloadUncached == nil || *job.Request.DownloadUncached != want {
					t.Fatalf("restored policy = %v, want %t", job.Request.DownloadUncached, want)
				}
			})
		}
	}
}

func TestCopiedPlacementCleanupKeepsAccountFenceAfterParentRetires(t *testing.T) {
	var deletes int
	provider := &routingTestClient{cfg: config.Debrid{Name: "torbox", APIKey: "original"}, delete: func(string) error { deletes++; return nil }}
	m, req := submissionTestManager(t, t.TempDir(), provider)
	parent := submissionTestEntry(t, m, req)
	intent, _, err := m.storage.BeginProviderSubmission(parent, "torbox", submissionAccount(provider.cfg), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storage.RecordProviderSubmissionID(parent, intent.Attempt, "42", false); err != nil {
		t.Fatal(err)
	}
	bound, err := m.storage.BindProviderSubmission(parent, intent.Attempt, submittedTestTorrent(parent.InfoHash, "42"))
	if err != nil {
		t.Fatal(err)
	}
	refreshSubmissionEntry(parent, bound)
	child := convertToMultiSeason(parent, []SeasonInfo{{InfoHash: strings.Repeat("b", 40), Name: "Example S01"}})[0]
	if err := m.queue.Add(child); err != nil {
		t.Fatal(err)
	}
	if err := m.queue.DeleteEntryOnly(parent.InfoHash); err != nil {
		t.Fatal(err)
	}
	provider.cfg.APIKey = "replacement"
	if err := m.RemoveTorrentPlacements(child); !errors.Is(err, storage.ErrProviderPlacementAccount) || deletes != 0 {
		t.Fatalf("copied cleanup crossed account fence: %v deletes=%d", err, deletes)
	}
	provider.cfg.APIKey = "original"
	if err := m.RemoveTorrentPlacements(child); err != nil || deletes != 1 {
		t.Fatalf("acknowledged original account lost cleanup behavior: %v deletes=%d", err, deletes)
	}
}

func TestTorboxSeasonChildCannotDeleteInReplacementAccount(t *testing.T) {
	root := t.TempDir()
	var reads, deletes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") != "Bearer replacement-fixture-key" {
			t.Error("fixture did not receive replacement-account authorization")
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/torrents/mylist":
			reads.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]any{"id": 42, "hash": strings.Repeat("c", 40), "name": "Unrelated account transfer", "download_state": "error", "download_finished": false, "files": []any{}}})
		case r.Method == http.MethodPost && r.URL.Path == "/api/torrents/controltorrent":
			deletes.Add(1)
			_, _ = w.Write([]byte(`{"success":true}`))
		default:
			t.Errorf("unexpected fixture request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	cfg := config.Debrid{Name: "torbox", Provider: "torbox", APIKey: "original-fixture-key"}
	provider, err := torbox.New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	provider.Host = server.URL
	m, req := submissionTestManager(t, root, provider)
	parent := submissionTestEntry(t, m, req)
	intent, _, err := m.storage.BeginProviderSubmission(parent, "torbox", submissionAccount(cfg), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storage.RecordProviderSubmissionID(parent, intent.Attempt, "42", false); err != nil {
		t.Fatal(err)
	}
	bound, err := m.storage.BindProviderSubmission(parent, intent.Attempt, submittedTestTorrent(parent.InfoHash, "42"))
	if err != nil {
		t.Fatal(err)
	}
	refreshSubmissionEntry(parent, bound)
	child := convertToMultiSeason(parent, []SeasonInfo{{SeasonNumber: 1, InfoHash: strings.Repeat("b", 40), Name: "Example S01"}})[0]
	if err := m.queue.Add(child); err != nil {
		t.Fatal(err)
	}
	if err := m.storage.Close(); err != nil {
		t.Fatal(err)
	}
	cfg.APIKey = "replacement-fixture-key"
	replacement, err := torbox.New(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	replacement.Host = server.URL
	restarted, _ := submissionTestManager(t, root, replacement)
	restarted.processingEntries = xsync.NewMap[string, struct{}]()
	if err := restarted.recoverInterruptedDownloads(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted.processQueuedEntries(t.Context())
	restarted.background.Wait()
	if reads.Load() != 0 || deletes.Load() != 0 {
		t.Fatalf("real TorBox client crossed original account fence: reads=%d destructive POSTs=%d", reads.Load(), deletes.Load())
	}
}

func TestAcknowledgedCacheMissResumesCachedFirstFallback(t *testing.T) {
	for _, recovering := range []bool{false, true} {
		name := "fresh"
		if recovering {
			name = "recovering"
		}
		t.Run(name, func(t *testing.T) {
			var deletes, fallback int
			first := &routingTestClient{cfg: config.Debrid{Name: "torbox", APIKey: "fixture-key"}}
			first.submit = func(remote *debridTypes.Torrent) (*debridTypes.Torrent, error) {
				remote.Id, remote.Debrid = "acknowledged", "torbox"
				return remote, nil
			}
			first.check = func(remote *debridTypes.Torrent) (*debridTypes.Torrent, error) {
				return remote, customerror.NewTorrentNotCachedError(remote.Name)
			}
			first.delete = func(string) error { deletes++; return nil }
			second := &routingTestClient{cfg: config.Debrid{Name: "second"}}
			second.submit = func(remote *debridTypes.Torrent) (*debridTypes.Torrent, error) {
				fallback++
				remote.Id, remote.Debrid, remote.Status = "fallback", "second", debridTypes.TorrentStatusDownloaded
				return remote, nil
			}
			m, req := submissionTestManager(t, t.TempDir(), first, second)
			entry := submissionTestEntry(t, m, req)
			first.get = func(id string) (*debridTypes.Torrent, error) { return submittedTestTorrent(entry.InfoHash, id), nil }
			if recovering {
				intent, _, err := m.storage.BeginProviderSubmission(entry, "torbox", submissionAccount(first.cfg), false)
				if err != nil {
					t.Fatal(err)
				}
				if err := m.storage.RecordProviderSubmissionID(entry, intent.Attempt, "acknowledged", false); err != nil {
					t.Fatal(err)
				}
			}
			remote, err := m.sendToDebrid(t.Context(), req, entry)
			if err != nil || remote == nil || remote.Id != "fallback" || deletes != 1 || fallback != 1 {
				t.Fatalf("definite cache miss failed to finish owned rollback and fallback: remote=%v err=%v deletes=%d fallback=%d", remote, err, deletes, fallback)
			}
		})
	}
}

func TestSeasonChildRetainsAccountFenceAfterRestart(t *testing.T) {
	root := t.TempDir()
	var checks, deletes atomic.Int32
	provider := &routingTestClient{cfg: config.Debrid{Name: "torbox", APIKey: "original-fixture-key"}}
	provider.check = func(remote *debridTypes.Torrent) (*debridTypes.Torrent, error) {
		checks.Add(1)
		remote.Status = debridTypes.TorrentStatusError
		return remote, errors.New("fixture: transfer in replacement account failed")
	}
	provider.delete = func(string) error { deletes.Add(1); return nil }
	m, req := submissionTestManager(t, root, provider)
	parent := submissionTestEntry(t, m, req)
	intent, _, err := m.storage.BeginProviderSubmission(parent, "torbox", submissionAccount(provider.cfg), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storage.RecordProviderSubmissionID(parent, intent.Attempt, "42", false); err != nil {
		t.Fatal(err)
	}
	bound, err := m.storage.BindProviderSubmission(parent, intent.Attempt, submittedTestTorrent(parent.InfoHash, "42"))
	if err != nil {
		t.Fatal(err)
	}
	refreshSubmissionEntry(parent, bound)
	child := convertToMultiSeason(parent, []SeasonInfo{{SeasonNumber: 1, InfoHash: strings.Repeat("b", 40), Name: "Example S01"}})[0]
	if err := m.queue.Add(child); err != nil {
		t.Fatal(err)
	}
	if err := m.storage.Close(); err != nil {
		t.Fatal(err)
	}
	provider.cfg.APIKey = "replacement-fixture-key"
	restarted, _ := submissionTestManager(t, root, provider)
	restarted.processingEntries = xsync.NewMap[string, struct{}]()
	if err := restarted.recoverInterruptedDownloads(t.Context()); err != nil {
		t.Fatal(err)
	}
	currentParent, err := restarted.queue.GetTorrent(parent.InfoHash)
	if err != nil || currentParent.State != storage.EntryStateError {
		t.Fatalf("control: parent did not reject changed account: %v %v", currentParent, err)
	}
	restarted.processQueuedEntries(t.Context())
	restarted.background.Wait()
	if checks.Load() != 0 || deletes.Load() != 0 {
		t.Fatalf("season child used the old provider ID in the replacement account: checks=%d deletes=%d", checks.Load(), deletes.Load())
	}
}

func TestRestartPreservesInheritedUncachedPolicy(t *testing.T) {
	for _, restart := range []bool{false, true} {
		name := "fresh"
		if restart {
			name = "restart"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			var uncached int
			provider := &routingTestClient{cfg: config.Debrid{Name: "torbox", APIKey: "fixture-key", DownloadUncached: true}}
			provider.submit = func(remote *debridTypes.Torrent) (*debridTypes.Torrent, error) {
				if !remote.DownloadUncached {
					return nil, customerror.NewTorrentNotCachedError(remote.Name)
				}
				uncached++
				remote.Id, remote.Debrid, remote.Status = "accepted", "torbox", debridTypes.TorrentStatusDownloading
				return remote, nil
			}
			m, req := submissionTestManager(t, root, provider)
			if req.DownloadUncached != nil {
				t.Fatal("fixture did not inherit provider policy")
			}
			req.Magnet.Link += "&dn=Example"
			entry := submissionTestEntry(t, m, req)
			if restart {
				if err := m.storage.Close(); err != nil {
					t.Fatal(err)
				}
				m, _ = submissionTestManager(t, root, provider)
				if err := m.recoverInterruptedDownloads(context.Background()); err != nil {
					t.Fatal(err)
				}
				var err error
				entry, err = m.queue.GetTorrent(entry.InfoHash)
				if err != nil {
					t.Fatal(err)
				}
				job, err := m.rebuildQueuedTorrentJob(entry)
				if err != nil {
					t.Fatal(err)
				}
				req = job.Request
			}
			remote, err := m.sendToDebrid(t.Context(), req, entry)
			if err != nil || remote == nil || uncached != 1 {
				t.Fatalf("provider inheritance changed after durable restore: override=%v uncached=%d remote=%v err=%v", req.DownloadUncached, uncached, remote, err)
			}
		})
	}
}
