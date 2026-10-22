package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/request"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

type submissionTestProvider struct {
	*routingTestClient
	list func() ([]*debridTypes.Torrent, error)
}

type httpSubmissionTestProvider struct {
	*submissionTestProvider
	client *request.Client
	url    string
}

func (p *httpSubmissionTestProvider) SubmitMagnetContext(ctx context.Context, _ *debridTypes.Torrent) (*debridTypes.Torrent, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.url, strings.NewReader("magnet"))
	if err != nil {
		return nil, err
	}
	res, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	return nil, fmt.Errorf("provider create returned HTTP %d", res.StatusCode)
}

func (p *submissionTestProvider) GetTorrentsContext(ctx context.Context) ([]*debridTypes.Torrent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return p.list()
}

func submissionTestManager(t *testing.T, root string, providers ...debrid.Client) (*Manager, *ImportRequest) {
	t.Helper()
	oldPath := config.GetMainPath()
	if err := config.SetConfigPath(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = config.SetConfigPath(oldPath) })
	store, err := storage.NewStorage(filepath.Join(root, "db"))
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := newEntryLifecycle()
	m := &Manager{storage: store, queue: newQueue(store, "", lifecycle), entryLifecycle: lifecycle, logger: zerolog.Nop(),
		clients: xsync.NewMap[string, debrid.Client](), arr: arr.NewStorage(), config: &config.Config{DownloadFolder: filepath.Join(root, "downloads")}}
	for _, provider := range providers {
		m.clients.Store(provider.Config().Name, provider)
		m.config.Debrids = append(m.config.Debrids, provider.Config())
	}
	m.resetLifecycle()
	t.Cleanup(func() { m.cancel(); _ = store.Close() })
	hash := strings.Repeat("a", 40)
	req := NewTorrentRequest("", m.config.DownloadFolder, &utils.Magnet{InfoHash: hash, Name: "Example", Link: "magnet:?xt=urn:btih:" + hash},
		&arr.Arr{Name: "sonarr"}, config.DownloadActionNone, nil, "", ImportTypeAPI, false)
	return m, req
}

func submissionTestEntry(t *testing.T, m *Manager, req *ImportRequest) *storage.Entry {
	t.Helper()
	entry := newTorrentQueueEntry(req, debridTypes.TorrentStatusQueued)
	if err := m.queue.Add(entry); err != nil {
		t.Fatal(err)
	}
	return entry
}

func submittedTestTorrent(hash, id string) *debridTypes.Torrent {
	return &debridTypes.Torrent{InfoHash: hash, Id: id, Debrid: "torbox", Name: "Example", Status: debridTypes.TorrentStatusDownloaded,
		Files: map[string]debridTypes.File{"Example.mkv": {Id: "file", Name: "Example.mkv", Size: 100, Link: "https://example.invalid/media"}}}
}

func TestSubmissionAccountTagPinsPrimaryKeyAndProvider(t *testing.T) {
	cfg := config.Debrid{Name: "primary", Provider: "torbox", APIKey: "fixture-original-key"}
	tag := submissionAccount(cfg)
	if !strings.HasPrefix(tag, "hmac-sha256-v1:") || strings.Contains(tag, cfg.APIKey) {
		t.Fatal("account tag is unversioned or exposes the credential")
	}
	cfg.DownloadAPIKeys, cfg.Workers = []string{"different-download-key"}, 17
	if submissionAccount(cfg) != tag {
		t.Fatal("download-only settings changed creation identity")
	}
	cfg.APIKey = "fixture-new-key"
	if submissionAccount(cfg) == tag {
		t.Fatal("credential rotation retained old account authority")
	}
	cfg.APIKey, cfg.Provider = "fixture-original-key", "realdebrid"
	if submissionAccount(cfg) == tag {
		t.Fatal("a different provider retained old account authority")
	}
}

func TestProviderSubmissionTimeoutReconcilesWithoutCreateOrDelete(t *testing.T) {
	for _, matches := range []int{0, 1, 2} {
		t.Run(string(rune('0'+matches)), func(t *testing.T) {
			calls, deletes, fallback := 0, 0, 0
			provider := &submissionTestProvider{routingTestClient: &routingTestClient{cfg: config.Debrid{Name: "torbox", APIKey: "fixture-key"}}}
			provider.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
				calls++
				return nil, context.DeadlineExceeded
			}
			provider.delete = func(string) error { deletes++; return nil }
			provider.check = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
				t.Fatal("hash recovery mutated provider state")
				return nil, nil
			}
			second := &routingTestClient{cfg: config.Debrid{Name: "second"}, submit: func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
				fallback++
				return nil, errors.New("unexpected fallback")
			}}
			m, req := submissionTestManager(t, t.TempDir(), provider, second)
			provider.list = func() ([]*debridTypes.Torrent, error) {
				result := make([]*debridTypes.Torrent, matches)
				for i := range result {
					result[i] = submittedTestTorrent(req.Magnet.InfoHash, string(rune('1'+i)))
				}
				return result, nil
			}
			entry := submissionTestEntry(t, m, req)
			if _, err := m.sendToDebrid(context.Background(), req, entry); !errors.Is(err, ErrProviderSubmissionPending) {
				t.Fatal(err)
			}
			remote, err := m.sendToDebrid(context.Background(), req, entry)
			if matches == 1 {
				if err != nil || remote.Id != "1" || entry.GetActiveProvider() == nil || entry.GetActiveProvider().ID != "1" {
					t.Fatalf("unique completed transfer not recovered: %+v %v", remote, err)
				}
				if err := m.rollbackQueuedProviderTorrent(entry, provider, remote.Id); !errors.Is(err, ErrProviderSubmissionPending) {
					t.Fatal("later polling granted rollback to a hash match", err)
				}
			} else if !errors.Is(err, ErrProviderSubmissionPending) {
				t.Fatalf("ambiguous outcome resumed: %v", err)
			}
			if calls != 1 || fallback != 0 || deletes != 0 {
				t.Fatalf("reconciliation created/deleted work: creates=%d fallback=%d deletes=%d", calls, fallback, deletes)
			}
		})
	}
}

func TestProviderCreateHTTPFailureIsNotReplayedByTransport(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); w.WriteHeader(http.StatusBadGateway) }))
	defer server.Close()
	provider := &httpSubmissionTestProvider{submissionTestProvider: &submissionTestProvider{routingTestClient: &routingTestClient{cfg: config.Debrid{Name: "torbox"}}},
		client: request.New(request.WithMaxRetries(3), request.WithRetryableStatus(http.StatusBadGateway)), url: server.URL}
	m, req := submissionTestManager(t, t.TempDir(), provider)
	entry := submissionTestEntry(t, m, req)
	provider.list = func() ([]*debridTypes.Torrent, error) {
		return []*debridTypes.Torrent{submittedTestTorrent(entry.InfoHash, "accepted-before-502")}, nil
	}
	if _, err := m.sendToDebrid(context.Background(), req, entry); !errors.Is(err, ErrProviderSubmissionPending) {
		t.Fatal(err)
	}
	if _, err := m.sendToDebrid(context.Background(), req, entry); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("HTTP layer created %d times for one intent", calls.Load())
	}
}

func TestRecoveredPlacementCannotBeDeletedThroughSeasonChildren(t *testing.T) {
	deletes := 0
	provider := &routingTestClient{cfg: config.Debrid{Name: "torbox"}, delete: func(string) error { deletes++; return nil }}
	m, req := submissionTestManager(t, t.TempDir(), provider)
	parent := submissionTestEntry(t, m, req)
	intent, _, err := m.storage.BeginProviderSubmission(parent, "torbox", submissionAccount(provider.cfg), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storage.RecordProviderSubmissionID(parent, intent.Attempt, "matched", true); err != nil {
		t.Fatal(err)
	}
	bound, err := m.storage.BindProviderSubmission(parent, intent.Attempt, submittedTestTorrent(parent.InfoHash, "matched"))
	if err != nil {
		t.Fatal(err)
	}
	refreshSubmissionEntry(parent, bound)
	child := convertToMultiSeason(parent, []SeasonInfo{{SeasonNumber: 1, InfoHash: strings.Repeat("b", 40), Name: "Example S01", Files: []*storage.File{{Name: "Example.S01E01.mkv", Size: 100}}}})[0]
	if err := m.queue.Add(child); err != nil {
		t.Fatal(err)
	}
	if err := m.queue.DeleteEntryOnly(parent.InfoHash); err != nil {
		t.Fatal(err)
	}
	if child.GetActiveProvider() == nil || child.GetActiveProvider().ID != "matched" {
		t.Fatal("producer did not inherit provider placement")
	}
	if err := m.RemoveFromProvider(child.GetActiveProvider()); !errors.Is(err, storage.ErrRecoveredProviderPlacement) {
		t.Fatal("season cleanup acquired unowned placement", err)
	}
	if err := m.deleteProviderTorrent(provider, "matched"); !errors.Is(err, storage.ErrRecoveredProviderPlacement) {
		t.Fatal("rollback bypassed retired parent fence", err)
	}
	if deletes != 0 {
		t.Fatalf("deleted an unowned provider placement %d times", deletes)
	}
	if err := m.deleteProviderTorrent(provider, "other"); err != nil || deletes != 1 {
		t.Fatalf("unrelated cleanup was blocked: %d %v", deletes, err)
	}
}

func TestPendingProviderSubmissionPinsAccountAndRejectsIncompleteHashMatch(t *testing.T) {
	provider := &submissionTestProvider{routingTestClient: &routingTestClient{cfg: config.Debrid{Name: "torbox", APIKey: "original"}}}
	m, req := submissionTestManager(t, t.TempDir(), provider)
	entry := submissionTestEntry(t, m, req)
	if _, _, err := m.storage.BeginProviderSubmission(entry, "torbox", submissionAccount(provider.cfg), false); err != nil {
		t.Fatal(err)
	}
	provider.list = func() ([]*debridTypes.Torrent, error) {
		remote := submittedTestTorrent(entry.InfoHash, "matching")
		remote.Status = debridTypes.TorrentStatusDownloading
		return []*debridTypes.Torrent{remote}, nil
	}
	provider.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		t.Fatal("resubmitted pending operation")
		return nil, nil
	}
	provider.delete = func(string) error { t.Fatal("deleted hash match"); return nil }
	provider.check = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		t.Fatal("selected files on unowned transfer")
		return nil, nil
	}
	if _, err := m.sendToDebrid(context.Background(), req, entry); !errors.Is(err, ErrProviderSubmissionPending) {
		t.Fatal(err)
	}
	provider.cfg.APIKey = "replacement"
	provider.list = func() ([]*debridTypes.Torrent, error) { t.Fatal("queried a different account"); return nil, nil }
	if _, err := m.sendToDebrid(context.Background(), req, entry); !errors.Is(err, ErrProviderSubmissionPending) {
		t.Fatal(err)
	}
	if err := m.recoverInterruptedDownloads(context.Background()); err != nil {
		t.Fatal(err)
	}
	current, err := m.queue.GetTorrent(entry.InfoHash)
	if err != nil || current.State != storage.EntryStateError {
		t.Fatalf("changed account was restored: %+v %v", current, err)
	}
}

func TestDefiniteProviderRefusalPreservesCachedFirstRouting(t *testing.T) {
	var attempts []string
	first := &routingTestClient{cfg: config.Debrid{Name: "torbox", DownloadUncached: true}, submit: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
		attempts = append(attempts, "torbox")
		if torrent.DownloadUncached {
			t.Fatal("uncached pass ran before cached fallback")
		}
		return nil, customerror.NewTorrentNotCachedError(torrent.Name)
	}}
	second := &routingTestClient{cfg: config.Debrid{Name: "second"}, submit: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
		attempts = append(attempts, "second")
		torrent.Id, torrent.Debrid, torrent.Status = "accepted", "second", debridTypes.TorrentStatusDownloaded
		return torrent, nil
	}}
	m, req := submissionTestManager(t, t.TempDir(), first, second)
	entry := submissionTestEntry(t, m, req)
	if _, err := m.sendToDebrid(context.Background(), req, entry); err != nil {
		t.Fatal(err)
	}
	if strings.Join(attempts, ",") != "torbox,second" || entry.ActiveProvider != "second" {
		t.Fatalf("routing changed: %v %+v", attempts, entry)
	}
}

func TestProviderRollbackFailureBlocksFallbackAndDeleteReplay(t *testing.T) {
	deletes, fallback := 0, 0
	provider := &routingTestClient{cfg: config.Debrid{Name: "torbox"}, submit: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
		torrent.Id, torrent.Debrid = "created", "torbox"
		return torrent, nil
	}, check: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
		return torrent, customerror.NewTorrentNotCachedError(torrent.Name)
	},
		delete: func(string) error { deletes++; return errors.New("delete response lost") }}
	second := &routingTestClient{cfg: config.Debrid{Name: "second"}, submit: func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		fallback++
		return nil, errors.New("unexpected fallback")
	}}
	m, req := submissionTestManager(t, t.TempDir(), provider, second)
	entry := submissionTestEntry(t, m, req)
	if _, err := m.sendToDebrid(context.Background(), req, entry); !errors.Is(err, ErrProviderSubmissionPending) {
		t.Fatal(err)
	}
	provider.get = func(string) (*debridTypes.Torrent, error) {
		return submittedTestTorrent(entry.InfoHash, "created"), nil
	}
	if _, err := m.sendToDebrid(context.Background(), req, entry); !errors.Is(err, ErrProviderSubmissionPending) {
		t.Fatal(err)
	}
	if deletes != 1 || fallback != 0 {
		t.Fatalf("ambiguous cleanup replayed or fell back: deletes=%d fallback=%d", deletes, fallback)
	}
}

func TestBoundRollbackRestoresReconciliationBeforeProviderPolling(t *testing.T) {
	provider := &routingTestClient{cfg: config.Debrid{Name: "torbox"}, submit: func(torrent *debridTypes.Torrent) (*debridTypes.Torrent, error) {
		torrent.Id, torrent.Debrid, torrent.Status = "bound", "torbox", debridTypes.TorrentStatusDownloading
		return torrent, nil
	}, delete: func(string) error { return errors.New("rollback uncertain") }}
	m, req := submissionTestManager(t, t.TempDir(), provider)
	entry := submissionTestEntry(t, m, req)
	if _, err := m.sendToDebrid(context.Background(), req, entry); err != nil {
		t.Fatal(err)
	}
	entry.Status = debridTypes.TorrentStatusDownloading
	if err := m.queue.Update(entry); err != nil {
		t.Fatal(err)
	}
	if err := m.rollbackQueuedProviderTorrent(entry, provider, "bound"); !errors.Is(err, ErrProviderSubmissionPending) {
		t.Fatal(err)
	}
	if err := m.recoverInterruptedDownloads(context.Background()); err != nil {
		t.Fatal(err)
	}
	current, err := m.queue.GetTorrent(entry.InfoHash)
	if err != nil || current.Status != debridTypes.TorrentStatusQueued {
		t.Fatalf("scheduler could bypass cleanup reconciliation: %+v %v", current, err)
	}
	job, err := m.rebuildQueuedTorrentJob(current)
	if err != nil || job.ResumeExisting || job.Request == nil {
		t.Fatalf("restore bypassed pending cleanup: %+v %v", job, err)
	}
}

func TestProviderWorkerTimeoutIsVisibleAndRestoredOnRestart(t *testing.T) {
	provider := &submissionTestProvider{routingTestClient: &routingTestClient{cfg: config.Debrid{Name: "torbox"}, submit: func(*debridTypes.Torrent) (*debridTypes.Torrent, error) { return nil, context.DeadlineExceeded }}}
	m, req := submissionTestManager(t, t.TempDir(), provider)
	entry := submissionTestEntry(t, m, req)
	m.processJob(context.Background(), &Job{ID: entry.InfoHash, Type: JobTypeTorrent, Entry: entry, Request: req})
	failed, err := m.queue.GetTorrent(entry.InfoHash)
	if err != nil || failed.State != storage.EntryStateError || !strings.Contains(failed.LastError, ErrProviderSubmissionPending.Error()) {
		t.Fatalf("timeout silently lost work: %+v %v", failed, err)
	}
	if err := m.recoverInterruptedDownloads(context.Background()); err != nil {
		t.Fatal(err)
	}
	restored, err := m.queue.GetTorrent(entry.InfoHash)
	if err != nil || restored.State != storage.EntryStateDownloading || restored.Status != debridTypes.TorrentStatusQueued {
		t.Fatalf("pending timeout not restored: %+v %v", restored, err)
	}
}

func TestSynchronousAdmissionPersistsBeforeProviderAcceptance(t *testing.T) {
	provider := &routingTestClient{cfg: config.Debrid{Name: "torbox"}}
	m, req := submissionTestManager(t, t.TempDir(), provider)
	m.jobQueue = NewJobQueueWithCapacity(m.ctx, 1, 4, m.processJob, m.entryLifecycle)
	t.Cleanup(func() { m.jobQueue.Close() })
	provider.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		entry, err := m.queue.GetTorrent(req.Magnet.InfoHash)
		if err != nil {
			t.Fatal("provider called before durable queue admission", err)
		}
		intent, err := m.storage.GetProviderSubmission(entry)
		if err != nil || intent == nil || intent.Phase != storage.SubmissionPending {
			t.Fatal("provider called before durable intent", err)
		}
		return nil, context.DeadlineExceeded
	}
	if err := m.AddNewTorrent(context.Background(), req); !errors.Is(err, ErrProviderSubmissionPending) {
		t.Fatal(err)
	}
	entry, err := m.queue.GetTorrent(req.Magnet.InfoHash)
	if err != nil || entry.State != storage.EntryStateError {
		t.Fatalf("synchronous error lost recovery input: %+v %v", entry, err)
	}
}

func TestProviderSubmissionCrashHelper(t *testing.T) {
	root := os.Getenv("TESSARR_SUBMISSION_CRASH_ROOT")
	if root == "" {
		return
	}
	phase := os.Getenv("TESSARR_SUBMISSION_CRASH_PHASE")
	provider := &routingTestClient{cfg: config.Debrid{Name: "torbox", APIKey: "fixture-key"}}
	m, req := submissionTestManager(t, root, provider)
	entry := submissionTestEntry(t, m, req)
	if phase == "intent" {
		if _, _, err := m.storage.BeginProviderSubmission(entry, "torbox", submissionAccount(provider.cfg), false); err != nil {
			t.Fatal(err)
		}
		os.Exit(88)
	}
	provider.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
		remote := submittedTestTorrent(entry.InfoHash, "remote-accepted")
		data, err := json.Marshal(remote)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "remote.json"), data, 0600); err != nil {
			t.Fatal(err)
		}
		if phase == "accepted" {
			os.Exit(88)
		}
		return remote, nil
	}
	provider.check = func(remote *debridTypes.Torrent) (*debridTypes.Torrent, error) {
		if phase == "acknowledged" {
			os.Exit(88)
		}
		return remote, nil
	}
	if _, err := m.sendToDebrid(context.Background(), req, entry); err != nil {
		t.Fatal(err)
	}
	os.Exit(88) // No Close or final fsync may mask a missing checkpoint.
}

func TestProviderSubmissionRecoversAcrossRealProcessCrash(t *testing.T) {
	for _, phase := range []string{"intent", "accepted", "acknowledged", "bound"} {
		t.Run(phase, func(t *testing.T) {
			root := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestProviderSubmissionCrashHelper$")
			cmd.Env = append(os.Environ(), "TESSARR_SUBMISSION_CRASH_ROOT="+root, "TESSARR_SUBMISSION_CRASH_PHASE="+phase)
			output, err := cmd.CombinedOutput()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 88 {
				t.Fatalf("crash helper: %v\n%s", err, output)
			}
			provider := &submissionTestProvider{routingTestClient: &routingTestClient{cfg: config.Debrid{Name: "torbox", APIKey: "fixture-key"}}}
			m, req := submissionTestManager(t, root, provider)
			var remote debridTypes.Torrent
			data, readErr := os.ReadFile(filepath.Join(root, "remote.json"))
			if phase != "intent" {
				if readErr != nil {
					t.Fatal(readErr)
				}
				if err := json.Unmarshal(data, &remote); err != nil {
					t.Fatal(err)
				}
			}
			provider.list = func() ([]*debridTypes.Torrent, error) {
				if phase == "intent" {
					return nil, nil
				}
				return []*debridTypes.Torrent{remote.Copy()}, nil
			}
			provider.submit = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) {
				t.Fatal("restart resubmitted accepted or uncertain work")
				return nil, nil
			}
			provider.delete = func(string) error { t.Fatal("restart deleted existing work"); return nil }
			provider.check = func(*debridTypes.Torrent) (*debridTypes.Torrent, error) { return remote.Copy(), nil }
			provider.get = func(string) (*debridTypes.Torrent, error) { return remote.Copy(), nil }
			if err := m.recoverInterruptedDownloads(context.Background()); err != nil {
				t.Fatal(err)
			}
			entry, err := m.queue.GetTorrent(req.Magnet.InfoHash)
			if err != nil {
				t.Fatal(err)
			}
			result, err := m.sendToDebrid(context.Background(), req, entry)
			if phase == "intent" {
				if !errors.Is(err, ErrProviderSubmissionPending) {
					t.Fatalf("empty snapshot authorized a retry: %v", err)
				}
				return
			}
			if err != nil || result.Id != "remote-accepted" {
				t.Fatalf("accepted work not recovered: %+v %v", result, err)
			}
			intent, err := m.storage.GetProviderSubmission(entry)
			if err != nil || intent.Phase != storage.SubmissionBound || entry.GetActiveProvider().ID != "remote-accepted" {
				t.Fatalf("recovered binding not durable: %+v %v", intent, err)
			}
		})
	}
}
