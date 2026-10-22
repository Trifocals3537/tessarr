package manager

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/workflow"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/notifications"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

type legacyRecoveryProvider struct {
	*routingTestClient
	list func() ([]*debridTypes.Torrent, error)
}

func (p *legacyRecoveryProvider) GetTorrents() ([]*debridTypes.Torrent, error) { return p.list() }

type unsafeRecoveryPanic struct{}

func (unsafeRecoveryPanic) String() string { panic("do not format provider secrets") }

func TestLegacyProviderReadPanicsAreContained(t *testing.T) {
	for _, list := range []bool{false, true} {
		t.Run(map[bool]string{false: "get", true: "list"}[list], func(t *testing.T) {
			provider := &legacyRecoveryProvider{routingTestClient: &routingTestClient{}}
			provider.get = func(string) (*debridTypes.Torrent, error) { panic(unsafeRecoveryPanic{}) }
			provider.list = func() ([]*debridTypes.Torrent, error) { panic(unsafeRecoveryPanic{}) }
			var err error
			if list {
				_, err = getProviderTorrents(t.Context(), provider)
			} else {
				_, err = getProviderTorrent(t.Context(), provider, "42")
			}
			if err != workflow.ErrPanicked {
				t.Fatalf("provider panic was not sanitized: %v", err)
			}
		})
	}
}

func TestLegacyProviderReadContainsPanicAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	provider := &routingTestClient{get: func(string) (*debridTypes.Torrent, error) {
		close(entered)
		defer close(finished)
		<-release
		panic("fixture signed URL must not escape")
	}}
	returned := make(chan error, 1)
	go func() { _, err := getProviderTorrent(ctx, provider, "42"); returned <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("legacy getter did not start")
	}
	cancel()
	if err := <-returned; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	close(release)
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("late legacy getter could not finish")
	}
}

func TestSavedSubmissionRecoveryChecksHistoricalAccountConflictBeforeProviderAccess(t *testing.T) {
	for _, scenario := range []string{"acknowledged", "cleanup", "hash recovered"} {
		t.Run(scenario, func(t *testing.T) {
			root := t.TempDir()
			accesses := 0
			provider := &legacyRecoveryProvider{routingTestClient: &routingTestClient{cfg: config.Debrid{Name: "torbox", APIKey: "current-fixture-key"}}}
			provider.get = func(id string) (*debridTypes.Torrent, error) {
				accesses++
				return submittedTestTorrent(strings.Repeat("a", 40), id), nil
			}
			provider.list = func() ([]*debridTypes.Torrent, error) {
				accesses++
				return []*debridTypes.Torrent{submittedTestTorrent(strings.Repeat("a", 40), "42")}, nil
			}
			provider.check = func(remote *debridTypes.Torrent) (*debridTypes.Torrent, error) {
				accesses++
				return submittedTestTorrent(remote.InfoHash, remote.Id), nil
			}
			provider.delete = func(string) error { accesses++; return nil }
			m, req := submissionTestManager(t, root, provider)
			entry := submissionTestEntry(t, m, req)
			intent, _, err := m.storage.BeginProviderSubmission(entry, "torbox", submissionAccount(provider.cfg), false)
			if err != nil {
				t.Fatal(err)
			}
			if err := m.storage.RecordProviderSubmissionID(entry, intent.Attempt, "42", scenario == "hash recovered"); err != nil {
				t.Fatal(err)
			}
			if scenario == "cleanup" {
				if err := m.storage.SetProviderSubmissionPhase(entry, intent.Attempt, storage.SubmissionCleanup); err != nil {
					t.Fatal(err)
				}
			}
			if err := m.storage.Close(); err != nil {
				t.Fatal(err)
			}
			// Earlier binaries could record the same provider ID for different
			// accounts. Preserve that journal evidence and let startup fence it.
			state, err := hybrid.New(hybrid.Config{DataPath: filepath.Join(root, "db", "storage_state.db")})
			if err != nil {
				t.Fatal(err)
			}
			hash := strings.Repeat("b", 40)
			historical := &storage.ProviderSubmission{Version: 1, InfoHash: hash, QueueIncarnation: "prior", Attempt: "prior", Provider: "torbox", Account: "prior-account", ProviderID: "42", Phase: storage.SubmissionBound}
			data, err := json.Marshal(historical)
			if err != nil {
				t.Fatal(err)
			}
			if err := state.Put("provider_submission/"+hex.EncodeToString([]byte(hash)), data, nil); err != nil {
				t.Fatal(err)
			}
			if err := state.Close(); err != nil {
				t.Fatal(err)
			}
			restarted, _ := submissionTestManager(t, root, provider)
			current, err := restarted.queue.GetTorrent(entry.InfoHash)
			if err != nil {
				t.Fatal(err)
			}
			if current.GetActiveProvider() != nil {
				t.Fatal("fixture already has a published placement")
			}
			_, err = restarted.sendToDebrid(t.Context(), req, current)
			if !errors.Is(err, ErrProviderSubmissionPending) || !errors.Is(err, storage.ErrProviderPlacementAccount) || accesses != 0 {
				t.Fatalf("saved-ID recovery crossed the conflict fence: %v accesses=%d", err, accesses)
			}
		})
	}
}

func TestSeasonChildRecoveryPollsOnlyItsPersistedFileScope(t *testing.T) {
	root := t.TempDir()
	checks := 0
	remote := &debridTypes.Torrent{Id: "42", InfoHash: strings.Repeat("a", 40), Debrid: "torbox", Name: "Example Complete Series", OriginalFilename: "Example Complete Series", Status: debridTypes.TorrentStatusDownloaded, Size: 300,
		Files: map[string]debridTypes.File{
			"Example.S01E01.mkv": {Id: "s1", Name: "Example.S01E01.mkv", Path: "Season 01/Example.S01E01.mkv", Size: 100, Link: "https://example.invalid/s1"},
			"Example.S02E01.mkv": {Id: "s2", Name: "Example.S02E01.mkv", Path: "Season 02/Example.S02E01.mkv", Size: 200, Link: "https://example.invalid/s2"},
		}}
	provider := &routingTestClient{cfg: config.Debrid{Name: "torbox"}, check: func(*debridTypes.Torrent) (*debridTypes.Torrent, error) { checks++; return remote.Copy(), nil }}
	m, req := submissionTestManager(t, root, provider)
	parent := newTorrentQueueEntry(req, debridTypes.TorrentStatusDownloaded)
	parent.Category = ""
	if err := applyDebridTorrentToEntry(parent, remote); err != nil {
		t.Fatal(err)
	}
	downloader := &Downloader{logger: zerolog.Nop()}
	multi, seasons := downloader.detectMultiSeason(parent)
	if !multi || len(seasons) != 2 {
		t.Fatal("fixture did not split into seasons")
	}
	child := convertToMultiSeason(parent, seasons)[0]
	if err := m.queue.Add(child); err != nil {
		t.Fatal(err)
	}
	if err := m.storage.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, _ := submissionTestManager(t, root, provider)
	restarted.processingEntries = xsync.NewMap[string, struct{}]()
	restarted.entry = NewEntryCache(restarted)
	restarted.Notifications = notifications.New(&config.Get().Notifications, zerolog.Nop())
	restarted.downloader = &Downloader{manager: restarted, logger: zerolog.Nop()}
	if err := restarted.recoverInterruptedDownloads(t.Context()); err != nil {
		t.Fatal(err)
	}
	current, err := restarted.queue.GetTorrent(child.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if !current.FileScopeRestricted || !current.SkipMultiSeason || current.IsDownloading {
		t.Fatal("restart lost child scope or retained the process-local claim")
	}
	if err := restarted.processQueuedTorrent(t.Context(), current); !errors.Is(err, errDeleteQueueEntryOnJobFinish) {
		t.Fatalf("recovered child did not finish: %v", err)
	}
	restarted.background.Wait()
	completed, err := restarted.queue.GetTorrent(child.InfoHash)
	if err != nil {
		t.Fatal(err)
	}
	if checks != 1 || len(completed.Files) != 1 || len(completed.GetActiveProvider().Files) != 1 || completed.Name != child.Name || completed.Size != child.Size || !completed.IsComplete {
		t.Fatalf("full-parent polling changed the season child: checks=%d name=%s size=%d files=%v complete=%t", checks, completed.Name, completed.Size, completed.Files, completed.IsComplete)
	}
	if multi, _ := downloader.detectMultiSeason(completed); multi {
		t.Fatal("recovery created a multi-season child")
	}
}

func TestSeasonChildReadinessRequiresOnlyItsOwnCompleteFiles(t *testing.T) {
	for _, scenario := range []string{"other season link missing", "own link missing", "own file missing"} {
		t.Run(scenario, func(t *testing.T) {
			entry := &storage.Entry{Name: "Example S01", FileScopeRestricted: true, Files: map[string]*storage.File{"s1.mkv": {Name: "s1.mkv", Path: "Season 1/s1.mkv", Size: 100}},
				Providers: map[string]*storage.ProviderEntry{"torbox": {ID: "42", Files: map[string]*storage.ProviderFile{"s1.mkv": {Id: "s1", Path: "Season 1/s1.mkv"}}}}}
			remote := &debridTypes.Torrent{Debrid: "torbox", Id: "42", Name: "Example Complete Series", Status: debridTypes.TorrentStatusDownloaded, Files: map[string]debridTypes.File{
				"s1.mkv": {Id: "s1", Name: "s1.mkv", Path: "Season 1/s1.mkv", Size: 100, Link: "https://example.invalid/s1"},
				"s2.mkv": {Id: "s2", Name: "s2.mkv", Path: "Season 2/s2.mkv", Size: 200},
			}}
			if scenario == "own link missing" {
				file := remote.Files["s1.mkv"]
				file.Link = ""
				remote.Files[file.Name] = file
			}
			if scenario == "own file missing" {
				delete(remote.Files, "s1.mkv")
			}
			ready, err := applyCompletedTorrentFiles(entry, remote)
			if err != nil || ready != (scenario == "other season link missing") || len(entry.Files) != 1 || entry.Name != "Example S01" {
				t.Fatalf("scoped readiness is incorrect: ready=%t err=%v entry=%+v", ready, err, entry)
			}
		})
	}
}
