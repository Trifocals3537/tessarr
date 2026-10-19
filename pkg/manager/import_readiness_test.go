package manager

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/rs/zerolog"
)

type readinessTestFile struct {
	size      int64
	shortRead bool

	mu      sync.Mutex
	offsets []int64
}

type unsupportedReadinessMount struct{}

func (*unsupportedReadinessMount) Start(context.Context) error { return nil }
func (*unsupportedReadinessMount) Stop() error                 { return nil }
func (*unsupportedReadinessMount) Stats() map[string]any       { return nil }
func (*unsupportedReadinessMount) IsReady() bool               { return true }
func (*unsupportedReadinessMount) Type() string                { return "unsupported-test" }
func (*unsupportedReadinessMount) Refresh([]string) error      { return nil }

type canceledReadinessFile struct {
	started chan struct{}
	once    sync.Once
}

func (f *canceledReadinessFile) ReadAtContext(ctx context.Context, _ []byte, _ int64) (int, error) {
	f.once.Do(func() { close(f.started) })
	<-ctx.Done()
	return 0, ctx.Err()
}

func (*canceledReadinessFile) Size() int64  { return 1024 }
func (*canceledReadinessFile) Close() error { return nil }

type concurrentReadinessFile struct {
	size      int64
	shortRead bool
	entered   *atomic.Int32
	bothReady chan struct{}
	once      sync.Once
}

func (f *concurrentReadinessFile) ReadAtContext(ctx context.Context, p []byte, _ int64) (int, error) {
	f.once.Do(func() {
		if f.entered.Add(1) == 2 {
			close(f.bothReady)
		}
	})
	select {
	case <-f.bothReady:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	if f.shortRead && len(p) > 1 {
		return len(p) - 1, io.EOF
	}
	return len(p), nil
}

func (f *concurrentReadinessFile) Size() int64 { return f.size }
func (*concurrentReadinessFile) Close() error  { return nil }

func (f *readinessTestFile) ReadAtContext(_ context.Context, p []byte, off int64) (int, error) {
	f.mu.Lock()
	f.offsets = append(f.offsets, off)
	f.mu.Unlock()
	if f.shortRead && len(p) > 1 {
		return len(p) - 1, io.EOF
	}
	return len(p), nil
}

func (f *readinessTestFile) Size() int64  { return f.size }
func (f *readinessTestFile) Close() error { return nil }

func (f *readinessTestFile) readOffsets() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.offsets...)
}

func TestEvenlySampleReadinessFilesCoversWholeRelease(t *testing.T) {
	files := make([]importReadinessFile, 10)
	for i := range files {
		files[i] = importReadinessFile{logicalName: string(rune('a' + i))}
	}
	got := evenlySampleReadinessFiles(files, 5)
	names := make([]string, 0, len(got))
	for _, file := range got {
		names = append(names, file.logicalName)
	}
	if !slices.Equal(names, []string{"a", "c", "e", "g", "j"}) {
		t.Fatalf("sampled files = %v, want release-wide deterministic sample", names)
	}
}

func TestImportReadinessFilesMapsTorrentOutputsAndSkipsNonMedia(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	downloadRoot := t.TempDir()
	config.Get().DownloadFolder = downloadRoot
	entry := readinessTestEntry(downloadRoot, map[string]int64{
		"Season/episode-01.mkv": 101,
		"Season/episode-02.mkv": 102,
		"Season/release.nfo":    50,
	})
	root := entry.DownloadPath()
	paths := []string{
		filepath.Join(root, "Season", "release.nfo"),
		filepath.Join(root, "Season", "episode-02.mkv"),
		filepath.Join(root, "Season", "episode-01.mkv"),
	}
	d := &Downloader{dest: downloadRoot}
	got, err := d.importReadinessFiles(entry, paths, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].logicalName != "Season/episode-01.mkv" || got[0].expectedSize != 101 ||
		got[1].logicalName != "Season/episode-02.mkv" || got[1].expectedSize != 102 {
		t.Fatalf("readiness files = %+v", got)
	}
}

func TestImportReadinessFilesMapsNZBOutputs(t *testing.T) {
	config.SetConfigPath(t.TempDir())
	downloadRoot := t.TempDir()
	config.Get().DownloadFolder = downloadRoot
	entry := readinessTestEntry(downloadRoot, map[string]int64{
		"Pokémon - S20E01 - Alola to New Adventure!.mkv": 4096,
		"release.nfo": 64,
	})
	entry.Protocol = config.ProtocolNZB
	entry.Name = "Pokemon.S20.Pack.nzb"
	entry.OutputName = ""
	entry.Category = "sonarr"
	entry.SavePath = filepath.Join(downloadRoot, entry.Category)
	root := entry.DownloadPath()
	paths := []string{
		filepath.Join(root, "release.nfo"),
		filepath.Join(root, "Pokémon - S20E01 - Alola to New Adventure!.mkv"),
	}

	got, err := (&Downloader{dest: downloadRoot}).importReadinessFiles(entry, paths, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].logicalName != "Pokémon - S20E01 - Alola to New Adventure!.mkv" || got[0].expectedSize != 4096 {
		t.Fatalf("readiness files = %+v", got)
	}
}

func TestVerifyImportReadinessChecksSizeAndHeadMiddleTail(t *testing.T) {
	const size = int64(1024 * 1024)
	manager, file := readinessTestManager(t, size)
	probe := importReadinessFile{path: "movie.mkv", logicalName: "movie.mkv", expectedSize: size}
	if err := manager.verifyImportReadiness(context.Background(), "readiness-hash", []importReadinessFile{probe}); err != nil {
		t.Fatal(err)
	}
	want := importReadinessOffsets(size, importReadinessProbeSize)
	if got := file.readOffsets(); !slices.Equal(got, want) {
		t.Fatalf("read offsets = %v, want %v", got, want)
	}
}

func TestVerifyImportReadinessRejectsSizeMismatch(t *testing.T) {
	manager, _ := readinessTestManager(t, 1024)
	err := manager.verifyImportReadiness(context.Background(), "readiness-hash", []importReadinessFile{{
		path:         "movie.mkv",
		logicalName:  "movie.mkv",
		expectedSize: 2048,
	}})
	if err == nil || !strings.Contains(err.Error(), "does not match expected size") {
		t.Fatalf("verify error = %v, want size mismatch", err)
	}
}

func TestVerifyImportReadinessRejectsTruncatedRange(t *testing.T) {
	manager, file := readinessTestManager(t, 1024)
	file.shortRead = true
	err := manager.verifyImportReadiness(context.Background(), "readiness-hash", []importReadinessFile{{
		path:         "movie.mkv",
		logicalName:  "movie.mkv",
		expectedSize: 1024,
	}})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("verify error = %v, want unexpected EOF", err)
	}
}

func TestVerifyImportReadinessRejectsProviderTargetChange(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	entry := readinessTestEntry(t.TempDir(), map[string]int64{"movie.mkv": 1024})
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	file := &readinessTestFile{size: 1024}
	var once sync.Once
	manager := &Manager{storage: store, logger: zerolog.Nop()}
	manager.mountManager = &testCacheWarmMount{
		ready: true,
		opener: func(context.Context, string) (CacheWarmFile, error) {
			once.Do(func() {
				current, getErr := store.Get(entry.InfoHash)
				if getErr != nil {
					t.Errorf("load target for mutation: %v", getErr)
					return
				}
				current.Providers[current.ActiveProvider].Files["movie.mkv"].Path = "replacement/movie.mkv"
				if updateErr := store.AddOrUpdate(current); updateErr != nil {
					t.Errorf("mutate target: %v", updateErr)
				}
			})
			return file, nil
		},
	}
	err = manager.verifyImportReadiness(context.Background(), entry.InfoHash, []importReadinessFile{{
		path:         "movie.mkv",
		logicalName:  "movie.mkv",
		expectedSize: 1024,
	}})
	if err == nil || !strings.Contains(err.Error(), "provider target changed") {
		t.Fatalf("verify error = %v, want provider target change", err)
	}
}

func TestVerifyImportReadinessReportsUnsupportedMount(t *testing.T) {
	manager, _ := readinessTestManager(t, 1024)
	manager.mountManager = &unsupportedReadinessMount{}
	err := manager.verifyImportReadiness(context.Background(), "readiness-hash", []importReadinessFile{{
		path:         "movie.mkv",
		logicalName:  "movie.mkv",
		expectedSize: 1024,
	}})
	if !errors.Is(err, ErrCacheWarmUnavailable) {
		t.Fatalf("verify error = %v, want %v", err, ErrCacheWarmUnavailable)
	}
}

func TestVerifyImportReadinessCancellationIsBounded(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	entry := readinessTestEntry(t.TempDir(), map[string]int64{"movie.mkv": 1024})
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	file := &canceledReadinessFile{started: make(chan struct{})}
	manager := &Manager{storage: store, logger: zerolog.Nop()}
	manager.mountManager = &testCacheWarmMount{
		ready: true,
		opener: func(context.Context, string) (CacheWarmFile, error) {
			return file, nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- manager.verifyImportReadiness(ctx, entry.InfoHash, []importReadinessFile{{
			path: "movie.mkv", logicalName: "movie.mkv", expectedSize: 1024,
		}})
	}()
	select {
	case <-file.started:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("readiness probe did not start")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("verify error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("readiness probe did not stop after cancellation")
	}
}

func TestVerifyImportReadinessIsolatesBadFileFromConcurrentHealthyProbe(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	entry := readinessTestEntry(t.TempDir(), map[string]int64{
		"bad.mkv":  1024,
		"good.mkv": 1024,
	})
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	var entered atomic.Int32
	bothReady := make(chan struct{})
	files := map[string]*concurrentReadinessFile{
		"bad.mkv":  {size: 1024, shortRead: true, entered: &entered, bothReady: bothReady},
		"good.mkv": {size: 1024, entered: &entered, bothReady: bothReady},
	}
	manager := &Manager{storage: store, logger: zerolog.Nop()}
	manager.mountManager = &testCacheWarmMount{
		ready: true,
		opener: func(_ context.Context, path string) (CacheWarmFile, error) {
			file := files[path]
			if file == nil {
				return nil, errors.New("unexpected path")
			}
			return file, nil
		},
	}
	err = manager.verifyImportReadiness(context.Background(), entry.InfoHash, []importReadinessFile{
		{path: "bad.mkv", logicalName: "bad.mkv", expectedSize: 1024},
		{path: "good.mkv", logicalName: "good.mkv", expectedSize: 1024},
	})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("verify error = %v, want unexpected EOF", err)
	}
	if got := entered.Load(); got != 2 {
		t.Fatalf("concurrent probes started = %d, want 2", got)
	}
}

func TestVerifyImportReadinessSmallFileUsesOneExactRange(t *testing.T) {
	manager, file := readinessTestManager(t, 1024)
	probe := importReadinessFile{path: "movie.mkv", logicalName: "movie.mkv", expectedSize: 1024}
	if err := manager.verifyImportReadiness(context.Background(), "readiness-hash", []importReadinessFile{probe}); err != nil {
		t.Fatal(err)
	}
	if got := file.readOffsets(); !slices.Equal(got, []int64{0}) {
		t.Fatalf("read offsets = %v, want one deduplicated range", got)
	}
}

func readinessTestManager(t *testing.T, size int64) (*Manager, *readinessTestFile) {
	t.Helper()
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	entry := readinessTestEntry(t.TempDir(), map[string]int64{"movie.mkv": size})
	if err := store.AddOrUpdate(entry); err != nil {
		t.Fatal(err)
	}
	file := &readinessTestFile{size: size}
	manager := &Manager{storage: store, logger: zerolog.Nop()}
	manager.mountManager = &testCacheWarmMount{
		ready: true,
		opener: func(context.Context, string) (CacheWarmFile, error) {
			return file, nil
		},
	}
	return manager, file
}

func readinessTestEntry(downloadRoot string, files map[string]int64) *storage.Entry {
	entry := &storage.Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       "readiness-hash",
		Name:           "release",
		OutputName:     "release",
		SavePath:       downloadRoot,
		ActiveProvider: "provider",
		Providers: map[string]*storage.ProviderEntry{
			"provider": {
				Provider: "provider",
				ID:       "placement",
				Status:   debridTypes.TorrentStatusDownloaded,
				Files:    make(map[string]*storage.ProviderFile),
			},
		},
		Files: make(map[string]*storage.File),
	}
	for name, size := range files {
		entry.Files[name] = &storage.File{ID: name, Name: name, Path: name, Size: size, InfoHash: entry.InfoHash}
		entry.Providers[entry.ActiveProvider].Files[name] = &storage.ProviderFile{Id: name, Path: name, Link: "restricted"}
	}
	return entry
}
