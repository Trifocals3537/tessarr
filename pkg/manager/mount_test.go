package manager

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

type blockingWarmReader struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingWarmReader() *blockingWarmReader {
	return &blockingWarmReader{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (r *blockingWarmReader) markEntered() {
	r.once.Do(func() {
		close(r.entered)
	})
}

func (r *blockingWarmReader) ReadAt(_ []byte, _ int64) (int, error) {
	r.markEntered()
	<-r.release
	return 0, io.ErrClosedPipe
}

func (r *blockingWarmReader) ReadAtContext(ctx context.Context, _ []byte, _ int64) (int, error) {
	r.markEntered()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-r.release:
		return 0, io.ErrClosedPipe
	}
}

func (r *blockingWarmReader) Size() int64 {
	return 1
}

func (r *blockingWarmReader) Close() error {
	return nil
}

func TestDrainRangeUsesContextAwareReader(t *testing.T) {
	reader := newBlockingWarmReader()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- drainRange(ctx, reader, 0, 1)
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("drainRange() error = %v, want context deadline", err)
		}
	case <-reader.entered:
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("drainRange() error = %v, want context deadline", err)
			}
		case <-time.After(100 * time.Millisecond):
			close(reader.release)
			<-done
			t.Fatal("drainRange used non-cancellable ReadAt and remained blocked past the context deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("drainRange did not start reading")
	}
}

type cacheWarmOpenerFunc func(context.Context, string) (CacheWarmFile, error)

func (f cacheWarmOpenerFunc) OpenCacheWarmFile(ctx context.Context, filePath string) (CacheWarmFile, error) {
	return f(ctx, filePath)
}

type testCacheWarmMount struct {
	opener cacheWarmOpenerFunc
	ready  bool
}

func (m *testCacheWarmMount) Start(context.Context) error { return nil }
func (m *testCacheWarmMount) Stop() error                 { return nil }
func (m *testCacheWarmMount) Stats() map[string]any       { return nil }
func (m *testCacheWarmMount) IsReady() bool               { return m.ready }
func (m *testCacheWarmMount) Type() string                { return "test" }
func (m *testCacheWarmMount) Refresh([]string) error      { return nil }
func (m *testCacheWarmMount) OpenCacheWarmFile(ctx context.Context, filePath string) (CacheWarmFile, error) {
	return m.opener(ctx, filePath)
}

type recordingWarmFile struct {
	size    int64
	offsets []int64
	lengths []int
}

func (f *recordingWarmFile) ReadAtContext(_ context.Context, p []byte, off int64) (int, error) {
	f.offsets = append(f.offsets, off)
	f.lengths = append(f.lengths, len(p))
	return len(p), nil
}

func (f *recordingWarmFile) Size() int64 {
	return f.size
}

func (f *recordingWarmFile) Close() error {
	return nil
}

func TestWarmFileCacheUsesHeadAndTailRanges(t *testing.T) {
	file := &recordingWarmFile{size: int64(cacheWarmHeadSize + cacheWarmTailSize + 1)}
	m := &Manager{
		logger: zerolog.Nop(),
		mountManager: &testCacheWarmMount{
			ready: true,
			opener: func(context.Context, string) (CacheWarmFile, error) {
				return file, nil
			},
		},
	}

	if err := m.WarmFileCache(context.Background(), []string{"movie.mkv"}); err != nil {
		t.Fatalf("WarmFileCache() error = %v", err)
	}

	wantOffsets := []int64{
		0,
		1 << 20,
		int64(cacheWarmHeadSize + 1),
		int64(cacheWarmHeadSize + 1 + (1 << 20)),
	}
	if len(file.offsets) != len(wantOffsets) {
		t.Fatalf("warm read offsets = %v, want %v", file.offsets, wantOffsets)
	}
	for i := range wantOffsets {
		if file.offsets[i] != wantOffsets[i] {
			t.Fatalf("warm read offset[%d] = %d, want %d", i, file.offsets[i], wantOffsets[i])
		}
	}
}

func TestWarmFileCacheHonorsCallerCancellation(t *testing.T) {
	entered := make(chan struct{})
	m := &Manager{
		logger: zerolog.Nop(),
		mountManager: &testCacheWarmMount{
			ready: true,
			opener: func(ctx context.Context, _ string) (CacheWarmFile, error) {
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := m.WarmFileCache(ctx, []string{"movie.mkv"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WarmFileCache() error = %v, want context deadline", err)
	}
	select {
	case <-entered:
	default:
		t.Fatal("cache warm opener was not called")
	}
}

func TestWarmFileCacheHonorsManagerShutdown(t *testing.T) {
	entered := make(chan struct{})
	m := &Manager{
		logger: zerolog.Nop(),
		mountManager: &testCacheWarmMount{
			ready: true,
			opener: func(ctx context.Context, _ string) (CacheWarmFile, error) {
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			},
		},
	}
	m.resetLifecycle()
	defer m.cancel()

	result := make(chan error, 1)
	go func() {
		result <- m.WarmFileCache(context.Background(), []string{"movie.mkv"})
	}()
	waitForSignal(t, entered, "cache warm opener")
	m.cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("WarmFileCache() error = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WarmFileCache() did not return after manager shutdown")
	}
}

type blockingWarmFile struct {
	active    *atomic.Int32
	maxActive *atomic.Int32
	release   <-chan struct{}
}

func (f *blockingWarmFile) ReadAtContext(ctx context.Context, p []byte, _ int64) (int, error) {
	active := f.active.Add(1)
	for {
		maxActive := f.maxActive.Load()
		if active <= maxActive || f.maxActive.CompareAndSwap(maxActive, active) {
			break
		}
	}
	defer f.active.Add(-1)

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-f.release:
		return len(p), nil
	}
}

func (f *blockingWarmFile) Size() int64 {
	return 1
}

func (f *blockingWarmFile) Close() error {
	return nil
}

func TestWarmFileCacheUsesGlobalWorkerLimitAcrossConcurrentCalls(t *testing.T) {
	var active atomic.Int32
	var maxActive atomic.Int32
	release := make(chan struct{})
	var opens atomic.Int32
	m := &Manager{
		logger: zerolog.Nop(),
		mountManager: &testCacheWarmMount{
			ready: true,
			opener: func(context.Context, string) (CacheWarmFile, error) {
				opens.Add(1)
				return &blockingWarmFile{
					active:    &active,
					maxActive: &maxActive,
					release:   release,
				}, nil
			},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan error, 2)
	paths := make([]string, MaxCacheWarmWorkers)
	for i := range paths {
		paths[i] = "movie.mkv"
	}
	for range 2 {
		go func() {
			results <- m.WarmFileCache(ctx, paths)
		}()
	}

	deadline := time.After(time.Second)
	for maxActive.Load() < int32(MaxCacheWarmWorkers) {
		select {
		case err := <-results:
			close(release)
			t.Fatalf("WarmFileCache() returned early: %v", err)
		case <-deadline:
			close(release)
			t.Fatalf("only %d concurrent warm reads started", maxActive.Load())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if got := maxActive.Load(); got > int32(MaxCacheWarmWorkers) {
		close(release)
		t.Fatalf("max concurrent warm reads = %d, want <= %d", got, MaxCacheWarmWorkers)
	}

	close(release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("WarmFileCache() error = %v", err)
		}
	}
	if opens.Load() != int32(2*MaxCacheWarmWorkers) {
		t.Fatalf("cache warm opens = %d, want %d", opens.Load(), 2*MaxCacheWarmWorkers)
	}
}
