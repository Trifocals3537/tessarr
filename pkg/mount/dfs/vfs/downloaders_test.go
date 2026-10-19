package vfs

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/mount/dfs/vfs/ranges"
)

const (
	testKiB = int64(1024)
	testMiB = 1024 * testKiB
)

func TestCurrentKickerInterval(t *testing.T) {
	dls := &Downloaders{}

	if got := dls.currentKickerInterval(); got != kickerInterval {
		t.Fatalf("unexpected interval without waiters: got %s, want %s", got, kickerInterval)
	}

	dls.waiterCount.Store(1)
	if got := dls.currentKickerInterval(); got != activeWaiterKickerInterval {
		t.Fatalf("unexpected interval with waiters: got %s, want %s", got, activeWaiterKickerInterval)
	}
}

func getMaxOffset(dl *downloader) int64 {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	return dl.maxOffset
}

func schedulerTestDownloader(dls *Downloaders, start, targetEnd int64, priority bool) *downloader {
	ctx, cancel := context.WithCancel(dls.ctx)
	return &downloader{
		dls:              dls,
		quit:             make(chan struct{}),
		kick:             make(chan struct{}, 1),
		ctx:              ctx,
		cancel:           cancel,
		start:            start,
		offset:           start,
		maxOffset:        targetEnd,
		baseChunkSize:    4 * testMiB,
		currentChunkSize: 4 * testMiB,
		priority:         priority,
	}
}

func downloaderStopped(dl *downloader) bool {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	return dl.stopped
}

func TestEnsureDownloaderLockedCapsActiveWorkersForLiveReaders(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache := &Cache{}
	item := &CacheItem{cache: cache, info: ItemInfo{Size: 512 * testMiB}}
	dls := &Downloaders{ctx: ctx, cancel: cancel, item: item, chunkSize: 4 * testMiB, readAheadSize: 16 * testMiB}
	first := schedulerTestDownloader(dls, 0, 16*testMiB, false)
	second := schedulerTestDownloader(dls, 128*testMiB, 144*testMiB, false)
	dls.dls = []*downloader{first, second}
	dls.waiters = []waiter{
		{r: ranges.Range{Pos: 0, Size: 128 * testKiB}},
		{r: ranges.Range{Pos: 128 * testMiB, Size: 128 * testKiB}},
		{r: ranges.Range{Pos: 256 * testMiB, Size: 128 * testKiB}, priority: true},
	}

	if err := dls.ensureDownloaderLocked(dls.waiters[2].r, true); err != nil {
		t.Fatalf("ensureDownloaderLocked returned error: %v", err)
	}
	if got := dls.activeDownloaderCountLocked(); got != maxFileDownloaders {
		t.Fatalf("active downloaders = %d, want %d", got, maxFileDownloaders)
	}
	if downloaderStopped(first) || downloaderStopped(second) {
		t.Fatal("scheduler preempted a worker still serving a live reader")
	}
	if got := cache.schedulerPreemptions.Load(); got != 0 {
		t.Fatalf("scheduler preemptions = %d, want 0", got)
	}
}

func TestEnsureDownloaderLockedPreemptsOnlyObsoleteReadAhead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cache := &Cache{}
	item := &CacheItem{cache: cache, info: ItemInfo{Size: 512 * testMiB}}
	dls := &Downloaders{ctx: ctx, cancel: cancel, item: item, chunkSize: 4 * testMiB, readAheadSize: 16 * testMiB}
	bulk := schedulerTestDownloader(dls, 0, 16*testMiB, false)
	probe := schedulerTestDownloader(dls, 128*testMiB, 129*testMiB, true)
	dls.dls = []*downloader{bulk, probe}
	request := ranges.Range{Pos: 256 * testMiB, Size: 128 * testKiB}
	if err := dls.appendWaiterLocked(waiter{r: request, errChan: make(chan error, 1)}); err != nil {
		t.Fatalf("append waiter: %v", err)
	}

	if err := dls.ensureDownloaderLocked(request, false); err != nil {
		t.Fatalf("ensureDownloaderLocked returned error: %v", err)
	}
	if !downloaderStopped(bulk) {
		t.Fatal("obsolete bulk read-ahead was not preempted")
	}
	if downloaderStopped(probe) {
		t.Fatal("bulk request preempted a priority probe")
	}
	if got := dls.activeDownloaderCountLocked(); got != 1 {
		t.Fatalf("active downloaders after preemption = %d, want 1", got)
	}
	if got := cache.schedulerPreemptions.Load(); got != 1 {
		t.Fatalf("scheduler preemptions = %d, want 1", got)
	}
	cancel()
}

func TestEnsureDownloaderLockedStartsReplacementOnlyAfterSlotIsFree(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cache := &Cache{}
	item := &CacheItem{cache: cache, info: ItemInfo{Size: 512 * testMiB}}
	dls := &Downloaders{
		parentCtx:     context.Background(),
		ctx:           ctx,
		cancel:        cancel,
		item:          item,
		chunkSize:     4 * testMiB,
		readAheadSize: 16 * testMiB,
	}
	bulk := schedulerTestDownloader(dls, 0, 16*testMiB, false)
	probe := schedulerTestDownloader(dls, 128*testMiB, 129*testMiB, true)
	dls.dls = []*downloader{bulk, probe}
	request := ranges.Range{Pos: 256 * testMiB, Size: 128 * testKiB}
	if err := dls.appendWaiterLocked(waiter{r: request, errChan: make(chan error, 1)}); err != nil {
		t.Fatalf("append waiter: %v", err)
	}

	if err := dls.ensureDownloaderLocked(request, false); err != nil {
		t.Fatalf("first ensureDownloaderLocked returned error: %v", err)
	}
	if got := cache.activeDownloads.Load(); got != 0 {
		t.Fatalf("replacement started before canceled worker exited: active=%d", got)
	}

	started := make(chan struct{}, 1)
	dls.runDownloader = func(dl *downloader) (int64, error) {
		started <- struct{}{}
		<-dl.ctx.Done()
		return 0, dl.ctx.Err()
	}
	if err := dls.ensureDownloaderLocked(request, false); err != nil {
		t.Fatalf("second ensureDownloaderLocked returned error: %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("replacement downloader did not start after a slot became available")
	}
	if got := dls.activeDownloaderCountLocked(); got != maxFileDownloaders {
		t.Fatalf("active downloaders = %d, want %d", got, maxFileDownloaders)
	}
	if got := cache.activeDownloads.Load(); got != 1 {
		t.Fatalf("running replacement gauge = %d, want 1", got)
	}

	dls.StopAll()
}

func TestEnsureDownloaderLockedIgnoresStoppedWorkerNearRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cache := &Cache{}
	item := &CacheItem{cache: cache, info: ItemInfo{Size: 64 * testMiB}}
	dls := &Downloaders{
		parentCtx:     context.Background(),
		ctx:           ctx,
		cancel:        cancel,
		item:          item,
		chunkSize:     4 * testMiB,
		readAheadSize: 16 * testMiB,
	}
	request := ranges.Range{Pos: 8 * testMiB, Size: 128 * testKiB}
	stopped := schedulerTestDownloader(dls, request.Pos, request.End()+16*testMiB, false)
	stopped.stop()
	dls.dls = []*downloader{stopped}
	if err := dls.appendWaiterLocked(waiter{r: request, errChan: make(chan error, 1)}); err != nil {
		t.Fatalf("append waiter: %v", err)
	}

	started := make(chan struct{}, 1)
	dls.runDownloader = func(dl *downloader) (int64, error) {
		started <- struct{}{}
		<-dl.ctx.Done()
		return 0, dl.ctx.Err()
	}
	if err := dls.ensureDownloaderLocked(request, false); err != nil {
		t.Fatalf("ensureDownloaderLocked returned error: %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("stopped nearby downloader prevented replacement from starting")
	}
	if got := dls.activeDownloaderCountLocked(); got != 1 {
		t.Fatalf("active downloaders = %d, want 1", got)
	}
	dls.StopAll()
}

func TestAppendWaiterLockedBoundsPerFileQueue(t *testing.T) {
	cache := &Cache{}
	dls := &Downloaders{item: &CacheItem{cache: cache}}
	for index := 0; index < maxFileWaiters; index++ {
		if err := dls.appendWaiterLocked(waiter{
			r:       ranges.Range{Pos: int64(index), Size: 1},
			errChan: make(chan error, 1),
		}); err != nil {
			t.Fatalf("append waiter %d: %v", index, err)
		}
	}
	err := dls.appendWaiterLocked(waiter{r: ranges.Range{Size: 1}, errChan: make(chan error, 1)})
	if err == nil {
		t.Fatal("queue accepted a waiter beyond its bound")
	}
	if !customerror.IsRetriableError(err) {
		t.Fatalf("queue-full error is not retryable: %v", err)
	}
	if got := cache.pendingReads.Load(); got != maxFileWaiters {
		t.Fatalf("pending read gauge = %d, want %d", got, maxFileWaiters)
	}
	if got := cache.schedulerQueueFull.Load(); got != 1 {
		t.Fatalf("queue-full count = %d, want 1", got)
	}
	for _, w := range dls.waiters {
		dls.finishWaiterLocked(w)
	}
	dls.waiters = nil
	if got := cache.pendingReads.Load(); got != 0 {
		t.Fatalf("pending read gauge after drain = %d, want 0", got)
	}
}

func TestFinishWaiterRecordsBoundedSchedulerLatency(t *testing.T) {
	cache := &Cache{}
	dls := &Downloaders{item: &CacheItem{cache: cache}}
	w := waiter{enqueuedAt: time.Now().Add(-10 * time.Millisecond)}
	dls.waiterCount.Store(1)
	cache.pendingReads.Store(1)
	dls.finishWaiterLocked(w)

	if got := cache.readWaitCount.Load(); got != 1 {
		t.Fatalf("read wait count = %d, want 1", got)
	}
	if got := cache.readWaitMaxNanos.Load(); got < int64(10*time.Millisecond) {
		t.Fatalf("max read wait = %s, want at least 10ms", time.Duration(got))
	}
	if got := dls.waiterCount.Load(); got != 0 {
		t.Fatalf("waiter count = %d, want 0", got)
	}
	if got := cache.pendingReads.Load(); got != 0 {
		t.Fatalf("pending reads = %d, want 0", got)
	}
}

func TestEnsureDownloaderLocked_ExtendsMissByReadAhead(t *testing.T) {
	const (
		reqPos    = 10 * testMiB
		reqSize   = 128 * testKiB
		readAhead = 16 * testMiB
	)

	item := &CacheItem{
		info: ItemInfo{
			Size: 64 * testMiB,
		},
	}

	dl := &downloader{
		start:     reqPos,
		offset:    reqPos,
		maxOffset: reqPos + reqSize,
	}

	dls := &Downloaders{
		item:          item,
		chunkSize:     4 * testMiB,
		readAheadSize: readAhead,
		dls:           []*downloader{dl},
	}

	req := ranges.Range{Pos: reqPos, Size: reqSize}
	if err := dls.ensureDownloaderLocked(req, false); err != nil {
		t.Fatalf("ensureDownloaderLocked returned error: %v", err)
	}

	want := req.End() + readAhead
	got := getMaxOffset(dl)
	if got != want {
		t.Fatalf("unexpected maxOffset: got %d, want %d", got, want)
	}
}

func TestEnsureDownloaderLocked_CachedRequestPrefetchesGap(t *testing.T) {
	const (
		reqPos    = 0
		reqSize   = 128 * testKiB
		readAhead = 16 * testMiB
	)

	item := &CacheItem{
		info: ItemInfo{
			Size: 64 * testMiB,
			Rs: ranges.Ranges{
				{Pos: 0, Size: 1 * testMiB}, // request is cached, look-ahead has a gap after 1 MiB
			},
		},
	}

	dl := &downloader{
		start:     512 * testKiB,
		offset:    2 * testMiB,
		maxOffset: 2 * testMiB,
	}

	dls := &Downloaders{
		item:          item,
		chunkSize:     4 * testMiB,
		readAheadSize: readAhead,
		dls:           []*downloader{dl},
	}

	req := ranges.Range{Pos: reqPos, Size: reqSize}
	if err := dls.ensureDownloaderLocked(req, false); err != nil {
		t.Fatalf("ensureDownloaderLocked returned error: %v", err)
	}

	want := req.End() + readAhead
	got := getMaxOffset(dl)
	if got != want {
		t.Fatalf("unexpected maxOffset: got %d, want %d", got, want)
	}
}

func TestEnsureDownloaderLocked_CachedWindowFullDoesNotExtend(t *testing.T) {
	const (
		reqPos    = 0
		reqSize   = 128 * testKiB
		readAhead = 16 * testMiB
	)

	item := &CacheItem{
		info: ItemInfo{
			Size: 64 * testMiB,
			Rs: ranges.Ranges{
				{Pos: 0, Size: 32 * testMiB}, // request + look-ahead fully cached
			},
		},
	}

	dl := &downloader{
		start:     0,
		offset:    2 * testMiB,
		maxOffset: 2 * testMiB,
	}

	dls := &Downloaders{
		item:          item,
		chunkSize:     4 * testMiB,
		readAheadSize: readAhead,
		dls:           []*downloader{dl},
	}

	req := ranges.Range{Pos: reqPos, Size: reqSize}
	if err := dls.ensureDownloaderLocked(req, false); err != nil {
		t.Fatalf("ensureDownloaderLocked returned error: %v", err)
	}

	want := int64(2 * testMiB)
	got := getMaxOffset(dl)
	if got != want {
		t.Fatalf("unexpected maxOffset when window is full: got %d, want %d", got, want)
	}
}

func TestStopAllClearsWaiters(t *testing.T) {
	parentCtx := context.Background()
	ctx, cancel := context.WithCancel(parentCtx)

	dls := &Downloaders{
		parentCtx: parentCtx,
		ctx:       ctx,
		cancel:    cancel,
	}

	errCh := make(chan error, 1)
	dls.waiters = append(dls.waiters, waiter{
		r:       ranges.Range{Pos: 0, Size: 1},
		errChan: errCh,
	})
	dls.waiterCount.Store(1)

	dls.StopAll()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected waiter to receive stop error")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("waiter was not unblocked by StopAll")
	}

	if got := dls.waiterCount.Load(); got != 0 {
		t.Fatalf("unexpected waiter count after StopAll: got %d, want 0", got)
	}
}

func TestCacheItemReleaseStopsDownloadersOnZeroOpens(t *testing.T) {
	parentCtx := context.Background()
	ctx, cancel := context.WithCancel(parentCtx)

	dls := &Downloaders{
		parentCtx: parentCtx,
		ctx:       ctx,
		cancel:    cancel,
	}

	item := &CacheItem{
		downloaders: dls,
	}
	item.opens.Store(1)

	item.Release()

	select {
	case <-ctx.Done():
	case <-time.After(200 * time.Millisecond):
		t.Fatal("expected downloader context to be canceled when opens reaches zero")
	}

	if got := item.opens.Load(); got != 0 {
		t.Fatalf("unexpected opens after release: got %d, want 0", got)
	}
}

type dfsTestStream struct {
	reader *bytes.Reader
	size   int64
	closed bool
}

func newDFSTestStream(data []byte, offset int64) *dfsTestStream {
	reader := bytes.NewReader(data)
	_, _ = reader.Seek(offset, io.SeekStart)
	return &dfsTestStream{reader: reader, size: int64(len(data))}
}

func (s *dfsTestStream) Read(p []byte) (int, error)         { return s.reader.Read(p) }
func (s *dfsTestStream) Seek(o int64, w int) (int64, error) { return s.reader.Seek(o, w) }
func (s *dfsTestStream) Size() int64                        { return s.size }
func (s *dfsTestStream) Prime() error                       { return nil }
func (s *dfsTestStream) Close() error {
	s.closed = true
	return nil
}

func TestDownloaderReusesPersistentStreamAcrossChunks(t *testing.T) {
	data := []byte("abcdefgh")
	cache := newQuotaTestCache(t, t.TempDir(), 0)
	item := newQuotaTestItem(t, cache, "entry/video.mkv", int64(len(data)))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dls := &Downloaders{ctx: ctx, cancel: cancel, item: item}
	var opened []*dfsTestStream
	dls.openStream = func(_ context.Context, offset int64) (manager.StreamReader, error) {
		stream := newDFSTestStream(data, offset)
		opened = append(opened, stream)
		return stream, nil
	}
	dl := schedulerTestDownloader(dls, 0, int64(len(data)), false)

	if written, err := dl.streamChunk(0, 4); err != nil || written != 4 {
		t.Fatalf("first chunk = %d, %v", written, err)
	}
	if written, err := dl.streamChunk(4, 8); err != nil || written != 4 {
		t.Fatalf("second chunk = %d, %v", written, err)
	}
	if len(opened) != 1 {
		t.Fatalf("stream opens = %d, want 1", len(opened))
	}

	got := make([]byte, len(data))
	if n, err := item.buf.ReadAt(got, 0); err != nil || n != len(got) || !bytes.Equal(got, data) {
		t.Fatalf("cached data = %d, %q, %v", n, got, err)
	}
	dl.close()
	if !opened[0].closed {
		t.Fatal("persistent stream was not closed with downloader")
	}
}

func TestDownloaderRepositionsPersistentStreamForDisjointRange(t *testing.T) {
	data := []byte("abcdefgh")
	cache := newQuotaTestCache(t, t.TempDir(), 0)
	item := newQuotaTestItem(t, cache, "entry/video.mkv", int64(len(data)))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dls := &Downloaders{ctx: ctx, cancel: cancel, item: item}
	openCount := 0
	dls.openStream = func(_ context.Context, offset int64) (manager.StreamReader, error) {
		openCount++
		return newDFSTestStream(data, offset), nil
	}
	dl := schedulerTestDownloader(dls, 4, int64(len(data)), true)
	defer dl.close()

	if _, err := dl.streamChunk(4, 8); err != nil {
		t.Fatal(err)
	}
	if _, err := dl.streamChunk(0, 4); err != nil {
		t.Fatal(err)
	}
	if openCount != 1 {
		t.Fatalf("stream opens = %d, want 1", openCount)
	}
}

func TestDownloaderRejectsTruncatedPersistentStream(t *testing.T) {
	cache := newQuotaTestCache(t, t.TempDir(), 0)
	item := newQuotaTestItem(t, cache, "entry/video.mkv", 4)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dls := &Downloaders{ctx: ctx, cancel: cancel, item: item}
	dls.openStream = func(_ context.Context, offset int64) (manager.StreamReader, error) {
		return newDFSTestStream([]byte("ab"), offset), nil
	}
	dl := schedulerTestDownloader(dls, 0, 4, false)
	defer dl.close()

	written, err := dl.streamChunk(0, 4)
	if written != 2 || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated chunk = %d, %v", written, err)
	}
}
