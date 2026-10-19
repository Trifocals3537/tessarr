package vfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	fuseconfig "github.com/Trifocals3537/tessarr/pkg/mount/dfs/config"
	"github.com/Trifocals3537/tessarr/pkg/mount/dfs/vfs/ranges"
)

const (
	// maxDownloaderIdleTime is how long a downloader waits before stopping
	maxDownloaderIdleTime = 5 * time.Second
	// maxSkipBytes is how far a downloader will skip before restarting
	maxSkipBytes = 1 << 20 // 1MB
	// maxErrorCount is the number of errors before giving up
	maxErrorCount = 10
	// downloaderWindow is how close a read must be to reuse a downloader
	downloaderWindow = 4 * 1024 * 1024 // 4MB
	// probeChunkSize is the (small) initial chunk a priority downloader uses.
	// Latency-sensitive probe reads (ffprobe seeking to the moov atom near EOF)
	// must return a few bytes fast and release the NNTP connection quickly,
	// instead of being bundled behind a multi-MB read-ahead chunk.
	probeChunkSize = 1 * 1024 * 1024 // 1MB
	// kickerInterval is how often the safety-net ticker checks waiters and idle timeout
	kickerInterval = 5 * time.Second
	// activeWaiterKickerInterval is the faster fallback cadence while readers are blocked.
	activeWaiterKickerInterval = 1 * time.Second
	// idleTimeout is how long before stopping all downloaders due to inactivity
	idleTimeout = 30 * time.Second
	// circuitCooldownDuration is how long to block requests after max errors
	// reached. Kept short: a probe-heavy workload (Sonarr/Radarr library
	// scans) issues many short-lived reads, so a long lockout turns a brief
	// provider hiccup into minutes of "unable to detect if file is a sample".
	circuitCooldownDuration = 2 * time.Minute
	// maxChunkSizeMultiplier caps adaptive chunk growth at this multiple of baseChunkSize.
	// Without a cap, binary doubling eventually produces chunk sizes in the GB range,
	// causing oversized HTTP range requests that are wasteful on seeks.
	maxChunkSizeMultiplier = 16
	// maxFileDownloaders bounds provider and CPU pressure from disjoint reads
	// against one file. Two workers allow sequential playback plus one probe,
	// seek, or second reader without letting seek storms fan out indefinitely.
	maxFileDownloaders = 2
	// maxFileWaiters bounds memory retained by callers waiting on one file. A
	// full queue fails new reads explicitly and retryably instead of allowing a
	// stalled provider to accumulate an unbounded backlog inside the FUSE mount.
	maxFileWaiters = 256
)

// Downloaders coordinates multiple concurrent downloads to a cache item
type Downloaders struct {
	parentCtx     context.Context
	ctx           context.Context
	cancel        context.CancelFunc
	item          *CacheItem
	manager       *manager.Manager
	chunkSize     int64
	readAheadSize int64
	retries       int
	// openStream is an internal seam for deterministic stream-session tests.
	// Production uses Manager.OpenStreamUntrackedWithOptions.
	openStream func(context.Context, int64) (manager.StreamReader, error)
	// runDownloader is an internal seam for deterministic scheduler tests. A
	// nil value uses downloader.run in production.
	runDownloader func(*downloader) (int64, error)

	mu         sync.Mutex
	dls        []*downloader
	waiters    []waiter
	errorCount int
	lastErr    error
	closed     bool
	// stopping is true while StopAll() is tearing down the current session.
	// It blocks the idle-restart path from spinning up a fresh kicker before
	// the previous one has fully exited and the new ctx is installed.
	stopping bool
	// stopCond is broadcast when StopAll finishes tearing down a session
	// (stopping goes false and a fresh ctx is installed). Download waits on
	// it instead of proceeding mid-teardown: a downloader spawned on the old,
	// already-canceled ctx exits instantly and fails every pending waiter
	// with a spurious "context canceled" read error. Lazily initialized under
	// mu via stopCondLocked.
	stopCond *sync.Cond
	// idle is true when all downloaders have stopped. Guarded by mu so that
	// the restart decision in Download() and the teardown in StopAll() are
	// serialized with kicker lifecycle changes.
	idle bool

	// streamID is the active stream registration ID for tracking
	streamID string

	// Atomic waiter count for fast-path check (avoids locking dls.mu in Write() when no waiters)
	waiterCount atomic.Int32

	// Idle timeout tracking
	lastActivity atomic.Int64  // Unix nano timestamp of last download activity
	kickerDone   chan struct{} // Signals kicker goroutine has exited; a fresh channel per session

	// Circuit breaker - blocks all requests when max errors reached
	circuitOpen   atomic.Bool  // True when circuit is "open" (blocking all requests)
	circuitOpenAt atomic.Int64 // Unix nano timestamp when circuit opened
}

// ensureStreamTracked makes sure the active stream is registered when reads begin.
func (dls *Downloaders) ensureStreamTracked() {
	dls.mu.Lock()
	defer dls.mu.Unlock()

	if dls.closed || dls.streamID != "" {
		return
	}

	dls.streamID = dls.manager.TrackStream(dls.item.entry, dls.item.filename, "DFS")
}

// untrackStreamLocked removes the stream registration. Caller must hold dls.mu.
func (dls *Downloaders) untrackStreamLocked() {
	if dls.streamID == "" {
		return
	}
	dls.manager.UntrackStream(dls.streamID)
	dls.streamID = ""
}

// waiter represents a caller waiting for a range to be downloaded
type waiter struct {
	r          ranges.Range
	errChan    chan<- error
	enqueuedAt time.Time
	// priority marks a latency-sensitive read (e.g. ffprobe's near-EOF moov
	// seek). Priority waiters spawn a dedicated small-chunk downloader with no
	// read-ahead extension so they don't queue behind bulk prefetch.
	priority bool
}

// downloader represents a single download goroutine
type downloader struct {
	dls    *Downloaders
	quit   chan struct{}
	kick   chan struct{}
	ctx    context.Context    // per-downloader context; canceled by stop()
	cancel context.CancelFunc // cancels ctx

	mu        sync.Mutex
	start     int64 // Starting offset
	offset    int64 // Current offset
	maxOffset int64 // How far to download
	skipped   int64 // Consecutive skipped bytes
	stopped   bool
	closed    bool

	baseChunkSize    int64
	currentChunkSize int64
	// priority downloaders keep a small fixed chunk size (no adaptive growth)
	// so they satisfy probes without filling unnecessary read-ahead cache.
	priority bool

	wg sync.WaitGroup

	idleTimer *time.Timer
	stream    manager.StreamReader
}

// NewDownloaders creates a new download coordinator
func NewDownloaders(ctx context.Context, mgr *manager.Manager, item *CacheItem, cfg *fuseconfig.FuseConfig) *Downloaders {
	parentCtx := ctx
	ctx, cancel := context.WithCancel(parentCtx)
	chunkSize := cfg.ChunkSize
	if chunkSize <= 0 {
		chunkSize = 4 * 1024 * 1024
	}
	readAheadSize := cfg.ReadAheadSize
	if readAheadSize <= 0 {
		readAheadSize = chunkSize * 4 // Default: 4 chunks ahead
	}
	retries := cfg.Retries
	if retries <= 0 {
		retries = 3
	}

	dls := &Downloaders{
		parentCtx:     parentCtx,
		ctx:           ctx,
		cancel:        cancel,
		item:          item,
		manager:       mgr,
		chunkSize:     chunkSize,
		readAheadSize: readAheadSize,
		retries:       retries,
		// streamID is populated lazily when the first read occurs.
		streamID: "",
	}
	dls.openStream = func(streamCtx context.Context, offset int64) (manager.StreamReader, error) {
		dls.mu.Lock()
		streamID := dls.streamID
		dls.mu.Unlock()
		return mgr.OpenStreamUntrackedWithOptions(
			manager.WithActiveStreamID(streamCtx, streamID),
			item.entry,
			item.filename,
			offset,
			manager.StreamSessionOptions{MaxAttempts: retries},
		)
	}
	dls.touchActivity() // Initialize activity timestamp

	// Background kicker to handle stalled waiters and idle detection
	dls.startKicker()

	return dls
}

// Download blocks until the range r is on disk, or until ctx is canceled.
func (dls *Downloaders) Download(ctx context.Context, r ranges.Range) error {
	return dls.DownloadWithPriority(ctx, r, false)
}

// DownloadWithPriority is Download with an optional priority hint. Priority
// reads (small/random/near-EOF, e.g. ffprobe) get a dedicated small-chunk
// downloader with no read-ahead extension so they are not starved behind bulk
// sequential prefetch under high connection load.
func (dls *Downloaders) DownloadWithPriority(ctx context.Context, r ranges.Range, priority bool) error {
	// Circuit breaker: reject immediately if circuit is open
	if dls.isCircuitOpen() {
		lastErr := dls.getLastErr()
		if lastErr == nil {
			return errors.New("circuit breaker open, cooldown active")
		}
		return fmt.Errorf("circuit breaker open, cooldown active: last error: %w", lastErr)
	}

	dls.ensureStreamTracked()

	// Update activity timestamp for idle detection
	dls.touchActivity()

	dls.mu.Lock()
	// Wait out an in-flight StopAll without making request cancellation wait
	// for downloader teardown. The callback takes mu so its broadcast cannot
	// be lost between checking ctx and entering Cond.Wait.
	if dls.stopping && !dls.closed {
		cond := dls.stopCondLocked()
		stopWake := context.AfterFunc(ctx, func() {
			dls.mu.Lock()
			cond.Broadcast()
			dls.mu.Unlock()
		})
		defer stopWake()
		for dls.stopping && !dls.closed && ctx.Err() == nil {
			cond.Wait()
		}
	}
	if err := ctx.Err(); err != nil {
		dls.mu.Unlock()
		return err
	}
	if dls.closed {
		dls.mu.Unlock()
		return errors.New("downloaders closed")
	}

	// Lazy restart: if we went idle, restart the kicker goroutine.
	if dls.idle {
		dls.idle = false
		dls.ensureKickerRunningLocked()
	}

	// Fast path: already have it
	if dls.item.HasRange(r) {
		if err := dls.ensureDownloaderLocked(r, priority); err != nil {
			dls.mu.Unlock()
			return err
		}
		dls.mu.Unlock()
		return nil
	}
	// Create waiter channel. Admission is bounded per file so a stalled source
	// cannot retain an unlimited number of blocked FUSE requests.
	errChan := make(chan error, 1)
	if err := dls.appendWaiterLocked(waiter{r: r, errChan: errChan, priority: priority}); err != nil {
		dls.mu.Unlock()
		return err
	}

	// Ensure downloader running
	if err := dls.ensureDownloaderLocked(r, priority); err != nil {
		// Remove our waiter on error
		dls.removeWaiterLocked(errChan)
		dls.mu.Unlock()
		return err
	}

	dls.mu.Unlock()

	// Block until range is fulfilled, caller canceled, or error.
	// Selecting on ctx.Done() prevents goroutine leaks when the FUSE read
	// is interrupted (client disconnect, read timeout, unmount).
	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		dls.mu.Lock()
		dls.removeWaiterLocked(errChan)
		dls.mu.Unlock()
		return ctx.Err()
	}
}

func (dls *Downloaders) appendWaiterLocked(w waiter) error {
	if len(dls.waiters) >= maxFileWaiters {
		if dls.item != nil && dls.item.cache != nil {
			dls.item.cache.schedulerQueueFull.Add(1)
		}
		return customerror.NewError(
			fmt.Errorf("DFS read queue full for file (%d pending reads)", len(dls.waiters)),
			http.StatusServiceUnavailable,
			"dfs_read_queue_full",
			true,
			false,
		).Retryable()
	}
	if w.enqueuedAt.IsZero() {
		w.enqueuedAt = time.Now()
	}
	dls.waiters = append(dls.waiters, w)
	dls.waiterCount.Add(1)
	if dls.item != nil && dls.item.cache != nil {
		dls.item.cache.pendingReads.Add(1)
	}
	return nil
}

func (dls *Downloaders) finishWaiterLocked(w waiter) {
	dls.waiterCount.Add(-1)
	if dls.item == nil || dls.item.cache == nil {
		return
	}
	dls.item.cache.pendingReads.Add(-1)
	if !w.enqueuedAt.IsZero() {
		dls.item.cache.recordReadWait(time.Since(w.enqueuedAt))
	}
}

const (
	// downloadRetryAttempts bounds how many times a read re-attempts a
	// transient download failure before surfacing EIO to FUSE. ffprobe treats
	// one EIO as fatal ("unable to detect if file is a sample"), so a single
	// connection-starvation blip under load must not fail it — but we must not
	// block it long either, since ffprobe has its own I/O timeout.
	downloadRetryAttempts = 3
	downloadRetryBackoff  = 150 * time.Millisecond

	// probeTailZone: a read whose end lands in the final zone of the file is
	// treated as a latency-sensitive probe (ffprobe seeking to the MP4 moov
	// atom / MKV cues near EOF). For sequential playback this only matters at
	// the very end, where read-ahead is clipped anyway — negligible cost.
	probeTailZone = 64 * 1024 * 1024
)

// isProbeRead reports whether a read looks like a media-probe access pattern
// (near-EOF), which should be prioritized over bulk sequential prefetch.
func isProbeRead(off, length, fileSize int64) bool {
	if fileSize <= 0 || length <= 0 {
		return false
	}
	return off+length >= fileSize-probeTailZone
}

// DownloadWithRetry blocks until r is available, re-attempting transient
// failures a few times with short backoff so a momentary connection shortage
// under high load does not surface as a hard read error to ffprobe. It fails
// fast (no retry) on cancellation, an open circuit breaker, or a non-transient
// error — retrying those would only spin.
func (dls *Downloaders) DownloadWithRetry(ctx context.Context, r ranges.Range, priority bool) error {
	var err error
	for attempt := range downloadRetryAttempts {
		if err = dls.DownloadWithPriority(ctx, r, priority); err == nil {
			return nil
		}
		var exhausted *manager.StreamSessionExhaustedError
		if ctx.Err() != nil || dls.isCircuitOpen() || errors.As(err, &exhausted) || !customerror.IsRetriableError(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(downloadRetryBackoff * time.Duration(attempt+1)):
		}
	}
	return err
}

// removeWaiterLocked removes a waiter by its channel (call with lock held).
// Order is not significant, so swap-with-last is used for O(1) removal.
func (dls *Downloaders) removeWaiterLocked(errChan chan<- error) {
	for i, w := range dls.waiters {
		if w.errChan == errChan {
			last := len(dls.waiters) - 1
			dls.waiters[i] = dls.waiters[last]
			dls.waiters = dls.waiters[:last]
			dls.finishWaiterLocked(w)
			return
		}
	}
}

func (dls *Downloaders) getLastErr() error {
	dls.mu.Lock()
	defer dls.mu.Unlock()
	return dls.lastErr
}

// ensureDownloaderLocked finds or creates a downloader for the range.
// It extends the requested range by readAheadSize before clipping to missing
// data, so the downloader's target always stays ahead of the read position.
// The downloader idle timeout naturally limits waste on probes/seeks.
func (dls *Downloaders) ensureDownloaderLocked(r ranges.Range, priority bool) error {
	// Priority reads fetch only the missing bytes they actually need (no
	// read-ahead extension) so they complete and release the connection fast.
	// Bulk reads extend by read-ahead then clip to actual missing bytes.
	if priority {
		r = dls.item.FindMissing(r)
	} else {
		r = dls.extendAndFindMissingRangeLocked(r)
	}

	// If the requested range + read-ahead is already present, we just need
	// to kick an existing downloader to prevent idle timeout. No new download needed.
	if r.IsEmpty() {
		dls.kickExistingDownloaderLocked(r.Pos)
		return nil
	}

	// Target end: download the full missing range
	targetEnd := r.Pos + r.Size

	// Check error count
	if dls.errorCount >= maxErrorCount {
		if dls.lastErr == nil {
			return fmt.Errorf("too many errors (%d)", dls.errorCount)
		}
		return fmt.Errorf("too many errors (%d): last error: %w", dls.errorCount, dls.lastErr)
	}

	// Look for existing downloader in range
	dls.removeClosed()
	if dl := dls.findDownloaderForPosLocked(r.Pos); dl != nil {
		// Extend existing downloader
		dl.setMaxOffset(targetEnd)
		return nil
	}

	// Start new downloader when a per-file slot is available. If both slots
	// are occupied, cancel only obsolete read-ahead (a worker that no current
	// waiter still needs). The queued waiter will be reconsidered when that
	// worker exits and calls kickWaiters. We deliberately do not start the
	// replacement here, which keeps the hard concurrency bound intact.
	if dls.activeDownloaderCountLocked() < maxFileDownloaders {
		return dls.newDownloaderLocked(r, targetEnd, priority)
	}
	if stale := dls.obsoleteDownloaderLocked(r.Pos, priority); stale != nil {
		stale.stop()
		if dls.item != nil && dls.item.cache != nil {
			dls.item.cache.schedulerPreemptions.Add(1)
		}
	}
	return nil
}

func (dls *Downloaders) activeDownloaderCountLocked() int {
	count := 0
	for _, dl := range dls.dls {
		if dl.isActive() {
			count++
		}
	}
	return count
}

// obsoleteDownloaderLocked chooses a worker that is doing read-ahead only:
// no live waiter intersects the bytes it can still produce. Non-priority
// workers are preferred so a short probe already in flight can finish.
func (dls *Downloaders) obsoleteDownloaderLocked(requestPos int64, requestPriority bool) *downloader {
	var best *downloader
	bestPriority := false
	bestDistance := int64(-1)
	for _, dl := range dls.dls {
		start, offset, targetEnd, priority, active := dl.schedulingState()
		if !active || dls.downloaderHasWaiterLocked(start, offset, targetEnd) {
			continue
		}
		// Bulk playback must not evict a short latency-sensitive probe. A new
		// probe may evict obsolete work of either class.
		if !requestPriority && priority {
			continue
		}
		if best != nil && !bestPriority && priority {
			continue
		}
		if best != nil && bestPriority && !priority {
			best = nil
			bestDistance = -1
		}
		distance := requestPos - offset
		if distance < 0 {
			distance = -distance
		}
		if best == nil || distance > bestDistance {
			best = dl
			bestPriority = priority
			bestDistance = distance
		}
	}
	return best
}

func (dls *Downloaders) downloaderHasWaiterLocked(start, offset, targetEnd int64) bool {
	if targetEnd < offset {
		targetEnd = offset
	}
	coverage := ranges.Range{Pos: start, Size: targetEnd - start}
	if coverage.IsEmpty() {
		return false
	}
	for _, w := range dls.waiters {
		if !coverage.Intersection(w.r).IsEmpty() {
			return true
		}
	}
	return false
}

// extendAndFindMissingRangeLocked expands a request by read-ahead and returns
// only the missing tail that must be downloaded.
func (dls *Downloaders) extendAndFindMissingRangeLocked(r ranges.Range) ranges.Range {
	bufferWindow := dls.readAheadSize
	if bufferWindow <= 0 {
		bufferWindow = dls.chunkSize * 4
	}

	r.Size += bufferWindow
	if r.Pos+r.Size > dls.item.info.Size {
		r.Size = dls.item.info.Size - r.Pos
	}
	return dls.item.FindMissing(r)
}

func (dls *Downloaders) downloaderMatchWindowLocked() int64 {
	window := int64(downloaderWindow)
	if half := dls.chunkSize / 2; half > window {
		window = half
	}
	return window
}

func (dls *Downloaders) findDownloaderForPosLocked(pos int64) *downloader {
	window := dls.downloaderMatchWindowLocked()
	for _, dl := range dls.dls {
		if !dl.isActive() {
			continue
		}
		start, offset := dl.getRange()
		if pos >= start && pos < offset+window {
			return dl
		}
	}
	return nil
}

// kickExistingDownloaderLocked kicks a nearby downloader to prevent idle timeout.
// Caller must hold dls.mu.
func (dls *Downloaders) kickExistingDownloaderLocked(pos int64) {
	dl := dls.findDownloaderForPosLocked(pos)
	if dl == nil {
		return
	}
	_, offset := dl.getRange()
	dl.setMaxOffset(offset) // kick without extending
}

// newDownloaderLocked creates and starts a new downloader
func (dls *Downloaders) newDownloaderLocked(r ranges.Range, targetEnd int64, priority bool) error {
	baseChunk := dls.chunkSize
	if baseChunk <= 0 {
		baseChunk = 4 * 1024 * 1024
	}
	// Priority downloaders use a small fixed chunk so the latency-sensitive
	// bytes return quickly and the NNTP connection is freed for other reads.
	if priority && baseChunk > probeChunkSize {
		baseChunk = probeChunkSize
	}

	// Each downloader gets its own context derived from the Downloaders context.
	// stop() cancels dlCtx, which interrupts any in-flight stream-session read
	// without having to cancel the shared dls.ctx.
	dlCtx, dlCancel := context.WithCancel(dls.ctx)

	dl := &downloader{
		dls:              dls,
		quit:             make(chan struct{}),
		kick:             make(chan struct{}, 1),
		ctx:              dlCtx,
		cancel:           dlCancel,
		start:            r.Pos,
		offset:           r.Pos,
		maxOffset:        targetEnd,
		baseChunkSize:    baseChunk,
		currentChunkSize: baseChunk,
		priority:         priority,
	}

	dls.dls = append(dls.dls, dl)

	// Track active download count.
	if dls.item != nil && dls.item.cache != nil {
		dls.item.cache.activeDownloads.Add(1)
	}

	dl.wg.Go(func() {
		defer dlCancel() // always release the per-downloader context
		run := dl.run
		if dls.runDownloader != nil {
			run = func() (int64, error) { return dls.runDownloader(dl) }
		}
		n, err := run()
		dl.close()
		// Only count real errors. If dl.ctx was canceled (intentional stop/close),
		// the error is not a network/server failure and must not trip the circuit breaker.
		if dl.ctx.Err() == nil {
			dls.countErrors(n, err)
		}
		// Release the slot before scheduling queued work. This makes
		// active_downloads reflect the hard per-file scheduler bound even while
		// this goroutine performs its final waiter notification.
		if dls.item != nil && dls.item.cache != nil {
			dls.item.cache.activeDownloads.Add(-1)
		}
		dls.kickWaiters()
	})

	return nil
}

// removeClosed removes closed downloaders from the list
func (dls *Downloaders) removeClosed() {
	newDls := dls.dls[:0]
	for _, dl := range dls.dls {
		if !dl.isClosed() {
			newDls = append(newDls, dl)
		}
	}
	dls.dls = newDls
}

// countErrors tracks errors and resets on success.
// Context cancellation (intentional stop/close) is never counted — it must not
// trip the circuit breaker, because the downloader was halted on purpose.
func (dls *Downloaders) countErrors(n int64, err error) {
	dls.mu.Lock()
	defer dls.mu.Unlock()

	if err == nil && n > 0 {
		dls.errorCount = 0
		dls.lastErr = nil
		// Success resets circuit breaker
		dls.resetCircuitLocked()
		return
	}
	if err != nil {
		// Intentional stop/shutdown — not a real failure.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return
		}
		dls.errorCount++
		dls.lastErr = err
		// Only a genuinely permanent provider failure (article missing, auth,
		// payment/permission) fast-trips the breaker — retrying those 10× is
		// pointless. Transient/ambiguous errors (timeout, stall, "stream
		// produced no data", "exhausted retries") only increment, so the
		// breaker requires SUSTAINED failure. This is what stops one bad
		// moment under load from locking a file out of every ffprobe.
		if nntp.IsArticleNotFoundError(err) || customerror.IsPermanentError(err) {
			dls.errorCount = maxErrorCount
		}
		// Trip circuit breaker when max errors reached
		if dls.errorCount >= maxErrorCount {
			dls.openCircuitLocked()
		}
	}
}

// kickWaiters checks all waiters and fulfills completed ones
func (dls *Downloaders) kickWaiters() {
	dls.mu.Lock()
	defer dls.mu.Unlock()

	if len(dls.waiters) == 0 {
		return
	}

	// Check circuit state once to avoid spinning
	circuitOpen := dls.circuitOpen.Load()

	remaining := dls.waiters[:0]
	for _, w := range dls.waiters {
		// Clip range to actual file size
		r := w.r
		r.Clip(dls.item.info.Size)

		if dls.item.HasRange(r) {
			w.errChan <- nil // Fulfilled!
			dls.finishWaiterLocked(w)
		} else if circuitOpen || dls.errorCount >= maxErrorCount {
			// Circuit is open or max errors reached - fail waiter without creating new downloaders
			w.errChan <- dls.lastErr
			dls.finishWaiterLocked(w)
		} else {
			remaining = append(remaining, w)
		}
	}
	dls.waiters = remaining
	// Spawn at most one missing downloader per kick. Re-ensuring for every
	// waiter can create duplicate provider sessions for the same range under load.
	if len(remaining) == 0 || circuitOpen || dls.errorCount >= maxErrorCount {
		return
	}
	if dls.closed || dls.stopping {
		return
	}

	// If the shared context is already canceled, fail all remaining waiters
	// immediately instead of spawning downloaders that exit instantly and
	// call kickWaiters() again — that creates a CPU-spinning goroutine loop.
	if dls.ctx.Err() != nil {
		ctxErr := dls.ctx.Err()
		for _, w := range remaining {
			w.errChan <- ctxErr
			dls.finishWaiterLocked(w)
		}
		dls.waiters = remaining[:0]
		return
	}

	dls.removeClosed()
	for _, w := range remaining {
		var missing ranges.Range
		if w.priority {
			missing = dls.item.FindMissing(w.r)
		} else {
			missing = dls.extendAndFindMissingRangeLocked(w.r)
		}
		if missing.IsEmpty() {
			continue
		}
		if dls.findDownloaderForPosLocked(missing.Pos) != nil {
			continue
		}
		_ = dls.ensureDownloaderLocked(w.r, w.priority)
		break
	}
}

// Close stops all downloaders and returns unfulfilled waiters with error
func (dls *Downloaders) Close(inErr error) error {
	dls.mu.Lock()
	if dls.closed {
		dls.mu.Unlock()
		return nil
	}
	dls.closed = true
	dls.untrackStreamLocked()
	// An item can die with its breaker open (e.g. idle eviction after a bad
	// spell); release its slot in the cache-wide gauge or it leaks forever.
	dls.resetCircuitLocked()

	// Copy slice before unlocking to avoid races while waiting.
	dlsCopy := make([]*downloader, len(dls.dls))
	copy(dlsCopy, dls.dls)

	// Stop all downloaders
	for _, dl := range dlsCopy {
		dl.stop()
	}
	oldKickerDone := dls.kickerDone
	dls.mu.Unlock()

	// Cancel first so any blocked stream operation can exit promptly.
	dls.cancel()

	// Wait for downloaders to finish
	for _, dl := range dlsCopy {
		dl.wg.Wait()
	}

	// Wait for the kicker goroutine via its per-session sentinel.
	if oldKickerDone != nil {
		<-oldKickerDone
	}

	// Close remaining waiters
	dls.mu.Lock()
	for _, w := range dls.waiters {
		if inErr != nil {
			w.errChan <- inErr
		} else {
			w.errChan <- errors.New("downloaders closed")
		}
		dls.finishWaiterLocked(w)
	}
	dls.waiters = nil
	dls.dls = nil
	dls.mu.Unlock()

	return nil
}

// touchActivity updates the last activity timestamp
func (dls *Downloaders) touchActivity() {
	dls.lastActivity.Store(time.Now().UnixNano())
}

// isCircuitOpen returns true if the circuit breaker is open and cooldown hasn't expired
func (dls *Downloaders) isCircuitOpen() bool {
	if !dls.circuitOpen.Load() {
		return false
	}
	// Check if cooldown has expired using raw nanoseconds (no allocation)
	openedAt := dls.circuitOpenAt.Load()
	if openedAt == 0 {
		return false
	}
	if time.Now().UnixNano()-openedAt >= int64(circuitCooldownDuration) {
		// Cooldown expired - reset circuit and clear error budget. Go through
		// resetCircuitLocked so the cache-wide circuit_breakers gauge is
		// decremented — the previous inline reset skipped that, leaking +1
		// into the gauge on every cooldown expiry.
		dls.mu.Lock()
		openedAt = dls.circuitOpenAt.Load()
		if openedAt != 0 && time.Now().UnixNano()-openedAt >= int64(circuitCooldownDuration) {
			dls.resetCircuitLocked()
			dls.errorCount = 0
			dls.lastErr = nil
		}
		dls.mu.Unlock()
		return false
	}
	return true
}

// openCircuitLocked trips the circuit breaker. Caller must hold dls.mu.
func (dls *Downloaders) openCircuitLocked() {
	if dls.circuitOpen.Load() {
		return // Already open
	}
	dls.circuitOpen.Store(true)
	dls.circuitOpenAt.Store(time.Now().UnixNano())
	dls.item.cache.circuitBreakers.Add(1)
}

// resetCircuitLocked resets the circuit breaker after successful download. Caller must hold dls.mu.
func (dls *Downloaders) resetCircuitLocked() {
	if !dls.circuitOpen.Load() {
		return // Already closed
	}
	dls.circuitOpen.Store(false)
	dls.circuitOpenAt.Store(0)
	dls.item.cache.circuitBreakers.Add(-1)
}

// checkIdleTimeout returns true if idle timeout has been reached and stops all downloaders
func (dls *Downloaders) checkIdleTimeout() bool {
	dls.mu.Lock()
	defer dls.mu.Unlock()

	// Don't timeout if already closed or already idle
	if dls.closed {
		return true
	}

	// Don't timeout if there are active waiters
	if len(dls.waiters) > 0 {
		return false
	}

	// Check if any downloaders are still running
	activeDownloaders := 0
	for _, dl := range dls.dls {
		if !dl.isClosed() {
			activeDownloaders++
		}
	}

	// Check idle timeout
	lastActivity := dls.lastActivity.Load()
	if lastActivity == 0 {
		return false
	}

	idleDuration := time.Since(time.Unix(0, lastActivity))
	if idleDuration < idleTimeout {
		return false
	}

	// Idle timeout reached - stop all downloaders
	for _, dl := range dls.dls {
		dl.stop()
	}
	dls.dls = nil
	dls.untrackStreamLocked()
	dls.idle = true

	// Reset error budget so the next session starts fresh.
	// Errors from the previous session must not carry into a resumed session —
	// that would shrink the error budget and could immediately trip the circuit
	// breaker the next time the user starts playback.
	dls.errorCount = 0
	dls.lastErr = nil
	dls.resetCircuitLocked()

	return true
}

// StopAll stops all active downloaders but keeps the Downloaders struct alive
// for potential reuse. This is called when all file handles are closed.
func (dls *Downloaders) StopAll() {
	dls.mu.Lock()
	if dls.closed || dls.stopping {
		// Another StopAll() is already tearing this session down, or we are
		// already closed. Either way there is nothing left for us to do.
		dls.mu.Unlock()
		return
	}
	dls.stopping = true
	dls.untrackStreamLocked()

	// Copy slice before unlocking to avoid race during Wait
	dlsCopy := make([]*downloader, len(dls.dls))
	copy(dlsCopy, dls.dls)
	waitersCopy := make([]waiter, len(dls.waiters))
	copy(waitersCopy, dls.waiters)
	for _, w := range dls.waiters {
		dls.finishWaiterLocked(w)
	}
	dls.waiters = nil

	// Stop all downloaders
	for _, dl := range dlsCopy {
		dl.stop()
	}
	dls.dls = nil
	dls.idle = true
	oldCancel := dls.cancel
	oldKickerDone := dls.kickerDone
	dls.mu.Unlock()

	// Cancel active context so in-flight session reads can be interrupted.
	oldCancel()

	// Unblock any pending readers waiting on ranges from old downloaders.
	for _, w := range waitersCopy {
		w.errChan <- errors.New("downloaders stopped")
	}

	// Wait for them to finish (using copy, safe to iterate without lock)
	for _, dl := range dlsCopy {
		dl.wg.Wait()
	}
	// Wait for the kicker goroutine to exit using its per-session sentinel.
	// Each startKicker() allocates a fresh channel, so this never observes a
	// channel that a future kicker has reused.
	if oldKickerDone != nil {
		<-oldKickerDone
	}

	dls.mu.Lock()
	if !dls.closed {
		dls.ctx, dls.cancel = context.WithCancel(dls.parentCtx)
		// Reset error budget for the next session. Accumulated errors from
		// the current session must not poison resumed playback.
		dls.errorCount = 0
		dls.lastErr = nil
		dls.resetCircuitLocked()
	}
	dls.stopping = false
	dls.stopCondLocked().Broadcast() // wake Downloads parked on the teardown
	dls.mu.Unlock()
}

// stopCondLocked lazily initializes the teardown condition variable. Caller
// must hold dls.mu.
func (dls *Downloaders) stopCondLocked() *sync.Cond {
	if dls.stopCond == nil {
		dls.stopCond = sync.NewCond(&dls.mu)
	}
	return dls.stopCond
}

// ensureKickerRunningLocked restarts the kicker goroutine if it has stopped.
// Caller must hold dls.mu. Refuses to start a new kicker while the session
// is closed or being torn down — that would race against StopAll()/Close()
// waiting on the previous kicker's exit sentinel.
func (dls *Downloaders) ensureKickerRunningLocked() {
	if dls.closed || dls.stopping {
		return
	}
	// Check if kicker has exited (non-blocking check)
	select {
	case <-dls.kickerDone:
		// Kicker has exited, need to restart it
		dls.startKicker()
	default:
		// Kicker still running
	}
}

func (dls *Downloaders) currentKickerInterval() time.Duration {
	if dls.waiterCount.Load() > 0 {
		return activeWaiterKickerInterval
	}
	return kickerInterval
}

// startKicker starts a background safety-net goroutine that periodically checks
// waiters and handles idle timeout. The primary notification path is direct
// kickWaiters() calls from cacheWriter.Write(); this ticker is only a fallback.
func (dls *Downloaders) startKicker() {
	ctx := dls.ctx
	done := make(chan struct{})
	dls.kickerDone = done
	go func() {
		defer close(done)

		interval := dls.currentKickerInterval()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Adjust cadence if waiter presence changed since last tick.
				if next := dls.currentKickerInterval(); next != interval {
					interval = next
					ticker.Reset(next)
				}
				dls.kickWaiters()
				if dls.checkIdleTimeout() {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// downloader methods

// run is the main download loop
func (dl *downloader) run() (totalBytes int64, err error) {
	for {
		// Single lock to get all state
		start, targetEnd, chunkSize, fileSize, stopped := dl.getState()
		if stopped || start >= fileSize {
			return totalBytes, nil
		}

		// Nothing to do - wait for more work or timeout
		if start >= targetEnd {
			// Do not hold a provider connection while waiting for a future range.
			// Active sequential chunks still share one session; a later kick opens
			// exactly at its new missing offset.
			dl.closeStream()
			if !dl.waitForWork() {
				return totalBytes, nil
			}
			continue
		}

		// Calculate chunk boundaries
		// Always download at least chunkSize to amortize cache bookkeeping.
		chunkEnd := min(start+chunkSize, fileSize)

		// Ensure we're downloading something meaningful
		if chunkEnd <= start {
			continue
		}

		written, chunkErr := dl.downloadChunk(start, chunkEnd)
		totalBytes += written

		if chunkErr != nil {
			if errors.Is(chunkErr, io.EOF) {
				return totalBytes, nil
			}
			if dl.ctx.Err() != nil {
				return totalBytes, dl.ctx.Err()
			}
			return totalBytes, chunkErr
		}
	}
}

// getState returns current download state with single lock acquisition
func (dl *downloader) getState() (start, targetEnd, chunkSize, fileSize int64, stopped bool) {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	chunkSize = dl.currentChunkSize
	if chunkSize <= 0 {
		chunkSize = dl.baseChunkSize
		if chunkSize <= 0 {
			chunkSize = 4 * 1024 * 1024
		}
	}

	fileSize = dl.dls.item.info.Size
	targetEnd = min(dl.maxOffset, fileSize)

	return dl.offset, targetEnd, chunkSize, fileSize, dl.stopped
}

// waitForWork blocks until new work arrives or timeout
func (dl *downloader) waitForWork() bool {
	if dl.idleTimer == nil {
		dl.idleTimer = time.NewTimer(maxDownloaderIdleTime)
	} else {
		if !dl.idleTimer.Stop() {
			select {
			case <-dl.idleTimer.C:
			default:
			}
		}
		dl.idleTimer.Reset(maxDownloaderIdleTime)
	}
	select {
	case <-dl.quit:
		return false
	case <-dl.kick:
		return true
	case <-dl.idleTimer.C:
		return false
	}
}

// downloadChunk downloads one adaptive cache chunk. Provider retries, exact
// offset resume, and backoff are owned by the persistent stream session; DFS
// must not multiply that budget with another outer retry loop.
func (dl *downloader) downloadChunk(start, end int64) (int64, error) {
	chunkLen := end - start
	written, err := dl.streamChunk(start, end)
	if err == nil {
		dl.adjustChunkSize(chunkLen, written, true)
		return written, nil
	}
	dl.adjustChunkSize(chunkLen, written, false)
	if dl.ctx.Err() != nil {
		return written, dl.ctx.Err()
	}
	return written, err
}

// getRange returns the current download range
func (dl *downloader) getRange() (start, offset int64) {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	return dl.start, dl.offset
}

func (dl *downloader) schedulingState() (start, offset, targetEnd int64, priority, active bool) {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	return dl.start, dl.offset, dl.maxOffset, dl.priority, !dl.stopped && !dl.closed
}

func (dl *downloader) isActive() bool {
	_, _, _, _, active := dl.schedulingState()
	return active
}

func (dl *downloader) streamChunk(start, end int64) (int64, error) {
	dl.mu.Lock()
	if dl.stopped {
		dl.mu.Unlock()
		return 0, io.EOF
	}
	dl.mu.Unlock()

	// Check if this range is already cached BEFORE calling the streaming function.
	// This avoids expensive network/reader operations for already-present data.
	requestedRange := ranges.Range{Pos: start, Size: end - start}
	missingRange := dl.dls.item.FindMissing(requestedRange)
	if missingRange.Size <= 0 {
		// All data already present - just advance offset and return
		dl.mu.Lock()
		dl.offset = end
		dl.mu.Unlock()
		return 0, nil
	}

	// Stream the missing portion
	// Advance offset to skip already-cached data before the missing range
	if missingRange.Pos > start {
		dl.mu.Lock()
		dl.offset = missingRange.Pos
		dl.mu.Unlock()
	}

	writer := &cacheWriter{
		dl:     dl,
		item:   dl.dls.item,
		offset: missingRange.Pos,
	}

	stream, err := dl.streamAt(missingRange.Pos)
	if err == nil {
		_, err = io.CopyN(writer, stream, missingRange.Size)
	}

	// Always drain the coalescing buffer — partial trailing bytes from the
	// stream still need to land on disk and in info.Rs before we return so
	// readers see them.
	flushErr := writer.flush()
	if err == nil {
		err = flushErr
	}

	if err != nil {
		if dl.ctx.Err() != nil {
			return writer.written, dl.ctx.Err()
		}
		if errors.Is(err, io.EOF) && writer.offset < missingRange.End() {
			err = io.ErrUnexpectedEOF
		}
		return writer.written, err
	}

	// Ensure we made progress (either written data or skipped existing data).
	// If offset hasn't moved, re-check whether a concurrent downloader filled
	// this range while we were streaming. Under high load (ffprobe + import
	// scan + prefetch all touching the same file) overlapping downloaders are
	// common; the range being present now is a benign race, NOT a failure —
	// treating it as one used to trip the circuit breaker and lock the file
	// out for minutes.
	if writer.offset == missingRange.Pos {
		if dl.dls.item.FindMissing(requestedRange).Size <= 0 {
			dl.mu.Lock()
			dl.offset = end
			dl.mu.Unlock()
			return writer.written, nil
		}
		return writer.written, errors.New("stream produced no data")
	}

	// Final kick to notify waiters of any remaining data
	if writer.written > 0 {
		dl.dls.kickWaiters()
	}

	return writer.written, nil
}

func (dl *downloader) streamAt(offset int64) (manager.StreamReader, error) {
	if dl.stream == nil {
		if dl.dls.openStream == nil {
			return nil, errors.New("DFS stream opener is not configured")
		}
		stream, err := dl.dls.openStream(dl.ctx, offset)
		if err != nil {
			return nil, err
		}
		dl.stream = stream
		return stream, nil
	}
	position, err := dl.stream.Seek(offset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	if position != offset {
		return nil, fmt.Errorf("DFS stream seek returned offset %d, want %d", position, offset)
	}
	return dl.stream, nil
}

func (dl *downloader) closeStream() {
	if dl.stream == nil {
		return
	}
	_ = dl.stream.Close()
	dl.stream = nil
}

// setMaxOffset extends the download range
func (dl *downloader) setMaxOffset(max int64) {
	dl.mu.Lock()
	if max > dl.maxOffset {
		dl.maxOffset = max
	}
	dl.mu.Unlock()

	// Kick to wake up if waiting
	select {
	case dl.kick <- struct{}{}:
	default:
	}
}

func (dl *downloader) adjustChunkSize(chunkLen, written int64, success bool) {
	dl.mu.Lock()
	defer dl.mu.Unlock()

	// Only reset on actual failure, not on partial writes due to pre-cached data
	// If success is false, it means the stream itself failed
	if !success {
		dl.currentChunkSize = dl.baseChunkSize
		return
	}

	// If no data needed to be written (all cached), don't change chunk size
	if chunkLen <= 0 {
		return
	}

	// Priority downloaders stay small on purpose — never ramp them up, or a
	// probe stream would start hogging a connection like a bulk download.
	if dl.priority {
		dl.currentChunkSize = dl.baseChunkSize
		return
	}

	// Double chunk size on successful download to quickly ramp up on good connections,
	// but cap at maxChunkSizeMultiplier × base to avoid oversized HTTP range requests on seeks.
	next := dl.currentChunkSize * 2
	if maxChunk := dl.baseChunkSize * maxChunkSizeMultiplier; next > maxChunk {
		next = maxChunk
	}
	if next <= 0 {
		next = dl.baseChunkSize
	}
	dl.currentChunkSize = next
}

// stop signals the downloader to stop and cancels its context so any
// in-flight stream-session read is interrupted promptly.
func (dl *downloader) stop() {
	dl.mu.Lock()
	if !dl.stopped {
		dl.stopped = true
		close(dl.quit)
	}
	dl.mu.Unlock()
	dl.cancel() // interrupt in-flight manager.Stream
}

// close marks the downloader as closed
func (dl *downloader) close() {
	dl.closeStream()
	dl.mu.Lock()
	dl.closed = true
	dl.mu.Unlock()
}

// isClosed returns true if downloader is closed
func (dl *downloader) isClosed() bool {
	dl.mu.Lock()
	defer dl.mu.Unlock()
	return dl.closed
}

// cacheWriter writes streamed body bytes straight through to the cache item.
//
// Earlier iterations buffered writes here into a 4–16 MB scratch slice
// before flushing — the idea being to amortize sparse-file WriteAt syscalls.
// That was the right optimization *before* the cache item had a real block
// cache underneath. With the buffer.Buffer in place, the buffer's 1 MB
// blocks already batch small writes into block-sized disk writes at the
// correct layer; a second coalescer in front of WriteAtNoOverwrite only
// delays when bytes become visible to waiting readers (a 16 MB buffer
// pushes the first-byte latency for a player by seconds at typical NNTP
// throughput, since the waiter is only woken when the buffer flushes).
//
// So Write goes directly to WriteAtNoOverwrite. The buffer underneath does
// the actual write batching — at the right granularity, with the right
// liveness, and without holding bytes hostage from readers.
type cacheWriter struct {
	dl      *downloader
	item    *CacheItem
	offset  int64 // next write offset; advances with Write
	written int64
}

func (w *cacheWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	n, skipped, err := w.item.writeAtNoOverwriteContext(w.dl.ctx, p, w.offset)
	if err != nil {
		// WriteAtNoOverwrite's legacy count is len(p), even on failure.
		// Passing that count through lets io.CopyN hide an exact-length
		// quota/cancellation error as success. No bytes were acknowledged.
		return 0, err
	}
	w.dl.mu.Lock()
	if skipped == n {
		w.dl.skipped += int64(skipped)
	} else {
		w.dl.skipped = 0
	}
	w.dl.offset += int64(n)
	if w.dl.skipped > maxSkipBytes {
		w.dl.stopped = true
		w.dl.mu.Unlock()
		return n, io.EOF
	}
	w.dl.mu.Unlock()

	w.offset += int64(n)
	actuallyWritten := int64(n - skipped)
	w.written += actuallyWritten

	if actuallyWritten > 0 {
		w.dl.dls.item.cache.AddDownloadedBytes(actuallyWritten)
		if w.dl.dls.waiterCount.Load() > 0 {
			w.dl.dls.kickWaiters()
		}
	}
	return n, nil
}

// flush is a no-op shim kept so streamChunk's existing call site stays
// legible. The coalescer is gone; nothing buffers.
func (w *cacheWriter) flush() error { return nil }
