package buffer

import (
	"errors"
	"sync"
	"sync/atomic"
)

// Pool is the buffer service: it owns a RAM budget and a disk limit shared
// across every Buffer it hands out, plus the eviction policy that enforces
// them. Instantiate one per workload (e.g. one for DFS, one for usenet) so the
// two have independent budgets and can't starve each other.
//
// Memory: per-Buffer Config.MemorySize is a ceiling on one stream's hot
// working set; the Pool's MemoryBudget caps every owned block allocation
// across all its Buffers, including allocator reuse and blocks awaiting an
// asynchronous release. When the budget is exhausted, reusable blocks are
// released first. A Buffer then evicts its own trailing (LRU) blocks before
// caching new ones, and writes fall through to disk rather than growing RAM
// (self-eviction; no cross-Buffer LRU).
//
// Disk: the Pool tracks the total on-disk present bytes across its Buffers.
// When that exceeds DiskLimit a background worker punches holes behind each
// Buffer's read head (keeping a BackWindow of recent history) until the total
// is back under the limit. This is what bounds a cache directory even when a
// single huge file is being streamed and never closed — the case whole-file
// eviction can't handle. DiskLimit == 0 disables the disk backstop entirely
// (usenet relies on its own playback-aware sliding window instead).
type Pool struct {
	name       string
	memBudget  atomic.Int64 // RAM ceiling across Buffers; 0 = unlimited
	diskLimit  atomic.Int64 // on-disk present bytes ceiling; 0 = unlimited
	backWindow int64        // bytes retained behind a read head before punching

	memInUse     atomic.Int64 // active block bytes across Buffers
	memAllocated atomic.Int64 // all owned blocks, including reuse and pending release
	memReusable  atomic.Int64 // allocated bytes held on allocator free lists
	diskInUse    atomic.Int64 // sum of on-disk present bytes across Buffers

	mu      sync.RWMutex
	buffers map[*Buffer]struct{}
	// releaseWG lets Close wait for blocks already queued on the shared unmap
	// worker, so a replacement service cannot overlap the old pool's ownership.
	releaseWG sync.WaitGroup
	// reclaimMu serializes background backstop work with synchronous cache
	// pressure reclamation so two scans cannot punch the same ranges at once.
	reclaimMu sync.Mutex

	evictSig chan struct{}
	stopCh   chan struct{}
	wg       sync.WaitGroup
	closed   atomic.Bool

	statsPunches   atomic.Int64
	statsReclaimed atomic.Int64
}

// PoolConfig configures a Pool.
type PoolConfig struct {
	// Name labels the pool for logging/metrics ("dfs", "usenet").
	Name string

	// MemoryBudget caps allocated block bytes, including reusable and pending
	// release blocks. 0 = unlimited.
	MemoryBudget int64

	// DiskLimit caps the sum of on-disk present bytes across all Buffers. When
	// exceeded, the pool punches holes behind read heads to reclaim disk.
	// 0 = no disk backstop.
	DiskLimit int64

	// BackWindow is how many bytes behind a Buffer's read head disk reclamation
	// preserves (short seek-backs stay local). It applies to both the optional
	// DiskLimit worker and explicit ReclaimDisk calls.
	BackWindow int64
}

// PoolStats reports pool-wide counters.
type PoolStats struct {
	MemoryInUse int64
	// MemoryAllocated includes active blocks, allocator reuse, and releases
	// still queued for munmap. It is allocation size, not process RSS.
	MemoryAllocated int64
	MemoryBudget    int64
	DiskInUse       int64
	DiskLimit       int64
	Buffers         int
	DiskPunches     int64
	BytesReclaimed  int64
}

// NewPool creates a Pool. If DiskLimit > 0 it starts a background worker that
// reclaims disk by punching behind read heads when the limit is breached.
func NewPool(cfg PoolConfig) *Pool {
	if cfg.MemoryBudget < 0 {
		cfg.MemoryBudget = 0
	}
	if cfg.DiskLimit < 0 {
		cfg.DiskLimit = 0
	}
	if cfg.BackWindow < 0 {
		cfg.BackWindow = 0
	}
	p := &Pool{
		name:       cfg.Name,
		backWindow: cfg.BackWindow,
		buffers:    make(map[*Buffer]struct{}),
		evictSig:   make(chan struct{}, 1),
		stopCh:     make(chan struct{}),
	}
	p.memBudget.Store(cfg.MemoryBudget)
	p.diskLimit.Store(cfg.DiskLimit)
	if cfg.DiskLimit > 0 {
		p.wg.Add(1)
		go p.diskEvictLoop()
	}
	return p
}

// NewBuffer creates a Buffer bound to this pool. Its RAM and disk usage count
// against the pool's budgets.
func (p *Pool) NewBuffer(cfg Config) (*Buffer, error) {
	if p.closed.Load() {
		return nil, ErrClosed
	}
	b, err := newBuffer(p, cfg)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	if p.closed.Load() {
		p.mu.Unlock()
		_ = b.Close()
		return nil, ErrClosed
	}
	p.buffers[b] = struct{}{}
	p.mu.Unlock()
	return b, nil
}

// remove unregisters a Buffer from the pool. Called from Buffer.Close.
func (p *Pool) remove(b *Buffer) {
	p.mu.Lock()
	delete(p.buffers, b)
	p.mu.Unlock()
}

// Stats returns a snapshot of the pool's counters.
func (p *Pool) Stats() PoolStats {
	p.mu.RLock()
	n := len(p.buffers)
	p.mu.RUnlock()
	return PoolStats{
		MemoryInUse:     p.memInUse.Load(),
		MemoryAllocated: p.memAllocated.Load(),
		MemoryBudget:    p.memBudget.Load(),
		DiskInUse:       p.diskInUse.Load(),
		DiskLimit:       p.diskLimit.Load(),
		Buffers:         n,
		DiskPunches:     p.statsPunches.Load(),
		BytesReclaimed:  p.statsReclaimed.Load(),
	}
}

// Close stops the disk worker and closes every Buffer still registered. Safe
// to call when callers already own their Buffers' lifetimes — Buffer.Close is
// idempotent.
func (p *Pool) Close() error {
	p.mu.Lock()
	if !p.closed.CompareAndSwap(false, true) {
		p.mu.Unlock()
		return nil
	}
	bufs := make([]*Buffer, 0, len(p.buffers))
	for b := range p.buffers {
		bufs = append(bufs, b)
	}
	p.mu.Unlock()

	if p.diskLimit.Load() > 0 {
		close(p.stopCh)
		p.wg.Wait()
	}
	var firstErr error
	for _, b := range bufs {
		if err := b.Close(); err != nil && !errors.Is(err, ErrClosed) && firstErr == nil {
			firstErr = err
		}
	}
	p.releaseWG.Wait()
	return firstErr
}

// --- RAM admission ----------------------------------------------------------

// wouldExceedMemory is an optimistic active-working-set check. Final admission
// also reserves the underlying allocation atomically in tryReserveAllocation.
func (p *Pool) wouldExceedMemory() bool {
	if p.closed.Load() {
		return true
	}
	b := p.memBudget.Load()
	return b > 0 && p.memInUse.Load() > b-int64(blockSize)
}

// tryReserveBlock atomically admits one resident block against the shared RAM
// budget. Keeping the check and increment in one compare-and-swap loop prevents
// concurrent Buffers from each observing the same remaining capacity.
func (p *Pool) tryReserveBlock() bool {
	if p.closed.Load() {
		return false
	}
	for {
		current := p.memInUse.Load()
		budget := p.memBudget.Load()
		if budget > 0 && current > budget-int64(blockSize) {
			return false
		}
		if p.memInUse.CompareAndSwap(current, current+int64(blockSize)) {
			return true
		}
		if p.closed.Load() {
			return false
		}
	}
}

// tryReserveAllocation atomically charges one newly allocated block. Reusing
// an existing block does not call this method and therefore cannot double
// charge the allocation.
func (p *Pool) tryReserveAllocation() bool {
	if p.closed.Load() {
		return false
	}
	for {
		current := p.memAllocated.Load()
		budget := p.memBudget.Load()
		if budget > 0 && current > budget-int64(blockSize) {
			return false
		}
		if p.memAllocated.CompareAndSwap(current, current+int64(blockSize)) {
			return true
		}
		if p.closed.Load() {
			return false
		}
	}
}

// releaseReusable returns inactive allocator blocks to the OS before active
// data is denied. It snapshots the Buffer set so no pool lock is held while an
// allocator is drained.
func (p *Pool) releaseReusable() {
	if p.memReusable.Load() == 0 {
		return
	}
	p.mu.RLock()
	bufs := make([]*Buffer, 0, len(p.buffers))
	for b := range p.buffers {
		bufs = append(bufs, b)
	}
	p.mu.RUnlock()
	for _, b := range bufs {
		b.alloc.drain()
	}
}

func (p *Pool) dropBytes(n int64) {
	if n > 0 {
		p.memInUse.Add(-n)
	}
}

// --- Disk accounting + backstop ---------------------------------------------

// addDisk records newly-present on-disk bytes and pokes the backstop if the
// limit is now exceeded.
func (p *Pool) addDisk(n int64) {
	if n <= 0 {
		return
	}
	v := p.diskInUse.Add(n)
	if limit := p.diskLimit.Load(); limit > 0 && v > limit {
		p.signalDiskEvict()
	}
}

// subDisk records on-disk bytes that are no longer present (punched, discarded,
// or a closed Buffer's residual).
func (p *Pool) subDisk(n int64) {
	if n > 0 {
		p.diskInUse.Add(-n)
	}
}

func (p *Pool) signalDiskEvict() {
	select {
	case p.evictSig <- struct{}{}:
	default:
	}
}

func (p *Pool) diskEvictLoop() {
	defer p.wg.Done()
	for {
		select {
		case <-p.stopCh:
			return
		case <-p.evictSig:
			p.reclaimDisk()
		}
	}
}

// reclaimDisk punches holes behind every Buffer's read head (keeping the
// BackWindow) until the pool is back under DiskLimit or there is nothing left
// to safely reclaim. Each pass snapshots the buffer set so it never holds the
// pool lock across a punch syscall.
func (p *Pool) reclaimDisk() {
	limit := p.diskLimit.Load()
	if limit <= 0 {
		return
	}
	p.ReclaimDisk(max(p.diskInUse.Load()-limit, 0))
}

// ReclaimDisk synchronously punches up to n safely-reclaimable bytes behind
// active read heads. It is used by DFS admission before rejecting a cache
// write. The return value is the number of logical present bytes reclaimed.
func (p *Pool) ReclaimDisk(n int64) int64 {
	if p == nil || n <= 0 || p.closed.Load() {
		return 0
	}
	p.reclaimMu.Lock()
	defer p.reclaimMu.Unlock()

	var total int64
	for total < n {
		p.mu.RLock()
		bufs := make([]*Buffer, 0, len(p.buffers))
		for b := range p.buffers {
			bufs = append(bufs, b)
		}
		p.mu.RUnlock()
		if len(bufs) == 0 {
			return total
		}
		var reclaimed int64
		for _, b := range bufs {
			reclaimed += b.punchBehindWindow(p.backWindow)
			if total+reclaimed >= n {
				break
			}
		}
		total += reclaimed
		if reclaimed == 0 {
			return total
		}
	}
	return total
}
