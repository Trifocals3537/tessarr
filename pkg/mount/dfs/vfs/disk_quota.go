package vfs

import (
	"context"
	"errors"
	"sync"

	"github.com/Trifocals3537/tessarr/internal/utils"
)

var ErrCacheDiskLimit = errors.New("dfs cache: disk quota exhausted")

// Internal wakeup, never a downloader failure.
var errQuotaSpaceChanged = errors.New("dfs cache: quota space changed")

// diskQuota is the authoritative process-lifetime accounting for the DFS
// cache directory. Unlike buffer.Pool accounting, used bytes remain charged
// when an idle buffer closes because its sparse cache file persists on disk.
// Reservations are included in admission so concurrent writers cannot all
// observe the same remaining capacity.
type diskQuota struct {
	mu           sync.Mutex
	limit        int64
	used         int64
	reserved     int64
	spaceChanged chan struct{}
}

// spaceSignal is captured BEFORE checking admission so a physical release
// cannot be lost between the quota check and parking behind maintenance.
func (q *diskQuota) spaceSignal() <-chan struct{} {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.spaceChanged == nil {
		q.spaceChanged = make(chan struct{})
	}
	return q.spaceChanged
}
func (q *diskQuota) notifySpaceLocked() {
	if q.spaceChanged != nil {
		close(q.spaceChanged)
	}
	q.spaceChanged = make(chan struct{})
}

type diskQuotaSnapshot struct {
	Limit     int64
	Used      int64
	Reserved  int64
	Available int64
}

func newDiskQuota(limit int64) *diskQuota {
	if limit < 0 {
		limit = 0
	}
	return &diskQuota{limit: limit}
}

// initialize is used once, before NewCache publishes the cache to readers.
func (q *diskQuota) initialize(used int64) {
	if used < 0 {
		used = 0
	}
	q.mu.Lock()
	q.used = used
	q.reserved = 0
	q.mu.Unlock()
}

func (q *diskQuota) tryReserve(n int64) bool {
	if q == nil || n <= 0 {
		return true
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.limit > 0 && (n > q.limit || q.used > q.limit-n-q.reserved) {
		return false
	}
	q.reserved += n
	return true
}

// commit converts a reservation into persistent cache usage. committed may be
// smaller than reserved when a caller deliberately accepts a partial write.
func (q *diskQuota) commit(reserved, committed int64) {
	if q == nil || reserved <= 0 {
		return
	}
	if committed < 0 {
		committed = 0
	}
	if committed > reserved {
		committed = reserved
	}
	q.mu.Lock()
	q.reserved -= min(reserved, q.reserved)
	q.used += committed
	if committed < reserved {
		q.notifySpaceLocked()
	}
	q.mu.Unlock()
}

func (q *diskQuota) cancel(reserved int64) {
	if q == nil || reserved <= 0 {
		return
	}
	q.mu.Lock()
	canceled := min(reserved, q.reserved)
	q.reserved -= canceled
	if canceled > 0 {
		q.notifySpaceLocked()
	}
	q.mu.Unlock()
}

// release records physical cache bytes that were actually deleted or
// hole-punched. Closing a persistent buffer intentionally does not call this.
func (q *diskQuota) release(n int64) int64 {
	if q == nil || n <= 0 {
		return 0
	}
	q.mu.Lock()
	released := min(n, q.used)
	q.used -= released
	if released > 0 {
		q.notifySpaceLocked()
	}
	q.mu.Unlock()
	return released
}

func (q *diskQuota) shortfall(request int64) int64 {
	if q == nil || request <= 0 {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.limit <= 0 {
		return 0
	}
	return max(q.used+q.reserved+request-q.limit, 0)
}

func (q *diskQuota) snapshot() diskQuotaSnapshot {
	if q == nil {
		return diskQuotaSnapshot{}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	available := int64(0)
	if q.limit > 0 {
		available = max(q.limit-q.used-q.reserved, 0)
	}
	return diskQuotaSnapshot{
		Limit:     q.limit,
		Used:      q.used,
		Reserved:  q.reserved,
		Available: available,
	}
}

func (c *Cache) reserveDisk(excludedKey string, n int64) bool {
	return c.reserveDiskContext(context.Background(), excludedKey, n) == nil
}

func (c *Cache) reserveDiskContext(ctx context.Context, excludedKey string, n int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if n <= 0 || c == nil || c.quota == nil {
		return nil
	}
	pressure := false
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		space := c.quota.spaceSignal()
		if c.quota.tryReserve(n) {
			return nil
		}
		if !pressure {
			c.quotaPressure.Add(1)
			pressure = true
		}
		err := c.reclaimForReservation(ctx, excludedKey, n, space)
		// This is a predicate/notification wait, not a polling retry. Another
		// physical release (or canceled reservation) permits fresh admission even
		// if an unrelated cleanup is still joining a slow source. Every loop
		// follows a distinct quota generation or a context cancellation.
		if errors.Is(err, errQuotaSpaceChanged) {
			continue
		}
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.quota.tryReserve(n) {
			return nil
		}
		c.quotaDenials.Add(1)
		return ErrCacheDiskLimit
	}
}

func (c *Cache) releaseDisk(n int64) int64 {
	if c == nil || c.quota == nil || n <= 0 {
		return 0
	}
	released := c.quota.release(n)
	if released > 0 {
		c.quotaReclaimed.Add(released)
	}
	return released
}

// reclaimForReservation synchronously frees safe active read-behind ranges,
// then cold persistent entries. It never closes the item performing the write.
// cleanupMu serializes this path with periodic cleanup and other pressure
// writers, preventing a thundering herd of directory scans and removals.
func (c *Cache) reclaimForReservation(ctx context.Context, excludedKey string, request int64, space <-chan struct{}) error {
	if c == nil || c.quota == nil || request <= 0 {
		return nil
	}

	// Close cancels its downloader before joining it. A canceled downloader
	// exits this admission wait, while active writers wait for real reclamation.
	if err := c.cleanupMu.LockContextOrSpace(ctx, space); err != nil {
		return err
	}
	defer c.cleanupMu.Unlock()

	shortfall := c.quota.shortfall(request)
	if shortfall <= 0 {
		return nil
	}
	if c.pool != nil {
		// Fence hole-punch callbacks out of a writer's interval between its
		// buffer publish and metadata/quota commit. Do not hold this while
		// closing idle items: Close waits for their per-item writer lock.
		c.diskOpMu.Lock()
		c.pool.ReclaimDisk(shortfall)
		c.diskOpMu.Unlock()
		shortfall = c.quota.shortfall(request)
		if shortfall <= 0 {
			return nil
		}
	}

	// Convert idle in-memory items to closed persistent candidates, excluding
	// the writer that triggered pressure, then remove the coldest candidates
	// until the reservation fits or nothing safe remains.
	c.cleanupItemsExcept(utils.Now(), true, excludedKey)
	scan := c.scanDiskCandidates()
	target := max(scan.totalSize-shortfall, 0)
	remaining, _, removalErrors, removed := c.evictCandidates(
		utils.Now(),
		scan.candidates,
		scan.totalSize,
		target,
	)
	scan.errors += removalErrors
	c.releaseDisk(scan.totalSize - remaining)
	c.storeDiskStats(scan.candidates, removed)
	return nil
}
