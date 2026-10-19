package vfs

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/buffer"
	fuseconfig "github.com/Trifocals3537/tessarr/pkg/mount/dfs/config"
	"github.com/Trifocals3537/tessarr/pkg/mount/dfs/vfs/ranges"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
)

func TestDiskQuotaConcurrentReservationsNeverExceedLimit(t *testing.T) {
	const (
		limit   = int64(32)
		writers = 128
	)
	q := newDiskQuota(limit)
	start := make(chan struct{})
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if q.tryReserve(1) {
				admitted.Add(1)
				q.commit(1, 1)
			}
		}()
	}
	close(start)
	wg.Wait()

	snapshot := q.snapshot()
	if got := admitted.Load(); got != limit {
		t.Fatalf("admitted writers = %d, want %d", got, limit)
	}
	if snapshot.Used != limit || snapshot.Reserved != 0 || snapshot.Available != 0 {
		t.Fatalf("quota snapshot = %+v, want used=%d reserved=0 available=0", snapshot, limit)
	}
}

func TestDiskQuotaCommitCancelAndRelease(t *testing.T) {
	q := newDiskQuota(100)
	q.initialize(40)
	if !q.tryReserve(30) || !q.tryReserve(20) {
		t.Fatal("expected reservations within the limit to succeed")
	}
	if q.tryReserve(11) {
		t.Fatal("reservation that exceeds used plus reserved budget succeeded")
	}
	q.commit(30, 25)
	q.cancel(20)
	if released := q.release(10); released != 10 {
		t.Fatalf("release = %d, want 10", released)
	}

	snapshot := q.snapshot()
	if snapshot.Used != 55 || snapshot.Reserved != 0 || snapshot.Available != 45 {
		t.Fatalf("quota snapshot = %+v, want used=55 reserved=0 available=45", snapshot)
	}
}

func newQuotaTestCache(t *testing.T, cacheDir string, limit int64) *Cache {
	t.Helper()
	pool := buffer.NewPool(buffer.PoolConfig{
		Name:         "dfs-quota-test",
		MemoryBudget: 0,
		DiskLimit:    0,
		BackWindow:   0,
	})
	t.Cleanup(func() { _ = pool.Close() })
	return &Cache{
		config: &fuseconfig.FuseConfig{
			CacheDir:      cacheDir,
			CacheDiskSize: limit,
			CacheExpiry:   0,
		},
		items:  xsync.NewMap[string, *CacheItem](),
		logger: zerolog.Nop(),
		pool:   pool,
		quota:  newDiskQuota(limit),
	}
}

func newQuotaTestItem(t *testing.T, c *Cache, key string, size int64) *CacheItem {
	t.Helper()
	dataPath := filepath.Join(c.config.CacheDir, filepath.FromSlash(key))
	var item *CacheItem
	buf, err := c.pool.NewBuffer(buffer.Config{
		DiskPath:  dataPath,
		TotalSize: size,
		OnEvict: func(off, length int64) {
			if item != nil {
				item.onBufferEvict(off, length)
			}
		},
	})
	if err != nil {
		t.Fatalf("NewBuffer: %v", err)
	}
	item = &CacheItem{
		cache:    c,
		key:      key,
		buf:      buf,
		metaPath: dataPath + ".json",
		info: ItemInfo{
			Size:    size,
			ATime:   time.Now(),
			ModTime: time.Now(),
		},
	}
	return item
}

func TestWriteAtNoOverwriteRejectsGrowthAtQuota(t *testing.T) {
	c := newQuotaTestCache(t, t.TempDir(), 32)
	item := newQuotaTestItem(t, c, "entry/video.mkv", 64)

	if n, skipped, err := item.WriteAtNoOverwrite(make([]byte, 32), 0); err != nil || n != 32 || skipped != 0 {
		t.Fatalf("initial write = (%d, %d, %v), want (32, 0, nil)", n, skipped, err)
	}
	if _, _, err := item.WriteAtNoOverwrite([]byte{1}, 32); !errors.Is(err, ErrCacheDiskLimit) {
		t.Fatalf("over-limit write error = %v, want %v", err, ErrCacheDiskLimit)
	}

	snapshot := c.quota.snapshot()
	if snapshot.Used != 32 || snapshot.Reserved != 0 {
		t.Fatalf("quota after denial = %+v, want used=32 reserved=0", snapshot)
	}
	if got := item.info.Rs.Size(); got != 32 {
		t.Fatalf("published cached range size = %d, want 32", got)
	}
	if got := c.quotaDenials.Load(); got != 1 {
		t.Fatalf("quota denials = %d, want 1", got)
	}
}

func TestFailedBufferWriteCancelsReservation(t *testing.T) {
	c := newQuotaTestCache(t, t.TempDir(), 32)
	item := newQuotaTestItem(t, c, "entry/video.mkv", 32)
	if err := item.buf.Close(); err != nil {
		t.Fatalf("close buffer: %v", err)
	}

	if _, _, err := item.WriteAtNoOverwrite(make([]byte, 8), 0); !errors.Is(err, buffer.ErrClosed) {
		t.Fatalf("write to closed buffer error = %v, want %v", err, buffer.ErrClosed)
	}
	if snapshot := c.quota.snapshot(); snapshot.Used != 0 || snapshot.Reserved != 0 {
		t.Fatalf("quota after failed write = %+v, want empty", snapshot)
	}
}

func TestClosedPersistentItemRemainsCharged(t *testing.T) {
	c := newQuotaTestCache(t, t.TempDir(), 64)
	item := newQuotaTestItem(t, c, "entry/video.mkv", 64)
	if _, _, err := item.WriteAtNoOverwrite(make([]byte, 32), 0); err != nil {
		t.Fatalf("WriteAtNoOverwrite: %v", err)
	}
	if err := item.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	snapshot := c.quota.snapshot()
	if snapshot.Used != 32 || snapshot.Reserved != 0 {
		t.Fatalf("quota after persistent buffer close = %+v, want used=32 reserved=0", snapshot)
	}
	scan := c.scanDiskCandidates()
	if scan.totalSize != 32 {
		t.Fatalf("persisted metadata size = %d, want 32", scan.totalSize)
	}
}

func TestConcurrentCacheWritersShareOneHardQuota(t *testing.T) {
	const (
		limit      = int64(64)
		writerSize = int64(16)
		writers    = 8
	)
	c := newQuotaTestCache(t, t.TempDir(), limit)
	items := make([]*CacheItem, 0, writers)
	for i := range writers {
		items = append(items, newQuotaTestItem(t, c, filepath.ToSlash(filepath.Join("entry", "video-"+string(rune('a'+i))+".mkv")), writerSize))
	}

	start := make(chan struct{})
	results := make(chan error, writers)
	var wg sync.WaitGroup
	for _, item := range items {
		wg.Add(1)
		go func(item *CacheItem) {
			defer wg.Done()
			<-start
			_, _, err := item.WriteAtNoOverwrite(make([]byte, writerSize), 0)
			results <- err
		}(item)
	}
	close(start)
	wg.Wait()
	close(results)

	var successes, denials int
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrCacheDiskLimit):
			denials++
		default:
			t.Fatalf("unexpected writer error: %v", err)
		}
	}
	if successes != int(limit/writerSize) || denials != writers-successes {
		t.Fatalf("writer outcomes = %d successes, %d denials; want 4 and 4", successes, denials)
	}
	snapshot := c.quota.snapshot()
	if snapshot.Used != limit || snapshot.Reserved != 0 {
		t.Fatalf("quota after concurrent writes = %+v, want used=%d reserved=0", snapshot, limit)
	}
}

func TestQuotaPressureEvictsColdPersistentItemBeforeDenying(t *testing.T) {
	cacheDir := t.TempDir()
	entryDir := filepath.Join(cacheDir, "entry")
	if err := os.MkdirAll(entryDir, 0o755); err != nil {
		t.Fatal(err)
	}
	dataPath := filepath.Join(entryDir, "cold.mkv")
	metaPath := dataPath + ".json"
	if err := os.WriteFile(dataPath, make([]byte, 40), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(metaPath, []byte(`{"size":40,"ranges":[{"Pos":0,"Size":40}]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	c := newQuotaTestCache(t, cacheDir, 40)
	c.quota.initialize(40)
	if !c.reserveDisk("entry/writer.mkv", 10) {
		t.Fatal("reservation failed even though a cold persistent item was reclaimable")
	}
	c.quota.cancel(10)

	if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
		t.Fatalf("cold data file was not evicted, stat error = %v", err)
	}
	if _, err := os.Stat(metaPath); !os.IsNotExist(err) {
		t.Fatalf("cold metadata file was not evicted, stat error = %v", err)
	}
	if snapshot := c.quota.snapshot(); snapshot.Used != 0 || snapshot.Reserved != 0 {
		t.Fatalf("quota after pressure eviction = %+v, want empty", snapshot)
	}
	if c.quotaPressure.Load() != 1 || c.quotaDenials.Load() != 0 || c.quotaReclaimed.Load() != 40 {
		t.Fatalf("pressure stats = pressure:%d denials:%d reclaimed:%d", c.quotaPressure.Load(), c.quotaDenials.Load(), c.quotaReclaimed.Load())
	}
}

func TestQuotaPressureReclaimsActiveReadBehindBeforeColdFiles(t *testing.T) {
	c := newQuotaTestCache(t, t.TempDir(), 64)
	item := newQuotaTestItem(t, c, "entry/playing.mkv", 64)
	if _, _, err := item.WriteAtNoOverwrite(make([]byte, 64), 0); err != nil {
		t.Fatalf("WriteAtNoOverwrite: %v", err)
	}
	item.buf.SetReadHead(64)

	if !c.reserveDisk("entry/next.mkv", 16) {
		t.Fatal("reservation failed even though active read-behind was reclaimable")
	}
	c.quota.cancel(16)

	if got := item.info.Rs.Size(); got != 0 {
		t.Fatalf("cached range after read-behind reclaim = %d, want 0", got)
	}
	if snapshot := c.quota.snapshot(); snapshot.Used != 0 || snapshot.Reserved != 0 {
		t.Fatalf("quota after active reclaim = %+v, want empty", snapshot)
	}
	if c.quotaPressure.Load() != 1 || c.quotaDenials.Load() != 0 || c.quotaReclaimed.Load() != 64 {
		t.Fatalf("pressure stats = pressure:%d denials:%d reclaimed:%d", c.quotaPressure.Load(), c.quotaDenials.Load(), c.quotaReclaimed.Load())
	}
}

func TestCacheInitializationAccountsPersistentBytesBeforeServing(t *testing.T) {
	cacheDir := t.TempDir()
	entryDir := filepath.Join(cacheDir, "entry")
	if err := os.MkdirAll(entryDir, 0o755); err != nil {
		t.Fatal(err)
	}
	dataPath := filepath.Join(entryDir, "video.mkv")
	if err := os.WriteFile(dataPath, make([]byte, 50), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(dataPath+".json", []byte(`{"size":100,"ranges":[{"Pos":0,"Size":50}]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	c := newQuotaTestCache(t, cacheDir, 100)
	c.threshold = 90
	c.initializeDiskState()
	c.evict()

	stats := c.GetStats()
	if stats["total_size"] != int64(50) || stats["available_size"] != int64(50) || stats["item_count"] != int64(1) {
		t.Fatalf("startup cache stats = %#v, want 50 used, 50 available, 1 item", stats)
	}
}

func TestOverlappingWritesAreChargedOnce(t *testing.T) {
	c := newQuotaTestCache(t, t.TempDir(), 32)
	item := newQuotaTestItem(t, c, "entry/video.mkv", 32)
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, err := item.WriteAtNoOverwrite(make([]byte, 32), 0)
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("overlapping write failed: %v", err)
		}
	}
	if snapshot := c.quota.snapshot(); snapshot.Used != 32 || snapshot.Reserved != 0 {
		t.Fatalf("quota after overlapping writes = %+v, want used=32 reserved=0", snapshot)
	}
	if got := item.info.Rs; !got.Equal(ranges.Ranges{{Pos: 0, Size: 32}}) {
		t.Fatalf("published ranges = %#v, want one 32-byte range", got)
	}
}
