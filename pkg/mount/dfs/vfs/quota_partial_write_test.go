package vfs

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/mount/dfs/vfs/ranges"
)

func TestPartialCacheWritePersistsPublishedRangesAndRetriesExactly(t *testing.T) {
	c := newQuotaTestCache(t, t.TempDir(), 4)
	item := newQuotaTestItem(t, c, "partial/data", 4)
	t.Cleanup(func() { _ = item.Close() })
	c.items.Store(item.key, item)
	c.itemCount.Add(1)
	want := []byte{10, 11, 12, 13}
	if _, _, err := item.WriteAtNoOverwrite(want[1:2], 1); err != nil {
		t.Fatal(err)
	}
	item.flushMetadata(false)
	// Another writer holds one byte. The first missing span fits; the second
	// cannot fit until that independent reservation is canceled.
	if !c.quota.tryReserve(1) {
		t.Fatal("could not reserve independent writer capacity")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dls := &Downloaders{ctx: ctx, cancel: cancel, item: item}
	opens := 0
	dls.openStream = func(context.Context, int64) (manager.StreamReader, error) {
		opens++
		return newDFSTestStream(want, 0), nil
	}
	dl := schedulerTestDownloader(dls, 0, 4, false)
	defer dl.close()
	if n, err := dl.streamChunk(0, 4); n != 0 || !errors.Is(err, ErrCacheDiskLimit) {
		t.Fatalf("partial copy acknowledged failed input: n=%d err=%v", n, err)
	}
	if q := c.quota.snapshot(); q.Used != 2 || q.Reserved != 1 {
		t.Fatalf("partial publication accounting: %+v", q)
	}
	// Exercise the ordinary metadata flush, without Close's forced flush. The
	// bytes published before the failure must be represented on disk as well.
	item.flushMetadata(false)
	var persisted ItemInfo
	if err := decodeJSONFile(item.metaPath, &persisted); err != nil {
		t.Fatal(err)
	}
	if !persisted.Rs.Present(ranges.Range{Pos: 0, Size: 2}) || persisted.Rs.Present(ranges.Range{Pos: 2, Size: 2}) {
		t.Errorf("persisted ranges lost partial publication or advertised missing bytes: %+v", persisted.Rs)
	}
	c.quota.cancel(1)
	if n, err := dl.streamChunk(0, 4); err != nil || n != 2 {
		t.Fatalf("retry did not resume only missing bytes: n=%d err=%v", n, err)
	}
	got := make([]byte, len(want))
	if n, err := item.buf.ReadAt(got, 0); err != nil || n != len(want) || !bytes.Equal(got, want) {
		t.Fatalf("retry data = %v n=%d err=%v, want %v", got, n, err, want)
	}
	if q := c.quota.snapshot(); q.Used != 4 || q.Reserved != 0 {
		t.Errorf("retry double-counted cached bytes or leaked reservations: %+v", q)
	}
	if opens != 1 || dl.offset != 4 {
		t.Errorf("retry did not seek and resume the existing source: opens=%d offset=%d", opens, dl.offset)
	}
}

func TestQuotaCapacityNotificationContract(t *testing.T) {
	for _, operation := range []string{"full-commit", "partial-commit", "cancel", "release"} {
		t.Run(operation, func(t *testing.T) {
			q := newDiskQuota(8)
			if !q.tryReserve(8) {
				t.Fatal("initial reservation failed")
			}
			before := q.spaceSignal()
			available := int64(0)
			switch operation {
			case "full-commit":
				q.commit(8, 8)
			case "partial-commit":
				q.commit(8, 3)
				available = 5
			case "cancel":
				q.cancel(8)
				available = 8
			case "release":
				q.commit(8, 8)
				q.release(3)
				available = 3
			}
			select {
			case <-before:
				if available == 0 {
					t.Fatal("unchanged capacity generated a wakeup")
				}
			default:
				if available > 0 {
					t.Fatal("capacity became available without waking existing admission")
				}
			}
			if got := q.snapshot().Available; got != available {
				t.Fatalf("available=%d, want %d", got, available)
			}
			if next := q.spaceSignal(); available > 0 && next == before {
				t.Fatal("new admission inherited a stale notification")
			} else {
				select {
				case <-next:
					t.Fatal("new admission would spin on an already closed notification")
				default:
				}
			}
		})
	}
}

func TestConcurrentAdmissionWakesOnReservationCancellation(t *testing.T) {
	c := newQuotaTestCache(t, t.TempDir(), 8)
	if !c.quota.tryReserve(8) {
		t.Fatal("initial reservation failed")
	}
	c.cleanupMu.Lock()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	const writers = 64
	results := make(chan error, writers)
	var workers sync.WaitGroup
	defer func() {
		cancel()
		c.cleanupMu.Unlock()
		workers.Wait()
	}()
	start := make(chan struct{})
	for range writers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			results <- c.reserveDiskContext(ctx, "writer", 1)
		}()
	}
	close(start)
	// All writers must first encounter the full quota. This verifies wakeups
	// for parked admission, rather than only fresh admission after release.
	for c.quotaPressure.Load() != writers {
		if ctx.Err() != nil {
			t.Fatal("writers did not reach full-quota admission")
		}
		runtime.Gosched()
	}
	c.quota.cancel(4)
	for range 4 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("available capacity stayed behind maintenance: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("reservation cancellation failed to admit available writers")
		}
	}
	if q := c.quota.snapshot(); q.Used != 0 || q.Reserved != 8 || q.Available != 0 {
		t.Errorf("concurrent writers exceeded hard quota: %+v", q)
	}
	cancel()
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("canceled writers remained behind maintenance")
	}
	for range writers - 4 {
		if err := <-results; !errors.Is(err, context.Canceled) {
			t.Errorf("parked writer did not cancel without a quota denial: %v", err)
		}
	}
	if c.quotaDenials.Load() != 0 {
		t.Error("maintenance contention counted as hard quota exhaustion")
	}
}
