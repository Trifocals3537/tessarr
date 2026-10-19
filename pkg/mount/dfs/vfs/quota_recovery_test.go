package vfs

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/mount/dfs/vfs/ranges"
)

type quotaCleanupGateStream struct {
	*dfsTestStream
	ctx              context.Context
	started, release chan struct{}
}

func (s *quotaCleanupGateStream) Read(p []byte) (int, error) {
	close(s.started)
	<-s.ctx.Done()
	<-s.release
	return 0, s.ctx.Err()
}

func waitQuotaAdmission(t *testing.T, c *Cache) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if c.quotaDenials.Load() > 0 {
			return
		}
		stacks := make([]byte, 65536)
		n := runtime.Stack(stacks, true)
		if strings.Contains(string(stacks[:n]), "(*cleanupMutex).LockContext") {
			return
		}
		runtime.Gosched()
	}
	t.Fatal("real writer never reached quota admission")
}

func TestQuotaAdmissionCancellationWhileCleanupOwnsLock(t *testing.T) {
	c := newQuotaTestCache(t, t.TempDir(), 1)
	item := newQuotaTestItem(t, c, "cancel/data", 2)
	if _, _, err := item.WriteAtNoOverwrite([]byte{1}, 0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dls := &Downloaders{ctx: ctx, cancel: cancel, item: item}
	dls.openStream = func(context.Context, int64) (manager.StreamReader, error) { return newDFSTestStream([]byte{2}, 0), nil }
	dl := schedulerTestDownloader(dls, 1, 2, false)
	defer dl.close()
	c.cleanupMu.Lock()
	done := make(chan error, 1)
	go func() { _, err := dl.streamChunk(1, 2); done <- err }()
	waitQuotaAdmission(t, c)
	cancel()
	var err error
	select {
	case err = <-done:
	case <-time.After(time.Second):
		c.cleanupMu.Unlock()
		t.Fatal("canceled writer did not exit before cleanup released lock")
	}
	c.cleanupMu.Unlock()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("admission cancellation=%v", err)
	}
	if c.quotaDenials.Load() != 0 {
		t.Fatal("cancellation charged quota denial")
	}
}

func TestQuotaPressureEvictsToZeroTarget(t *testing.T) {
	c := newQuotaTestCache(t, t.TempDir(), 4)
	cold := newQuotaTestItem(t, c, "cold/data", 4)
	if _, _, err := cold.WriteAtNoOverwrite([]byte{1, 1, 1, 1}, 0); err != nil {
		t.Fatal(err)
	}
	if err := cold.Close(); err != nil {
		t.Fatal(err)
	}
	active := newQuotaTestItem(t, c, "active/data", 4)
	if _, _, err := active.WriteAtNoOverwrite([]byte{9, 9, 9, 9}, 0); err != nil {
		t.Fatalf("full-size reservation could not reclaim closed persistent file: %v", err)
	}
	if q := c.quota.snapshot(); q.Used != 4 || q.Reserved != 0 {
		t.Fatalf("quota=%+v", q)
	}
}

func TestSpaceReclaimedWhilePeriodicCleanupStillJoins(t *testing.T) {
	c := newQuotaTestCache(t, t.TempDir(), 8)
	idle := newQuotaTestItem(t, c, "idle/data", 7)
	if _, _, err := idle.WriteAtNoOverwrite(bytes.Repeat([]byte{1}, 6), 0); err != nil {
		t.Fatal(err)
	}
	idle.info.ATime = time.Now().Add(-2 * itemIdleTimeout)
	c.items.Store(idle.key, idle)
	c.itemCount.Add(1)
	active := newQuotaTestItem(t, c, "active/data", 3)
	if _, _, err := active.WriteAtNoOverwrite([]byte{3, 3}, 0); err != nil {
		t.Fatal(err)
	}
	active.Open()
	defer active.Release()
	c.items.Store(active.key, active)
	c.itemCount.Add(1)
	session, cancel := context.WithCancel(t.Context())
	idleDls := &Downloaders{parentCtx: t.Context(), ctx: session, cancel: cancel, item: idle}
	idle.downloaders = idleDls
	started, release := make(chan struct{}), make(chan struct{})
	idleDls.openStream = func(ctx context.Context, off int64) (manager.StreamReader, error) {
		return &quotaCleanupGateStream{dfsTestStream: newDFSTestStream(bytes.Repeat([]byte{1}, 7), off), ctx: ctx, started: started, release: release}, nil
	}
	idleDls.mu.Lock()
	if err := idleDls.newDownloaderLocked(ranges.Range{Pos: 6, Size: 1}, 7, false); err != nil {
		t.Fatal(err)
	}
	dl := idleDls.dls[0]
	idleDls.mu.Unlock()
	<-started
	cleanupDone := make(chan struct{})
	go func() { c.evict(); close(cleanupDone) }()
	<-dl.ctx.Done()
	activeCtx, activeCancel := context.WithCancel(t.Context())
	dls := &Downloaders{parentCtx: t.Context(), ctx: activeCtx, cancel: activeCancel, item: active, chunkSize: 1, readAheadSize: 1, retries: 1}
	active.downloaders = dls
	dls.openStream = func(context.Context, int64) (manager.StreamReader, error) {
		return newDFSTestStream([]byte{3, 3, 9}, 2), nil
	}
	ctx, stop := context.WithTimeout(t.Context(), 2*time.Second)
	defer stop()
	type result struct {
		n    int
		err  error
		data byte
	}
	done := make(chan result, 1)
	go func() { p := make([]byte, 1); n, err := active.ReadAtContext(ctx, p, 2); done <- result{n, err, p[0]} }()
	waitQuotaAdmission(t, c)
	// Actual hole-punch plus production OnEvict releases persistent quota while
	// periodic cleanup is still held in the unrelated canceled source join.
	c.diskOpMu.Lock()
	c.pool.ReclaimDisk(2)
	c.diskOpMu.Unlock()
	if c.quota.snapshot().Available < 1 {
		close(release)
		<-cleanupDone
		t.Fatal("real read-behind did not free quota")
	}
	var r result
	returned := false
	select {
	case r = <-done:
		returned = true
	case <-time.After(250 * time.Millisecond):
		t.Error("active reader stayed behind global cleanup after physical SpaceReclaimed")
	}
	close(release)
	<-cleanupDone
	if !returned {
		r = <-done
	}
	_ = active.Close()
	t.Logf("read after independent reclamation n=%d byte=%d err=%v quota=%+v", r.n, r.data, r.err, c.quota.snapshot())
	if r.n != 1 || r.data != 9 || r.err != nil || c.quotaDenials.Load() != 0 || dls.errorCount != 0 {
		t.Fatal("reader did not recover cleanly")
	}
}

func TestCacheWriterPreservesQuotaError(t *testing.T) {
	c := newQuotaTestCache(t, t.TempDir(), 1)
	item := newQuotaTestItem(t, c, "full/data", 2)
	if _, _, err := item.WriteAtNoOverwrite([]byte{1}, 0); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	dls := &Downloaders{ctx: ctx, cancel: cancel, item: item}
	dls.openStream = func(context.Context, int64) (manager.StreamReader, error) { return newDFSTestStream([]byte{2}, 0), nil }
	dl := schedulerTestDownloader(dls, 1, 2, false)
	defer dl.close()
	n, err := dl.streamChunk(1, 2)
	t.Logf("streamChunk n=%d err=%v quota=%+v", n, err, c.quota.snapshot())
	if n != 0 || !errors.Is(err, ErrCacheDiskLimit) {
		t.Fatalf("quota error lost at real CopyN boundary: n=%d err=%v", n, err)
	}
}

// The idle worker is held at the synthetic source boundary after cancellation.
// A real active reader and downloader contend with real periodic cleanup at a
// full quota. A separate closed persistent file supplies reclaimable space.
func TestFullQuotaReaderDuringPeriodicCleanup(t *testing.T) {
	testFullQuotaReaderDuringPeriodicCleanup(t, false)
}

func TestFullQuotaCanceledReaderDuringPeriodicCleanup(t *testing.T) {
	testFullQuotaReaderDuringPeriodicCleanup(t, true)
}

func testFullQuotaReaderDuringPeriodicCleanup(t *testing.T, cancelRead bool) {
	c := newQuotaTestCache(t, t.TempDir(), 8)
	c.threshold = 6
	cold := newQuotaTestItem(t, c, "cold/data", 6)
	if _, _, err := cold.WriteAtNoOverwrite(bytes.Repeat([]byte{3}, 6), 0); err != nil {
		t.Fatal(err)
	}
	if err := cold.Close(); err != nil {
		t.Fatal(err)
	}
	idle := newQuotaTestItem(t, c, "idle/data", 3)
	if _, _, err := idle.WriteAtNoOverwrite([]byte{1, 1}, 0); err != nil {
		t.Fatal(err)
	}
	idle.info.ATime = time.Now().Add(-2 * itemIdleTimeout)
	c.items.Store(idle.key, idle)
	c.itemCount.Add(1)
	parent := t.Context()
	session, cancel := context.WithCancel(parent)
	idleDls := &Downloaders{parentCtx: parent, ctx: session, cancel: cancel, item: idle}
	idle.downloaders = idleDls
	started, release := make(chan struct{}), make(chan struct{})
	idleDls.openStream = func(ctx context.Context, off int64) (manager.StreamReader, error) {
		return &quotaCleanupGateStream{dfsTestStream: newDFSTestStream([]byte{1, 1, 2}, off), ctx: ctx, started: started, release: release}, nil
	}
	idleDls.mu.Lock()
	if err := idleDls.newDownloaderLocked(ranges.Range{Pos: 2, Size: 1}, 3, false); err != nil {
		t.Fatal(err)
	}
	dl := idleDls.dls[0]
	idleDls.mu.Unlock()
	<-started
	cleanupDone := make(chan struct{})
	go func() { c.evict(); close(cleanupDone) }()
	<-dl.ctx.Done() // Real Close owns cleanupMu and is joining the worker.
	active := newQuotaTestItem(t, c, "active/data", 1)
	active.Open()
	defer active.Release()
	c.items.Store(active.key, active)
	c.itemCount.Add(1)
	activeSession, activeCancel := context.WithCancel(parent)
	dls := &Downloaders{parentCtx: parent, ctx: activeSession, cancel: activeCancel, item: active, chunkSize: 1, readAheadSize: 1, retries: 1}
	active.downloaders = dls
	sourceRead := make(chan struct{}, 1)
	dls.openStream = func(ctx context.Context, off int64) (manager.StreamReader, error) {
		select {
		case sourceRead <- struct{}{}:
		default:
		}
		return newDFSTestStream([]byte{9}, off), nil
	}
	ctx, stop := context.WithTimeout(parent, 2*time.Second)
	defer stop()
	type result struct {
		n    int
		err  error
		data byte
	}
	done := make(chan result, 1)
	go func() { p := make([]byte, 1); n, err := active.ReadAtContext(ctx, p, 0); done <- result{n, err, p[0]} }()
	<-sourceRead
	// Observe actual admission: either the old denial, or the repaired writer
	// parked in the cancellable maintenance lock. No sleep establishes ordering.
	waitQuotaAdmission(t, c)
	before := c.quotaDenials.Load()
	var r result
	if cancelRead {
		stop()
		select {
		case r = <-done:
		case <-time.After(time.Second):
			close(release)
			<-cleanupDone
			t.Fatal("request cancellation blocked behind periodic cleanup")
		}
		released := make(chan struct{})
		go func() { active.Release(); close(released) }()
		select {
		case <-released:
		case <-time.After(time.Second):
			close(release)
			<-cleanupDone
			t.Fatal("last-handle teardown blocked behind periodic cleanup")
		}
		if dls.waiterCount.Load() != 0 || c.pendingReads.Load() != 0 {
			t.Error("canceled read leaked waiters")
		}
	}
	close(release)
	<-cleanupDone
	if !cancelRead {
		r = <-done
	}
	_ = active.Close()
	t.Logf("read n=%d byte=%d err=%v denials_during_cleanup=%d total_denials=%d pressure=%d reclaimed=%d quota=%+v", r.n, r.data, r.err, before, c.quotaDenials.Load(), c.quotaPressure.Load(), c.quotaReclaimed.Load(), c.quota.snapshot())
	if before != 0 {
		t.Errorf("cleanup contention rejected active download %d times before space could be reclaimed", before)
	}
	if cancelRead {
		if r.n != 0 || !errors.Is(r.err, context.Canceled) {
			t.Errorf("canceled read=%+v", r)
		}
	} else if r.err != nil || r.n != 1 || r.data != 9 {
		t.Errorf("active read did not recover after reclamation: %+v", r)
	}
	if c.quotaDenials.Load() != 0 || dls.errorCount != 0 || dls.circuitOpen.Load() {
		t.Error("cleanup contention poisoned admission/error budget")
	}
	q := c.quota.snapshot()
	if q.Used+q.Reserved > q.Limit || q.Reserved != 0 {
		t.Fatalf("hard quota violated: %+v", q)
	}
}
