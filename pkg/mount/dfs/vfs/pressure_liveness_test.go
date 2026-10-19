package vfs

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/mount/dfs/vfs/ranges"
)

// A real StopAll owns the teardown; the worker gate keeps it in wg.Wait.
// Always release and join the gate, including on the deliberately RED build.
func TestDownloadCancellationDuringTeardown(t *testing.T) {
	for _, mode := range []string{"already_canceled", "cancel_parked", "deadline_parked", "resume"} {
		t.Run(mode, func(t *testing.T) { testDownloadDuringTeardown(t, mode) })
	}
}

func testDownloadDuringTeardown(t *testing.T, mode string) {
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	session, cancelSession := context.WithCancel(parent)
	item := &CacheItem{cache: &Cache{}, info: ItemInfo{Size: 1}}
	item.info.Rs.Insert(ranges.Range{Pos: 0, Size: 1})
	dls := &Downloaders{parentCtx: parent, ctx: session, cancel: cancelSession, item: item}
	release := make(chan struct{})
	dls.runDownloader = func(dl *downloader) (int64, error) {
		<-release
		return 0, dl.ctx.Err()
	}
	dls.mu.Lock()
	if err := dls.newDownloaderLocked(ranges.Range{Pos: 0, Size: 1}, 1, false); err != nil {
		t.Fatal(err)
	}
	dl := dls.dls[0]
	dls.mu.Unlock()
	stopped := make(chan struct{})
	go func() { dls.StopAll(); close(stopped) }()
	<-dl.ctx.Done() // StopAll has set stopping under mu and canceled this worker.
	ctx, cancel := context.WithCancel(context.Background())
	if mode == "deadline_parked" {
		cancel()
		ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	}
	defer cancel()
	if mode == "already_canceled" {
		cancel()
	}
	result := make(chan error, 1)
	go func() { result <- dls.Download(ctx, ranges.Range{Pos: 0, Size: 1}) }()
	if mode != "already_canceled" {
		// Holding mu and observing stopCond means the request released mu
		// inside Cond.Wait. StopAll remains gated throughout this handshake.
		until := time.Now().Add(time.Second)
		for {
			dls.mu.Lock()
			parked := dls.stopCond != nil
			dls.mu.Unlock()
			if parked {
				break
			}
			if time.Now().After(until) {
				close(release)
				<-stopped
				<-result
				t.Fatal("request never entered teardown wait")
			}
			runtime.Gosched()
		}
		if mode == "cancel_parked" {
			cancel()
		}
	}
	var err error
	returned := false
	if mode == "resume" {
		select {
		case err = <-result:
			returned = true
			t.Error("uncanceled read bypassed in-flight teardown")
		default:
		}
	} else {
		select {
		case err = <-result:
			returned = true
		case <-time.After(250 * time.Millisecond):
			t.Error("request cancellation ignored while real StopAll waits for its downloader")
		}
	}
	close(release)
	<-stopped
	if !returned {
		err = <-result
	}
	want := context.Canceled
	if mode == "deadline_parked" {
		want = context.DeadlineExceeded
	}
	if mode == "resume" {
		want = nil
	}
	if !errors.Is(err, want) {
		t.Errorf("Download error = %v, want %v", err, want)
	}
	if dls.waiterCount.Load() != 0 || item.cache.pendingReads.Load() != 0 {
		t.Error("canceled teardown request changed waiter accounting")
	}
	dls.mu.Lock()
	if dls.stopping {
		t.Error("StopAll did not complete lifecycle")
	}
	if dls.errorCount != 0 || dls.circuitOpen.Load() {
		t.Error("cancellation poisoned error budget")
	}
	dls.mu.Unlock()
	_ = dls.Close(nil)
}

// The RED cycle is a genuine deadlock, not a sleep-based model of the locks.
// Run it in a bounded subprocess: even the RED path leaves no blocked worker
// in the parent test/race process. The child dumps actual blocked stacks.
func TestCleanupPressureWriterCycle(t *testing.T) {
	if os.Getenv("TESSARR_PRESSURE_CHILD") == "1" {
		testCleanupPressureWriterCycle(t)
		return
	}
	for _, mode := range []string{"periodic", "reservation"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCleanupPressureWriterCycle$", "-test.v", "-test.timeout=8s")
			cmd.Env = append(os.Environ(), "TESSARR_PRESSURE_CHILD=1", "TESSARR_PRESSURE_MODE="+mode)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("real cleanup/writer child failed: %v\n%s", err, out)
			}
			t.Logf("%s", out)
		})
	}
}

func testCleanupPressureWriterCycle(t *testing.T) {
	c := newQuotaTestCache(t, t.TempDir(), 2)
	item := newQuotaTestItem(t, c, "synthetic/payload.bin", 3)
	if _, _, err := item.WriteAtNoOverwrite([]byte{1, 1}, 0); err != nil {
		t.Fatal(err)
	}
	item.info.ATime = time.Now().Add(-2 * itemIdleTimeout)
	c.items.Store(item.key, item)
	c.itemCount.Store(1)
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	session, cancelSession := context.WithCancel(parent)
	dls := &Downloaders{parentCtx: parent, ctx: session, cancel: cancelSession, item: item}
	item.downloaders = dls
	started := make(chan struct{})
	dls.openStream = func(ctx context.Context, offset int64) (manager.StreamReader, error) {
		return &pressureTrailingStream{Reader: bytes.NewReader([]byte{2}), ctx: ctx, started: started}, nil
	}
	dls.mu.Lock()
	if err := dls.newDownloaderLocked(ranges.Range{Pos: 2, Size: 1}, 3, false); err != nil {
		t.Fatal(err)
	}
	dls.mu.Unlock()
	<-started // Real downloader.run is inside stream Read, before cleanup.
	cleanupDone := make(chan struct{})
	reservation := os.Getenv("TESSARR_PRESSURE_MODE") == "reservation"
	admitted := false
	go func() {
		if reservation {
			admitted = c.reserveDisk("synthetic/requester.bin", 1)
			if admitted {
				c.quota.cancel(1)
			}
		} else {
			c.evict()
		}
		close(cleanupDone)
	}()
	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		stacks := make([]byte, 65536)
		n := runtime.Stack(stacks, true)
		s := string(stacks[:n])
		if !strings.Contains(s, "reclaimForReservation") || !strings.Contains(s, "(*Downloaders).Close") || !strings.Contains(s, "(*Cache).cleanupItemsExcept") {
			t.Fatalf("unexpected stall, not claimed cycle:\n%s", s)
		}
		t.Fatalf("cleanupMu holder waits for downloader; real cacheWriter waits for cleanupMu:\n%s", s)
	}
	wantPressure := int64(0)
	if reservation {
		wantPressure = 1
		if !admitted {
			t.Fatal("outer pressure request was not admitted after cold-file reclamation")
		}
	}
	if c.quotaDenials.Load() != 0 || c.quotaPressure.Load() != wantPressure {
		t.Fatal("canceled downloader's trailing write charged admission/denial")
	}
	if item.HasRange(ranges.Range{Pos: 2, Size: 1}) {
		t.Fatal("canceled trailing byte was published")
	}
	if c.quota.snapshot().Reserved != 0 {
		t.Fatal("reservation leaked")
	}
	if c.itemCount.Load() != 0 {
		t.Fatal("cleanup did not finish item lifecycle")
	}
	if dls.waiterCount.Load() != 0 || c.pendingReads.Load() != 0 || c.circuitBreakers.Load() != 0 || c.activeDownloads.Load() != 0 {
		t.Fatal("cleanup leaked downloader gauges")
	}
}

// A deterministic provider boundary: the in-flight read returns its trailing
// byte only once cleanup cancels it. All scheduling, copying, cache writing,
// quota reservation, closing and joining use production implementations.
type pressureTrailingStream struct {
	*bytes.Reader
	ctx     context.Context
	started chan struct{}
}

func (s *pressureTrailingStream) Read(p []byte) (int, error) {
	close(s.started)
	<-s.ctx.Done()
	return s.Reader.Read(p)
}
func (s *pressureTrailingStream) Close() error { return nil }
func (s *pressureTrailingStream) Prime() error { return nil }
