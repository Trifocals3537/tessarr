package buffer

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestBufferRejectsOutOfRangeOperations(t *testing.T) {
	pool := NewPool(PoolConfig{Name: "bounds"})
	t.Cleanup(func() { _ = pool.Close() })

	b, err := pool.NewBuffer(Config{
		DiskPath:  filepath.Join(t.TempDir(), "buffer.bin"),
		TotalSize: 16,
	})
	if err != nil {
		t.Fatalf("NewBuffer: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	if _, err := b.WriteAt([]byte{1}, 16); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("WriteAt past TotalSize error = %v, want %v", err, ErrOutOfRange)
	}
	if _, err := b.ReadAt(make([]byte, 2), 15); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("ReadAt past TotalSize error = %v, want %v", err, ErrOutOfRange)
	}
	if err := b.Discard(15, 2); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("Discard past TotalSize error = %v, want %v", err, ErrOutOfRange)
	}
	if b.HasRange(15, 2) {
		t.Fatal("HasRange reported an invalid range as present")
	}
	if got := b.Present(15, 2); got != nil {
		t.Fatalf("Present invalid range = %v, want nil", got)
	}

	if _, err := pool.NewBuffer(Config{
		TotalSize:     16,
		InitialRanges: []Range{{Off: 15, Size: 2}},
	}); !errors.Is(err, ErrOutOfRange) {
		t.Fatalf("NewBuffer invalid InitialRanges error = %v, want %v", err, ErrOutOfRange)
	}
}

func TestCheckedRangeEndRejectsOverflow(t *testing.T) {
	tests := []struct {
		name   string
		off    int64
		length int64
	}{
		{name: "negative offset", off: -1, length: 1},
		{name: "negative length", off: 0, length: -1},
		{name: "addition overflow", off: maxLogicalSize, length: 1},
		{name: "alignment overflow", off: maxLogicalSize + 1, length: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := checkedRangeEnd(tt.off, tt.length, 0); !errors.Is(err, ErrOutOfRange) {
				t.Fatalf("checkedRangeEnd(%d, %d) error = %v, want %v", tt.off, tt.length, err, ErrOutOfRange)
			}
		})
	}
}

func TestBufferAllowsTotalSizeBoundary(t *testing.T) {
	pool := NewPool(PoolConfig{Name: "boundary"})
	t.Cleanup(func() { _ = pool.Close() })

	b, err := pool.NewBuffer(Config{
		DiskPath:  filepath.Join(t.TempDir(), "buffer.bin"),
		TotalSize: 16,
	})
	if err != nil {
		t.Fatalf("NewBuffer: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })

	if n, err := b.WriteAt([]byte{42}, 15); err != nil || n != 1 {
		t.Fatalf("WriteAt boundary = (%d, %v), want (1, nil)", n, err)
	}
	got := make([]byte, 1)
	if n, err := b.ReadAt(got, 15); err != nil || n != 1 {
		t.Fatalf("ReadAt boundary = (%d, %v), want (1, nil)", n, err)
	}
	if got[0] != 42 {
		t.Fatalf("ReadAt boundary byte = %d, want 42", got[0])
	}
}

func TestPoolRejectsNewBufferAfterClose(t *testing.T) {
	pool := NewPool(PoolConfig{Name: "closed"})
	if err := pool.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := pool.NewBuffer(Config{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("NewBuffer after Close error = %v, want %v", err, ErrClosed)
	}
}

func TestPoolCloseSynchronizesBufferRegistration(t *testing.T) {
	root := t.TempDir()
	for i := 0; i < 100; i++ {
		pool := NewPool(PoolConfig{Name: "registration-race"})
		start := make(chan struct{})
		var (
			b      *Buffer
			newErr error
			wg     sync.WaitGroup
		)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			b, newErr = pool.NewBuffer(Config{
				DiskPath: filepath.Join(root, fmt.Sprintf("buffer-%d.bin", i)),
			})
		}()
		close(start)
		if err := pool.Close(); err != nil {
			t.Fatalf("iteration %d: Close: %v", i, err)
		}
		wg.Wait()

		switch {
		case newErr == nil:
			if _, err := b.WriteAt([]byte{1}, 0); !errors.Is(err, ErrClosed) {
				t.Fatalf("iteration %d: registered Buffer remained open after Pool.Close: %v", i, err)
			}
		case !errors.Is(newErr, ErrClosed):
			t.Fatalf("iteration %d: NewBuffer error = %v, want nil or %v", i, newErr, ErrClosed)
		}
		if stats := pool.Stats(); stats.Buffers != 0 || stats.MemoryInUse != 0 {
			t.Fatalf("iteration %d: pool stats after Close = %+v", i, stats)
		}
	}
}

func TestPoolMemoryBudgetIsAtomicAcrossBuffers(t *testing.T) {
	const bufferCount = 64
	root := t.TempDir()
	pool := NewPool(PoolConfig{
		Name:         "concurrent-admission",
		MemoryBudget: blockSize,
	})
	t.Cleanup(func() { _ = pool.Close() })

	buffers := make([]*Buffer, bufferCount)
	for i := range buffers {
		b, err := pool.NewBuffer(Config{
			DiskPath:  filepath.Join(root, fmt.Sprintf("buffer-%d.bin", i)),
			TotalSize: 1,
		})
		if err != nil {
			t.Fatalf("NewBuffer %d: %v", i, err)
		}
		buffers[i] = b
	}

	start := make(chan struct{})
	errs := make(chan error, bufferCount)
	var wg sync.WaitGroup
	for _, b := range buffers {
		wg.Add(1)
		go func(b *Buffer) {
			defer wg.Done()
			<-start
			_, err := b.WriteAt([]byte{1}, 0)
			errs <- err
		}(b)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent WriteAt: %v", err)
		}
	}

	stats := pool.Stats()
	if stats.MemoryInUse > stats.MemoryBudget || stats.MemoryAllocated > stats.MemoryBudget {
		t.Fatalf("memory exceeds budget: %+v", stats)
	}
}

func TestBufferCloseFlushesCachedData(t *testing.T) {
	pool := NewPool(PoolConfig{Name: "close-flush"})
	t.Cleanup(func() { _ = pool.Close() })
	path := filepath.Join(t.TempDir(), "buffer.bin")
	b, err := pool.NewBuffer(Config{DiskPath: path, TotalSize: 5})
	if err != nil {
		t.Fatalf("NewBuffer: %v", err)
	}

	want := []byte("hello")
	if _, err := b.WriteAt(want, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("persisted bytes = %q, want %q", got, want)
	}
}

func TestBufferCloseReportsFlushFailure(t *testing.T) {
	pool := NewPool(PoolConfig{Name: "close-error"})
	t.Cleanup(func() { _ = pool.Close() })
	b, err := pool.NewBuffer(Config{
		DiskPath:  filepath.Join(t.TempDir(), "buffer.bin"),
		TotalSize: 1,
	})
	if err != nil {
		t.Fatalf("NewBuffer: %v", err)
	}
	if _, err := b.WriteAt([]byte{1}, 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	if err := b.file.Close(); err != nil {
		t.Fatalf("close backing file: %v", err)
	}
	if err := b.Close(); err == nil {
		t.Fatal("Close error = nil, want dirty-flush failure")
	}
	stats := pool.Stats()
	if stats.MemoryInUse != 0 || stats.Buffers != 0 {
		t.Fatalf("pool stats after failed flush = %+v, want memory and buffers released", stats)
	}
}

func TestPoolReclaimDiskPunchesSafeReadBehind(t *testing.T) {
	pool := NewPool(PoolConfig{
		Name:       "synchronous-reclaim",
		BackWindow: 16,
	})
	t.Cleanup(func() { _ = pool.Close() })

	var evictedOff, evictedSize int64
	b, err := pool.NewBuffer(Config{
		DiskPath:  filepath.Join(t.TempDir(), "buffer.bin"),
		TotalSize: 64,
		OnEvict: func(off, size int64) {
			evictedOff = off
			evictedSize += size
		},
	})
	if err != nil {
		t.Fatalf("NewBuffer: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if _, err := b.WriteAt(make([]byte, 64), 0); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
	b.SetReadHead(64)

	reclaimed := pool.ReclaimDisk(32)
	if reclaimed != 48 {
		t.Fatalf("ReclaimDisk = %d, want 48 bytes below the 16-byte back-window", reclaimed)
	}
	if evictedOff != 0 || evictedSize != 48 {
		t.Fatalf("OnEvict = %d+%d, want 0+48", evictedOff, evictedSize)
	}
	if stats := pool.Stats(); stats.DiskInUse != 16 {
		t.Fatalf("pool disk usage after reclaim = %d, want 16", stats.DiskInUse)
	}
	if !b.HasRange(48, 16) || b.HasRange(0, 1) {
		t.Fatal("reclaim did not preserve only the protected read-behind window")
	}
}
