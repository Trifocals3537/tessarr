package reader

import (
	"context"
	"errors"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/buffer"
	"github.com/rs/zerolog"
)

func TestPoolsAreScopedToOneServiceRun(t *testing.T) {
	first := NewPools(64 << 20)
	if got := first.Stats()["memory_budget"]; got != int64(64<<20) {
		t.Fatalf("first pool budget = %v, want 64 MiB", got)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second := NewPools(8 << 20)
	t.Cleanup(func() { _ = second.Close() })
	if first == second {
		t.Fatal("new service run reused the prior pool")
	}
	if got := second.Stats()["memory_budget"]; got != int64(8<<20) {
		t.Fatalf("second pool budget = %v, want 8 MiB", got)
	}
}

func TestSegmentCacheUsesAndReleasesProvidedPools(t *testing.T) {
	pools := NewPools(4 << 20)
	t.Cleanup(func() { _ = pools.Close() })
	cfg := DefaultConfig()
	cfg.Pools = pools
	cfg.DiskPath = t.TempDir()

	cache, err := NewSegmentCache(
		context.Background(),
		[]SegmentMeta{{MessageID: "<one@example>", Number: 1, Bytes: 1, StartOffset: 0, EndOffset: 0}},
		cfg,
		&ReaderStats{},
		zerolog.Nop(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := pools.Stats()["buffers"]; got != 1 {
		t.Fatalf("registered buffers = %v, want 1", got)
	}
	if err := cache.Close(); err != nil {
		t.Fatal(err)
	}
	if got := pools.Stats()["buffers"]; got != 0 {
		t.Fatalf("registered buffers after cache close = %v, want 0", got)
	}
}

func TestClosedPoolsRejectNewSegmentCache(t *testing.T) {
	pools := NewPools(4 << 20)
	if err := pools.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.Pools = pools
	cfg.DiskPath = t.TempDir()
	_, err := NewSegmentCache(
		context.Background(),
		[]SegmentMeta{{MessageID: "<one@example>", Number: 1, Bytes: 1, StartOffset: 0, EndOffset: 0}},
		cfg,
		&ReaderStats{},
		zerolog.Nop(),
	)
	if !errors.Is(err, buffer.ErrClosed) {
		t.Fatalf("NewSegmentCache error = %v, want %v", err, buffer.ErrClosed)
	}
}

func TestClosingPoolsClosesRegisteredSegmentCaches(t *testing.T) {
	pools := NewPools(4 << 20)
	cfg := DefaultConfig()
	cfg.Pools = pools
	cfg.DiskPath = t.TempDir()
	cache, err := NewSegmentCache(
		context.Background(),
		[]SegmentMeta{{MessageID: "<one@example>", Number: 1, Bytes: 1, StartOffset: 0, EndOffset: 0}},
		cfg,
		&ReaderStats{},
		zerolog.Nop(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := pools.Close(); err != nil {
		t.Fatal(err)
	}
	if !cache.closed.Load() {
		t.Fatal("pool close left a registered segment cache running")
	}
	stats := pools.Stats()
	if stats["buffers"] != 0 || stats["memory_allocated"] != int64(0) {
		t.Fatalf("pool close left buffer resources: %#v", stats)
	}
	if err := cache.Close(); err != nil {
		t.Fatalf("cache close after pool close = %v", err)
	}
}
