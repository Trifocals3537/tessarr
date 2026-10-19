package reader

import (
	"context"
	"errors"
	"testing"

	appConfig "github.com/Trifocals3537/tessarr/internal/config"
	"github.com/rs/zerolog"
)

func TestSegmentWriterCommitsOnlyCompleteSlice(t *testing.T) {
	cache := newIntegrityTestCache(t, SegmentMeta{
		MessageID:        "segment-1",
		Bytes:            3,
		StartOffset:      0,
		EndOffset:        2,
		SegmentDataStart: 2,
	})
	writer := cache.StreamWriter(0)
	if writer == nil {
		t.Fatal("StreamWriter returned nil")
	}
	if n, err := writer.Write([]byte("xxabc-padding")); err != nil || n != len("xxabc-padding") {
		t.Fatalf("Write = %d, %v", n, err)
	}
	if err := writer.Finalize(); err != nil {
		t.Fatalf("Finalize error = %v", err)
	}
	if state := cache.GetState(0); state != StateOnDisk {
		t.Fatalf("state = %s, want OnDisk", state)
	}
	data, ok := cache.Get(0)
	if !ok || string(data) != "abc" {
		t.Fatalf("cached data = %q, ok=%v; want abc", data, ok)
	}
}

func TestSegmentWriterRejectsIncompleteSlice(t *testing.T) {
	cache := newIntegrityTestCache(t, SegmentMeta{
		MessageID:   "segment-1",
		Bytes:       5,
		StartOffset: 0,
		EndOffset:   4,
	})
	writer := cache.StreamWriter(0)
	if writer == nil {
		t.Fatal("StreamWriter returned nil")
	}
	if _, err := writer.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Finalize(); !errors.Is(err, errIncompleteSegment) {
		t.Fatalf("Finalize error = %v, want incomplete-segment error", err)
	}
	if state := cache.GetState(0); state == StateOnDisk {
		t.Fatal("incomplete segment was published as OnDisk")
	}
	if _, ok := cache.Get(0); ok {
		t.Fatal("incomplete segment became readable")
	}
}

func newIntegrityTestCache(t *testing.T, segment SegmentMeta) *SegmentCache {
	t.Helper()
	appConfig.SetConfigPath(t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	config := DefaultConfig()
	config.DiskPath = t.TempDir()
	cache, err := NewSegmentCache(ctx, []SegmentMeta{segment}, config, &ReaderStats{}, zerolog.Nop())
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		if err := cache.Close(); err != nil {
			t.Errorf("close cache: %v", err)
		}
	})
	return cache
}
