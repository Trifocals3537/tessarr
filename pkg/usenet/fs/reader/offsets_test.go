package reader

import (
	"math"
	"testing"
)

func TestComputeOffsetsUsesContiguousMetadata(t *testing.T) {
	segments := []SegmentMeta{
		{Bytes: 100, StartOffset: 0, EndOffset: 99},
		{Bytes: 100, StartOffset: 100, EndOffset: 199},
		{Bytes: 50, StartOffset: 200, EndOffset: 249},
	}

	assertOffsets(t, computeOffsets(segments), []int64{0, 100, 200, 250})
}

func TestComputeOffsetsFallsBackOnInvalidLegacyMetadata(t *testing.T) {
	tests := map[string][]SegmentMeta{
		"overlapping zero-filled slot": {
			{Bytes: 100, StartOffset: 0, EndOffset: 99},
			{Bytes: 0, StartOffset: 0, EndOffset: 0},
			{Bytes: 100, StartOffset: 100, EndOffset: 199},
		},
		"gap": {
			{Bytes: 100, StartOffset: 0, EndOffset: 99},
			{Bytes: 100, StartOffset: 120, EndOffset: 219},
		},
		"out of order": {
			{Bytes: 100, StartOffset: 0, EndOffset: 99},
			{Bytes: 100, StartOffset: 200, EndOffset: 299},
			{Bytes: 100, StartOffset: 100, EndOffset: 199},
		},
		"invalid range": {
			{Bytes: 100, StartOffset: 0, EndOffset: 99},
			{Bytes: 100, StartOffset: 100, EndOffset: 99},
		},
		"length mismatch": {
			{Bytes: 99, StartOffset: 0, EndOffset: 99},
		},
		"overflowing end": {
			{Bytes: math.MaxInt64, StartOffset: 0, EndOffset: math.MaxInt64},
		},
	}

	for name, segments := range tests {
		t.Run(name, func(t *testing.T) {
			got := computeOffsets(segments)
			if len(got) != len(segments)+1 {
				t.Fatalf("offset count = %d, want %d", len(got), len(segments)+1)
			}
			for i := 1; i < len(got); i++ {
				if got[i] < got[i-1] {
					t.Fatalf("fallback offsets are not ascending: %v", got)
				}
			}
			for i, segment := range segments {
				wantSize := segment.Bytes
				if wantSize <= 0 {
					wantSize = 750 * 1024
				}
				if got[i+1]-got[i] != wantSize {
					t.Errorf("segment %d fallback size = %d, want %d", i, got[i+1]-got[i], wantSize)
				}
			}
		})
	}
}

func TestOffsetsAreContiguousRejectsEmptyAndNonZeroStart(t *testing.T) {
	if offsetsAreContiguous(nil) {
		t.Error("empty segment table reported contiguous")
	}
	if offsetsAreContiguous([]SegmentMeta{{Bytes: 10, StartOffset: 5, EndOffset: 14}}) {
		t.Error("non-zero first offset reported contiguous")
	}
}

func assertOffsets(t *testing.T, got, want []int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("offset count = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("offsets = %v, want %v", got, want)
		}
	}
}
