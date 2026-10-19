package buffer

import (
	"math/rand"
	"testing"
)

func TestRangeSetInsertReportsNewCoverageAndMerges(t *testing.T) {
	rs := newRangeSet()
	tests := []struct {
		off, length int64
		added       int64
		want        []extent
	}{
		{off: 10, length: 10, added: 10, want: []extent{{10, 20}}},
		{off: 30, length: 10, added: 10, want: []extent{{10, 20}, {30, 40}}},
		{off: 20, length: 10, added: 10, want: []extent{{10, 40}}},
		{off: 15, length: 10, added: 0, want: []extent{{10, 40}}},
		{off: 0, length: 50, added: 20, want: []extent{{0, 50}}},
	}

	for _, tt := range tests {
		if got := rs.insert(tt.off, tt.length); got != tt.added {
			t.Fatalf("insert(%d, %d) added %d bytes, want %d", tt.off, tt.length, got, tt.added)
		}
		if !equalExtents(rs.rs, tt.want) {
			t.Fatalf("insert(%d, %d) ranges = %v, want %v", tt.off, tt.length, rs.rs, tt.want)
		}
	}
}

func TestRangeSetMatchesByteModel(t *testing.T) {
	const size = 4096
	model := make([]bool, size)
	rs := newRangeSet()
	rng := rand.New(rand.NewSource(1))

	for operation := 0; operation < 2000; operation++ {
		off := rng.Intn(size)
		length := rng.Intn(size-off) + 1

		if rng.Intn(2) == 0 {
			var wantAdded int64
			for i := off; i < off+length; i++ {
				if !model[i] {
					wantAdded++
					model[i] = true
				}
			}
			if got := rs.insert(int64(off), int64(length)); got != wantAdded {
				t.Fatalf("operation %d: insert added %d bytes, want %d", operation, got, wantAdded)
			}
		} else {
			var wantRemoved int64
			for i := off; i < off+length; i++ {
				if model[i] {
					wantRemoved++
					model[i] = false
				}
			}
			if got := rs.remove(int64(off), int64(length)); got != wantRemoved {
				t.Fatalf("operation %d: remove removed %d bytes, want %d", operation, got, wantRemoved)
			}
		}

		var wantTotal int64
		for i, present := range model {
			if present {
				wantTotal++
			}
			if got := rs.present(int64(i), 1); got != present {
				t.Fatalf("operation %d: present(%d, 1) = %t, want %t", operation, i, got, present)
			}
		}
		if got := rs.totalSize(); got != wantTotal {
			t.Fatalf("operation %d: totalSize() = %d, want %d", operation, got, wantTotal)
		}
	}
}

func BenchmarkRangeSetInsertSequential(b *testing.B) {
	rs := newRangeSet()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rs.insert(int64(i)*4096, 4096)
	}
}

func BenchmarkRangeSetInsertExistingFragmented(b *testing.B) {
	const extentCount = 4096
	rs := newRangeSet()
	rs.rs = make([]extent, extentCount)
	for i := range rs.rs {
		off := int64(i * 8192)
		rs.rs[i] = extent{off: off, end: off + 4096}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := i & (extentCount - 1)
		rs.insert(int64(index*8192), 4096)
	}
}

func BenchmarkRangeSetPresentFragmented(b *testing.B) {
	const extentCount = 4096
	rs := newRangeSet()
	rs.rs = make([]extent, extentCount)
	for i := range rs.rs {
		off := int64(i * 8192)
		rs.rs[i] = extent{off: off, end: off + 4096}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := i & (extentCount - 1)
		rs.present(int64(index*8192), 4096)
	}
}

func equalExtents(a, b []extent) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
