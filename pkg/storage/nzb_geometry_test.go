package storage

import (
	"math"
	"testing"
)

func TestNZBArticleGeometryMatches(t *testing.T) {
	g := &NZBArticleGeometry{Size: 192, Offset: 64, Bytes: 64, Part: 2, Total: 3}
	for _, tc := range []struct {
		name                                   string
		size, begin, end, part, total, decoded int64
		valid                                  bool
	}{
		{"exact", 192, 65, 128, 2, 3, 64, true},
		{"omitted optional counters", 192, 65, 128, 0, 0, 64, true},
		{"wrong size", 256, 65, 128, 2, 3, 64, false},
		{"logical rather than source offset", 192, 1, 64, 2, 3, 64, false},
		{"wrong end", 192, 65, 129, 2, 3, 64, false},
		{"neighboring part", 192, 65, 128, 3, 3, 64, false},
		{"wrong count", 192, 65, 128, 2, 4, 64, false},
		{"negative part", 192, 65, 128, -1, 3, 64, false},
		{"negative count", 192, 65, 128, 2, -1, 64, false},
		{"truncated body", 192, 65, 128, 2, 3, 63, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := g.Matches(tc.size, tc.begin, tc.end, tc.part, tc.total, tc.decoded); got != tc.valid {
				t.Fatalf("Matches = %v, want %v", got, tc.valid)
			}
		})
	}
}

func TestNZBArticleGeometryRejectsInvalidOrOverflowingRanges(t *testing.T) {
	for _, g := range []*NZBArticleGeometry{
		nil, {},
		{Size: 10, Offset: -1, Bytes: 1, Part: 1, Total: 1},
		{Size: 10, Offset: 10, Bytes: 1, Part: 1, Total: 1},
		{Size: math.MaxInt64, Offset: math.MaxInt64 - 1, Bytes: 2, Part: 1, Total: 1},
		{Size: 10, Bytes: -1, Part: 1, Total: 1},
		{Size: 10, Bytes: 1, Part: 0, Total: 1},
		{Size: 10, Bytes: 1, Part: 2, Total: 1},
	} {
		if g.Valid() || g.Matches(10, 1, 1, 0, 0, 1) {
			t.Fatalf("invalid geometry accepted: %+v", g)
		}
	}
	g := &NZBArticleGeometry{Size: math.MaxInt64, Offset: math.MaxInt64 - 1, Bytes: 1, Part: 1, Total: 1}
	if !g.Matches(math.MaxInt64, math.MaxInt64, math.MaxInt64, 1, 1, 1) {
		t.Fatal("valid maximum range rejected")
	}
}
