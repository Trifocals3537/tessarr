package qbit

import (
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestConvertToQBitTorrentTorrentCalculatesETA(t *testing.T) {
	tests := []struct {
		name     string
		entry    *storage.Entry
		wantLeft int64
		wantETA  int64
	}{
		{
			name:     "active download",
			entry:    &storage.Entry{Size: 2_000, Progress: 0.25, Speed: 100},
			wantLeft: 1_500,
			wantETA:  15,
		},
		{
			name:     "partial second rounds up",
			entry:    &storage.Entry{Size: 100, Progress: 0.5, Speed: 30},
			wantLeft: 50,
			wantETA:  2,
		},
		{
			name:     "unknown speed uses qBittorrent infinity sentinel",
			entry:    &storage.Entry{Size: 100, Progress: 0.5},
			wantLeft: 50,
			wantETA:  qbitInfiniteETA,
		},
		{
			name:     "completed download",
			entry:    &storage.Entry{Size: 100, Progress: 1, Speed: 30},
			wantLeft: 0,
			wantETA:  0,
		},
		{
			name:     "over-complete progress is clamped",
			entry:    &storage.Entry{Size: 100, Progress: 1.1, Speed: 30},
			wantLeft: 0,
			wantETA:  0,
		},
		{
			name:     "very long ETA is capped",
			entry:    &storage.Entry{Size: qbitInfiniteETA + 1, Speed: 1},
			wantLeft: qbitInfiniteETA + 1,
			wantETA:  qbitInfiniteETA,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := convertToQBitTorrentTorrent(test.entry)
			if got.AmountLeft != test.wantLeft {
				t.Fatalf("AmountLeft = %d, want %d", got.AmountLeft, test.wantLeft)
			}
			if got.Eta != test.wantETA {
				t.Fatalf("Eta = %d, want %d", got.Eta, test.wantETA)
			}
		})
	}
}
