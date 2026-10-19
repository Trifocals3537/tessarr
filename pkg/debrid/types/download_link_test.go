package types

import (
	"testing"
	"time"
)

func TestDownloadLinkNeedsRefresh(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name string
		link DownloadLink
		at   time.Time
		want bool
	}{
		{name: "no expiry", link: DownloadLink{}, at: now, want: false},
		{name: "expired", link: DownloadLink{ExpiresAt: now.Add(-time.Second)}, at: now, want: true},
		{
			name: "long lived link refreshes one minute early",
			link: DownloadLink{Generated: now.Add(-time.Hour), ExpiresAt: now.Add(30 * time.Second)},
			at:   now,
			want: true,
		},
		{
			name: "short lived link keeps most of its lifetime",
			link: DownloadLink{Generated: now, ExpiresAt: now.Add(30 * time.Second)},
			at:   now,
			want: false,
		},
		{
			name: "short lived link refreshes at ten percent remaining",
			link: DownloadLink{Generated: now, ExpiresAt: now.Add(30 * time.Second)},
			at:   now.Add(27 * time.Second),
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.link.NeedsRefresh(tt.at); got != tt.want {
				t.Fatalf("NeedsRefresh() = %v, want %v", got, tt.want)
			}
		})
	}
}
