package server

import "testing"

func boolPointer(value bool) *bool { return &value }

func TestNormalizeContentVerificationOptions(t *testing.T) {
	tests := []struct {
		name           string
		verify         bool
		autoRepair     *bool
		protocol       string
		wantAutoRepair *bool
		wantProtocol   string
		wantError      bool
	}{
		{
			name:           "ordinary run remains unchanged",
			verify:         false,
			wantAutoRepair: nil,
		},
		{
			name:           "deep run defaults to detect only NZB",
			verify:         true,
			wantAutoRepair: boolPointer(false),
			wantProtocol:   "nzb",
		},
		{
			name:           "explicit repair and all protocol are preserved",
			verify:         true,
			autoRepair:     boolPointer(true),
			protocol:       "all",
			wantAutoRepair: boolPointer(true),
			wantProtocol:   "all",
		},
		{
			name:         "torrent-only deep run is rejected",
			verify:       true,
			protocol:     "torrent",
			wantError:    true,
			wantProtocol: "torrent",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotAutoRepair, gotProtocol, err := normalizeContentVerificationOptions(tc.verify, tc.autoRepair, tc.protocol)
			if (err != nil) != tc.wantError {
				t.Fatalf("error = %v, wantError %v", err, tc.wantError)
			}
			if gotProtocol != tc.wantProtocol {
				t.Fatalf("protocol = %q, want %q", gotProtocol, tc.wantProtocol)
			}
			switch {
			case gotAutoRepair == nil && tc.wantAutoRepair == nil:
			case gotAutoRepair == nil || tc.wantAutoRepair == nil:
				t.Fatalf("autoRepair = %v, want %v", gotAutoRepair, tc.wantAutoRepair)
			case *gotAutoRepair != *tc.wantAutoRepair:
				t.Fatalf("autoRepair = %v, want %v", *gotAutoRepair, *tc.wantAutoRepair)
			}
		})
	}
}
