package storage

import (
	"testing"

	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

func TestProviderMutationsPreferConfiguredAccountKey(t *testing.T) {
	entry := &Entry{
		ActiveProvider: "torbox-primary",
		Providers: map[string]*ProviderEntry{
			"torbox-primary": {
				Provider: "torbox",
				ID:       "17",
				Status:   debridTypes.TorrentStatusDownloaded,
			},
			"torbox-secondary": {
				Provider: "torbox",
				ID:       "99",
				Status:   debridTypes.TorrentStatusDownloaded,
			},
		},
	}

	if err := entry.ActivatePlacement("torbox-secondary"); err != nil {
		t.Fatalf("ActivatePlacement() error = %v", err)
	}
	if entry.ActiveProvider != "torbox-secondary" {
		t.Fatalf("active provider = %q, want torbox-secondary", entry.ActiveProvider)
	}

	entry.RemoveProvider("torbox-primary")
	if _, exists := entry.Providers["torbox-primary"]; exists {
		t.Fatal("configured source account remains after local removal")
	}
	if target := entry.Providers["torbox-secondary"]; target == nil || target.ID != "99" {
		t.Fatalf("sibling account was removed or changed: %#v", target)
	}
}
