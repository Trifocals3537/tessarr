package storage

import "testing"

func TestAddUsenetProviderDoesNotCreateFileNamedPlacements(t *testing.T) {
	entry := &Entry{
		MountPath: "/mnt/usenet/release",
		Providers: map[string]*ProviderEntry{
			"existing": {Provider: "existing", ID: "placement"},
		},
	}
	metadata := &NZB{
		ID: "nzb-id",
		Files: []NZBFile{
			{Name: "Season 01/episode-01.mkv"},
			{Name: "Season 01/episode-02.mkv"},
		},
	}

	provider := entry.AddUsenetProvider(metadata)

	if len(entry.Providers) != 2 {
		t.Fatalf("placements = %#v, want only existing and usenet", entry.Providers)
	}
	if entry.Providers["usenet"] != provider {
		t.Fatal("usenet placement was not installed under the usenet key")
	}
	if len(provider.Files) != len(metadata.Files) {
		t.Fatalf("usenet files = %d, want %d", len(provider.Files), len(metadata.Files))
	}
	for _, file := range metadata.Files {
		if _, exists := entry.Providers[file.Name]; exists {
			t.Fatalf("file name %q was incorrectly added as a placement", file.Name)
		}
	}
}
