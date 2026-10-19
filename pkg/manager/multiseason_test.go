package manager

import (
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/rs/zerolog"
)

func TestDetectMultiSeasonRequiresMultiplePopulatedSeasonGroups(t *testing.T) {
	downloader := &Downloader{logger: zerolog.Nop()}

	t.Run("name hint with one season stays a single entry", func(t *testing.T) {
		entry := &storage.Entry{
			Name:     "Example Complete Series",
			InfoHash: "one-season",
			Files: map[string]*storage.File{
				"Example.S01E01.mkv": {Name: "Example.S01E01.mkv", Path: "Season 01/Example.S01E01.mkv"},
			},
		}
		multi, seasons := downloader.detectMultiSeason(entry)
		if multi || seasons != nil {
			t.Fatalf("multi = %v, seasons = %#v; want unsplit entry", multi, seasons)
		}
	})

	t.Run("two populated seasons are split", func(t *testing.T) {
		entry := &storage.Entry{
			Name:     "Example Complete Series",
			InfoHash: "two-seasons",
			Files: map[string]*storage.File{
				"Example.S01E01.mkv": {Name: "Example.S01E01.mkv", Path: "Season 01/Example.S01E01.mkv"},
				"Example.S02E01.mkv": {Name: "Example.S02E01.mkv", Path: "Season 02/Example.S02E01.mkv"},
			},
		}
		multi, seasons := downloader.detectMultiSeason(entry)
		if !multi || len(seasons) != 2 {
			t.Fatalf("multi = %v, seasons = %#v; want two season entries", multi, seasons)
		}
	})
}
