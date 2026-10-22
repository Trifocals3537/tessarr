package manager

import (
	"context"
	"errors"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/notifications"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/rs/zerolog"
)

func TestMultiSeasonChildrenCompleteThroughWorkflowCheckpoint(t *testing.T) {
	m := newQueueRecoveryTestManager(t)
	m.Notifications = notifications.New(&config.Get().Notifications, zerolog.Nop())
	d := &Downloader{manager: m, logger: zerolog.Nop()}
	parent := interruptedQueueTestEntry("1111111111111111111111111111111111111111")
	parent.Name = "Example Complete Series"
	parent.OriginalFilename = parent.Name
	parent.Action = config.DownloadActionNone
	parent.Files = map[string]*storage.File{
		"Example.S01E01.mkv": {Name: "Example.S01E01.mkv", Size: 100},
		"Example.S02E01.mkv": {Name: "Example.S02E01.mkv", Size: 200},
	}
	multi, seasons := d.detectMultiSeason(parent)
	if !multi || len(seasons) != 2 {
		t.Fatalf("expected two populated seasons, got multi=%v seasons=%d", multi, len(seasons))
	}
	// Use the production producer without repairing its state in the fixture.
	for _, child := range convertToMultiSeason(parent, seasons) {
		if err := m.queue.Add(child); err != nil {
			t.Fatal(err)
		}
		if err := d.process(context.Background(), child, ""); !errors.Is(err, errDeleteQueueEntryOnJobFinish) {
			t.Fatalf("season %s did not complete: %v", child.Name, err)
		}
		current, err := m.queue.GetTorrent(child.InfoHash)
		if err != nil {
			t.Fatal(err)
		}
		if !current.IsComplete || current.State != storage.EntryStatePausedUP || current.LastError != "" {
			t.Fatalf("season %s has incomplete checkpoint: %+v", child.Name, current)
		}
	}
}

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
