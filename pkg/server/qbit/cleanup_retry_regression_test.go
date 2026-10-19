package qbit

import (
	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestOmittedDeleteFlagCannotCompleteDestructiveRetry(t *testing.T) {
	old := config.GetMainPath()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() { config.SetConfigPath(old) })
	config.Get().DownloadFolder = t.TempDir()
	m := manager.New()
	t.Cleanup(func() {
		if err := m.Stop(); err != nil {
			t.Error(err)
		}
		if err := logger.Close(); err != nil {
			t.Error(err)
		}
	})
	e := &storage.Entry{InfoHash: "missing-flag-retry", Name: "release", Protocol: config.ProtocolTorrent, State: storage.EntryStateStalledDL, DownloadUncached: true, HandoffReason: "terminal", Category: "sonarr", ClientEndpoint: "host:8282", ActiveProvider: "offline", Providers: map[string]*storage.ProviderEntry{"offline": {Provider: "offline", ID: "owned"}}, SavePath: config.Get().DownloadFolder}
	path := filepath.Join(e.SavePath, e.Name)
	if err := os.MkdirAll(path, 0700); err != nil {
		t.Fatal(err)
	}
	if err := m.Queue().Add(e); err != nil {
		t.Fatal(err)
	}
	if err := m.Storage().AuthorizeUncachedHandoff(e); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteQBitQueueEntry(e.InfoHash, true); err == nil {
		t.Fatal("unavailable provider unexpectedly deleted")
	}
	// Model a durably acknowledged provider cleanup with staging cleanup still
	// pending. This makes the HTTP assertion distinguish policy rejection from
	// another provider failure: a preserving retry must not discard the intent.
	pending, err := m.Storage().QueuedDeletionIntents()
	if err != nil || len(pending) != 1 {
		t.Fatalf("setup intent: %+v %v", pending, err)
	}
	if err := m.Storage().MarkQueuedDeletionPlacementsClean(e.InfoHash, pending[0].QueueIncarnation); err != nil {
		t.Fatal(err)
	}
	q := &QBit{manager: m}
	handler := hashesContext(http.HandlerFunc(q.handleTorrentsDelete))
	for _, flag := range []string{"omitted", "false"} {
		form := url.Values{"hashes": {e.InfoHash}}
		if flag != "omitted" {
			form.Set("deleteFiles", flag)
		}
		request := httptest.NewRequest(http.MethodPost, "/api/v2/torrents/delete", strings.NewReader(form.Encode()))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusInternalServerError {
			t.Errorf("%s retry status=%d body=%s", flag, response.Code, response.Body.String())
		}
		if _, err := os.Stat(path); err != nil {
			t.Error(err)
		}
		intents, err := m.Storage().QueuedDeletionIntents()
		if err != nil || len(intents) != 1 || intents[0].PlacementCleanupPending || intents[0].PreserveFiles {
			t.Fatalf("lost destructive obligation: %+v %v", intents, err)
		}
	}
}
