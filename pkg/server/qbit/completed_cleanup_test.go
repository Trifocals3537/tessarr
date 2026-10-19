package qbit

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestQBitDeleteDoesNotSwallowUntypedNotFoundErrors(t *testing.T) {
	oldPath := config.GetMainPath()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() { config.SetConfigPath(oldPath) })
	config.Get().DownloadFolder = t.TempDir()
	m := manager.New()
	t.Cleanup(func() {
		if err := m.Stop(); err != nil {
			t.Error(err)
		}
		if err := logger.Close(); err != nil {
			t.Errorf("close process logger: %v", err)
		}
	})
	e := &storage.Entry{InfoHash: "unsafe-cleanup", Name: "release", Protocol: config.ProtocolTorrent, SavePath: filepath.Join(t.TempDir(), "not found")}
	if err := m.Queue().Add(e); err != nil {
		t.Fatal(err)
	}
	q := &QBit{manager: m}
	handler := hashesContext(http.HandlerFunc(q.handleTorrentsDelete))
	form := url.Values{"hashes": {e.InfoHash}, "deleteFiles": {"true"}}
	request := httptest.NewRequest(http.MethodPost, "/api/v2/torrents/delete", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError {
		t.Fatalf("unsafe cleanup error was swallowed: %d %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), "not found") {
		t.Fatalf("fixture did not exercise misleading error text: %s", response.Body.String())
	}
}

// Arr sends deleteFiles=true after successful import, not only on failures.
func TestCompletedDownloadCleanupPreservesPersistentEntry(t *testing.T) {
	previousPath := config.GetMainPath()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() { config.SetConfigPath(previousPath) })
	config.Get().DownloadFolder = t.TempDir()
	m := manager.New()
	t.Cleanup(func() {
		if err := m.Stop(); err != nil {
			t.Error(err)
		}
		if err := logger.Close(); err != nil {
			t.Errorf("close process logger: %v", err)
		}
	})
	payload := filepath.Join(t.TempDir(), "episode.mkv")
	if err := os.WriteFile(payload, []byte("provider-backed fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	libraryLink := filepath.Join(t.TempDir(), "episode.mkv")
	if err := os.Symlink(payload, libraryLink); err != nil {
		// ERROR_PRIVILEGE_NOT_HELD: ordinary Windows accounts may lack
		// symlink permission even when all application operations are allowed.
		if runtime.GOOS == "windows" && errors.Is(err, syscall.Errno(1314)) {
			t.Skipf("symlink fixture requires Windows symlink privilege: %v", err)
		}
		t.Fatal(err)
	}
	entry := &storage.Entry{
		InfoHash: "completed-cached", Name: "release", Protocol: config.ProtocolTorrent,
		State: storage.EntryStatePausedUP, IsComplete: true, Progress: 1,
		SavePath: config.Get().DownloadFolder, Action: config.DownloadActionSymlink,
		Files: make(map[string]*storage.File), Providers: make(map[string]*storage.ProviderEntry),
	}
	if err := m.Queue().Add(entry); err != nil {
		t.Fatal(err)
	}
	mainEntry := *entry
	if err := m.AddOrUpdate(&mainEntry, nil); err != nil {
		t.Fatal(err)
	}
	q := &QBit{manager: m}
	handler := hashesContext(http.HandlerFunc(q.handleTorrentsDelete))
	for i := 0; i < 2; i++ {
		form := url.Values{"hashes": {entry.InfoHash}, "deleteFiles": {"true"}}
		request := httptest.NewRequest(http.MethodPost, "/api/v2/torrents/delete", strings.NewReader(form.Encode()))
		request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("delete status = %d: %s", response.Code, response.Body.String())
		}
	}
	if _, err := m.Queue().GetTorrent(entry.InfoHash); !storage.IsQueuedEntryNotFound(err) {
		t.Fatalf("queue row remained: %v", err)
	}
	if _, err := m.GetEntry(entry.InfoHash); err != nil {
		t.Fatalf("successful import lost persistent entry: %v", err)
	}
	if b, err := os.ReadFile(libraryLink); err != nil || string(b) != "provider-backed fixture" {
		t.Fatalf("library link unreadable: %q, %v", b, err)
	}
}
