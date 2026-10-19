package qbit

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestSIDCookieDefaultsAreBrowserSafe(t *testing.T) {
	cookie := newSIDCookie("test-session", true)
	if !cookie.HttpOnly {
		t.Fatal("SID cookie must be HttpOnly")
	}
	if cookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("SameSite = %v, want Lax", cookie.SameSite)
	}
	if cookie.Path != "/" {
		t.Fatalf("Path = %q, want /", cookie.Path)
	}
	if !cookie.Secure {
		t.Fatal("SID cookie must be Secure for an HTTPS listener")
	}
	if cookie.MaxAge != int(qbitSessionTTL/time.Second) {
		t.Fatalf("MaxAge = %d, want %d", cookie.MaxAge, int(qbitSessionTTL/time.Second))
	}
}

func TestQbitCookieSecurityFollowsActualTransport(t *testing.T) {
	httpRequest := httptest.NewRequest(http.MethodPost, "http://private.example/api/v2/auth/login", nil)
	if qbitCookieSecure(httpRequest) {
		t.Fatal("private HTTP compatibility client received a Secure-only cookie")
	}

	httpsRequest := httptest.NewRequest(http.MethodPost, "https://private.example/api/v2/auth/login", nil)
	httpsRequest.TLS = &tls.ConnectionState{}
	if !qbitCookieSecure(httpsRequest) {
		t.Fatal("TLS compatibility client did not receive a Secure cookie")
	}
}

func TestNormalizeStateFilterTreatsAllAsUnfiltered(t *testing.T) {
	for _, raw := range []string{"all", " ALL ", ""} {
		if got := normalizeStateFilter(raw); got != "" {
			t.Fatalf("normalizeStateFilter(%q) = %q, want empty", raw, got)
		}
	}
	if got := normalizeStateFilter(" downloading "); got != "downloading" {
		t.Fatalf("normalizeStateFilter() = %q, want downloading", got)
	}
}

func TestContainsAllHashes(t *testing.T) {
	if !containsAllHashes([]string{"hash", " ALL "}) {
		t.Fatal("containsAllHashes() did not recognize all")
	}
	if containsAllHashes([]string{"hash-a", "hash-b"}) {
		t.Fatal("containsAllHashes() matched ordinary hashes")
	}
}

func TestTorrentHashPredicateTargetsOnlySelectedHashes(t *testing.T) {
	predicate := torrentHashPredicate([]string{" HASH-A ", "hash-c"})

	for _, test := range []struct {
		entry *storage.Entry
		want  bool
	}{
		{entry: &storage.Entry{InfoHash: "hash-a"}, want: true},
		{entry: &storage.Entry{InfoHash: "HASH-C"}, want: true},
		{entry: &storage.Entry{InfoHash: "hash-b"}, want: false},
		{entry: nil, want: false},
	} {
		if got := predicate(test.entry); got != test.want {
			t.Fatalf("torrentHashPredicate()(%v) = %t, want %t", test.entry, got, test.want)
		}
	}
}

func TestTorrentHashPredicateHonorsExplicitAll(t *testing.T) {
	predicate := torrentHashPredicate([]string{"hash-a", " ALL "})
	if !predicate(&storage.Entry{InfoHash: "unlisted"}) {
		t.Fatal("torrentHashPredicate() did not honor explicit all selector")
	}
}

func TestHandleSetCategoryUpdatesOnlySelectedHashes(t *testing.T) {
	previousConfigPath := config.GetMainPath()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(func() { config.SetConfigPath(previousConfigPath) })

	m := manager.New()
	t.Cleanup(func() {
		if err := m.Stop(); err != nil {
			t.Errorf("stop manager: %v", err)
		}
		if err := logger.Close(); err != nil {
			t.Errorf("close process logger: %v", err)
		}
	})
	for _, entry := range []*storage.Entry{
		{InfoHash: "hash-a", Name: "selected", Category: "old", Protocol: config.ProtocolTorrent},
		{InfoHash: "hash-b", Name: "unselected", Category: "keep", Protocol: config.ProtocolTorrent},
	} {
		if err := m.Queue().Add(entry); err != nil {
			t.Fatalf("Queue().Add(%q) error = %v", entry.InfoHash, err)
		}
	}

	q := &QBit{manager: m}
	handler := q.categoryContext(hashesContext(http.HandlerFunc(q.handleSetCategory)))
	missingHashes := url.Values{"category": {"wrong"}}
	request := httptest.NewRequest(http.MethodPost, "/torrents/setCategory", strings.NewReader(missingHashes.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("setCategory without hashes status = %d, want %d", response.Code, http.StatusBadRequest)
	}

	form := url.Values{"hashes": {"hash-a"}, "category": {"sonarr"}}
	request = httptest.NewRequest(http.MethodPost, "/torrents/setCategory", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("setCategory status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	selected, err := m.Queue().GetTorrent("hash-a")
	if err != nil {
		t.Fatalf("GetTorrent(selected) error = %v", err)
	}
	if selected.Category != "sonarr" {
		t.Fatalf("selected category = %q, want sonarr", selected.Category)
	}
	unselected, err := m.Queue().GetTorrent("hash-b")
	if err != nil {
		t.Fatalf("GetTorrent(unselected) error = %v", err)
	}
	if unselected.Category != "keep" {
		t.Fatalf("unselected category = %q, want keep", unselected.Category)
	}
}

func TestShouldDeleteTorrentFilesRequiresExplicitTrue(t *testing.T) {
	for _, test := range []struct {
		name string
		form url.Values
		want bool
	}{
		{name: "missing", form: url.Values{}},
		{name: "false", form: url.Values{"deleteFiles": {"false"}}},
		{name: "explicit true", form: url.Values{"deleteFiles": {"true"}}, want: true},
		{name: "case and whitespace", form: url.Values{"deleteFiles": {" TRUE "}}, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(
				http.MethodPost,
				"/api/v2/torrents/delete",
				strings.NewReader(test.form.Encode()),
			)
			request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			if got := shouldDeleteTorrentFiles(request); got != test.want {
				t.Fatalf("shouldDeleteTorrentFiles() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestHandleTorrentsDeleteWithFilesPreservesMain(t *testing.T) {
	previousConfigPath := config.GetMainPath()
	configRoot := t.TempDir()
	config.SetConfigPath(configRoot)
	t.Cleanup(func() { config.SetConfigPath(previousConfigPath) })
	downloadRoot := t.TempDir()
	config.Get().DownloadFolder = downloadRoot

	m := manager.New()
	t.Cleanup(func() {
		if err := m.Stop(); err != nil {
			t.Errorf("stop manager: %v", err)
		}
	})
	entry := &storage.Entry{
		InfoHash:  "delete-with-files",
		Name:      "release",
		Protocol:  config.ProtocolTorrent,
		State:     storage.EntryStateDownloading,
		SavePath:  downloadRoot,
		Files:     make(map[string]*storage.File),
		Providers: make(map[string]*storage.ProviderEntry),
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
	form := url.Values{"hashes": {entry.InfoHash}, "deleteFiles": {"true"}}
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v2/torrents/delete",
		strings.NewReader(form.Encode()),
	)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("delete status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	if _, err := m.Queue().GetTorrent(entry.InfoHash); !storage.IsQueuedEntryNotFound(err) {
		t.Fatalf("queue entry remained after delete: %v", err)
	}
	if _, err := m.GetEntry(entry.InfoHash); err != nil {
		t.Fatalf("routine cleanup lost main entry: %v", err)
	}
}
