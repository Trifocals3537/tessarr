package sabnzbd

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func newSABRouteTestServer(t *testing.T) (*SABnzbd, http.Handler) {
	t.Helper()

	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)
	downloadFolder := t.TempDir()
	if _, err := config.Update(func(cfg *config.Config) error {
		cfg.DownloadFolder = downloadFolder
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	mgr := manager.New()
	t.Cleanup(func() {
		if err := mgr.Stop(); err != nil {
			t.Error(err)
		}
		if err := logger.Close(); err != nil {
			t.Error(err)
		}
	})
	downloadUncached := true
	mgr.Arr().AddOrUpdate(arr.New(
		"tv",
		"https://sonarr.example.test",
		"arr-token",
		false,
		&downloadUncached,
		"torbox",
		"config",
	))

	sab := New(mgr)
	return sab, sab.Routes()
}

func newSABFormRequest(values url.Values) *http.Request {
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/",
		strings.NewReader(values.Encode()),
	)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return request
}

func TestSABRouteContracts(t *testing.T) {
	sab, router := newSABRouteTestServer(t)

	t.Run("query and form fields", func(t *testing.T) {
		testRouterAcceptsSABContractFromQueryAndForm(t, sab, router)
	})
	t.Run("queue actions", func(t *testing.T) {
		testRouterAcceptsQueueActionsFromForm(t, router)
	})
	t.Run("delete identifiers", func(t *testing.T) {
		testRouterAcceptsDeleteIdentifiersFromForm(t, sab, router)
	})
	t.Run("get-files identifier", func(t *testing.T) {
		testRouterAcceptsGetFilesIdentifierFromForm(t, router)
	})
	t.Run("add-URL name", func(t *testing.T) {
		testRouterAcceptsAddURLNameFromForm(t, router)
	})
}

func testRouterAcceptsSABContractFromQueryAndForm(
	t *testing.T,
	sab *SABnzbd,
	router http.Handler,
) {
	t.Helper()

	for _, entry := range []*storage.Entry{
		{
			InfoHash: "matching-entry",
			Name:     "Matching.Release.nzb",
			Category: "tv",
			Protocol: config.ProtocolNZB,
			State:    storage.EntryStateDownloading,
			Size:     4 << 20,
			Progress: 0.25,
		},
		{
			InfoHash: "other-entry",
			Name:     "Other.Release.nzb",
			Category: "movies",
			Protocol: config.ProtocolNZB,
			State:    storage.EntryStateDownloading,
			Size:     8 << 20,
			Progress: 0.5,
		},
	} {
		if err := sab.manager.Queue().Add(entry); err != nil {
			t.Fatal(err)
		}
	}

	for _, test := range []struct {
		name   string
		method string
		key    string
	}{
		{name: "query category", method: http.MethodGet, key: "category"},
		{name: "query category alias", method: http.MethodGet, key: "cat"},
		{name: "form category", method: http.MethodPost, key: "category"},
		{name: "form category alias", method: http.MethodPost, key: "cat"},
	} {
		t.Run(test.name, func(t *testing.T) {
			values := url.Values{
				"mode":        {"queue"},
				test.key:      {"tv"},
				"ma_username": {"https://sonarr.example.test"},
				"ma_password": {"arr-token"},
				"nzo_ids":     {"matching-entry"},
			}
			var request *http.Request
			if test.method == http.MethodPost {
				request = newSABFormRequest(values)
			} else {
				request = httptest.NewRequest(
					http.MethodGet,
					"/api/?"+values.Encode(),
					nil,
				)
			}

			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}

			var got QueueResponse
			if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
				t.Fatal(err)
			}
			if len(got.Queue.Slots) != 1 {
				t.Fatalf("slots = %#v, want one filtered entry", got.Queue.Slots)
			}
			if got.Queue.Slots[0].NzoId != "matching-entry" {
				t.Fatalf("nzo_id = %q, want matching-entry", got.Queue.Slots[0].NzoId)
			}
		})
	}
}

func testRouterAcceptsQueueActionsFromForm(t *testing.T, router http.Handler) {
	t.Helper()
	response := httptest.NewRecorder()
	router.ServeHTTP(response, newSABFormRequest(url.Values{
		"mode":        {"queue"},
		"name":        {"pause"},
		"cat":         {"tv"},
		"ma_username": {"https://sonarr.example.test"},
		"ma_password": {"arr-token"},
	}))

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var got StatusResponse
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !got.Status {
		t.Fatalf("response = %#v", got)
	}
}

func testRouterAcceptsDeleteIdentifiersFromForm(
	t *testing.T,
	sab *SABnzbd,
	router http.Handler,
) {
	t.Helper()
	if err := sab.manager.Queue().Add(&storage.Entry{
		InfoHash: "unsafe-entry",
		Name:     "Unsafe.Release",
		Protocol: config.ProtocolTorrent,
		SavePath: t.TempDir(),
	}); err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	router.ServeHTTP(response, newSABFormRequest(url.Values{
		"mode":        {"queue"},
		"name":        {"delete"},
		"value":       {"missing-entry,unsafe-entry"},
		"cat":         {"tv"},
		"ma_username": {"https://sonarr.example.test"},
		"ma_password": {"arr-token"},
	}))

	// Queue deletion is deliberately idempotent, so an unknown identifier is
	// still successful. A form value that was ignored would instead take the
	// empty-value path and return 400.
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var got StatusResponse
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !got.Status || !strings.Contains(got.Error, "unsafe-entry") {
		t.Fatalf("response = %#v", got)
	}
}

func testRouterAcceptsGetFilesIdentifierFromForm(t *testing.T, router http.Handler) {
	t.Helper()
	response := httptest.NewRecorder()
	router.ServeHTTP(response, newSABFormRequest(url.Values{
		"mode":        {"get_files"},
		"value":       {"missing-entry"},
		"ma_username": {"https://sonarr.example.test"},
		"ma_password": {"arr-token"},
	}))

	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func testRouterAcceptsAddURLNameFromForm(t *testing.T, router http.Handler) {
	t.Helper()
	response := httptest.NewRecorder()
	router.ServeHTTP(response, newSABFormRequest(url.Values{
		"mode":        {"addurl"},
		"name":        {"://invalid"},
		"cat":         {"tv"},
		"ma_username": {"https://sonarr.example.test"},
		"ma_password": {"arr-token"},
	}))

	if response.Code == http.StatusBadRequest && strings.Contains(response.Body.String(), "URL is required") {
		t.Fatalf("form URL was ignored: %s", response.Body.String())
	}
}
