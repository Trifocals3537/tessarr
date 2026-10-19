package premiumize

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/request"
)

func TestDoRejectsTrailingJSONAndDoesNotExposeErrorBody(t *testing.T) {
	config.SetConfigPath(t.TempDir())

	t.Run("trailing JSON", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(`{} {}`))
		}))
		defer server.Close()

		client := &Premiumize{Host: server.URL, client: request.New()}
		req, err := http.NewRequest(http.MethodGet, server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		var result map[string]any
		if _, err = client.do(req, &result); err == nil ||
			!strings.Contains(err.Error(), "multiple values") {
			t.Fatalf("do error = %v, want trailing JSON rejection", err)
		}
	})

	t.Run("redacted error body", func(t *testing.T) {
		const secret = "https://cdn.invalid/movie?token=do-not-log"
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, secret, http.StatusTeapot)
		}))
		defer server.Close()

		client := &Premiumize{Host: server.URL, client: request.New()}
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.do(req, nil)
		if err == nil || strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "do-not-log") {
			t.Fatalf("do error exposed provider body: %v", err)
		}
	})
}

func TestFilesForTransferPreservesNestedDuplicateBasenames(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"status":"success",
			"content":[
				{"id":"1","name":"Season 01/Episode.mkv","type":"file","size":1024,"link":"https://download.invalid/1"},
				{"id":"2","name":"Season 02/Episode.mkv","type":"file","size":2048,"link":"https://download.invalid/2"}
			]
		}`))
	}))
	defer server.Close()

	client := &Premiumize{
		Host:          server.URL,
		client:        request.New(),
		isFileAllowed: func(string, int64) error { return nil },
	}
	files, _, _, err := client.filesForTransfer(premiumizeTransfer{
		ID:       "pm",
		FolderID: nullableString("root"),
	})
	if err != nil {
		t.Fatal(err)
	}
	wantPaths := map[string]bool{"Season 01/Episode.mkv": true, "Season 02/Episode.mkv": true}
	for name, file := range files {
		if file.Name != name || strings.ContainsAny(name, `/\`) || !wantPaths[file.Path] {
			t.Fatalf("logical file %q = %#v", name, file)
		}
		delete(wantPaths, file.Path)
	}
	if len(wantPaths) != 0 {
		t.Fatalf("missing provider paths: %#v", wantPaths)
	}
}

func TestFilesForTransferRejectsFolderCycles(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"status":"success",
			"content":[{"id":"root","name":"loop","type":"folder"}]
		}`))
	}))
	defer server.Close()

	client := &Premiumize{
		Host:          server.URL,
		client:        request.New(request.WithMaxRetries(0)),
		isFileAllowed: func(string, int64) error { return nil },
	}
	if _, _, _, err := client.filesForTransfer(premiumizeTransfer{
		ID:       "pm",
		FolderID: nullableString("root"),
	}); err == nil || !strings.Contains(err.Error(), "repeats folder ID") {
		t.Fatalf("filesForTransfer error = %v, want folder cycle rejection", err)
	}
}
