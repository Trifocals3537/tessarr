package qbit

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/utils"
)

func TestHandleTorrentsAddRejectsOversizedRequest(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodPost, "/torrents/add", strings.NewReader("x"))
	request.ContentLength = utils.MaxImportRequestBytes + 1
	request.Header.Set("Content-Type", "multipart/form-data; boundary=unused")
	response := httptest.NewRecorder()

	(&QBit{}).handleTorrentsAdd(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected status 413, got %d: %s", response.Code, response.Body.String())
	}
}

func TestCategoryContextRejectsOversizeBeforeNextHandler(t *testing.T) {
	t.Parallel()

	called := false
	handler := (&QBit{}).categoryContext(http.HandlerFunc(func(
		http.ResponseWriter,
		*http.Request,
	) {
		called = true
	}))
	request := httptest.NewRequest(http.MethodPost, "/torrents/add", strings.NewReader("x"))
	request.ContentLength = utils.MaxImportRequestBytes + 1
	request.Header.Set("Content-Type", "multipart/form-data; boundary=unused")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected status 413, got %d: %s", response.Code, response.Body.String())
	}
	if called {
		t.Fatal("oversized request reached the downstream handler")
	}
}

func TestHandleTorrentsAddRejectsTooManyURLsBeforeDispatch(t *testing.T) {
	t.Parallel()

	urls := make([]string, utils.MaxImportItems+1)
	for i := range urls {
		urls[i] = fmt.Sprintf("magnet:?xt=urn:btih:%040x", i+1)
	}
	form := url.Values{"urls": {strings.Join(urls, "\n")}}
	request := httptest.NewRequest(
		http.MethodPost,
		"/torrents/add",
		strings.NewReader(form.Encode()),
	)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response := httptest.NewRecorder()

	(&QBit{}).handleTorrentsAdd(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected status 413, got %d: %s", response.Code, response.Body.String())
	}
}

func TestHandleTorrentsAddChunkedMultipartCleansTemporaryFiles(t *testing.T) {
	tempRoot := t.TempDir()
	t.Setenv("TMPDIR", tempRoot)
	t.Setenv("TMP", tempRoot)
	t.Setenv("TEMP", tempRoot)

	reader, writer := io.Pipe()
	multipartWriter := multipart.NewWriter(writer)
	contentType := multipartWriter.FormDataContentType()
	writeDone := make(chan error, 1)
	go func() {
		var writeErr error
		defer func() {
			if closeErr := multipartWriter.Close(); writeErr == nil {
				writeErr = closeErr
			}
			if closeErr := writer.CloseWithError(writeErr); writeErr == nil {
				writeErr = closeErr
			}
			writeDone <- writeErr
		}()

		part, err := multipartWriter.CreateFormFile("torrents", "large.torrent")
		if err != nil {
			writeErr = err
			return
		}
		if _, err := io.CopyN(part, zeroReader{}, utils.MaxMultipartMemoryBytes+1); err != nil {
			writeErr = err
			return
		}
		for i := 1; i < utils.MaxImportItems+1; i++ {
			if _, err := multipartWriter.CreateFormFile(
				"torrents",
				fmt.Sprintf("empty-%d.torrent", i),
			); err != nil {
				writeErr = err
				return
			}
		}
	}()

	request := httptest.NewRequest(http.MethodPost, "/torrents/add", reader)
	request.ContentLength = -1
	request.Header.Set("Content-Type", contentType)
	response := httptest.NewRecorder()

	(&QBit{}).handleTorrentsAdd(response, request)
	if err := <-writeDone; err != nil {
		t.Fatalf("failed to stream multipart request: %v", err)
	}

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected status 413, got %d: %s", response.Code, response.Body.String())
	}
	entries, err := os.ReadDir(tempRoot)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("multipart temporary files were not removed: %v", entries)
	}
}

func TestAddMagnetRedactsSignedURLOnFailure(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no", http.StatusForbidden)
	}))
	defer server.Close()

	rawURL := server.URL + "/private/passkey?token=secret-value"
	_, err := (&QBit{}).addMagnet(
		context.Background(),
		rawURL,
		nil,
		"",
		config.DownloadActionNone,
		"",
		false,
		false,
		32,
	)
	if err == nil {
		t.Fatal("expected remote status error")
	}
	for _, secret := range []string{"passkey", "secret-value"} {
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("qBittorrent error exposed %q: %v", secret, err)
		}
	}
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}
