package sabnzbd

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/utils"
)

func TestHandleAddFileRejectsOversizedMultipartRequest(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodPost, "/sabnzbd/api", strings.NewReader("x"))
	request.ContentLength = utils.MaxImportRequestBytes + 1
	request.Header.Set("Content-Type", "multipart/form-data; boundary=unused")
	response := httptest.NewRecorder()

	(&SABnzbd{}).handleAddFile(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected status 413, got %d: %s", response.Code, response.Body.String())
	}
}

func TestCategoryContextRejectsOversizeBeforeNextHandler(t *testing.T) {
	t.Parallel()

	called := false
	handler := (&SABnzbd{}).categoryContext(http.HandlerFunc(func(
		http.ResponseWriter,
		*http.Request,
	) {
		called = true
	}))
	request := httptest.NewRequest(http.MethodPost, "/sabnzbd/api", strings.NewReader("x"))
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

func TestHandleAddURLRejectsTooManyURLsBeforeDispatch(t *testing.T) {
	t.Parallel()

	urls := make([]string, utils.MaxImportItems+1)
	for i := range urls {
		urls[i] = fmt.Sprintf("https://example.invalid/%d.nzb", i)
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/sabnzbd/api?name="+url.QueryEscape(strings.Join(urls, "\n")),
		nil,
	)
	response := httptest.NewRecorder()

	(&SABnzbd{}).handleAddURL(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected status 413, got %d: %s", response.Code, response.Body.String())
	}
}
