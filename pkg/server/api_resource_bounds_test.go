package server

import (
	"bytes"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/utils"
)

func TestHandleAddContentRejectsOversizedMultipartRequest(t *testing.T) {
	t.Parallel()

	request := httptest.NewRequest(http.MethodPost, "/api/add", strings.NewReader("x"))
	request.ContentLength = utils.MaxImportRequestBytes + 1
	request.Header.Set("Content-Type", "multipart/form-data; boundary=unused")
	response := httptest.NewRecorder()

	(&Server{}).handleAddContent(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected status 413, got %d: %s", response.Code, response.Body.String())
	}
}

func TestHandleAddContentRejectsTooManyItemsBeforeDispatch(t *testing.T) {
	t.Parallel()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	urls := make([]string, utils.MaxImportItems+1)
	for i := range urls {
		urls[i] = fmt.Sprintf("magnet:?xt=urn:btih:%040x", i+1)
	}
	if err := writer.WriteField("urls", strings.Join(urls, "\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "/api/add", bytes.NewReader(body.Bytes()))
	request.Header.Set("Content-Type", writer.FormDataContentType())
	response := httptest.NewRecorder()

	(&Server{}).handleAddContent(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected status 413, got %d: %s", response.Code, response.Body.String())
	}
}
