package customerror

import (
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestArticleNotFoundErrorIsPermanentGone(t *testing.T) {
	cause := errors.New("NNTP 430")
	err := NewArticleNotFoundError(cause)

	if !errors.Is(err, cause) {
		t.Fatal("article-not-found error does not retain its cause")
	}
	if !err.IsPermanent() || err.IsRetryable() {
		t.Fatal("article-not-found error must be permanent and non-retryable")
	}
	if err.HTTPStatus() != http.StatusGone {
		t.Errorf("HTTPStatus() = %d, want %d", err.HTTPStatus(), http.StatusGone)
	}
	if err.Code != "usenet_article_not_found" {
		t.Errorf("Code = %q, want usenet_article_not_found", err.Code)
	}
}

func TestHTTPStatusDefaultsToInternalServerError(t *testing.T) {
	if got := NewPermanentError(errors.New("failure")).HTTPStatus(); got != http.StatusInternalServerError {
		t.Errorf("HTTPStatus() = %d, want %d", got, http.StatusInternalServerError)
	}
	var nilErr *Error
	if got := nilErr.HTTPStatus(); got != http.StatusInternalServerError {
		t.Errorf("nil HTTPStatus() = %d, want %d", got, http.StatusInternalServerError)
	}
}

func TestNewErrorRetainsHTTPMetadata(t *testing.T) {
	err := NewError(errors.New("busy"), http.StatusServiceUnavailable, "server.busy", true, false)
	if err.HTTPStatus() != http.StatusServiceUnavailable {
		t.Errorf("HTTPStatus() = %d, want %d", err.HTTPStatus(), http.StatusServiceUnavailable)
	}
	if err.Code != "server.busy" {
		t.Errorf("Code = %q, want server.busy", err.Code)
	}
}

func TestTorrentNotCachedErrorIsRetryableConflict(t *testing.T) {
	err := NewTorrentNotCachedError("Release")
	if err.HTTPStatus() != http.StatusConflict {
		t.Errorf("HTTPStatus() = %d, want %d", err.HTTPStatus(), http.StatusConflict)
	}
	if err.Code != "torrent_not_cached" {
		t.Errorf("Code = %q, want torrent_not_cached", err.Code)
	}
	if !err.IsRetryable() || err.IsPermanent() {
		t.Fatal("cache miss must be retryable and non-permanent")
	}
	if !strings.Contains(err.Error(), "Release") {
		t.Fatalf("error = %q, want release name", err.Error())
	}
}

func TestTorrentContentRejectedErrorIsPermanentUnprocessableEntity(t *testing.T) {
	err := NewTorrentContentRejectedError("Release")
	if err.HTTPStatus() != http.StatusUnprocessableEntity {
		t.Errorf("HTTPStatus() = %d, want %d", err.HTTPStatus(), http.StatusUnprocessableEntity)
	}
	if err.Code != "torrent_content_rejected" {
		t.Errorf("Code = %q, want torrent_content_rejected", err.Code)
	}
	if !err.IsPermanent() || err.IsRetryable() {
		t.Fatal("content rejection must be permanent and non-retryable")
	}
	if !strings.Contains(err.Error(), "Release") {
		t.Fatalf("error = %q, want release name", err.Error())
	}
}
