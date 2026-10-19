package qbit

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestTorrentAddErrorStatus(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		status     int
		idempotent bool
	}{
		{
			name:       "duplicate",
			err:        fmtWrap(manager.ErrJobQueueDuplicate),
			status:     http.StatusOK,
			idempotent: true,
		},
		{
			name:   "full",
			err:    fmtWrap(manager.ErrJobQueueFull),
			status: http.StatusTooManyRequests,
		},
		{
			name:   "closed",
			err:    fmtWrap(manager.ErrJobQueueClosed),
			status: http.StatusServiceUnavailable,
		},
		{
			name:   "deleting",
			err:    errors.Join(manager.ErrQueueEntryDeleting, storage.ErrQueuedEntryDeleting),
			status: http.StatusConflict,
		},
		{
			name:   "typed cache miss",
			err:    customerror.NewTorrentNotCachedError("Release"),
			status: http.StatusConflict,
		},
		{
			name:   "typed content rejection",
			err:    customerror.NewTorrentContentRejectedError("Release"),
			status: http.StatusUnprocessableEntity,
		},
		{
			name: "mixed provider failures stay generic",
			err: errors.Join(
				customerror.NewTorrentNotCachedError("Release"),
				errors.New("provider unavailable"),
			),
			status: http.StatusBadRequest,
		},
		{
			name: "mixed content rejection and outage stay generic",
			err: errors.Join(
				customerror.NewTorrentContentRejectedError("Release"),
				errors.New("provider unavailable"),
			),
			status: http.StatusBadRequest,
		},
		{
			name:   "invalid",
			err:    errors.New("invalid torrent"),
			status: http.StatusBadRequest,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, idempotent := torrentAddErrorStatus(test.err)
			if status != test.status || idempotent != test.idempotent {
				t.Fatalf(
					"torrentAddErrorStatus() = (%d, %t), want (%d, %t)",
					status,
					idempotent,
					test.status,
					test.idempotent,
				)
			}
		})
	}
}

func TestWriteTorrentAddErrorExposesMachineReadableCode(t *testing.T) {
	recorder := httptest.NewRecorder()
	err := customerror.NewTorrentNotCachedError("Release")

	writeTorrentAddError(recorder, err, http.StatusConflict)

	if got := recorder.Header().Get("X-Tessarr-Error-Code"); got != "torrent_not_cached" {
		t.Fatalf("X-Tessarr-Error-Code = %q, want torrent_not_cached", got)
	}
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusConflict)
	}
}

func TestWriteTorrentAddErrorDoesNotMislabelMixedFailures(t *testing.T) {
	recorder := httptest.NewRecorder()
	err := errors.Join(
		customerror.NewTorrentNotCachedError("Release"),
		errors.New("provider unavailable"),
	)

	writeTorrentAddError(recorder, err, http.StatusBadRequest)

	if got := recorder.Header().Get("X-Tessarr-Error-Code"); got != "" {
		t.Fatalf("X-Tessarr-Error-Code = %q, want empty", got)
	}
}

func TestWriteTorrentAddErrorDoesNotMislabelMixedContentFailures(t *testing.T) {
	recorder := httptest.NewRecorder()
	err := errors.Join(
		customerror.NewTorrentContentRejectedError("Release"),
		errors.New("provider unavailable"),
	)

	writeTorrentAddError(recorder, err, http.StatusBadRequest)

	if got := recorder.Header().Get("X-Tessarr-Error-Code"); got != "" {
		t.Fatalf("X-Tessarr-Error-Code = %q, want empty", got)
	}
}

func TestWriteTorrentAddErrorExposesContentRejectionCode(t *testing.T) {
	recorder := httptest.NewRecorder()
	err := customerror.NewTorrentContentRejectedError("Release")

	writeTorrentAddError(recorder, err, http.StatusUnprocessableEntity)

	if got := recorder.Header().Get("X-Tessarr-Error-Code"); got != "torrent_content_rejected" {
		t.Fatalf("X-Tessarr-Error-Code = %q, want torrent_content_rejected", got)
	}
	if recorder.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusUnprocessableEntity)
	}
}

func fmtWrap(err error) error {
	return errors.Join(errors.New("wrapped"), err)
}
