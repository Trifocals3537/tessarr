package manager

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/manager/link"
)

func TestStreamErrorHTTPStatus(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		status    int
		retryable bool
	}{
		{
			name:      "retryable provider failure",
			err:       StreamError{Err: link.ErrorCodeToLinkError("503"), Retryable: true},
			status:    http.StatusServiceUnavailable,
			retryable: true,
		},
		{
			name:      "exhausted replacement link",
			err:       StreamError{Err: link.ErrorCodeToLinkError("404"), LinkError: true},
			status:    http.StatusServiceUnavailable,
			retryable: true,
		},
		{
			name:   "range refusal",
			err:    StreamError{Err: link.ClassifyHTTPStatus(http.StatusRequestedRangeNotSatisfiable, nil)},
			status: http.StatusRequestedRangeNotSatisfiable,
		},
		{
			name:      "mixed joined failures prefer retryable",
			err:       fmt.Errorf("all failed: %w", errors.Join(StreamError{Err: link.ClassifyHTTPStatus(http.StatusRequestedRangeNotSatisfiable, nil)}, StreamError{Err: link.ErrorCodeToLinkError("503"), Retryable: true})),
			status:    http.StatusServiceUnavailable,
			retryable: true,
		},
		{
			name:   "internal failure",
			err:    errors.New("invariant failed"),
			status: http.StatusInternalServerError,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			status, retryable := StreamErrorHTTPStatus(test.err)
			if status != test.status || retryable != test.retryable {
				t.Fatalf("status/retryable = %d/%v, want %d/%v", status, retryable, test.status, test.retryable)
			}
		})
	}
}
