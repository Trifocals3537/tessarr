package sabnzbd

import (
	"errors"
	"net/http"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestSABAdmissionErrorStatus(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		status int
	}{
		{
			name:   "full",
			err:    errors.Join(errors.New("wrapped"), manager.ErrJobQueueFull),
			status: http.StatusTooManyRequests,
		},
		{
			name:   "closed",
			err:    manager.ErrJobQueueClosed,
			status: http.StatusServiceUnavailable,
		},
		{
			name:   "duplicate",
			err:    manager.ErrJobQueueDuplicate,
			status: http.StatusConflict,
		},
		{
			name:   "deleting",
			err:    storage.ErrQueuedEntryDeleting,
			status: http.StatusConflict,
		},
		{
			name: "other",
			err:  errors.New("invalid NZB"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if status := sabAdmissionErrorStatus(test.err); status != test.status {
				t.Fatalf(
					"sabAdmissionErrorStatus() = %d, want %d",
					status,
					test.status,
				)
			}
		})
	}
}
