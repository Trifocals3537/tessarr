package sabnzbd

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/Trifocals3537/tessarr/pkg/usenet/parser"
)

func TestSABAddFailureSummaryRequiresOnlyReleaseRejections(t *testing.T) {
	t.Parallel()

	rejected := errors.Join(
		errors.New("wrapped parse failure"),
		parser.ErrNZBArticlesUnavailable,
	)
	tests := []struct {
		name string
		errs []error
		want bool
	}{
		{name: "all rejected", errs: []error{rejected, rejected}, want: true},
		{name: "mixed", errs: []error{rejected, nntp.NewTimeoutError(errors.New("timeout"))}},
		{name: "empty"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			summary := sabAddFailureSummary{}
			for _, err := range test.errs {
				summary.record(err)
			}
			if got := summary.allReleaseRejected(); got != test.want {
				t.Fatalf("allReleaseRejected() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestWriteReleaseRejectedReturnsHealthySABResponseWithoutIDs(t *testing.T) {
	t.Parallel()

	response := httptest.NewRecorder()
	(&SABnzbd{}).writeReleaseRejected(response)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
	var body AddNZBResponse
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !body.Status {
		t.Fatal("SAB transport status must remain successful")
	}
	if body.NzoIds == nil || len(body.NzoIds) != 0 {
		t.Fatalf("nzo_ids = %#v, want a non-nil empty list", body.NzoIds)
	}
	if body.Error == "" {
		t.Fatal("expected a human-readable rejection reason")
	}
}
