package manager

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/Trifocals3537/tessarr/pkg/manager/link"
)

func TestDiagnoseStreamFailureCarriesCorrelationWithoutRawError(t *testing.T) {
	mgr := &Manager{}
	ctx, requestID := mgr.beginStreamRequest(context.Background())
	if requestID == "" || streamRequestID(ctx) != requestID {
		t.Fatalf("request correlation = %q / %q", requestID, streamRequestID(ctx))
	}

	secret := "https://cdn.example/media?token=do-not-log"
	err := wrapStreamRequestError(requestID, StreamError{
		Err:       link.NewRetryableError(errors.New(secret), "503"),
		Retryable: true,
	})
	diagnostic := DiagnoseStreamFailure(err)
	if diagnostic.RequestID != requestID {
		t.Fatalf("request ID = %q, want %q", diagnostic.RequestID, requestID)
	}
	if diagnostic.Class != "upstream_status" || diagnostic.Status != http.StatusServiceUnavailable || !diagnostic.Retryable {
		t.Fatalf("diagnostic = %+v", diagnostic)
	}
}

func TestWrapStreamRequestErrorPreservesExistingCorrelation(t *testing.T) {
	inner := wrapStreamRequestError("r0001", errors.New("failure"))
	outer := wrapStreamRequestError("r0002", inner)
	if outer != inner {
		t.Fatal("existing correlated error was wrapped again")
	}
	if got := DiagnoseStreamFailure(outer).RequestID; got != "r0001" {
		t.Fatalf("request ID = %q, want original", got)
	}
}

func TestWrapStreamRequestErrorReplacesEmptyNestedCorrelation(t *testing.T) {
	uncorrelated := errors.Join(
		StreamError{Err: errors.New("provider failure")},
		errors.New("fallback failure"),
	)
	wrapped := wrapStreamRequestError("r0003", uncorrelated)
	if got := DiagnoseStreamFailure(wrapped).RequestID; got != "r0003" {
		t.Fatalf("request ID = %q, want current request correlation", got)
	}
}

func TestActiveStreamContextRoundTrip(t *testing.T) {
	ctx := WithActiveStreamID(context.Background(), "s0001")
	if got := activeStreamID(ctx); got != "s0001" {
		t.Fatalf("active stream ID = %q, want s0001", got)
	}
	if got := activeStreamID(WithActiveStreamID(ctx, "")); got != "s0001" {
		t.Fatalf("empty active stream ID replaced existing correlation: %q", got)
	}
}
