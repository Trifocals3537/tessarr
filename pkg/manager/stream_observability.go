package manager

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
)

type streamRequestContextKey struct{}
type activeStreamContextKey struct{}

type streamRequestError struct {
	id  string
	err error
}

func (e streamRequestError) Error() string { return e.err.Error() }
func (e streamRequestError) Unwrap() error { return e.err }
func (e streamRequestError) RequestID() string {
	return e.id
}

func (m *Manager) beginStreamRequest(ctx context.Context) (context.Context, string) {
	if id := streamRequestID(ctx); id != "" {
		return ctx, id
	}
	id := fmt.Sprintf("r%016x", m.streamRequestSequence.Add(1))
	return context.WithValue(ctx, streamRequestContextKey{}, id), id
}

func streamRequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(streamRequestContextKey{}).(string)
	return id
}

// WithActiveStreamID associates a tracked consumer with work performed for
// that consumer. Provider handoffs can then update only the affected active
// stream instead of every client reading the same file.
func WithActiveStreamID(ctx context.Context, id string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, activeStreamContextKey{}, id)
}

func activeStreamID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	id, _ := ctx.Value(activeStreamContextKey{}).(string)
	return id
}

func wrapStreamRequestError(id string, err error) error {
	if err == nil || id == "" {
		return err
	}
	switch streamErr := err.(type) {
	case StreamError:
		if streamErr.StreamRequestID != "" {
			return err
		}
		streamErr.StreamRequestID = id
		return streamErr
	case *StreamError:
		if streamErr != nil {
			if streamErr.StreamRequestID != "" {
				return err
			}
			copy := *streamErr
			copy.StreamRequestID = id
			return &copy
		}
	}
	var existing interface{ RequestID() string }
	if errors.As(err, &existing) && existing.RequestID() != "" {
		return err
	}
	return streamRequestError{id: id, err: err}
}

// StreamFailureDiagnostic is safe to put in logs and API diagnostics. It
// intentionally contains no raw error text, URLs, credentials, or article IDs.
type StreamFailureDiagnostic struct {
	RequestID string
	Class     string
	Status    int
	Retryable bool
}

// DiagnoseStreamFailure reduces an arbitrary wrapped stream error to stable,
// secret-free fields suitable for correlation across failover and final HTTP
// handling.
func DiagnoseStreamFailure(err error) StreamFailureDiagnostic {
	diagnostic := StreamFailureDiagnostic{Class: "internal"}
	if err == nil {
		diagnostic.Class = "none"
		return diagnostic
	}
	var correlated interface{ RequestID() string }
	if errors.As(err, &correlated) {
		diagnostic.RequestID = correlated.RequestID()
	}
	diagnostic.Status, diagnostic.Retryable = StreamErrorHTTPStatus(err)
	var statusCarrier interface{ HTTPStatus() int }
	if errors.As(err, &statusCarrier) {
		if status := statusCarrier.HTTPStatus(); status >= 400 && status <= 599 {
			diagnostic.Status = status
		}
	}

	switch {
	case errors.Is(err, context.Canceled):
		diagnostic.Class = "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		diagnostic.Class = "timeout"
	case errors.Is(err, io.ErrUnexpectedEOF):
		diagnostic.Class = "unexpected_eof"
	default:
		if class, _ := classifyStreamProviderFailure(err); class != "" && class != "internal" {
			diagnostic.Class = class
		} else if class, retryable := classifyTerminalReadFailure(err); class != "other" && class != "none" {
			diagnostic.Class = class
			diagnostic.Retryable = diagnostic.Retryable || retryable
		}
	}
	if diagnostic.Status == 0 {
		diagnostic.Status = http.StatusInternalServerError
	}
	return diagnostic
}
