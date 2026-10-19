package customerror

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
)

type Error struct {
	err            error
	silent         bool
	statusCode     int
	requestID      string
	Code           string
	HeadersWritten bool // True if response headers were already written (can't send error status)
	retry          bool // True if the operation that caused the error is safe to retry
	permanent      bool // True if the error is permanent and should not be retried
}

// RequestID returns the stream request correlation identifier, when one was
// attached while the error crossed the streaming boundary.
func (e *Error) RequestID() string {
	if e == nil {
		return ""
	}
	return e.requestID
}

// Clone returns an independent copy that may safely carry request-local
// response state. Package-level sentinel errors are shared between requests,
// so callers must clone them before changing HeadersWritten, requestID, or
// retry classification.
func (e *Error) Clone() *Error {
	if e == nil {
		return nil
	}
	clone := *e
	return &clone
}

// WithRequestID attaches stream correlation without exposing or rewriting the
// wrapped provider error.
func (e *Error) WithRequestID(id string) *Error {
	if e != nil {
		e.requestID = id
	}
	return e
}

func (e *Error) Error() string {
	return e.err.Error()
}

func (e *Error) Unwrap() error {
	return e.err
}

func (e *Error) Retryable() *Error {
	e.retry = true
	return e
}

func (e *Error) Permanent() *Error {
	e.permanent = true
	return e
}

func (e *Error) IsRetryable() bool {
	return e.retry && !e.permanent
}

func (e *Error) IsPermanent() bool {
	return e.permanent
}

func (e *Error) IsSilent() bool {
	if e.err == nil {
		return false
	}
	if e.silent {
		return true
	}
	return IsSilentError(e.err)
}

// HTTPStatus returns the status associated with an error. Internal errors and
// legacy custom errors without an explicit status remain HTTP 500.
func (e *Error) HTTPStatus() int {
	if e == nil || e.statusCode < 400 || e.statusCode > 599 {
		return http.StatusInternalServerError
	}
	return e.statusCode
}

func NewError(err error, statusCode int, code string, silent bool, headersWritten bool) *Error {
	return &Error{
		err:            err,
		silent:         silent,
		statusCode:     statusCode,
		Code:           code,
		HeadersWritten: headersWritten,
	}
}

func NewSilentError(err error) *Error {
	return &Error{
		err:            err,
		silent:         true,
		statusCode:     http.StatusInternalServerError,
		HeadersWritten: false,
	}
}

func NewPermanentError(err error) *Error {
	e := &Error{
		err:            err,
		silent:         false,
		statusCode:     http.StatusInternalServerError,
		HeadersWritten: false,
	}
	return e.Permanent()
}

func FromError(err error) *Error {
	var customErr *Error
	if errors.As(err, &customErr) {
		return customErr
	}

	return &Error{
		err:            err,
		silent:         false,
		statusCode:     http.StatusInternalServerError,
		HeadersWritten: false,
	}
}

func IsSilentError(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, http.ErrHandlerTimeout) ||
		errors.Is(err, io.ErrClosedPipe) {
		return true
	}
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		if errors.Is(netErr.Err, syscall.EPIPE) || errors.Is(netErr.Err, syscall.ECONNRESET) {
			return true
		}
	}

	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "client disconnected") {
		return true
	}

	// Check for custom error type
	var customErr *Error
	if errors.As(err, &customErr) {
		return customErr.silent
	}

	return false
}

func NewArticleNotFoundError(err error) *Error {
	if err == nil {
		err = errors.New("article not found")
	}
	return (&Error{
		err:        err,
		statusCode: http.StatusGone,
		Code:       "usenet_article_not_found",
	}).Permanent()
}
