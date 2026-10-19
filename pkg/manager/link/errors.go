package link

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// ErrorCategory defines the type of link error and its retry behavior
type ErrorCategory int

// CodeLinkRefreshCooldown identifies local, per-file refresh suppression, not
// an observed provider throttle. It still uses the normal backoff semantics.
const CodeLinkRefreshCooldown = "link_refresh_cooldown"

const (
	// CategoryPermanent - Don't retry (file deleted, unauthorized)
	CategoryPermanent ErrorCategory = iota
	// CategoryRetryable - retry same link (timeout, 503)
	CategoryRetryable
	// CategoryRefetchable - Get new link (expired, invalid code)
	CategoryRefetchable
	// CategoryAccountIssue - Temporarily suspend account (bandwidth/quota pressure)
	CategoryAccountIssue
	// CategoryThrottled - wait and retry the same link (429)
	CategoryThrottled
)

// String returns a human-readable name for the error category
func (c ErrorCategory) String() string {
	switch c {
	case CategoryPermanent:
		return "permanent"
	case CategoryRetryable:
		return "retryable"
	case CategoryRefetchable:
		return "refetchable"
	case CategoryAccountIssue:
		return "account_issue"
	case CategoryThrottled:
		return "throttled"
	default:
		return "unknown"
	}
}

// Error represents a structured error with retry semantics
type Error struct {
	Err        error
	Category   ErrorCategory
	Code       string        // Stable code from a provider or local link policy
	RetryAfter time.Duration // delay from a throttled response or local cooldown
}

// Error implements the error interface
func (e *Error) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Err.Error())
	}
	return e.Err.Error()
}

// Unwrap returns the underlying error
func (e *Error) Unwrap() error {
	return e.Err
}

// ShouldRetry returns true if the same link should be retried
func (e *Error) ShouldRetry() bool {
	return e.Category == CategoryRetryable
}

// ShouldRefetch returns true if a new link should be fetched
func (e *Error) ShouldRefetch() bool {
	return e.Category == CategoryRefetchable
}

// ShouldSuspendAccount returns true when provider pressure should temporarily
// remove the account from selection and schedule a recovery probe.
func (e *Error) ShouldSuspendAccount() bool {
	return e.Category == CategoryAccountIssue
}

// ShouldDisableAccount is retained for source compatibility. Account-issue
// errors are now temporary suspensions rather than permanent disables.
func (e *Error) ShouldDisableAccount() bool {
	return e.ShouldSuspendAccount()
}

// ShouldBackoff reports whether the same link should be retried after a delay.
func (e *Error) ShouldBackoff() bool {
	return e.Category == CategoryThrottled
}

// IsRetryable allows callers outside this package to preserve transient error
// semantics without importing ErrorCategory.
func (e *Error) IsRetryable() bool {
	return e.Category != CategoryPermanent
}

// IsPermanent returns true if the error is permanent and no retry should happen
func (e *Error) IsPermanent() bool {
	return e.Category == CategoryPermanent
}

// Sentinel errors
var (
	ErrUnauthorized        = errors.New("unauthorized access to download link")
	ErrLinkNotFound        = errors.New("download link not found")
	ErrBandwidthExceeded   = errors.New("bandwidth limit exceeded")
	ErrInvalidDownloadCode = errors.New("invalid download code")
	ErrLinkExpired         = errors.New("download link expired")
	ErrFileNotAvailable    = errors.New("file not available for download")
	ErrNoActiveAccount     = errors.New("no active account available")
	ErrClientNotFound      = errors.New("debrid client not found")
	ErrPlacementNotFound   = errors.New("placement not found for entry")
	ErrFileMissing         = errors.New("file missing in entry")
	ErrEmptyLink           = errors.New("download link is empty")
)

// HTTP error sentinels
var (
	Err404 = errors.New("HTTP 404 Not Found")
	Err429 = errors.New("HTTP 429 Too Many Requests")
	Err503 = errors.New("HTTP 503 Service Unavailable")
)

// NewLinkError creates a new LinkError with the given error and category
func NewLinkError(err error, category ErrorCategory, code string) *Error {
	return &Error{
		Err:      err,
		Category: category,
		Code:     code,
	}
}

// NewPermanentError creates a permanent error
func NewPermanentError(err error, code string) *Error {
	return NewLinkError(err, CategoryPermanent, code)
}

// NewRetryableError creates a retryable error
func NewRetryableError(err error, code string) *Error {
	return NewLinkError(err, CategoryRetryable, code)
}

// NewRefetchableError creates an error that requires refetching the link
func NewRefetchableError(err error, code string) *Error {
	return NewLinkError(err, CategoryRefetchable, code)
}

// NewAccountError creates an error that temporarily suspends the account.
func NewAccountError(err error, code string) *Error {
	return NewLinkError(err, CategoryAccountIssue, code)
}

// ErrorCodeToLinkError converts an error code string to a LinkError with appropriate category
func ErrorCodeToLinkError(code string) *Error {
	switch code {
	case "link_not_found":
		return NewRefetchableError(ErrLinkNotFound, code)
	case "bandwidth_exceeded", "quota_exceeded", "daily_limit_exceeded", "bytes_limit_reached":
		return NewAccountError(ErrBandwidthExceeded, code)
	case "link_expired":
		return NewRefetchableError(ErrLinkExpired, code)
	case "file_not_available":
		return NewPermanentError(ErrFileNotAvailable, code)
	case "invalid_download_code":
		return NewRefetchableError(ErrInvalidDownloadCode, code)
	case "401", "unauthorized":
		return NewPermanentError(ErrUnauthorized, code)
	case "404":
		return NewRefetchableError(Err404, code)
	case "429":
		return NewLinkError(Err429, CategoryThrottled, code)
	case "408", "425", "500", "502", "503", "504", "read_pxy_timeout":
		return NewRetryableError(fmt.Errorf("temporary download-link failure: %s", code), code)
	default:
		return NewPermanentError(fmt.Errorf("unknown error code: %s", code), code)
	}
}

// ClassifyHTTPStatus classifies errors returned by a generated download URL.
// Authentication-shaped statuses at this layer usually mean the signed link
// rotated or expired, while throttling and 5xx failures should reuse the same
// URL instead of creating provider API traffic.
func ClassifyHTTPStatus(status int, header http.Header) *Error {
	switch {
	case status == http.StatusBadRequest || status == http.StatusUnauthorized ||
		status == http.StatusForbidden || status == http.StatusNotFound ||
		status == http.StatusGone:
		return NewRefetchableError(fmt.Errorf("HTTP %d: download link rejected", status), strconv.Itoa(status))
	case status == http.StatusRequestedRangeNotSatisfiable:
		return NewPermanentError(errors.New("HTTP 416: requested range not satisfiable"), "416")
	case status == http.StatusTooManyRequests:
		err := NewLinkError(Err429, CategoryThrottled, "429")
		err.RetryAfter = parseRetryAfter(header.Get("Retry-After"), time.Now())
		return err
	case status == http.StatusRequestTimeout || status == http.StatusTooEarly || status >= 500:
		return NewRetryableError(fmt.Errorf("HTTP %d", status), strconv.Itoa(status))
	default:
		return NewPermanentError(fmt.Errorf("unexpected HTTP status %d", status), strconv.Itoa(status))
	}
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		if delay := at.Sub(now); delay > 0 {
			return delay
		}
	}
	return 0
}

// IsLinkError checks if an error is a LinkError
func IsLinkError(err error) bool {
	var linkErr *Error
	return errors.As(err, &linkErr)
}

// GetLinkError extracts a LinkError from an error chain
func GetLinkError(err error) *Error {
	var linkErr *Error
	if errors.As(err, &linkErr) {
		return linkErr
	}
	return nil
}

// safeRefetchLogCode permits only known rejection codes, never arbitrary
// provider text that might contain signed links or account credentials.
func safeRefetchLogCode(code string) string {
	switch code {
	case "400", "401", "403", "404", "410",
		"link_not_found", "link_expired", "invalid_download_code",
		"range_probe_status", "range_probe_encoding", "range_probe_content_range",
		"range_probe_content_length", "range_probe_body_length":
		return code
	default:
		return "unknown"
	}
}
