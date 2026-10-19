package link

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestClassifyHTTPStatus(t *testing.T) {
	tests := []struct {
		status   int
		category ErrorCategory
	}{
		{status: http.StatusForbidden, category: CategoryRefetchable},
		{status: http.StatusNotFound, category: CategoryRefetchable},
		{status: http.StatusTooManyRequests, category: CategoryThrottled},
		{status: http.StatusBadGateway, category: CategoryRetryable},
		{status: http.StatusRequestedRangeNotSatisfiable, category: CategoryPermanent},
	}

	for _, tt := range tests {
		err := ClassifyHTTPStatus(tt.status, make(http.Header))
		if err.Category != tt.category {
			t.Fatalf("status %d category = %s, want %s", tt.status, err.Category, tt.category)
		}
	}
}

func TestRetryAfterParsing(t *testing.T) {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	if got := parseRetryAfter("7", now); got != 7*time.Second {
		t.Fatalf("delta Retry-After = %s, want 7s", got)
	}
	date := now.Add(11 * time.Second).Format(http.TimeFormat)
	if got := parseRetryAfter(date, now); got != 11*time.Second {
		t.Fatalf("date Retry-After = %s, want 11s", got)
	}
	if got := parseRetryAfter("invalid", now); got != 0 {
		t.Fatalf("invalid Retry-After = %s, want 0", got)
	}
}

func TestProviderErrorCodesKeepThrottleSeparateFromRefresh(t *testing.T) {
	if err := ErrorCodeToLinkError("429"); !err.ShouldBackoff() || err.ShouldRefetch() {
		t.Fatalf("429 classification = %s, want throttled only", err.Category)
	}
	if err := ErrorCodeToLinkError("read_pxy_timeout"); !err.ShouldRetry() || err.ShouldRefetch() {
		t.Fatalf("read_pxy_timeout classification = %s, want same-link retry", err.Category)
	}
	if err := ErrorCodeToLinkError("link_not_found"); !err.ShouldRefetch() {
		t.Fatalf("link_not_found classification = %s, want refetchable", err.Category)
	}
}

func TestLocalRefreshCooldownKeepsBackoffSemantics(t *testing.T) {
	err := NewLinkError(ErrLinkExpired, CategoryThrottled, CodeLinkRefreshCooldown)
	err.RetryAfter = 30 * time.Second
	if !err.ShouldBackoff() || !err.IsRetryable() || err.ShouldRetry() || err.ShouldRefetch() || err.ShouldSuspendAccount() || err.IsPermanent() {
		t.Fatalf("local cooldown changed retry or account behavior: %+v", err)
	}
	if err.RetryAfter != 30*time.Second || err.Unwrap() != ErrLinkExpired {
		t.Fatal("local cooldown lost its delay or underlying error")
	}
}

func TestSafeRefetchLogCode(t *testing.T) {
	for _, code := range []string{
		"400", "401", "403", "404", "410",
		"link_not_found", "link_expired", "invalid_download_code",
		"range_probe_status", "range_probe_encoding", "range_probe_content_range",
		"range_probe_content_length", "range_probe_body_length",
	} {
		if got := safeRefetchLogCode(code); got != code {
			t.Errorf("known rejection %q logged as %q", code, got)
		}
	}
	for _, code := range []string{
		"", "unknown", "synthetic-account-token",
		"https://example.invalid/file?token=synthetic", "403\ncredential=synthetic",
		strings.Repeat("x", 4096),
	} {
		if got := safeRefetchLogCode(code); got != "unknown" {
			t.Errorf("unrecognized rejection exposed raw code: %q", got)
		}
	}
}
