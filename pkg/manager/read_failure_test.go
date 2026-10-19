package manager

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/rs/zerolog"
)

func TestClassifyTerminalReadFailure(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		class     string
		retryable bool
	}{
		{name: "canceled", err: context.Canceled, class: "canceled"},
		{name: "deadline", err: context.DeadlineExceeded, class: "timeout", retryable: true},
		{name: "missing article", err: &nntp.Error{Type: nntp.ErrorTypeArticleNotFound}, class: "content_missing"},
		{name: "authentication", err: &nntp.Error{Type: nntp.ErrorTypeAuthentication}, class: "provider_auth"},
		{name: "truncated", err: io.ErrUnexpectedEOF, class: "unexpected_eof", retryable: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class, retryable := classifyTerminalReadFailure(tt.err)
			if class != tt.class || retryable != tt.retryable {
				t.Fatalf("classification = %q/%v, want %q/%v", class, retryable, tt.class, tt.retryable)
			}
		})
	}
}

func TestRecordTerminalReadFailureMarksDirtyRateLimitsAndRedactsError(t *testing.T) {
	store, err := storage.NewStorage(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	var logs bytes.Buffer
	m := &Manager{storage: store, logger: zerolog.New(&logs)}
	m.repair = NewRepair(m)
	m.repair.eventMu.Lock()
	m.repair.eventStarted = true
	m.repair.eventMu.Unlock()
	entry := &storage.Entry{
		Name:           "Movie.Release",
		InfoHash:       "safe-hash",
		Protocol:       config.ProtocolNZB,
		ActiveProvider: "",
	}
	secret := "signed-token-must-not-appear"
	readErr := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: secret}

	m.RecordTerminalReadFailure(entry, "Movie.mkv", 1024, 4096, readErr)
	m.RecordTerminalReadFailure(entry, "Movie.mkv", 1024, 4096, readErr)

	state, err := store.GetEntryHealth(entry.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !state.Dirty || state.DirtyReason != "read_failure:content_missing" || state.Protocol != config.ProtocolNZB {
		t.Fatalf("health state = %+v", state)
	}
	if got := m.terminalReadFailures.Load(); got != 2 {
		t.Fatalf("terminal read failures = %d, want 2", got)
	}
	if status := m.repair.eventQueueStatus(); status.Pending != 1 || status.Coalesced != 1 {
		t.Fatalf("event queue status = %+v, want one pending and one coalesced dirty event", status)
	}
	output := logs.String()
	if count := strings.Count(output, "Terminal media read failure"); count != 1 {
		t.Fatalf("terminal read failure log count = %d, want 1", count)
	}
	if strings.Contains(output, secret) {
		t.Fatal("raw provider error leaked into structured log")
	}
	if got := strings.Count(output, "Terminal media read failure"); got != 1 {
		t.Fatalf("log count = %d, want 1; output=%s", got, output)
	}
	for _, want := range []string{`"event":"stream.read_terminal_failure"`, `"outcome":"failed"`, `"failure_class":"content_missing"`, `"provider":"usenet"`, `"health_dirty_persisted":true`} {
		if !strings.Contains(output, want) {
			t.Fatalf("log missing %s: %s", want, output)
		}
	}
}

func TestRecordTerminalReadFailureIgnoresCancellation(t *testing.T) {
	m := &Manager{}
	m.RecordTerminalReadFailure(&storage.Entry{Name: "Movie.Release"}, "Movie.mkv", 0, 1, context.Canceled)
	if got := m.terminalReadFailures.Load(); got != 0 {
		t.Fatalf("terminal read failures = %d, want 0", got)
	}
}

func TestRecordTerminalReadFailureHandlesWrappedUnexpectedEOF(t *testing.T) {
	class, retryable := classifyTerminalReadFailure(errors.Join(errors.New("download failed"), io.ErrUnexpectedEOF))
	if class != "unexpected_eof" || !retryable {
		t.Fatalf("classification = %q/%v", class, retryable)
	}
}
