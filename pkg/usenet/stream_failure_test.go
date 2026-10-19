package usenet

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/puzpuzpuz/xsync/v4"
)

func TestCheckStreamReadyReturnsPersistentArticleFailure(t *testing.T) {
	u := &Usenet{failedFiles: xsync.NewMap[string, *failedFileState]()}
	cause := &nntp.Error{
		Type:    nntp.ErrorTypeArticleNotFound,
		Code:    430,
		Message: "article missing on all providers",
	}
	u.recordFailedFile(fsKey("nzb-id", "Movie.mkv"), 1024, cause)

	err := u.CheckStreamReady("nzb-id", "Movie.mkv")
	if err == nil {
		t.Fatal("expected previously failed file to be rejected")
	}
	if !errors.Is(err, cause) {
		t.Fatal("stream-ready error does not retain the NNTP failure")
	}
	var streamErr *customerror.Error
	if !errors.As(err, &streamErr) {
		t.Fatalf("error type = %T, want *customerror.Error", err)
	}
	if !streamErr.IsPermanent() || streamErr.HTTPStatus() != http.StatusGone {
		t.Fatalf("stream error permanent/status = %v/%d, want true/%d",
			streamErr.IsPermanent(), streamErr.HTTPStatus(), http.StatusGone)
	}

	if err := u.CheckStreamReady("nzb-id", "Other.mkv"); err != nil {
		t.Fatalf("unfailed file was rejected: %v", err)
	}
}

func TestGetOrCreateEntryRejectsFailedFileBeforeCachedReader(t *testing.T) {
	failedFiles := xsync.NewMap[string, *failedFileState]()
	entries := xsync.NewMap[string, *fsEntry]()
	u := &Usenet{failedFiles: failedFiles, fs: entries}
	key := fsKey("nzb-id", "Movie.mkv")
	entries.Store(key, &fsEntry{})
	u.recordFailedFile(key, 0, &nntp.Error{
		Type:    nntp.ErrorTypeArticleNotFound,
		Code:    430,
		Message: "gone",
	})

	entry, gotKey, err := u.getOrCreateEntry(context.Background(), "nzb-id", "Movie.mkv")
	if err == nil {
		t.Fatal("expected cached reader to be fenced after a permanent stream failure")
	}
	if entry != nil {
		t.Fatal("failed file returned a cached reader")
	}
	if gotKey != key {
		t.Errorf("cache key = %q, want %q", gotKey, key)
	}
}

func TestEnsureStreamReadyRecoversExactFailedOffsetAfterCooldown(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	u := &Usenet{
		failedFiles: xsync.NewMap[string, *failedFileState](),
		failureNow:  func() time.Time { return now },
	}
	cause := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "gone"}
	key := fsKey("nzb-id", "Movie.mkv")
	u.recordFailedFile(key, 4096, cause)

	var probes atomic.Int32
	u.failureProbe = func(_ context.Context, nzoID, filename string, offset int64) error {
		probes.Add(1)
		if nzoID != "nzb-id" || filename != "Movie.mkv" || offset != 4096 {
			t.Fatalf("probe = %q/%q@%d, want nzb-id/Movie.mkv@4096", nzoID, filename, offset)
		}
		return nil
	}

	if err := u.EnsureStreamReady(context.Background(), "nzb-id", "Movie.mkv"); err == nil {
		t.Fatal("quarantine was bypassed before cooldown")
	}
	if probes.Load() != 0 {
		t.Fatal("recovery probe ran before cooldown")
	}

	now = now.Add(failedFileInitialBackoff)
	if err := u.EnsureStreamReady(context.Background(), "nzb-id", "Movie.mkv"); err != nil {
		t.Fatalf("recovery probe failed: %v", err)
	}
	if probes.Load() != 1 {
		t.Fatalf("probes = %d, want 1", probes.Load())
	}
	if err := u.CheckStreamReady("nzb-id", "Movie.mkv"); err != nil {
		t.Fatalf("successful recovery did not clear quarantine: %v", err)
	}
}

func TestEnsureStreamReadyCoalescesRecoveryProbe(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	u := &Usenet{
		failedFiles: xsync.NewMap[string, *failedFileState](),
		failureNow:  func() time.Time { return now },
	}
	cause := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "gone"}
	u.recordFailedFile(fsKey("nzb-id", "Movie.mkv"), 7, cause)
	now = now.Add(failedFileInitialBackoff)

	started := make(chan struct{})
	release := make(chan struct{})
	u.failureProbe = func(context.Context, string, string, int64) error {
		close(started)
		<-release
		return nil
	}

	first := make(chan error, 1)
	go func() { first <- u.EnsureStreamReady(context.Background(), "nzb-id", "Movie.mkv") }()
	<-started
	if err := u.EnsureStreamReady(context.Background(), "nzb-id", "Movie.mkv"); err == nil {
		t.Fatal("concurrent request joined or bypassed the single recovery probe")
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("recovery probe failed: %v", err)
	}
}

func TestEnsureStreamReadyBacksOffAfterFailedRecovery(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	u := &Usenet{
		failedFiles: xsync.NewMap[string, *failedFileState](),
		failureNow:  func() time.Time { return now },
	}
	cause := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "gone"}
	key := fsKey("nzb-id", "Movie.mkv")
	u.recordFailedFile(key, 9, cause)
	now = now.Add(failedFileInitialBackoff)
	u.failureProbe = func(context.Context, string, string, int64) error { return cause }

	if err := u.EnsureStreamReady(context.Background(), "nzb-id", "Movie.mkv"); !errors.Is(err, cause) {
		t.Fatalf("recovery error = %v, want original cause", err)
	}
	state, ok := u.failedFiles.Load(key)
	if !ok {
		t.Fatal("failed recovery unexpectedly cleared quarantine")
	}
	state.mu.Lock()
	retryAfter := state.retryAfter
	probeInFlight := state.probeInFlight
	state.mu.Unlock()
	if probeInFlight || !retryAfter.Equal(now.Add(5*time.Minute)) {
		t.Fatalf("state after failed recovery: probe=%v retry=%s", probeInFlight, retryAfter)
	}
}

func TestConcurrentReadFailuresDoNotEscalateRecoveryBackoff(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	u := &Usenet{
		failedFiles: xsync.NewMap[string, *failedFileState](),
		failureNow:  func() time.Time { return now },
	}
	key := fsKey("nzb-id", "Movie.mkv")
	for offset := int64(1); offset <= 8; offset++ {
		u.recordFailedFile(key, offset, &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430})
	}
	state, ok := u.failedFiles.Load(key)
	if !ok {
		t.Fatal("missing quarantine state")
	}
	state.mu.Lock()
	failures, retryAfter := state.failures, state.retryAfter
	state.mu.Unlock()
	if failures != 1 || !retryAfter.Equal(now.Add(failedFileInitialBackoff)) {
		t.Fatalf("state = failures %d retry %s", failures, retryAfter)
	}
}

func TestEnsureStreamReadyDoesNotClearConcurrentFailure(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	u := &Usenet{
		failedFiles: xsync.NewMap[string, *failedFileState](),
		failureNow:  func() time.Time { return now },
	}
	firstCause := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "first"}
	newCause := &nntp.Error{Type: nntp.ErrorTypeArticleNotFound, Code: 430, Message: "new"}
	key := fsKey("nzb-id", "Movie.mkv")
	u.recordFailedFile(key, 9, firstCause)
	now = now.Add(failedFileInitialBackoff)

	started := make(chan struct{})
	release := make(chan struct{})
	u.failureProbe = func(context.Context, string, string, int64) error {
		close(started)
		<-release
		return nil
	}

	done := make(chan error, 1)
	go func() { done <- u.EnsureStreamReady(context.Background(), "nzb-id", "Movie.mkv") }()
	<-started
	u.recordFailedFile(key, 42, newCause)
	close(release)
	if err := <-done; !errors.Is(err, newCause) {
		t.Fatalf("recovery result = %v, want concurrent failure", err)
	}
	if err := u.CheckStreamReady("nzb-id", "Movie.mkv"); !errors.Is(err, newCause) {
		t.Fatalf("quarantine = %v, want concurrent failure", err)
	}
}

func TestCheckStreamReadyToleratesUninitializedFailureCache(t *testing.T) {
	if err := (*Usenet)(nil).CheckStreamReady("nzb-id", "Movie.mkv"); err != nil {
		t.Fatalf("nil usenet returned error: %v", err)
	}
	if err := (&Usenet{}).CheckStreamReady("nzb-id", "Movie.mkv"); err != nil {
		t.Fatalf("empty usenet returned error: %v", err)
	}
}

func TestFailedReadOffsetPointsAtFirstUndeliveredByte(t *testing.T) {
	tests := []struct {
		name                string
		start, end, written int64
		want                int64
	}{
		{name: "first byte", start: 100, end: 199, written: 0, want: 100},
		{name: "mid range", start: 100, end: 199, written: 37, want: 137},
		{name: "clamped", start: 100, end: 199, written: 200, want: 199},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := failedReadOffset(tt.start, tt.end, tt.written); got != tt.want {
				t.Fatalf("failedReadOffset(%d, %d, %d) = %d, want %d", tt.start, tt.end, tt.written, got, tt.want)
			}
		})
	}
}
