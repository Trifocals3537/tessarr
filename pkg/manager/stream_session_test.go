package manager

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
)

type sessionSource struct {
	mu      sync.Mutex
	data    []byte
	offsets []int64
	open    func(context.Context, int64) (io.ReadCloser, error)
}

func (s *sessionSource) openAt(ctx context.Context, offset int64) (io.ReadCloser, error) {
	s.mu.Lock()
	s.offsets = append(s.offsets, offset)
	open := s.open
	data := s.data
	s.mu.Unlock()
	if open != nil {
		return open(ctx, offset)
	}
	return io.NopCloser(bytes.NewReader(data[offset:])), nil
}

func (s *sessionSource) openedAt() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.offsets...)
}

func newTestStreamSession(t *testing.T, source *sessionSource, offset int64) *streamSession {
	t.Helper()
	session, err := newStreamSession(t.Context(), int64(len(source.data)), offset, source.openAt, nil)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func TestStreamSessionReusesBodyAcrossSequentialReads(t *testing.T) {
	source := &sessionSource{data: []byte("abcdefghij")}
	session := newTestStreamSession(t, source, 0)
	defer session.Close()

	first := make([]byte, 4)
	if n, err := session.Read(first); err != nil || n != 4 || string(first) != "abcd" {
		t.Fatalf("first read = %d, %q, %v", n, first, err)
	}
	second := make([]byte, 3)
	if n, err := session.Read(second); err != nil || n != 3 || string(second) != "efg" {
		t.Fatalf("second read = %d, %q, %v", n, second, err)
	}
	if got := source.openedAt(); len(got) != 1 || got[0] != 0 {
		t.Fatalf("open offsets = %#v, want [0]", got)
	}
}

func TestStreamSessionStatsAggregateReuseRecoveryIdleAndClose(t *testing.T) {
	data := []byte("abcdef")
	var opens int
	source := &sessionSource{data: data}
	source.open = func(_ context.Context, offset int64) (io.ReadCloser, error) {
		opens++
		if opens == 1 {
			return io.NopCloser(bytes.NewReader(data[offset : offset+2])), nil
		}
		return io.NopCloser(bytes.NewReader(data[offset:])), nil
	}
	manager := &Manager{}
	session, err := newStreamSessionWithOptions(
		t.Context(),
		int64(len(data)),
		0,
		source.openAt,
		func(context.Context, error, int) error {
			time.Sleep(2 * time.Millisecond)
			return nil
		},
		StreamSessionOptions{MaxAttempts: 2, IdleTimeout: 10 * time.Millisecond},
	)
	if err != nil {
		t.Fatal(err)
	}
	session.metrics = &manager.streamSessionMetrics
	manager.streamSessionMetrics.sessionsOpened.Add(1)

	for want := byte('a'); want <= 'c'; want++ {
		buf := make([]byte, 1)
		if n, readErr := session.Read(buf); readErr != nil || n != 1 || buf[0] != want {
			t.Fatalf("read %q = %d, %q, %v", want, n, buf, readErr)
		}
	}

	deadline := time.Now().Add(time.Second)
	for manager.StreamSessionStats().IdleCloses == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	stats := manager.StreamSessionStats()
	if stats.SessionsOpened != 1 || stats.ActiveSessions != 1 || stats.SourceOpens != 2 {
		t.Fatalf("open stats = %+v", stats)
	}
	if stats.ReusedReads != 2 || stats.RecoveryAttempts != 1 || stats.RecoveriesScheduled != 1 || stats.RecoveriesExhausted != 0 {
		t.Fatalf("recovery stats = %+v", stats)
	}
	if stats.IdleCloses != 1 || stats.RecoveryWaitTotalMS == 0 || stats.RecoveryWaitMaxMS == 0 {
		t.Fatalf("timing stats = %+v", stats)
	}

	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	stats = manager.StreamSessionStats()
	if stats.SessionsClosed != 1 || stats.ActiveSessions != 0 {
		t.Fatalf("closed stats = %+v", stats)
	}
}

func TestStreamSessionStatsCountExhaustionWithoutRetry(t *testing.T) {
	source := &sessionSource{data: []byte("x")}
	source.open = func(context.Context, int64) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	manager := &Manager{}
	session, err := newStreamSessionWithOptions(
		t.Context(),
		1,
		0,
		source.openAt,
		func(context.Context, error, int) error { return nil },
		StreamSessionOptions{MaxAttempts: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	session.metrics = &manager.streamSessionMetrics
	manager.streamSessionMetrics.sessionsOpened.Add(1)
	defer session.Close()

	if _, readErr := session.Read(make([]byte, 1)); readErr == nil {
		t.Fatal("exhausted read succeeded")
	}
	stats := manager.StreamSessionStats()
	if stats.SourceOpens != 1 || stats.RecoveryAttempts != 0 || stats.RecoveriesExhausted != 1 {
		t.Fatalf("exhaustion stats = %+v", stats)
	}
}

func TestStreamSessionSeekReopensAtExactOffset(t *testing.T) {
	source := &sessionSource{data: []byte("abcdefghij")}
	session := newTestStreamSession(t, source, 0)
	defer session.Close()

	buf := make([]byte, 2)
	if _, err := session.Read(buf); err != nil {
		t.Fatal(err)
	}
	if got, err := session.Seek(6, io.SeekStart); err != nil || got != 6 {
		t.Fatalf("seek = %d, %v", got, err)
	}
	if n, err := session.Read(buf); err != nil || n != 2 || string(buf) != "gh" {
		t.Fatalf("read after seek = %d, %q, %v", n, buf, err)
	}
	if got := source.openedAt(); len(got) != 2 || got[0] != 0 || got[1] != 6 {
		t.Fatalf("open offsets = %#v, want [0 6]", got)
	}
}

func TestStreamSessionPrimeDoesNotAdvance(t *testing.T) {
	source := &sessionSource{data: []byte("prime")}
	session := newTestStreamSession(t, source, 0)
	defer session.Close()

	if err := session.Prime(); err != nil {
		t.Fatal(err)
	}
	if err := session.Prime(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2)
	if n, err := session.Read(buf); err != nil || n != 2 || string(buf) != "pr" {
		t.Fatalf("read after prime = %d, %q, %v", n, buf, err)
	}
	if got := source.openedAt(); len(got) != 1 || got[0] != 0 {
		t.Fatalf("open offsets = %#v, want [0]", got)
	}
}

func TestStreamSessionResumesFromConfirmedByte(t *testing.T) {
	data := []byte("abcdefgh")
	var opens int
	source := &sessionSource{data: data}
	source.open = func(_ context.Context, offset int64) (io.ReadCloser, error) {
		opens++
		if opens == 1 {
			return io.NopCloser(bytes.NewReader(data[offset : offset+2])), nil
		}
		return io.NopCloser(bytes.NewReader(data[offset:])), nil
	}
	session := newTestStreamSession(t, source, 0)
	defer session.Close()

	buf := make([]byte, 5)
	if _, err := io.ReadFull(session, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "abcde" {
		t.Fatalf("resumed data = %q", buf)
	}
	if got := source.openedAt(); len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Fatalf("open offsets = %#v, want [0 2]", got)
	}
}

func TestStreamSessionBoundsResumeAttempts(t *testing.T) {
	source := &sessionSource{data: []byte("retry")}
	source.open = func(context.Context, int64) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	session := newTestStreamSession(t, source, 0)
	session.recover = func(context.Context, error, int) error { return nil }
	defer session.Close()

	if n, err := session.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("bounded failure = %d, %v", n, err)
	} else {
		var exhausted *StreamSessionExhaustedError
		if !errors.As(err, &exhausted) || exhausted.Attempts != streamSessionMaxResumes+1 || exhausted.Offset != 0 {
			t.Fatalf("exhausted error = %#v", exhausted)
		}
	}
	if got := len(source.openedAt()); got != streamSessionMaxResumes+1 {
		t.Fatalf("open attempts = %d, want %d", got, streamSessionMaxResumes+1)
	}
}

func TestStreamSessionOptionsBoundTotalAttempts(t *testing.T) {
	source := &sessionSource{data: []byte("retry")}
	source.open = func(context.Context, int64) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	session, err := newStreamSessionWithOptions(
		t.Context(),
		int64(len(source.data)),
		0,
		source.openAt,
		func(context.Context, error, int) error { return nil },
		StreamSessionOptions{MaxAttempts: 2},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	if _, err := session.Read(make([]byte, 1)); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("bounded failure = %v", err)
	}
	if got := len(source.openedAt()); got != 2 {
		t.Fatalf("open attempts = %d, want 2", got)
	}
}

func TestRecoverStreamSessionHonorsExplicitRetryability(t *testing.T) {
	retryable := StreamError{Err: errors.New("provider returned 404 during a retryable refresh"), Retryable: true}
	if err := recoverStreamSession(t.Context(), retryable, 0); err != nil {
		t.Fatalf("explicitly retryable error was rejected: %v", err)
	}

	permanent := StreamError{Err: io.ErrUnexpectedEOF, Retryable: false}
	if err := recoverStreamSession(t.Context(), permanent, 0); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("explicitly permanent error = %v", err)
	}
}

type blockingStreamBody struct {
	ctx context.Context
}

func (b *blockingStreamBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (*blockingStreamBody) Close() error { return nil }

func TestStreamSessionCloseCancelsBlockedRead(t *testing.T) {
	started := make(chan struct{})
	var startedOnce sync.Once
	source := &sessionSource{data: []byte("cancel")}
	source.open = func(ctx context.Context, _ int64) (io.ReadCloser, error) {
		startedOnce.Do(func() { close(started) })
		return &blockingStreamBody{ctx: ctx}, nil
	}
	session := newTestStreamSession(t, source, 0)
	session.recover = func(_ context.Context, err error, _ int) error { return err }

	readDone := make(chan error, 1)
	go func() {
		_, readErr := session.Read(make([]byte, 1))
		readDone <- readErr
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("read did not start")
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case readErr := <-readDone:
		if !errors.Is(readErr, context.Canceled) && !errors.Is(readErr, os.ErrClosed) {
			t.Fatalf("blocked read error = %v", readErr)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not unblock read")
	}
	if _, err := session.Read(make([]byte, 1)); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("read after close = %v", err)
	}
}

func TestStreamSessionIdleClosesAndReopensBody(t *testing.T) {
	source := &sessionSource{data: []byte("idle")}
	session := newTestStreamSession(t, source, 0)
	session.idleTimeout = 10 * time.Millisecond
	defer session.Close()

	buf := make([]byte, 1)
	if _, err := session.Read(buf); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		session.mu.Lock()
		idleClosed := session.body == nil
		session.mu.Unlock()
		if idleClosed {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("idle body did not close")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := session.Read(buf); err != nil {
		t.Fatal(err)
	}
	if got := source.openedAt(); len(got) != 2 || got[1] != 1 {
		t.Fatalf("open offsets = %#v, want [0 1]", got)
	}
}

func TestStreamSessionStallCancelsAndResumes(t *testing.T) {
	data := []byte("stall")
	var opens int
	source := &sessionSource{data: data}
	source.open = func(ctx context.Context, offset int64) (io.ReadCloser, error) {
		opens++
		if opens == 1 {
			return &blockingStreamBody{ctx: ctx}, nil
		}
		return io.NopCloser(bytes.NewReader(data[offset:])), nil
	}
	session := newTestStreamSession(t, source, 0)
	manager := &Manager{}
	session.metrics = &manager.streamSessionMetrics
	manager.streamSessionMetrics.sessionsOpened.Add(1)
	session.stallTimeout = 10 * time.Millisecond
	defer session.Close()

	buf := make([]byte, 2)
	if n, err := session.Read(buf); err != nil || n != 2 || string(buf) != "st" {
		t.Fatalf("read after stall = %d, %q, %v", n, buf, err)
	}
	if got := source.openedAt(); len(got) != 2 || got[0] != 0 || got[1] != 0 {
		t.Fatalf("open offsets = %#v, want [0 0]", got)
	}
	stats := manager.StreamSessionStats()
	if stats.ReadStalls != 1 || stats.OpenStalls != 0 || stats.RecoveriesScheduled != 1 {
		t.Fatalf("stall stats = %+v", stats)
	}
}

func TestStreamSessionOpenTimeoutCancelsAndResumes(t *testing.T) {
	data := []byte("connect")
	var opens int
	source := &sessionSource{data: data}
	source.open = func(ctx context.Context, offset int64) (io.ReadCloser, error) {
		opens++
		if opens == 1 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return io.NopCloser(bytes.NewReader(data[offset:])), nil
	}
	session := newTestStreamSession(t, source, 0)
	manager := &Manager{}
	session.metrics = &manager.streamSessionMetrics
	manager.streamSessionMetrics.sessionsOpened.Add(1)
	session.stallTimeout = 10 * time.Millisecond
	defer session.Close()

	buf := make([]byte, 2)
	if n, err := session.Read(buf); err != nil || n != 2 || string(buf) != "co" {
		t.Fatalf("read after open timeout = %d, %q, %v", n, buf, err)
	}
	if got := source.openedAt(); len(got) != 2 || got[0] != 0 || got[1] != 0 {
		t.Fatalf("open offsets = %#v, want [0 0]", got)
	}
	stats := manager.StreamSessionStats()
	if stats.OpenStalls != 1 || stats.ReadStalls != 0 || stats.RecoveriesScheduled != 1 {
		t.Fatalf("open timeout stats = %+v", stats)
	}
}

func TestStreamSessionBadSourceDoesNotBlockHealthySession(t *testing.T) {
	started := make(chan struct{})
	badSource := &sessionSource{data: []byte("bad")}
	badSource.open = func(ctx context.Context, _ int64) (io.ReadCloser, error) {
		close(started)
		return &blockingStreamBody{ctx: ctx}, nil
	}
	bad, err := newStreamSessionWithOptions(
		t.Context(),
		int64(len(badSource.data)),
		0,
		badSource.openAt,
		nil,
		StreamSessionOptions{MaxAttempts: 1, StallTimeout: 20 * time.Millisecond},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Close()
	badDone := make(chan error, 1)
	go func() {
		_, readErr := bad.Read(make([]byte, 1))
		badDone <- readErr
	}()
	<-started

	healthySource := &sessionSource{data: []byte("healthy")}
	healthy := newTestStreamSession(t, healthySource, 0)
	defer healthy.Close()
	buf := make([]byte, 3)
	if n, err := healthy.Read(buf); err != nil || n != 3 || string(buf) != "hea" {
		t.Fatalf("healthy read = %d, %q, %v", n, buf, err)
	}

	select {
	case err := <-badDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("bad read error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("bad session did not fail within its bound")
	}
}

func TestStreamSessionSeekValidation(t *testing.T) {
	source := &sessionSource{data: []byte("0123456789")}
	session := newTestStreamSession(t, source, 0)
	defer session.Close()

	if _, err := session.Seek(-1, io.SeekStart); err == nil {
		t.Fatal("negative seek succeeded")
	}
	if _, err := session.Seek(0, 99); err == nil {
		t.Fatal("invalid whence succeeded")
	}
	if _, err := session.Seek(math.MaxInt64, io.SeekEnd); err == nil {
		t.Fatal("overflowing seek succeeded")
	}
	if got, err := session.Seek(5, io.SeekEnd); err != nil || got != 15 {
		t.Fatalf("seek past EOF = %d, %v", got, err)
	}
	if n, err := session.Read(make([]byte, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("read past EOF = %d, %v", n, err)
	}
}

func TestOpenStreamTracksUntilClose(t *testing.T) {
	mgr := &Manager{activeStreams: xsync.NewMap[string, *ActiveStream]()}
	entry := &storage.Entry{
		Name: "movie",
		Files: map[string]*storage.File{
			"video.mkv": {Name: "video.mkv", Size: 1024},
		},
	}

	stream, err := mgr.OpenStream(t.Context(), entry, "video.mkv", 0, "test")
	if err != nil {
		t.Fatal(err)
	}
	if mgr.GetActiveStreamsCount() != 1 {
		t.Fatalf("active streams = %d, want 1", mgr.GetActiveStreamsCount())
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if mgr.GetActiveStreamsCount() != 0 {
		t.Fatalf("active streams after close = %d, want 0", mgr.GetActiveStreamsCount())
	}
}

func TestTrackStreamKeepsConcurrentSameFileConsumersSeparate(t *testing.T) {
	mgr := &Manager{activeStreams: xsync.NewMap[string, *ActiveStream]()}
	entry := &storage.Entry{
		Name:           "movie",
		ActiveProvider: "primary",
		Files: map[string]*storage.File{
			"video.mkv": {Name: "video.mkv", Size: 1024},
		},
	}

	first := mgr.TrackStream(entry, "video.mkv", "Plex")
	second := mgr.TrackStream(entry, "video.mkv", "Jellyfin")
	if first == "" || second == "" || first == second {
		t.Fatalf("stream IDs = %q and %q, want distinct non-empty IDs", first, second)
	}
	if got := mgr.GetActiveStreamsCount(); got != 2 {
		t.Fatalf("active streams = %d, want 2", got)
	}

	mgr.updateActiveStreamProvider(first, "fallback")
	streams := mgr.GetActiveStreams()
	providers := make(map[string]string, len(streams))
	for _, stream := range streams {
		providers[stream.ID] = stream.Debrid
	}
	if len(streams) != 2 || providers[first] != "fallback" || providers[second] != "primary" {
		t.Fatalf("active stream providers = %+v, want only first consumer on fallback", providers)
	}

	mgr.UntrackStream(first)
	if got := mgr.GetActiveStreamsCount(); got != 1 {
		t.Fatalf("active streams after first close = %d, want 1", got)
	}

	mgr.UntrackStream(second)
	if got := mgr.GetActiveStreamsCount(); got != 0 {
		t.Fatalf("active streams after both close = %d, want 0", got)
	}
}

func TestOpenStreamValidatesEntryAndOffset(t *testing.T) {
	mgr := &Manager{activeStreams: xsync.NewMap[string, *ActiveStream]()}
	entry := &storage.Entry{
		Name: "movie",
		Files: map[string]*storage.File{
			"video.mkv": {Name: "video.mkv", Size: 10},
			"empty.mkv": {Name: "empty.mkv"},
		},
	}

	tests := []struct {
		name     string
		entry    *storage.Entry
		filename string
		offset   int64
	}{
		{name: "nil entry", filename: "video.mkv"},
		{name: "missing file", entry: entry, filename: "missing.mkv"},
		{name: "empty file", entry: entry, filename: "empty.mkv"},
		{name: "negative offset", entry: entry, filename: "video.mkv", offset: -1},
		{name: "offset beyond size", entry: entry, filename: "video.mkv", offset: 11},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if stream, err := mgr.OpenStreamUntracked(t.Context(), tc.entry, tc.filename, tc.offset); err == nil {
				stream.Close()
				t.Fatal("invalid stream opened")
			}
		})
	}
}
