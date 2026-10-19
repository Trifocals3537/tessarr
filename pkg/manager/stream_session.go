package manager

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

const (
	defaultStreamSessionIdleTimeout  = 30 * time.Second
	defaultStreamSessionStallTimeout = 90 * time.Second
	streamSessionMaxResumes          = 4
	streamSessionResumeBaseDelay     = 500 * time.Millisecond
)

// StreamReader is a seekable, bounded view of one managed media file.
// Recovery happens from the last byte confirmed to the caller, so consumers do
// not need their own retry loop and never receive replayed bytes.
type StreamReader interface {
	io.ReadCloser
	io.Seeker
	Size() int64
	Prime() error
}

// StreamSessionOptions bounds one logical stream session. Zero values select
// the production defaults. MaxAttempts includes the initial connection.
type StreamSessionOptions struct {
	MaxAttempts     int
	IdleTimeout     time.Duration
	StallTimeout    time.Duration
	ResumeBaseDelay time.Duration
}

// StreamSessionExhaustedError reports a bounded terminal session failure.
// It deliberately retains the provider error for errors.Is/errors.As while
// allowing callers to avoid multiplying an already-consumed retry budget.
type StreamSessionExhaustedError struct {
	Err      error
	Attempts int
	Offset   int64
}

func (e *StreamSessionExhaustedError) Error() string {
	return fmt.Sprintf("stream session exhausted %d attempt(s) at offset %d: %v", e.Attempts, e.Offset, e.Err)
}

func (e *StreamSessionExhaustedError) Unwrap() error {
	return e.Err
}

type streamOpenFunc func(context.Context, int64) (io.ReadCloser, error)
type streamRecoverFunc func(context.Context, error, int) error

type streamSession struct {
	ctx     context.Context
	cancel  context.CancelFunc
	open    streamOpenFunc
	recover streamRecoverFunc
	size    int64

	idleTimeout  time.Duration
	stallTimeout time.Duration
	maxResumes   int
	metrics      *streamSessionMetrics

	mu         sync.Mutex
	pos        int64
	body       io.ReadCloser
	bodyCancel context.CancelFunc
	closed     bool
	onClose    func()

	idle           *time.Timer
	idleGeneration uint64

	stall *time.Timer
}

type streamStallState struct {
	expired atomic.Bool
}

func newStreamSession(
	ctx context.Context,
	size int64,
	offset int64,
	open streamOpenFunc,
	recover streamRecoverFunc,
) (*streamSession, error) {
	return newStreamSessionWithOptions(ctx, size, offset, open, recover, StreamSessionOptions{})
}

func newStreamSessionWithOptions(
	ctx context.Context,
	size int64,
	offset int64,
	open streamOpenFunc,
	recover streamRecoverFunc,
	options StreamSessionOptions,
) (*streamSession, error) {
	if size <= 0 {
		return nil, fmt.Errorf("invalid stream size %d", size)
	}
	if offset < 0 || offset > size {
		return nil, fmt.Errorf("stream offset %d is outside file size %d", offset, size)
	}
	if open == nil {
		return nil, fmt.Errorf("stream opener is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	idleTimeout := options.IdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = defaultStreamSessionIdleTimeout
	}
	stallTimeout := options.StallTimeout
	if stallTimeout <= 0 {
		stallTimeout = defaultStreamSessionStallTimeout
	}
	maxResumes := streamSessionMaxResumes
	if options.MaxAttempts > 0 {
		maxResumes = options.MaxAttempts - 1
	}
	resumeBaseDelay := options.ResumeBaseDelay
	if resumeBaseDelay <= 0 {
		resumeBaseDelay = streamSessionResumeBaseDelay
	}
	if recover == nil {
		recover = func(ctx context.Context, err error, attempt int) error {
			return recoverStreamSessionWithDelay(ctx, err, attempt, resumeBaseDelay)
		}
	}
	sessionCtx, cancel := context.WithCancel(ctx)
	session := &streamSession{
		ctx:          sessionCtx,
		cancel:       cancel,
		open:         open,
		recover:      recover,
		size:         size,
		pos:          offset,
		idleTimeout:  idleTimeout,
		stallTimeout: stallTimeout,
		maxResumes:   maxResumes,
	}
	return session, nil
}

func (s *streamSession) Size() int64 {
	return s.size
}

func (s *streamSession) Read(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, os.ErrClosed
	}
	if len(p) == 0 {
		return 0, nil
	}
	if s.pos >= s.size {
		return 0, io.EOF
	}
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}

	if remaining := s.size - s.pos; int64(len(p)) > remaining {
		p = p[:int(remaining)]
	}
	s.stopIdleLocked()
	if s.body != nil && s.metrics != nil {
		s.metrics.reusedReads.Add(1)
	}

	for attempt := 0; ; attempt++ {
		if err := s.ctx.Err(); err != nil {
			return 0, err
		}
		if s.body == nil {
			if err := s.connectLocked(); err != nil {
				if recoveryErr := s.recoverLocked(err, attempt); recoveryErr != nil {
					return 0, recoveryErr
				}
				continue
			}
		}

		stallState := s.armStallLocked()
		n, err := s.body.Read(p)
		stalled := s.finishStallLocked(stallState)
		if stalled && s.metrics != nil {
			s.metrics.readStalls.Add(1)
		}
		if n > len(p) {
			s.closeBodyLocked()
			return 0, fmt.Errorf("stream body returned %d bytes for a %d-byte read", n, len(p))
		}
		s.pos += int64(n)

		if n > 0 {
			if err != nil || stalled {
				s.closeBodyLocked()
			} else {
				s.armIdleLocked()
			}
			// Bytes are returned exactly once. A simultaneous source error is
			// handled by reopening from s.pos on the next call.
			return n, nil
		}

		s.closeBodyLocked()
		switch {
		case stalled:
			err = fmt.Errorf("stream read stalled after %s: %w", s.stallTimeout, context.DeadlineExceeded)
		case err == nil:
			err = io.ErrNoProgress
		case errors.Is(err, io.EOF):
			if s.pos >= s.size {
				return 0, io.EOF
			}
			err = io.ErrUnexpectedEOF
		}
		if recoveryErr := s.recoverLocked(err, attempt); recoveryErr != nil {
			return 0, recoveryErr
		}
	}
}

// Prime opens and validates the source at the current offset without consuming
// any bytes. It is intended for callers that must fail before committing a
// successful response or exposing a file handle.
func (s *streamSession) Prime() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return os.ErrClosed
	}
	if s.pos >= s.size || s.body != nil {
		return nil
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}

	for attempt := 0; ; attempt++ {
		if err := s.connectLocked(); err == nil {
			s.armIdleLocked()
			return nil
		} else if recoveryErr := s.recoverLocked(err, attempt); recoveryErr != nil {
			return recoveryErr
		}
	}
}

func (s *streamSession) Seek(offset int64, whence int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return 0, os.ErrClosed
	}

	base := int64(0)
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		base = s.pos
	case io.SeekEnd:
		base = s.size
	default:
		return 0, fmt.Errorf("invalid seek whence %d", whence)
	}

	next, ok := addStreamOffset(base, offset)
	if !ok {
		return 0, fmt.Errorf("stream seek overflows int64")
	}
	if next < 0 {
		return 0, fmt.Errorf("invalid seek to %d", next)
	}
	if next != s.pos {
		s.closeBodyLocked()
		s.stopIdleLocked()
		s.pos = next
	}
	return next, nil
}

func (s *streamSession) Close() error {
	// Cancellation happens before locking so a blocked body read can return.
	s.cancel()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.closeBodyLocked()
	s.stopIdleLocked()
	s.stopStallLocked()
	onClose := s.onClose
	s.onClose = nil
	s.mu.Unlock()

	if onClose != nil {
		onClose()
	}
	if s.metrics != nil {
		s.metrics.sessionsClosed.Add(1)
	}
	return nil
}

func (s *streamSession) connectLocked() error {
	if s.metrics != nil {
		s.metrics.sourceOpens.Add(1)
	}
	bodyCtx, cancel := context.WithCancel(s.ctx)
	state := &streamStallState{}
	var timer *time.Timer
	if s.stallTimeout > 0 {
		timer = time.AfterFunc(s.stallTimeout, func() {
			state.expired.Store(true)
			cancel()
		})
	}
	body, err := s.open(bodyCtx, s.pos)
	timedOut := false
	if timer != nil {
		timedOut = !timer.Stop() || state.expired.Load()
	}
	if timedOut {
		if s.metrics != nil {
			s.metrics.openStalls.Add(1)
		}
		if body != nil {
			_ = body.Close()
		}
		cancel()
		if contextErr := s.ctx.Err(); contextErr != nil {
			return contextErr
		}
		return fmt.Errorf("stream open stalled after %s: %w", s.stallTimeout, context.DeadlineExceeded)
	}
	if err != nil {
		cancel()
		return err
	}
	if body == nil {
		cancel()
		return fmt.Errorf("stream opener returned a nil body")
	}
	s.body = body
	s.bodyCancel = cancel
	return nil
}

func (s *streamSession) recoverLocked(err error, attempt int) error {
	if contextErr := s.ctx.Err(); contextErr != nil {
		return contextErr
	}
	if attempt >= s.maxResumes {
		if s.metrics != nil {
			s.metrics.recoveriesExhausted.Add(1)
		}
		return &StreamSessionExhaustedError{
			Err:      err,
			Attempts: attempt + 1,
			Offset:   s.pos,
		}
	}

	// Keep read, seek, and recovery state serialized. Close cancels s.ctx before
	// taking this lock, so it can still interrupt a backoff immediately without
	// allowing a concurrent seek to change this read's resume offset.
	started := time.Now()
	if s.metrics != nil {
		s.metrics.recoveryAttempts.Add(1)
	}
	recoveryErr := s.recover(s.ctx, err, attempt)
	if s.metrics != nil {
		if recoveryErr == nil {
			s.metrics.observeRecoveryWait(time.Since(started))
			s.metrics.recoveriesScheduled.Add(1)
		}
	}
	return recoveryErr
}

func (s *streamSession) closeBodyLocked() {
	if s.body != nil {
		_ = s.body.Close()
		s.body = nil
	}
	if s.bodyCancel != nil {
		s.bodyCancel()
		s.bodyCancel = nil
	}
}

func (s *streamSession) armIdleLocked() {
	if s.idleTimeout <= 0 || s.body == nil {
		return
	}
	if s.idle != nil {
		s.idle.Stop()
	}
	s.idleGeneration++
	generation := s.idleGeneration
	s.idle = time.AfterFunc(s.idleTimeout, func() { s.idleFired(generation) })
}

func (s *streamSession) stopIdleLocked() {
	s.idleGeneration++
	if s.idle != nil {
		s.idle.Stop()
	}
}

func (s *streamSession) idleFired(generation uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || generation != s.idleGeneration {
		return
	}
	s.idle = nil
	if s.body != nil && s.metrics != nil {
		s.metrics.idleCloses.Add(1)
	}
	s.closeBodyLocked()
}

func (s *streamSession) armStallLocked() *streamStallState {
	if s.stallTimeout <= 0 || s.bodyCancel == nil {
		return nil
	}
	if s.stall != nil {
		s.stall.Stop()
	}
	state := &streamStallState{}
	cancel := s.bodyCancel
	s.stall = time.AfterFunc(s.stallTimeout, func() {
		state.expired.Store(true)
		cancel()
	})
	return state
}

func (s *streamSession) stopStallLocked() {
	if s.stall != nil {
		s.stall.Stop()
		s.stall = nil
	}
}

func (s *streamSession) finishStallLocked(state *streamStallState) bool {
	if state == nil || s.stall == nil {
		return false
	}
	stopped := s.stall.Stop()
	s.stall = nil
	return !stopped || state.expired.Load()
}

func recoverStreamSession(ctx context.Context, err error, attempt int) error {
	return recoverStreamSessionWithDelay(ctx, err, attempt, streamSessionResumeBaseDelay)
}

func recoverStreamSessionWithDelay(ctx context.Context, err error, attempt int, baseDelay time.Duration) error {
	if err == nil {
		return nil
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}

	var streamErr StreamError
	if errors.As(err, &streamErr) {
		if !streamErr.Retryable {
			return err
		}
		return waitForStreamSessionRetry(ctx, attempt, baseDelay)
	}
	if customerror.IsPermanentError(err) {
		return err
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) &&
		!errors.Is(err, io.ErrNoProgress) &&
		!customerror.IsRetriableError(err) &&
		!isConnectionError(err) {
		return err
	}

	return waitForStreamSessionRetry(ctx, attempt, baseDelay)
}

func waitForStreamSessionRetry(ctx context.Context, attempt int, baseDelay time.Duration) error {
	delay := time.Duration(0)
	if attempt > 0 {
		delay = baseDelay << min(attempt-1, 3)
	}
	return waitForStreamRetry(ctx, delay)
}

func addStreamOffset(base, offset int64) (int64, bool) {
	if offset > 0 && base > math.MaxInt64-offset {
		return 0, false
	}
	if offset < 0 && base < math.MinInt64-offset {
		return 0, false
	}
	return base + offset, true
}

type managerStreamBody struct {
	reader io.Reader
	pipe   *io.PipeReader
	cancel context.CancelFunc
	once   sync.Once
}

func (b *managerStreamBody) Read(p []byte) (int, error) {
	return b.reader.Read(p)
}

func (b *managerStreamBody) Close() error {
	var err error
	b.once.Do(func() {
		b.cancel()
		err = b.pipe.Close()
	})
	return err
}

// OpenStream returns a tracked seekable session over one managed file.
// Connecting remains lazy; call Prime when a consumer must fail before it
// commits response headers.
func (m *Manager) OpenStream(
	ctx context.Context,
	entry *storage.Entry,
	filename string,
	offset int64,
	client string,
) (StreamReader, error) {
	streamID := m.TrackStream(entry, filename, client)
	ctx = WithActiveStreamID(ctx, streamID)
	session, err := m.openStreamUntracked(ctx, entry, filename, offset, client, StreamSessionOptions{})
	if err != nil {
		m.UntrackStream(streamID)
		return nil, err
	}
	session.onClose = func() { m.UntrackStream(streamID) }
	return session, nil
}

// OpenStreamUntracked returns a session for consumers that own aggregate
// stream accounting, such as the shared DFS downloader coordinator.
func (m *Manager) OpenStreamUntracked(
	ctx context.Context,
	entry *storage.Entry,
	filename string,
	offset int64,
) (StreamReader, error) {
	return m.openStreamUntracked(ctx, entry, filename, offset, "", StreamSessionOptions{})
}

// OpenStreamUntrackedWithOptions is OpenStreamUntracked with an explicit
// session budget for consumers such as DFS that already expose retry tuning.
func (m *Manager) OpenStreamUntrackedWithOptions(
	ctx context.Context,
	entry *storage.Entry,
	filename string,
	offset int64,
	options StreamSessionOptions,
) (StreamReader, error) {
	return m.openStreamUntracked(ctx, entry, filename, offset, "", options)
}

func (m *Manager) openStreamUntracked(
	ctx context.Context,
	entry *storage.Entry,
	filename string,
	offset int64,
	client string,
	options StreamSessionOptions,
) (*streamSession, error) {
	if m == nil {
		return nil, fmt.Errorf("manager is nil")
	}
	if entry == nil {
		return nil, fmt.Errorf("stream entry is nil")
	}
	file, ok := entry.Files[filename]
	if !ok || file == nil {
		return nil, fmt.Errorf("file %s not found in entry %s", filename, entry.Name)
	}
	if file.Size <= 0 {
		return nil, fmt.Errorf("file %s has invalid size %d", filename, file.Size)
	}

	session, err := newStreamSessionWithOptions(ctx, file.Size, offset, func(
		bodyCtx context.Context,
		start int64,
	) (io.ReadCloser, error) {
		return m.openManagerStreamBody(bodyCtx, entry, filename, start, file.Size, client)
	}, nil, options)
	if err != nil {
		return nil, err
	}
	session.metrics = &m.streamSessionMetrics
	m.streamSessionMetrics.sessionsOpened.Add(1)
	return session, nil
}

func (m *Manager) openManagerStreamBody(
	ctx context.Context,
	entry *storage.Entry,
	filename string,
	start int64,
	size int64,
	client string,
) (io.ReadCloser, error) {
	bodyCtx, cancel := context.WithCancel(ctx)
	reader, writer := io.Pipe()
	go func() {
		err := m.Stream(
			bodyCtx,
			entry,
			filename,
			start,
			size-1,
			writer,
			nil,
			client,
		)
		_ = writer.CloseWithError(err)
	}()

	var first [1]byte
	n, err := io.ReadFull(reader, first[:])
	if err != nil || n != 1 {
		cancel()
		_ = reader.Close()
		if err == nil || errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return nil, fmt.Errorf("stream body failed before first byte: %w", err)
	}

	return &managerStreamBody{
		reader: io.MultiReader(bytes.NewReader(first[:]), reader),
		pipe:   reader,
		cancel: cancel,
	}, nil
}
