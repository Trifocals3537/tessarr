package usenet

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/puzpuzpuz/xsync/v4"
)

const failedFileInitialBackoff = 2 * time.Minute

type failedFileState struct {
	mu            sync.Mutex
	cause         error
	failedOffset  int64
	failures      int
	revision      uint64
	retryAfter    time.Time
	probeInFlight bool
}

func failedFileBackoff(failures int) time.Duration {
	switch failures {
	case 0, 1:
		return failedFileInitialBackoff
	case 2:
		return 5 * time.Minute
	case 3:
		return 15 * time.Minute
	default:
		return 30 * time.Minute
	}
}

func (s *failedFileState) streamError() error {
	s.mu.Lock()
	cause := s.cause
	s.mu.Unlock()
	return customerror.NewArticleNotFoundError(cause)
}

func (u *Usenet) failureClock() time.Time {
	if u != nil && u.failureNow != nil {
		return u.failureNow()
	}
	return time.Now()
}

func (u *Usenet) recordFailedFile(key string, failedOffset int64, cause error) {
	if u == nil || u.failedFiles == nil || key == "" || cause == nil {
		return
	}
	if failedOffset < 0 {
		failedOffset = 0
	}
	now := u.failureClock()
	state, loaded := u.failedFiles.LoadOrStore(key, &failedFileState{})
	state.mu.Lock()
	if !loaded || state.failures < 1 {
		state.failures = 1
	}
	state.cause = cause
	state.failedOffset = failedOffset
	state.revision++
	state.retryAfter = now.Add(failedFileBackoff(state.failures))
	state.probeInFlight = false
	state.mu.Unlock()
}

func failedReadOffset(start, end, written int64) int64 {
	offset := start + written
	if offset < start {
		return start
	}
	if offset > end {
		return end
	}
	return offset
}

// EnsureStreamReady fences a known-bad file before response headers are sent,
// but periodically permits one caller to revalidate the exact byte that failed.
// This makes article failures recoverable without letting a probe storm consume
// all NNTP connections or requiring a service restart to clear process memory.
func (u *Usenet) EnsureStreamReady(ctx context.Context, nzoID, filename string) error {
	if u == nil || u.failedFiles == nil {
		return nil
	}
	key := fsKey(nzoID, filename)
	state, ok := u.failedFiles.Load(key)
	if !ok {
		return nil
	}

	now := u.failureClock()
	state.mu.Lock()
	if state.probeInFlight || now.Before(state.retryAfter) {
		err := customerror.NewArticleNotFoundError(state.cause)
		state.mu.Unlock()
		return err
	}
	state.probeInFlight = true
	offset := state.failedOffset
	revision := state.revision
	state.mu.Unlock()

	probe := u.failureProbe
	if probe == nil {
		probe = func(probeCtx context.Context, probeNZO, probeFile string, probeOffset int64) error {
			return u.streamInternal(probeCtx, probeNZO, probeFile, probeOffset, probeOffset, io.Discard, true, false)
		}
	}
	err := probe(ctx, nzoID, filename, offset)
	if err == nil {
		current, stillFailed := u.failedFiles.Compute(key, func(current *failedFileState, loaded bool) (*failedFileState, xsync.ComputeOp) {
			if !loaded || current != state {
				return current, xsync.CancelOp
			}
			current.mu.Lock()
			unchanged := current.revision == revision
			current.mu.Unlock()
			if unchanged {
				return current, xsync.DeleteOp
			}
			return current, xsync.CancelOp
		})
		if stillFailed {
			return current.streamError()
		}
		return nil
	}

	state.mu.Lock()
	state.probeInFlight = false
	if nntp.IsArticleNotFoundError(err) {
		state.cause = err
		state.failures++
		state.revision++
		state.retryAfter = u.failureClock().Add(failedFileBackoff(state.failures))
	} else {
		// Transient probe failures retain the known permanent cause. Retry later
		// instead of reopening the file or escalating the permanent backoff tier.
		state.retryAfter = u.failureClock().Add(failedFileInitialBackoff)
	}
	state.mu.Unlock()
	return err
}
