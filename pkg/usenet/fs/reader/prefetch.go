package reader

import (
	"context"
	"sync"
	"sync/atomic"
)

const defaultPrefetchQueueDepth = 256

// prefetchSession identifies one consumer of a shared StreamingReader. Each
// HTTP/WebDAV stream receives its own session, so canceling or seeking one
// consumer never discards another consumer's read-ahead hints.
type prefetchSession struct {
	lastEndSeg atomic.Int64
}

func newPrefetchSession() *prefetchSession {
	s := &prefetchSession{}
	s.lastEndSeg.Store(-1)
	return s
}

type prefetchSessionContextKey struct{}

// WithPrefetchSession attaches an independent prefetch identity to ctx. It is
// idempotent so wrappers can safely call it without splitting an existing
// consumer into multiple scheduler lanes.
func WithPrefetchSession(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if prefetchSessionFromContext(ctx) != nil {
		return ctx
	}
	return context.WithValue(ctx, prefetchSessionContextKey{}, newPrefetchSession())
}

func prefetchSessionFromContext(ctx context.Context) *prefetchSession {
	if ctx == nil {
		return nil
	}
	session, _ := ctx.Value(prefetchSessionContextKey{}).(*prefetchSession)
	return session
}

// observeRead reports whether this consumer jumped far enough to abandon its
// previous read-ahead window. State is per session rather than per shared
// reader, which is the key distinction that keeps concurrent viewers safe.
func (s *prefetchSession) observeRead(startSeg, endSeg, ahead int) bool {
	if s == nil {
		return false
	}
	prevEnd := s.lastEndSeg.Swap(int64(endSeg))
	if prevEnd < 0 || ahead <= 0 {
		return false
	}
	return int64(startSeg) > prevEnd+int64(ahead) || int64(endSeg) < prevEnd-int64(ahead)
}

type prefetchTask struct {
	session *prefetchSession
	segment int
}

type prefetchLane struct {
	session     *prefetchSession
	pending     []int
	queued      map[int]struct{} // pending and in-flight tasks for this session
	outstanding int
	stopCancel  func() bool
}

// prefetchScheduler keeps one bounded lane per active consumer and dispatches
// one segment from each lane in turn. Separate lanes intentionally may contain
// the same segment so canceling one consumer cannot erase another's interest;
// once one copy becomes active, the scheduler coalesces the others onto that
// shared fetch.
type prefetchScheduler struct {
	mu             sync.Mutex
	cond           *sync.Cond
	lanes          map[*prefetchSession]*prefetchLane
	order          []*prefetchLane
	active         map[int]struct{} // segments already assigned to a worker
	next           int
	pending        int
	maxPending     int
	closed         bool
	defaultSession *prefetchSession
	stats          *ReaderStats
}

func newPrefetchScheduler(maxPending int, stats *ReaderStats) *prefetchScheduler {
	if maxPending < 1 {
		maxPending = defaultPrefetchQueueDepth
	}
	s := &prefetchScheduler{
		lanes:          make(map[*prefetchSession]*prefetchLane),
		active:         make(map[int]struct{}),
		maxPending:     maxPending,
		defaultSession: newPrefetchSession(),
		stats:          stats,
	}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// addRange adds eligible segments without blocking the caller. When the
// global queue is full, farthest-ahead hints are trimmed from the largest lane
// until the new consumer has a fair share. Required reads do not use this
// queue, so dropping a hint can affect only read-ahead, never correctness.
func (s *prefetchScheduler) addRange(
	ctx context.Context,
	session *prefetchSession,
	startSeg, endSeg int,
	eligible func(int) bool,
) {
	if startSeg > endSeg || (ctx != nil && ctx.Err() != nil) {
		return
	}
	if session == nil {
		session = s.defaultSession
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}

	lane := s.lanes[session]
	if lane == nil {
		lane = &prefetchLane{
			session: session,
			queued:  make(map[int]struct{}),
		}
		s.lanes[session] = lane
		s.order = append(s.order, lane)
		if session != s.defaultSession && ctx != nil && ctx.Done() != nil {
			lane.stopCancel = context.AfterFunc(ctx, func() {
				s.dropPending(session)
			})
		}
	}

	added := 0
	for segIdx := startSeg; segIdx <= endSeg; segIdx++ {
		if eligible != nil && !eligible(segIdx) {
			continue
		}
		if _, exists := lane.queued[segIdx]; exists {
			continue
		}
		if !s.makeRoomLocked(lane) {
			if s.stats != nil {
				s.stats.PrefetchMisses.Add(1)
			}
			continue
		}
		lane.pending = append(lane.pending, segIdx)
		lane.queued[segIdx] = struct{}{}
		s.pending++
		added++
	}
	if added > 0 {
		s.cond.Broadcast()
	} else if lane.outstanding == 0 && len(lane.pending) == 0 {
		s.removeLaneLocked(lane)
	}
}

// makeRoomLocked equalizes pending work across active lanes. It removes only
// the tail of an overrepresented lane, preserving the segments closest to that
// consumer's current read position.
func (s *prefetchScheduler) makeRoomLocked(target *prefetchLane) bool {
	if s.pending < s.maxPending {
		return true
	}

	var donor *prefetchLane
	for _, lane := range s.order {
		if len(lane.pending) == 0 {
			continue
		}
		if donor == nil || len(lane.pending) > len(donor.pending) {
			donor = lane
		}
	}
	if donor == nil || len(donor.pending) <= len(target.pending) {
		return false
	}

	last := len(donor.pending) - 1
	segIdx := donor.pending[last]
	donor.pending = donor.pending[:last]
	delete(donor.queued, segIdx)
	s.pending--
	if s.stats != nil {
		s.stats.PrefetchRebalanced.Add(1)
	}
	if donor.outstanding == 0 && len(donor.pending) == 0 {
		s.removeLaneLocked(donor)
	}
	return true
}

// next blocks until a worker can claim the next round-robin task or the
// scheduler closes.
func (s *prefetchScheduler) nextTask() (prefetchTask, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		if s.closed {
			return prefetchTask{}, false
		}
		if task, ok := s.nextTaskLocked(); ok {
			return task, true
		}
		s.cond.Wait()
	}
}

func (s *prefetchScheduler) nextTaskLocked() (prefetchTask, bool) {
	for s.pending > 0 && len(s.order) > 0 {
		if s.next >= len(s.order) {
			s.next = 0
		}
		lane := s.order[s.next]
		s.next = (s.next + 1) % len(s.order)
		if len(lane.pending) == 0 {
			continue
		}

		segIdx := lane.pending[0]
		lane.pending = lane.pending[1:]
		s.pending--
		if _, alreadyActive := s.active[segIdx]; alreadyActive {
			// Another lane expressed the same interest after this segment was
			// assigned. The shared fetch already in progress satisfies both;
			// do not occupy a second worker waiting on the same promise.
			delete(lane.queued, segIdx)
			if s.stats != nil {
				s.stats.PrefetchCoalesced.Add(1)
			}
			if lane.outstanding == 0 && len(lane.pending) == 0 {
				s.removeLaneLocked(lane)
			}
			continue
		}
		s.active[segIdx] = struct{}{}
		lane.outstanding++
		return prefetchTask{session: lane.session, segment: segIdx}, true
	}
	return prefetchTask{}, false
}

func (s *prefetchScheduler) complete(task prefetchTask) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.active, task.segment)
	lane := s.lanes[task.session]
	if lane == nil {
		return
	}
	delete(lane.queued, task.segment)
	if lane.outstanding > 0 {
		lane.outstanding--
	}
	if lane.outstanding == 0 && len(lane.pending) == 0 {
		s.removeLaneLocked(lane)
	}
}

// dropPending removes only one consumer's queued hints. In-flight downloads
// finish and remain useful to any overlapping consumer.
func (s *prefetchScheduler) dropPending(session *prefetchSession) {
	if session == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	lane := s.lanes[session]
	if lane == nil {
		return
	}
	dropped := len(lane.pending)
	for _, segIdx := range lane.pending {
		delete(lane.queued, segIdx)
	}
	lane.pending = nil
	s.pending -= dropped
	if dropped > 0 && s.stats != nil {
		s.stats.PrefetchCancelled.Add(int64(dropped))
	}
	if lane.outstanding == 0 {
		s.removeLaneLocked(lane)
	}
}

func (s *prefetchScheduler) removeLaneLocked(lane *prefetchLane) {
	delete(s.lanes, lane.session)
	for i, candidate := range s.order {
		if candidate != lane {
			continue
		}
		s.order = append(s.order[:i], s.order[i+1:]...)
		if i < s.next {
			s.next--
		}
		if len(s.order) == 0 || s.next >= len(s.order) {
			s.next = 0
		}
		break
	}
	if lane.stopCancel != nil {
		lane.stopCancel()
		lane.stopCancel = nil
	}
}

func (s *prefetchScheduler) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	for _, lane := range s.order {
		if lane.stopCancel != nil {
			lane.stopCancel()
			lane.stopCancel = nil
		}
	}
	s.lanes = make(map[*prefetchSession]*prefetchLane)
	s.order = nil
	s.active = make(map[int]struct{})
	s.pending = 0
	s.cond.Broadcast()
	s.mu.Unlock()
}
