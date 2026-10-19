package manager

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/Trifocals3537/tessarr/pkg/usenet/parser"
	"github.com/rs/zerolog"
)

// JobType represents the type of processing job
type JobType string

const (
	JobTypeTorrent JobType = "torrent"
	JobTypeNZB     JobType = "nzb"

	jobQueueCompactThreshold = 1024

	// DefaultJobQueueCapacity is deliberately large enough for normal restore
	// and batch-import workloads, while preventing an unavailable provider from
	// retaining an unbounded amount of request metadata in memory.
	DefaultJobQueueCapacity = config.DefaultJobQueueCapacity
	// MaxJobQueueCapacity is the largest accepted configured capacity.
	MaxJobQueueCapacity = config.MaxJobQueueCapacity
)

var (
	ErrJobQueueFull      = errors.New("job queue is full")
	ErrJobQueueDuplicate = errors.New("job queue key is already admitted")
	ErrJobQueueClosed    = errors.New("job queue is closed")
)

// JobQueueFullError reports the configured admission limit.
type JobQueueFullError struct {
	Capacity int
}

func (e *JobQueueFullError) Error() string {
	return fmt.Sprintf("%s (capacity %d)", ErrJobQueueFull, e.Capacity)
}

func (e *JobQueueFullError) Unwrap() error {
	return ErrJobQueueFull
}

// DuplicateJobError identifies the normalized key that is already reserved,
// pending, active, or waiting for a delayed retry.
type DuplicateJobError struct {
	Key string
}

func (e *DuplicateJobError) Error() string {
	return fmt.Sprintf("%s: %s", ErrJobQueueDuplicate, e.Key)
}

func (e *DuplicateJobError) Unwrap() error {
	return ErrJobQueueDuplicate
}

// Job represents a unified processing job for both torrents and NZBs
type Job struct {
	ID             string
	Type           JobType
	Generation     uint64                       // In-process queue lifecycle generation
	Request        *ImportRequest               // The original import request
	DebridTorrent  *debridTypes.Torrent         // Torrent placement created before the active-download gate
	NZBMeta        *storage.NZB                 // NZB metadata parsed before the active-download gate
	NZBGroups      map[string]*parser.FileGroup // NZB file groups parsed before the active-download gate
	Entry          *storage.Entry               // Entry created during processing
	ResumeExisting bool                         // Continue an already persisted provider placement
	DeleteOnFinish bool                         // Run queue deletion after this job's lifecycle lease closes
	CreatedAt      time.Time
}

// NewJob creates a new job
func NewJob(jobType JobType, req *ImportRequest) *Job {
	id := ""
	if req != nil {
		id = req.Id
	}
	return &Job{
		ID:        id,
		Type:      jobType,
		Request:   req,
		CreatedAt: time.Now(),
	}
}

type jobAdmissionState uint8

const (
	jobReserved jobAdmissionState = iota + 1
	jobPending
	jobActive
	jobDelayed
	jobActiveWithRetry
)

type admittedJob struct {
	job          *Job
	state        jobAdmissionState
	reservation  uint64
	delayedRetry *retryItem
}

type retryItem struct {
	job      *Job
	key      string
	readyAt  time.Time
	sequence uint64
	index    int
}

type retryHeap []*retryItem

func (h retryHeap) Len() int { return len(h) }
func (h retryHeap) Less(i, j int) bool {
	if h[i].readyAt.Equal(h[j].readyAt) {
		return h[i].sequence < h[j].sequence
	}
	return h[i].readyAt.Before(h[j].readyAt)
}
func (h retryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *retryHeap) Push(value any) {
	item := value.(*retryItem)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *retryHeap) Pop() any {
	old := *h
	last := len(old) - 1
	item := old[last]
	old[last] = nil
	item.index = -1
	*h = old[:last]
	return item
}

// jobReservation holds one capacity slot while an admission caller performs
// the provider or filesystem transaction that precedes queue submission.
type jobReservation struct {
	queue *JobQueue
	key   string
	token uint64
	lease *entryWorkLease
	ctx   context.Context
	once  sync.Once
}

func (r *jobReservation) release() {
	if r == nil || r.queue == nil {
		return
	}
	r.once.Do(func() {
		r.queue.releaseReservation(r)
		if r.lease != nil {
			r.lease.Close()
		}
	})
}

func (r *jobReservation) Context() context.Context {
	if r != nil && r.lease != nil {
		return r.lease.Context()
	}
	if r != nil && r.ctx != nil {
		return r.ctx
	}
	return context.Background()
}

// JobQueue is a unified, bounded, thread-safe job queue with a fixed worker pool.
// It replaces the separate ImportRequest queue, nzbJobQueue, and unbounded goroutine
// fan-out with a single queue that processes both torrent and NZB jobs.
type JobQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	jobs   []*Job
	head   int
	closed bool

	maxWorkers int
	capacity   int
	logger     zerolog.Logger
	wg         sync.WaitGroup
	active     atomic.Int64
	admitted   map[string]*admittedJob
	retries    retryHeap
	retryWake  chan struct{}
	spaceWake  chan struct{}
	nextToken  uint64
	nextRetry  uint64
	closeOnce  sync.Once

	// processFunc is called by workers to process a job
	processFunc func(ctx context.Context, job *Job)
	afterFunc   func(job *Job)
	ctx         context.Context
	cancel      context.CancelFunc
	lifecycle   *entryLifecycle
}

// NewJobQueue creates a new unified job queue with the given number of workers
func NewJobQueue(ctx context.Context, maxWorkers int, processFunc func(ctx context.Context, job *Job), lifecycles ...*entryLifecycle) *JobQueue {
	return NewJobQueueWithCapacity(
		ctx,
		maxWorkers,
		DefaultJobQueueCapacity,
		processFunc,
		lifecycles...,
	)
}

// NewJobQueueWithCapacity creates a queue whose capacity includes reservations,
// pending jobs, active jobs, and delayed retries.
func NewJobQueueWithCapacity(
	ctx context.Context,
	maxWorkers int,
	capacity int,
	processFunc func(ctx context.Context, job *Job),
	lifecycles ...*entryLifecycle,
) *JobQueue {
	if maxWorkers <= 0 {
		maxWorkers = 5
	}
	capacity = normalizeJobQueueCapacity(capacity)
	var lifecycle *entryLifecycle
	if len(lifecycles) > 0 {
		lifecycle = lifecycles[0]
	}

	ctx, cancel := context.WithCancel(ctx)
	q := &JobQueue{
		jobs:        make([]*Job, 0, 64),
		maxWorkers:  maxWorkers,
		capacity:    capacity,
		logger:      logger.New("jobqueue"),
		processFunc: processFunc,
		ctx:         ctx,
		cancel:      cancel,
		lifecycle:   lifecycle,
		admitted:    make(map[string]*admittedJob, min(capacity, 64)),
		retryWake:   make(chan struct{}, 1),
		spaceWake:   make(chan struct{}, 1),
	}
	q.cond = sync.NewCond(&q.mu)
	heap.Init(&q.retries)

	// Start worker goroutines
	for i := 0; i < maxWorkers; i++ {
		q.wg.Add(1)
		go q.worker(i)
	}
	q.wg.Add(1)
	go q.retryScheduler()

	q.logger.Info().
		Int("workers", maxWorkers).
		Int("capacity", capacity).
		Msg("Job queue started")
	return q
}

// Submit adds a job to the queue (never blocks)
func (q *JobQueue) Submit(job *Job) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return ErrJobQueueClosed
	}
	key := normalizeQueueEntryKey(job.ID)
	if key == "" {
		return fmt.Errorf("job ID is empty")
	}
	if _, exists := q.admitted[key]; exists {
		return &DuplicateJobError{Key: key}
	}
	if len(q.admitted) >= q.capacity {
		return &JobQueueFullError{Capacity: q.capacity}
	}
	if err := q.validateJobLocked(job); err != nil {
		return err
	}

	q.admitted[key] = &admittedJob{job: job, state: jobPending}
	q.jobs = append(q.jobs, job)
	q.cond.Signal() // Wake one waiting worker
	q.logger.Debug().
		Str("id", job.ID).
		Str("type", string(job.Type)).
		Int("queued", q.pendingLocked()).
		Msg("Job submitted")
	return nil
}

// submitWait is reserved for bounded internal restore work. External
// admission remains non-blocking so HTTP callers receive immediate overload
// feedback instead of tying up request goroutines.
func (q *JobQueue) submitWait(ctx context.Context, job *Job) error {
	for {
		err := q.Submit(job)
		if !errors.Is(err, ErrJobQueueFull) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.ctx.Done():
			return ErrJobQueueClosed
		case <-q.spaceWake:
		}
	}
}

func normalizeJobQueueCapacity(capacity int) int {
	switch {
	case capacity <= 0:
		return DefaultJobQueueCapacity
	case capacity > MaxJobQueueCapacity:
		return MaxJobQueueCapacity
	default:
		return capacity
	}
}

func (q *JobQueue) validateJobLocked(job *Job) error {
	if q.lifecycle == nil {
		return nil
	}
	generation := job.Generation
	if generation == 0 && job.Entry != nil {
		generation = job.Entry.QueueGeneration
	}
	validated, err := q.lifecycle.validateSubmission(job.ID, generation)
	if err != nil {
		return err
	}
	job.Generation = validated
	if job.Entry != nil && job.Entry.QueueGeneration == 0 {
		job.Entry.QueueGeneration = validated
	}
	return nil
}

func (q *JobQueue) reserve(jobID string) (*jobReservation, error) {
	return q.reserveContext(q.ctx, jobID)
}

func (q *JobQueue) reserveContext(
	ctx context.Context,
	jobID string,
) (*jobReservation, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil, ErrJobQueueClosed
	}
	key := normalizeQueueEntryKey(jobID)
	if key == "" {
		return nil, fmt.Errorf("job ID is empty")
	}
	if _, exists := q.admitted[key]; exists {
		return nil, &DuplicateJobError{Key: key}
	}
	if len(q.admitted) >= q.capacity {
		return nil, &JobQueueFullError{Capacity: q.capacity}
	}
	var generation uint64
	if q.lifecycle != nil {
		validated, err := q.lifecycle.validateSubmission(jobID, 0)
		if err != nil {
			return nil, err
		}
		generation = validated
	}
	q.nextToken++
	if q.nextToken == 0 {
		q.nextToken++
	}
	if ctx == nil {
		ctx = q.ctx
	}
	reservation := &jobReservation{
		queue: q,
		key:   key,
		token: q.nextToken,
		ctx:   ctx,
	}
	if q.lifecycle != nil {
		lease, err := q.lifecycle.startWork(ctx, jobID, generation)
		if err != nil {
			return nil, err
		}
		reservation.lease = lease
	}
	q.admitted[key] = &admittedJob{
		state:       jobReserved,
		reservation: reservation.token,
	}
	return reservation, nil
}

func (q *JobQueue) releaseReservation(reservation *jobReservation) {
	q.mu.Lock()
	defer q.mu.Unlock()

	record := q.admitted[reservation.key]
	if record != nil &&
		record.state == jobReserved &&
		record.reservation == reservation.token {
		delete(q.admitted, reservation.key)
		q.wakeCapacityWaiter()
	}
}

func (q *JobQueue) submitReserved(reservation *jobReservation, job *Job) error {
	if reservation == nil || reservation.queue != q {
		return fmt.Errorf("invalid job queue reservation")
	}
	if job == nil {
		return fmt.Errorf("job is nil")
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	record := q.admitted[reservation.key]
	if record == nil ||
		record.state != jobReserved ||
		record.reservation != reservation.token {
		return fmt.Errorf("job queue reservation is no longer valid")
	}
	if q.closed {
		delete(q.admitted, reservation.key)
		q.wakeCapacityWaiter()
		return ErrJobQueueClosed
	}
	if normalizeQueueEntryKey(job.ID) != reservation.key {
		delete(q.admitted, reservation.key)
		q.wakeCapacityWaiter()
		return fmt.Errorf("job key does not match its reservation")
	}
	if err := q.validateJobLocked(job); err != nil {
		delete(q.admitted, reservation.key)
		q.wakeCapacityWaiter()
		return err
	}

	record.job = job
	record.state = jobPending
	record.reservation = 0
	q.jobs = append(q.jobs, job)
	q.cond.Signal()
	return nil
}

// Len returns the current number of pending jobs
func (q *JobQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pendingLocked()
}

// ActiveCount returns the number of jobs currently holding an active-download slot.
func (q *JobQueue) ActiveCount() int {
	return int(q.active.Load())
}

// OutstandingCount returns the capacity-consuming union of reserved, pending,
// active, and delayed jobs.
func (q *JobQueue) OutstandingCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.admitted)
}

// Capacity returns the queue's normalized hard admission limit.
func (q *JobQueue) Capacity() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.capacity
}

// Retry schedules a job again without holding an active worker slot. A retry
// requested by the currently active invocation atomically transitions that
// admission to the delayed state; it does not consume another capacity slot.
func (q *JobQueue) Retry(job *Job, delay time.Duration) error {
	if job == nil {
		return fmt.Errorf("job is nil")
	}
	if delay < 0 {
		delay = 0
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrJobQueueClosed
	}

	key := normalizeQueueEntryKey(job.ID)
	if key == "" {
		return fmt.Errorf("job ID is empty")
	}
	record := q.admitted[key]
	state := jobDelayed
	switch {
	case record == nil:
		if len(q.admitted) >= q.capacity {
			return &JobQueueFullError{Capacity: q.capacity}
		}
		if err := q.validateJobLocked(job); err != nil {
			return err
		}
		record = &admittedJob{job: job}
		q.admitted[key] = record
	case record.job == job && record.state == jobActive:
		state = jobActiveWithRetry
	default:
		return &DuplicateJobError{Key: key}
	}

	q.nextRetry++
	item := &retryItem{
		job:      job,
		key:      key,
		readyAt:  time.Now().Add(delay),
		sequence: q.nextRetry,
	}
	record.state = state
	record.delayedRetry = item
	heap.Push(&q.retries, item)
	q.wakeRetryScheduler()
	return nil
}

// Close signals all workers to stop and waits for them to finish
func (q *JobQueue) Close() {
	q.closeOnce.Do(func() {
		q.mu.Lock()
		q.closed = true
		clear(q.jobs)
		q.jobs = q.jobs[:0]
		q.head = 0
		clear(q.admitted)
		for len(q.retries) > 0 {
			heap.Pop(&q.retries)
		}
		q.mu.Unlock()
		if q.cancel != nil {
			q.cancel()
		}
		q.cond.Broadcast() // Wake all waiting workers
		q.wakeRetryScheduler()
		q.wakeCapacityWaiter()
		q.wg.Wait()
		q.logger.Info().Msg("Job queue stopped")
	})
}

// worker is the main loop for a single worker goroutine
func (q *JobQueue) worker(id int) {
	defer q.wg.Done()

	for {
		job := q.pop()
		if job == nil {
			q.logger.Debug().Int("worker_id", id).Msg("Worker exiting")
			return
		}

		q.logger.Debug().
			Int("worker_id", id).
			Str("job_id", job.ID).
			Str("type", string(job.Type)).
			Int("queued", q.Len()).
			Msg("Processing job")

		q.active.Add(1)
		q.runJob(job)
		q.active.Add(-1)
		if job.DeleteOnFinish && q.afterFunc != nil {
			q.runAfter(job)
		}
		q.finishJob(job)
	}
}

// runAfter performs durable post-processing before releasing the admission
// record so a duplicate cannot be admitted while persisted cleanup is pending.
func (q *JobQueue) runAfter(job *Job) {
	defer func() {
		if r := recover(); r != nil {
			q.logger.Error().
				Str("job_id", job.ID).
				Str("type", string(job.Type)).
				Interface("panic", r).
				Bytes("stack", debug.Stack()).
				Msg("Recovered from panic while finishing job")
		}
	}()
	q.afterFunc(job)
}

// runJob executes a single job, recovering from panics so that one bad job
// cannot permanently kill a worker goroutine. With a fixed worker pool, an
// unrecovered panic per worker silently drained the pool to zero — leaving
// every queued download stuck at 0% while the healthcheck still passed.
func (q *JobQueue) runJob(job *Job) {
	defer func() {
		if r := recover(); r != nil {
			q.logger.Error().
				Str("job_id", job.ID).
				Str("type", string(job.Type)).
				Interface("panic", r).
				Bytes("stack", debug.Stack()).
				Msg("Recovered from panic while processing job")
		}
	}()
	if q.lifecycle == nil {
		q.processFunc(q.ctx, job)
		return
	}

	work, err := q.lifecycle.startWork(q.ctx, job.ID, job.Generation)
	if err != nil {
		q.logger.Debug().
			Err(err).
			Str("job_id", job.ID).
			Uint64("generation", job.Generation).
			Msg("Skipping stale or deleting job")
		return
	}
	defer work.Close()
	q.processFunc(work.Context(), job)
}

func (q *JobQueue) finishJob(job *Job) {
	q.mu.Lock()
	defer q.mu.Unlock()

	key := normalizeQueueEntryKey(job.ID)
	record := q.admitted[key]
	if record == nil || record.job != job {
		return
	}
	switch record.state {
	case jobActive:
		delete(q.admitted, key)
		q.wakeCapacityWaiter()
	case jobActiveWithRetry:
		record.state = jobDelayed
		q.wakeRetryScheduler()
	}
}

// pop removes and returns the next job, blocking if queue is empty.
// Returns nil if the queue is closed and empty.
func (q *JobQueue) pop() *Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.pendingLocked() == 0 && !q.closed {
		q.cond.Wait()
	}

	if q.closed {
		return nil
	}

	job := q.jobs[q.head]
	q.jobs[q.head] = nil
	q.head++
	q.compactLocked()
	key := normalizeQueueEntryKey(job.ID)
	if record := q.admitted[key]; record != nil &&
		record.job == job &&
		record.state == jobPending {
		record.state = jobActive
	}
	return job
}

// DeleteJob removes a pending job by ID (before it's picked up by a worker).
// Returns true if the job was found and removed.
func (q *JobQueue) DeleteJob(jobID string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	for i := q.head; i < len(q.jobs); i++ {
		job := q.jobs[i]
		if job.ID == jobID {
			copy(q.jobs[i:], q.jobs[i+1:])
			q.jobs[len(q.jobs)-1] = nil
			q.jobs = q.jobs[:len(q.jobs)-1]
			q.compactLocked()
			key := normalizeQueueEntryKey(job.ID)
			if record := q.admitted[key]; record != nil &&
				record.job == job &&
				record.state == jobPending {
				delete(q.admitted, key)
				q.wakeCapacityWaiter()
			}
			return true
		}
	}
	key := normalizeQueueEntryKey(jobID)
	return q.removeDelayedLocked(key, jobID)
}

// DeleteJobs removes every pending incarnation of a queue key. An active job
// is canceled and awaited through entryLifecycle; jobs that have been popped
// but have not registered yet observe the deletion tombstone and cannot start.
func (q *JobQueue) DeleteJobs(jobID string) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	target := normalizeQueueEntryKey(jobID)
	removed := 0
	write := q.head
	for read := q.head; read < len(q.jobs); read++ {
		job := q.jobs[read]
		if job != nil && normalizeQueueEntryKey(job.ID) == target {
			if record := q.admitted[target]; record != nil &&
				record.job == job &&
				record.state == jobPending {
				delete(q.admitted, target)
				q.wakeCapacityWaiter()
			}
			q.jobs[read] = nil
			removed++
			continue
		}
		q.jobs[write] = job
		if write != read {
			q.jobs[read] = nil
		}
		write++
	}
	q.jobs = q.jobs[:write]
	q.compactLocked()
	if q.removeDelayedLocked(target, "") {
		removed++
	}
	return removed
}

func (q *JobQueue) removeDelayedLocked(key, exactID string) bool {
	record := q.admitted[key]
	if record == nil || record.delayedRetry == nil {
		return false
	}
	if exactID != "" && record.job != nil && record.job.ID != exactID {
		return false
	}
	heap.Remove(&q.retries, record.delayedRetry.index)
	record.delayedRetry = nil
	switch record.state {
	case jobDelayed:
		delete(q.admitted, key)
		q.wakeCapacityWaiter()
	case jobActiveWithRetry:
		record.state = jobActive
	default:
		return false
	}
	q.wakeRetryScheduler()
	return true
}

// FindJob returns a pending job by ID without removing it
func (q *JobQueue) FindJob(jobID string) *Job {
	q.mu.Lock()
	defer q.mu.Unlock()

	for _, job := range q.jobs[q.head:] {
		if job.ID == jobID {
			return job
		}
	}
	return nil
}

// PendingCount returns the count of pending jobs, optionally filtered by type
func (q *JobQueue) PendingCount(jobType JobType) int {
	q.mu.Lock()
	defer q.mu.Unlock()

	if jobType == "" {
		return q.pendingLocked()
	}

	count := 0
	for _, job := range q.jobs[q.head:] {
		if job.Type == jobType {
			count++
		}
	}
	return count
}

func (q *JobQueue) pendingLocked() int {
	return len(q.jobs) - q.head
}

// compactLocked clears consumed references immediately and periodically moves
// pending jobs back to the start of the reusable slice. This prevents a
// long-running queue from retaining completed jobs or allocating a new backing
// array for every enqueue/drain burst.
func (q *JobQueue) compactLocked() {
	if q.head == len(q.jobs) {
		q.jobs = q.jobs[:0]
		q.head = 0
		return
	}
	if q.head < jobQueueCompactThreshold || q.head*2 < len(q.jobs) {
		return
	}

	pending := copy(q.jobs, q.jobs[q.head:])
	clear(q.jobs[pending:])
	q.jobs = q.jobs[:pending]
	q.head = 0
}

func (q *JobQueue) retryScheduler() {
	defer q.wg.Done()

	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		q.mu.Lock()
		if q.closed {
			q.mu.Unlock()
			return
		}
		if len(q.retries) == 0 {
			q.mu.Unlock()
			select {
			case <-q.ctx.Done():
				return
			case <-q.retryWake:
				continue
			}
		}

		item := q.retries[0]
		record := q.admitted[item.key]
		if record == nil ||
			record.delayedRetry != item ||
			(record.state != jobDelayed && record.state != jobActiveWithRetry) {
			heap.Pop(&q.retries)
			q.mu.Unlock()
			continue
		}

		wait := time.Until(item.readyAt)
		if wait > 0 {
			q.mu.Unlock()
			if timer == nil {
				timer = time.NewTimer(wait)
			} else {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(wait)
			}
			select {
			case <-q.ctx.Done():
				return
			case <-q.retryWake:
				continue
			case <-timer.C:
				continue
			}
		}

		// A zero-delay retry requested by an active worker cannot run in
		// parallel with that same invocation. finishJob wakes us once it has
		// released the active slot.
		if record.state == jobActiveWithRetry {
			q.mu.Unlock()
			select {
			case <-q.ctx.Done():
				return
			case <-q.retryWake:
				continue
			}
		}

		heap.Pop(&q.retries)
		record.delayedRetry = nil
		if err := q.validateJobLocked(item.job); err != nil {
			delete(q.admitted, item.key)
			q.wakeCapacityWaiter()
			q.mu.Unlock()
			q.logger.Debug().
				Err(err).
				Str("job_id", item.job.ID).
				Msg("Discarded stale delayed retry")
			continue
		}
		record.state = jobPending
		q.jobs = append(q.jobs, item.job)
		q.cond.Signal()
		q.mu.Unlock()
	}
}

func (q *JobQueue) wakeRetryScheduler() {
	if q.retryWake == nil {
		return
	}
	select {
	case q.retryWake <- struct{}{}:
	default:
	}
}

func (q *JobQueue) wakeCapacityWaiter() {
	if q.spaceWake == nil {
		return
	}
	select {
	case q.spaceWake <- struct{}{}:
	default:
	}
}
