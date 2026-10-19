package manager

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func TestJobQueuePopClearsReferencesAndReusesStorage(t *testing.T) {
	q := &JobQueue{jobs: make([]*Job, 0, 2)}
	q.cond = sync.NewCond(&q.mu)
	first := &Job{ID: "first"}
	second := &Job{ID: "second"}
	q.jobs = append(q.jobs, first, second)
	backing := q.jobs[:cap(q.jobs)]

	if got := q.pop(); got != first {
		t.Fatalf("first pop = %p, want %p", got, first)
	}
	if backing[0] != nil {
		t.Fatal("first consumed job is still retained by the queue backing array")
	}

	if got := q.pop(); got != second {
		t.Fatalf("second pop = %p, want %p", got, second)
	}
	if len(q.jobs) != 0 || q.head != 0 {
		t.Fatalf("drained queue state = len %d, head %d; want zero values", len(q.jobs), q.head)
	}

	q.jobs = append(q.jobs, &Job{ID: "third"})
	if &q.jobs[:cap(q.jobs)][0] != &backing[0] {
		t.Fatal("drained queue did not reuse its backing array")
	}
}

func TestJobQueueDeleteClearsTailAfterConsumedPrefix(t *testing.T) {
	q := &JobQueue{jobs: make([]*Job, 0, 3)}
	q.cond = sync.NewCond(&q.mu)
	first := &Job{ID: "first", Type: JobTypeTorrent}
	second := &Job{ID: "second", Type: JobTypeNZB}
	third := &Job{ID: "third", Type: JobTypeTorrent}
	q.jobs = append(q.jobs, first, second, third)
	backing := q.jobs[:cap(q.jobs)]

	if got := q.pop(); got != first {
		t.Fatalf("pop = %p, want %p", got, first)
	}
	if !q.DeleteJob(second.ID) {
		t.Fatal("DeleteJob(second) = false, want true")
	}
	if backing[2] != nil {
		t.Fatal("deleted job left a stale pointer at the queue tail")
	}
	if got := q.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1", got)
	}
	if got := q.FindJob(third.ID); got != third {
		t.Fatalf("FindJob(third) = %p, want %p", got, third)
	}
	if got := q.PendingCount(JobTypeTorrent); got != 1 {
		t.Fatalf("PendingCount(torrent) = %d, want 1", got)
	}
	if got := q.PendingCount(JobTypeNZB); got != 0 {
		t.Fatalf("PendingCount(nzb) = %d, want 0", got)
	}
}

func BenchmarkJobQueueBatchReuse(b *testing.B) {
	const batchSize = 64
	q := &JobQueue{jobs: make([]*Job, 0, batchSize)}
	q.cond = sync.NewCond(&q.mu)

	jobs := make([]*Job, batchSize)
	for i := range jobs {
		jobs[i] = &Job{ID: "benchmark"}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		q.mu.Lock()
		q.jobs = append(q.jobs, jobs...)
		q.mu.Unlock()

		for range batchSize {
			if job := q.pop(); job == nil {
				b.Fatal("pop returned nil")
			}
		}
	}
}

func TestJobQueueCapacityAndDedupeAcrossActiveAndPending(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	queue := NewJobQueueWithCapacity(
		context.Background(),
		1,
		2,
		func(_ context.Context, job *Job) {
			if job.ID == "first" {
				close(started)
				<-release
			}
		},
	)
	t.Cleanup(queue.Close)

	first := &Job{ID: "first", Type: JobTypeTorrent}
	if err := queue.Submit(first); err != nil {
		t.Fatal(err)
	}
	<-started

	err := queue.Submit(&Job{ID: " first ", Type: JobTypeTorrent})
	var duplicate *DuplicateJobError
	if !errors.Is(err, ErrJobQueueDuplicate) || !errors.As(err, &duplicate) {
		t.Fatalf("active duplicate error = %v, want typed duplicate", err)
	}
	if duplicate.Key != "first" {
		t.Fatalf("duplicate key = %q, want first", duplicate.Key)
	}

	if err := queue.Submit(&Job{ID: "second", Type: JobTypeNZB}); err != nil {
		t.Fatal(err)
	}
	err = queue.Submit(&Job{ID: "third", Type: JobTypeTorrent})
	var full *JobQueueFullError
	if !errors.Is(err, ErrJobQueueFull) || !errors.As(err, &full) {
		t.Fatalf("capacity error = %v, want typed full error", err)
	}
	if full.Capacity != 2 {
		t.Fatalf("reported capacity = %d, want 2", full.Capacity)
	}
	if got := queue.OutstandingCount(); got != 2 {
		t.Fatalf("OutstandingCount() = %d, want 2", got)
	}

	close(release)
	waitForJobQueueCondition(t, func() bool {
		return queue.OutstandingCount() == 0
	})
}

func TestJobQueueReservationConsumesCapacityAndReleases(t *testing.T) {
	queue := NewJobQueueWithCapacity(
		context.Background(),
		1,
		1,
		func(context.Context, *Job) {},
	)
	t.Cleanup(queue.Close)

	reservation, err := queue.reserve("Release-ID")
	if err != nil {
		t.Fatal(err)
	}
	if got := queue.OutstandingCount(); got != 1 {
		t.Fatalf("OutstandingCount() = %d, want 1", got)
	}
	if _, err := queue.reserve("release-id"); !errors.Is(err, ErrJobQueueDuplicate) {
		t.Fatalf("duplicate reservation error = %v", err)
	}
	if _, err := queue.reserve("other"); !errors.Is(err, ErrJobQueueFull) {
		t.Fatalf("full reservation error = %v", err)
	}

	reservation.release()
	if _, err := queue.reserve("other"); err != nil {
		t.Fatalf("reserve after release: %v", err)
	}
}

func TestJobQueueReservationParticipatesInDeletionDrain(t *testing.T) {
	lifecycle := newEntryLifecycle()
	queue := NewJobQueueWithCapacity(
		context.Background(),
		1,
		2,
		func(context.Context, *Job) {},
		lifecycle,
	)
	t.Cleanup(queue.Close)

	reservation, err := queue.reserveContext(
		context.Background(),
		"reserved-entry",
	)
	if err != nil {
		t.Fatal(err)
	}
	deletion, err := lifecycle.beginDelete("reserved-entry")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-reservation.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("deletion did not cancel the pre-provider reservation")
	}

	waited := make(chan error, 1)
	go func() {
		waited <- deletion.Wait(context.Background())
	}()
	select {
	case err := <-waited:
		t.Fatalf("deletion drained before reservation release: %v", err)
	default:
	}

	reservation.release()
	if err := <-waited; err != nil {
		t.Fatal(err)
	}
	deletion.Finish(false)
}

func TestManagerReservationRejectsDurableRowsAndDeletionIntents(t *testing.T) {
	store := newLifecycleTestStorage(t)
	lifecycle := newEntryLifecycle()
	queue := newLifecycleTestQueue(store, lifecycle)
	jobQueue := NewJobQueueWithCapacity(
		context.Background(),
		1,
		2,
		func(context.Context, *Job) {},
		lifecycle,
	)
	t.Cleanup(jobQueue.Close)
	manager := &Manager{queue: queue, jobQueue: jobQueue}

	entry := &storage.Entry{InfoHash: "durable-admission"}
	if err := queue.Add(entry); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.reserveJob(
		context.Background(),
		entry.InfoHash,
	); !errors.Is(err, ErrJobQueueDuplicate) {
		t.Fatalf("durable duplicate reservation error = %v", err)
	}
	if got := jobQueue.OutstandingCount(); got != 0 {
		t.Fatalf("duplicate reservation retained %d capacity slots", got)
	}

	if _, err := store.PrepareQueuedDeletion(entry.InfoHash, false); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.reserveJob(
		context.Background(),
		entry.InfoHash,
	); !errors.Is(err, storage.ErrQueuedEntryDeleting) {
		t.Fatalf("deleting row reservation error = %v", err)
	}
	if got := jobQueue.OutstandingCount(); got != 0 {
		t.Fatalf("deletion reservation retained %d capacity slots", got)
	}

	reservation, err := manager.reserveJob(context.Background(), "new-entry")
	if err != nil {
		t.Fatalf("new reservation error = %v", err)
	}
	reservation.release()
}

func TestJobQueueDelayedRetryDoesNotRunConcurrently(t *testing.T) {
	var (
		queue      *JobQueue
		calls      atomic.Int32
		concurrent atomic.Int32
		maxActive  atomic.Int32
	)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	retryResult := make(chan error, 1)

	queue = NewJobQueueWithCapacity(
		context.Background(),
		2,
		1,
		func(_ context.Context, job *Job) {
			active := concurrent.Add(1)
			for {
				previous := maxActive.Load()
				if active <= previous || maxActive.CompareAndSwap(previous, active) {
					break
				}
			}
			defer concurrent.Add(-1)

			if calls.Add(1) == 1 {
				retryResult <- queue.Retry(job, 10*time.Millisecond)
				close(firstStarted)
				<-releaseFirst
				return
			}
			close(secondStarted)
		},
	)
	t.Cleanup(queue.Close)

	job := &Job{ID: "retry-key", Type: JobTypeTorrent}
	if err := queue.Submit(job); err != nil {
		t.Fatal(err)
	}
	<-firstStarted
	if err := <-retryResult; err != nil {
		t.Fatalf("Retry() error = %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls before first completion = %d, want 1", got)
	}
	if err := queue.Submit(&Job{ID: "retry-key"}); !errors.Is(err, ErrJobQueueDuplicate) {
		t.Fatalf("duplicate during delayed retry error = %v", err)
	}

	close(releaseFirst)
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("delayed retry did not run")
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("same-key maximum concurrency = %d, want 1", got)
	}
	waitForJobQueueCondition(t, func() bool {
		return queue.OutstandingCount() == 0
	})
}

func TestJobQueueDeleteJobsRemovesDelayedRetry(t *testing.T) {
	queue := NewJobQueueWithCapacity(
		context.Background(),
		1,
		1,
		func(context.Context, *Job) {},
	)
	t.Cleanup(queue.Close)

	if err := queue.Retry(&Job{ID: "Delayed-Key"}, time.Hour); err != nil {
		t.Fatal(err)
	}
	if removed := queue.DeleteJobs(" delayed-key "); removed != 1 {
		t.Fatalf("DeleteJobs() removed %d jobs, want 1", removed)
	}
	if got := queue.OutstandingCount(); got != 0 {
		t.Fatalf("OutstandingCount() = %d, want 0", got)
	}
	if err := queue.Submit(&Job{ID: "delayed-key"}); err != nil {
		t.Fatalf("Submit() after delayed deletion: %v", err)
	}
}

func TestJobQueueCloseCancelsLongDelayedRetry(t *testing.T) {
	queue := NewJobQueueWithCapacity(
		context.Background(),
		1,
		1,
		func(context.Context, *Job) {},
	)
	if err := queue.Retry(&Job{ID: "delayed"}, time.Hour); err != nil {
		t.Fatal(err)
	}

	closed := make(chan struct{})
	go func() {
		queue.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close() did not cancel the retry scheduler")
	}
	if err := queue.Submit(&Job{ID: "later"}); !errors.Is(err, ErrJobQueueClosed) {
		t.Fatalf("Submit() after Close error = %v", err)
	}
}

func TestNormalizeJobQueueCapacity(t *testing.T) {
	if got := normalizeJobQueueCapacity(0); got != DefaultJobQueueCapacity {
		t.Fatalf("default capacity = %d, want %d", got, DefaultJobQueueCapacity)
	}
	if got := normalizeJobQueueCapacity(MaxJobQueueCapacity + 1); got != MaxJobQueueCapacity {
		t.Fatalf("clamped capacity = %d, want %d", got, MaxJobQueueCapacity)
	}
	if got := normalizeJobQueueCapacity(17); got != 17 {
		t.Fatalf("explicit capacity = %d, want 17", got)
	}
}

func waitForJobQueueCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for job queue condition")
		}
		time.Sleep(time.Millisecond)
	}
}
