package manager

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-co-op/gocron/v2"
)

const schedulerSingletonTestTimeout = 30 * time.Second

func waitForSchedulerTestSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	timer := time.NewTimer(schedulerSingletonTestTimeout)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatalf("timed out waiting for %s", name)
	}
}

func TestManagerSchedulerPreventsOverlappingJobRuns(t *testing.T) {
	scheduler, err := newManagerScheduler(time.UTC, "scheduler-singleton-test")
	if err != nil {
		t.Fatalf("newManagerScheduler() error = %v", err)
	}

	var releaseOnce sync.Once
	releaseFirst := make(chan struct{})
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseFirst) })
		if err := scheduler.Shutdown(); err != nil {
			t.Errorf("scheduler.Shutdown() error = %v", err)
		}
	})

	firstStarted := make(chan struct{})
	firstFinished := make(chan struct{})
	laterStarted := make(chan struct{})
	var laterOnce sync.Once
	var runs atomic.Int32

	job, err := scheduler.NewJob(
		gocron.DurationJob(time.Hour),
		gocron.NewTask(func() {
			if runs.Add(1) == 1 {
				close(firstStarted)
				<-releaseFirst
				close(firstFinished)
				return
			}
			laterOnce.Do(func() { close(laterStarted) })
		}),
	)
	if err != nil {
		t.Fatalf("scheduler.NewJob() error = %v", err)
	}

	// A separate job is an event-driven barrier: once it starts, the scheduler
	// executor has handled every earlier RunNow request for the blocked job.
	barrierStarted := make(chan struct{})
	barrier, err := scheduler.NewJob(
		gocron.DurationJob(time.Hour),
		gocron.NewTask(func() { close(barrierStarted) }),
	)
	if err != nil {
		t.Fatalf("scheduler.NewJob(barrier) error = %v", err)
	}

	scheduler.Start()
	if err := job.RunNow(); err != nil {
		t.Fatalf("job.RunNow(first) error = %v", err)
	}
	waitForSchedulerTestSignal(t, firstStarted, "first job run")

	for range 8 {
		if err := job.RunNow(); err != nil {
			t.Fatalf("job.RunNow(overlap) error = %v", err)
		}
	}
	if err := barrier.RunNow(); err != nil {
		t.Fatalf("barrier.RunNow() error = %v", err)
	}
	waitForSchedulerTestSignal(t, barrierStarted, "scheduler barrier")
	if got := runs.Load(); got != 1 {
		t.Fatalf("blocked singleton job ran %d times, want 1", got)
	}

	releaseOnce.Do(func() { close(releaseFirst) })
	waitForSchedulerTestSignal(t, firstFinished, "first job completion")
	if err := job.RunNow(); err != nil {
		t.Fatalf("job.RunNow(after completion) error = %v", err)
	}
	waitForSchedulerTestSignal(t, laterStarted, "post-completion job run")
	if got := runs.Load(); got != 2 {
		t.Fatalf("singleton job run count = %d, want 2", got)
	}
}

func TestManagerSchedulerTagRemovesEveryGlobalJob(t *testing.T) {
	scheduler, err := newManagerScheduler(time.UTC, managerSchedulerTag)
	if err != nil {
		t.Fatalf("newManagerScheduler() error = %v", err)
	}
	t.Cleanup(func() {
		if err := scheduler.Shutdown(); err != nil {
			t.Errorf("scheduler.Shutdown() error = %v", err)
		}
	})

	for range 3 {
		if _, err := scheduler.NewJob(
			gocron.DurationJob(time.Hour),
			gocron.NewTask(func() {}),
		); err != nil {
			t.Fatalf("scheduler.NewJob() error = %v", err)
		}
	}
	if got := len(scheduler.Jobs()); got != 3 {
		t.Fatalf("scheduler job count = %d, want 3", got)
	}
	scheduler.RemoveByTags(managerSchedulerTag)
	if got := len(scheduler.Jobs()); got != 0 {
		t.Fatalf("scheduler job count after tag removal = %d, want 0", got)
	}
}
