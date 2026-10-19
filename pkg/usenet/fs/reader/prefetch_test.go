package reader

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestPrefetchSessionsAreIndependentAndIdempotent(t *testing.T) {
	base := context.Background()
	ctxA := WithPrefetchSession(base)
	ctxAAgain := WithPrefetchSession(ctxA)
	ctxB := WithPrefetchSession(base)

	sessionA := prefetchSessionFromContext(ctxA)
	if sessionA == nil {
		t.Fatal("first context has no prefetch session")
	}
	if got := prefetchSessionFromContext(ctxAAgain); got != sessionA {
		t.Fatal("wrapping an existing session created a second identity")
	}
	if got := prefetchSessionFromContext(ctxB); got == nil || got == sessionA {
		t.Fatal("independent context did not receive an independent session")
	}
}

func TestPrefetchSchedulerRoundRobinsConsumers(t *testing.T) {
	s := newPrefetchScheduler(16, &ReaderStats{})
	t.Cleanup(s.close)
	sessionA := newPrefetchSession()
	sessionB := newPrefetchSession()

	s.addRange(context.Background(), sessionA, 1, 3, nil)
	s.addRange(context.Background(), sessionB, 10, 12, nil)

	var got []int
	for range 6 {
		task, ok := s.nextTask()
		if !ok {
			t.Fatal("scheduler closed before all tasks were returned")
		}
		got = append(got, task.segment)
		s.complete(task)
	}
	want := []int{1, 10, 2, 11, 3, 12}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round-robin order = %v, want %v", got, want)
	}
}

func TestPrefetchSeekDropsOnlyThatConsumersHints(t *testing.T) {
	stats := &ReaderStats{}
	s := newPrefetchScheduler(16, stats)
	t.Cleanup(s.close)
	sessionA := newPrefetchSession()
	sessionB := newPrefetchSession()

	s.addRange(context.Background(), sessionA, 1, 3, nil)
	s.addRange(context.Background(), sessionB, 10, 12, nil)
	s.dropPending(sessionA)

	if got := stats.PrefetchCancelled.Load(); got != 3 {
		t.Fatalf("cancelled hints = %d, want 3", got)
	}
	var got []int
	for range 3 {
		task, ok := s.nextTask()
		if !ok {
			t.Fatal("scheduler closed before surviving consumer was served")
		}
		got = append(got, task.segment)
		s.complete(task)
	}
	want := []int{10, 11, 12}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("surviving tasks = %v, want %v", got, want)
	}
}

func TestPrefetchDuplicateInterestSurvivesOtherConsumerCancellation(t *testing.T) {
	s := newPrefetchScheduler(16, &ReaderStats{})
	t.Cleanup(s.close)
	sessionA := newPrefetchSession()
	sessionB := newPrefetchSession()

	s.addRange(context.Background(), sessionA, 5, 5, nil)
	s.addRange(context.Background(), sessionB, 5, 5, nil)
	s.dropPending(sessionA)

	task, ok := s.nextTask()
	if !ok {
		t.Fatal("second consumer's duplicate interest was lost")
	}
	if task.session != sessionB || task.segment != 5 {
		t.Fatalf("task = {%p %d}, want session B segment 5", task.session, task.segment)
	}
	s.complete(task)
}

func TestPrefetchDuplicateActiveWorkUsesOnlyOneWorker(t *testing.T) {
	stats := &ReaderStats{}
	s := newPrefetchScheduler(16, stats)
	t.Cleanup(s.close)
	sessionA := newPrefetchSession()
	sessionB := newPrefetchSession()

	s.addRange(context.Background(), sessionA, 5, 5, nil)
	s.addRange(context.Background(), sessionB, 5, 6, nil)

	first, ok := s.nextTask()
	if !ok || first.segment != 5 {
		t.Fatalf("first task = %+v, want segment 5", first)
	}
	second, ok := s.nextTask()
	if !ok || second.session != sessionB || second.segment != 6 {
		t.Fatalf("second task = %+v, want session B segment 6", second)
	}
	if got := stats.PrefetchCoalesced.Load(); got != 1 {
		t.Fatalf("coalesced hints = %d, want 1", got)
	}

	s.complete(first)
	s.complete(second)
}

func TestPrefetchSchedulerRebalancesFullQueue(t *testing.T) {
	stats := &ReaderStats{}
	s := newPrefetchScheduler(4, stats)
	t.Cleanup(s.close)
	sessionA := newPrefetchSession()
	sessionB := newPrefetchSession()

	s.addRange(context.Background(), sessionA, 1, 4, nil)
	s.addRange(context.Background(), sessionB, 10, 13, nil)

	var got []int
	for range 4 {
		task, ok := s.nextTask()
		if !ok {
			t.Fatal("scheduler closed before the bounded queue drained")
		}
		got = append(got, task.segment)
		s.complete(task)
	}
	want := []int{1, 10, 2, 11}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rebalanced order = %v, want %v", got, want)
	}
	if got := stats.PrefetchRebalanced.Load(); got != 2 {
		t.Fatalf("rebalanced hints = %d, want 2", got)
	}
	if got := stats.PrefetchMisses.Load(); got != 2 {
		t.Fatalf("dropped new hints = %d, want 2", got)
	}
}

func TestPrefetchContextCancellationDrainsOnlyItsLane(t *testing.T) {
	stats := &ReaderStats{}
	s := newPrefetchScheduler(16, stats)
	t.Cleanup(s.close)

	baseA, cancelA := context.WithCancel(context.Background())
	ctxA := WithPrefetchSession(baseA)
	ctxB := WithPrefetchSession(context.Background())
	sessionA := prefetchSessionFromContext(ctxA)
	sessionB := prefetchSessionFromContext(ctxB)
	s.addRange(ctxA, sessionA, 1, 3, nil)
	s.addRange(ctxB, sessionB, 10, 12, nil)
	cancelA()

	deadline := time.Now().Add(time.Second)
	for stats.PrefetchCancelled.Load() != 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := stats.PrefetchCancelled.Load(); got != 3 {
		t.Fatalf("context cancellation removed %d hints, want 3", got)
	}

	for _, want := range []int{10, 11, 12} {
		task, ok := s.nextTask()
		if !ok {
			t.Fatal("scheduler closed before the uncancelled lane drained")
		}
		if task.session != sessionB || task.segment != want {
			t.Fatalf("task = {%p %d}, want session B segment %d", task.session, task.segment, want)
		}
		s.complete(task)
	}
}

func TestConcurrentConsumerSeekDetectionIsIsolated(t *testing.T) {
	sessionA := newPrefetchSession()
	sessionB := newPrefetchSession()
	if sessionA.observeRead(0, 1, 8) || sessionB.observeRead(100, 101, 8) {
		t.Fatal("a first read must not be classified as a seek")
	}

	var wg sync.WaitGroup
	results := make(chan bool, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		results <- sessionA.observeRead(40, 41, 8)
	}()
	go func() {
		defer wg.Done()
		results <- sessionB.observeRead(102, 103, 8)
	}()
	wg.Wait()
	close(results)

	seeks := 0
	for result := range results {
		if result {
			seeks++
		}
	}
	if seeks != 1 {
		t.Fatalf("detected %d seeks, want only the distant consumer jump", seeks)
	}
}

func TestPrefetchSchedulerCloseUnblocksIdleWorker(t *testing.T) {
	s := newPrefetchScheduler(16, &ReaderStats{})
	done := make(chan bool, 1)
	go func() {
		_, ok := s.nextTask()
		done <- ok
	}()

	s.close()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("closed scheduler returned a task")
		}
	case <-time.After(time.Second):
		t.Fatal("idle scheduler worker did not unblock on close")
	}
}
