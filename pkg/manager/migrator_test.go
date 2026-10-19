package manager

import (
	"context"
	"testing"
	"time"
)

func TestMigratorStopCancelsAndWaitsForRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})

	m := &Migrator{
		cancelFunc: cancel,
		ctx:        ctx,
		done:       done,
	}
	go func() {
		<-ctx.Done()
		close(canceled)
		<-release
		close(done)
	}()

	result := make(chan error, 1)
	go func() {
		result <- m.Stop()
	}()

	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("Migrator.Stop() did not cancel the run")
	}

	select {
	case err := <-result:
		t.Fatalf("Migrator.Stop() returned before the run stopped: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("Migrator.Stop() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Migrator.Stop() did not return after the run stopped")
	}
}
