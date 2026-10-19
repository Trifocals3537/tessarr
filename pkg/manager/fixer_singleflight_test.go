package manager

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
)

func TestFixerClaimRepairHasExactlyOneOwner(t *testing.T) {
	fixer := &Fixer{inFlightRepairs: xsync.NewMap[string, *FixerRequest]()}
	entry := &storage.Entry{
		Protocol:       config.ProtocolTorrent,
		InfoHash:       "shared-hash",
		ActiveProvider: "realdebrid",
	}

	const callers = 64
	start := make(chan struct{})
	requests := make(chan *FixerRequest, callers)
	var owners atomic.Int64
	var workers sync.WaitGroup
	for range callers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			request, joined := fixer.claimRepair(entry)
			if !joined {
				owners.Add(1)
			}
			requests <- request
		}()
	}
	close(start)
	workers.Wait()
	close(requests)

	if got := owners.Load(); got != 1 {
		t.Fatalf("repair owners = %d, want 1", got)
	}
	var shared *FixerRequest
	for request := range requests {
		if shared == nil {
			shared = request
			continue
		}
		if request != shared {
			t.Fatal("concurrent callers received different repair requests")
		}
	}
}

func TestFixTorrentBroadcastsOneResultToEveryWaiter(t *testing.T) {
	fixer := &Fixer{inFlightRepairs: xsync.NewMap[string, *FixerRequest]()}
	entry := &storage.Entry{
		Protocol: config.ProtocolTorrent,
		InfoHash: "shared-hash",
		Name:     "Shared Release",
	}
	request := &FixerRequest{InfoHash: entry.InfoHash, done: make(chan struct{})}
	fixer.inFlightRepairs.Store(entry.InfoHash, request)

	const waiters = 32
	type outcome struct {
		result *FixResult
		err    error
	}
	outcomes := make(chan outcome, waiters)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for range waiters {
		go func() {
			result, err := fixer.FixTorrent(ctx, entry, false)
			outcomes <- outcome{result: result, err: err}
		}()
	}

	wantErr := errors.New("shared diagnostic")
	wantResult := &FixResult{Success: false, Error: wantErr, AttemptsCount: 1}
	request.complete(wantResult, wantErr)
	for range waiters {
		outcome := <-outcomes
		if outcome.result != wantResult {
			t.Fatalf("waiter result = %#v, want shared result %#v", outcome.result, wantResult)
		}
		if !errors.Is(outcome.err, wantErr) {
			t.Fatalf("waiter error = %v, want %v", outcome.err, wantErr)
		}
	}
}

func TestCancelledFixTorrentWaiterDoesNotConsumeSharedResult(t *testing.T) {
	fixer := &Fixer{inFlightRepairs: xsync.NewMap[string, *FixerRequest]()}
	entry := &storage.Entry{
		Protocol: config.ProtocolTorrent,
		InfoHash: "shared-hash",
		Name:     "Shared Release",
	}
	request := &FixerRequest{InfoHash: entry.InfoHash, done: make(chan struct{})}
	fixer.inFlightRepairs.Store(entry.InfoHash, request)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fixer.FixTorrent(cancelled, entry, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled waiter error = %v, want context cancellation", err)
	}

	want := &FixResult{Success: true, NewDebrid: "torbox", AttemptsCount: 1}
	request.complete(want, nil)
	result, err := fixer.FixTorrent(context.Background(), entry, false)
	if err != nil {
		t.Fatal(err)
	}
	if result != want {
		t.Fatalf("later waiter result = %#v, want shared result %#v", result, want)
	}
}
