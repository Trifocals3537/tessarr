package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Trifocals3537/tessarr/pkg/storage"
)

var (
	// ErrQueueEntryDeleting means an operation lost a race with an explicit
	// queue deletion. Callers must not start new work or recreate the row.
	ErrQueueEntryDeleting = errors.New("queue entry is being deleted")

	// ErrStaleQueueGeneration means work belongs to an older incarnation of a
	// queue key. This is what prevents a late worker from mutating a same-ID
	// entry that was added after deletion completed.
	ErrStaleQueueGeneration = errors.New("stale queue entry generation")
)

type entryLifecycle struct {
	mu     sync.Mutex
	next   uint64
	states map[string]*entryLifecycleState
}

type entryLifecycleState struct {
	mu         sync.Mutex
	generation uint64
	deleting   bool
	active     map[*entryWorkLease]struct{}
}

type entryWorkLease struct {
	lifecycle  *entryLifecycle
	state      *entryLifecycleState
	key        string
	generation uint64
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	once       sync.Once
}

type entryDeleteLease struct {
	lifecycle  *entryLifecycle
	state      *entryLifecycleState
	key        string
	generation uint64
	done       []<-chan struct{}
	once       sync.Once
}

func newEntryLifecycle() *entryLifecycle {
	return &entryLifecycle{
		states: make(map[string]*entryLifecycleState),
	}
}

func normalizeQueueEntryKey(key string) string {
	return strings.ToLower(strings.TrimSpace(key))
}

func (l *entryLifecycle) nextGenerationLocked() uint64 {
	l.next++
	if l.next == 0 {
		// A wrapped generation must never become the unbound zero value.
		l.next++
	}
	return l.next
}

func (l *entryLifecycle) stateFor(key string, create bool) (*entryLifecycleState, error) {
	key = normalizeQueueEntryKey(key)
	if key == "" {
		return nil, fmt.Errorf("queue entry key is empty")
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	state := l.states[key]
	if state == nil && create {
		state = &entryLifecycleState{
			generation: l.nextGenerationLocked(),
			active:     make(map[*entryWorkLease]struct{}),
		}
		l.states[key] = state
	}
	if state == nil {
		return nil, fmt.Errorf("%w: %s", ErrStaleQueueGeneration, key)
	}
	return state, nil
}

// bindEntry attaches the current in-process generation to an entry loaded from
// durable queue storage. Queue generations are intentionally not serialized:
// no worker survives a process restart, so a fresh process starts a fresh
// generation namespace.
func (l *entryLifecycle) bindEntry(entry *storage.Entry) error {
	if entry == nil {
		return fmt.Errorf("queue entry is nil")
	}
	state, err := l.stateFor(entry.InfoHash, true)
	if err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleting {
		return fmt.Errorf("%w: %s", ErrQueueEntryDeleting, entry.InfoHash)
	}
	entry.QueueGeneration = state.generation
	return nil
}

// withRead holds the per-key gate across the durable read and generation bind.
// A snapshot can therefore only receive the generation that owned the row it
// actually read; delete/re-add cannot grant an old payload the replacement's
// generation in the gap between storage.Get and bindEntry.
func (l *entryLifecycle) withRead(key string, read func() (*storage.Entry, error)) (*storage.Entry, error) {
	state, err := l.stateFor(key, true)
	if err != nil {
		return nil, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleting {
		return nil, fmt.Errorf("%w: %s", ErrQueueEntryDeleting, key)
	}
	entry, err := read()
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, fmt.Errorf("queue entry %s is nil", key)
	}
	entry.QueueGeneration = state.generation
	return entry, nil
}

func (l *entryLifecycle) withAdd(entry *storage.Entry, add func() error) error {
	if entry == nil {
		return fmt.Errorf("queue entry is nil")
	}
	state, err := l.stateFor(entry.InfoHash, true)
	if err != nil {
		return err
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleting {
		return fmt.Errorf("%w: %s", ErrQueueEntryDeleting, entry.InfoHash)
	}
	entry.QueueGeneration = state.generation
	return add()
}

// withUpdate holds the per-key gate for the complete durable mutation. A
// deletion either observes the finished update and removes it, or installs its
// tombstone first and makes this update fail; there is no check/put gap.
func (l *entryLifecycle) withUpdate(entry *storage.Entry, update func() error) error {
	if entry == nil {
		return fmt.Errorf("queue entry is nil")
	}
	state, err := l.stateFor(entry.InfoHash, false)
	if err != nil {
		return err
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleting {
		return fmt.Errorf("%w: %s", ErrQueueEntryDeleting, entry.InfoHash)
	}
	if entry.QueueGeneration == 0 || entry.QueueGeneration != state.generation {
		return fmt.Errorf(
			"%w: %s has generation %d, current generation is %d",
			ErrStaleQueueGeneration,
			entry.InfoHash,
			entry.QueueGeneration,
			state.generation,
		)
	}
	return update()
}

func (l *entryLifecycle) validateSubmission(key string, generation uint64) (uint64, error) {
	state, err := l.stateFor(key, true)
	if err != nil {
		return 0, err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleting {
		return 0, fmt.Errorf("%w: %s", ErrQueueEntryDeleting, key)
	}
	if generation == 0 {
		return state.generation, nil
	}
	if generation != state.generation {
		return 0, fmt.Errorf(
			"%w: %s has generation %d, current generation is %d",
			ErrStaleQueueGeneration,
			key,
			generation,
			state.generation,
		)
	}
	return generation, nil
}

func (l *entryLifecycle) startWork(parent context.Context, key string, generation uint64) (*entryWorkLease, error) {
	state, err := l.stateFor(key, false)
	if err != nil {
		return nil, err
	}
	if parent == nil {
		parent = context.Background()
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleting {
		return nil, fmt.Errorf("%w: %s", ErrQueueEntryDeleting, key)
	}
	if generation == 0 || generation != state.generation {
		return nil, fmt.Errorf(
			"%w: %s has generation %d, current generation is %d",
			ErrStaleQueueGeneration,
			key,
			generation,
			state.generation,
		)
	}

	ctx, cancel := context.WithCancel(parent)
	lease := &entryWorkLease{
		lifecycle:  l,
		state:      state,
		key:        normalizeQueueEntryKey(key),
		generation: generation,
		ctx:        ctx,
		cancel:     cancel,
		done:       make(chan struct{}),
	}
	state.active[lease] = struct{}{}
	return lease, nil
}

func (w *entryWorkLease) Context() context.Context {
	if w == nil || w.ctx == nil {
		return context.Background()
	}
	return w.ctx
}

func (w *entryWorkLease) Close() {
	if w == nil {
		return
	}
	w.once.Do(func() {
		w.cancel()
		w.state.mu.Lock()
		delete(w.state.active, w)
		close(w.done)
		w.state.mu.Unlock()
	})
}

// beginDelete installs the tombstone before canceling work. Work that was
// already registered is captured and canceled; work racing registration sees
// the tombstone and is rejected.
func (l *entryLifecycle) beginDelete(key string) (*entryDeleteLease, error) {
	state, err := l.stateFor(key, true)
	if err != nil {
		return nil, err
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	if state.deleting {
		return nil, fmt.Errorf("%w: %s", ErrQueueEntryDeleting, key)
	}
	state.deleting = true

	done := make([]<-chan struct{}, 0, len(state.active))
	for work := range state.active {
		done = append(done, work.done)
		work.cancel()
	}
	return &entryDeleteLease{
		lifecycle:  l,
		state:      state,
		key:        normalizeQueueEntryKey(key),
		generation: state.generation,
		done:       done,
	}, nil
}

func (d *entryDeleteLease) Wait(ctx context.Context) error {
	if d == nil {
		return nil
	}
	for _, done := range d.done {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// Finish removes the tombstone. A successful delete advances the generation
// before new work is admitted, invalidating delayed Retry goroutines and stale
// entry pointers. A failed delete retains the generation because the durable
// row was retained for a clean retry.
func (d *entryDeleteLease) Finish(success bool) {
	if d == nil {
		return
	}
	d.once.Do(func() {
		d.state.mu.Lock()
		defer d.state.mu.Unlock()
		if d.state.generation != d.generation {
			return
		}
		if success {
			d.lifecycle.mu.Lock()
			d.state.generation = d.lifecycle.nextGenerationLocked()
			d.lifecycle.mu.Unlock()
		}
		d.state.deleting = false
	})
}
