package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
)

var (
	// ErrEntryDeleting means an explicit deletion owns the main-entry key.
	// Callers must not retry by blindly upserting the same payload.
	ErrEntryDeleting = errors.New("main entry is being deleted")

	// ErrStaleEntryGeneration means a mutation was derived from a main-entry
	// snapshot that is no longer current.
	ErrStaleEntryGeneration = errors.New("stale main entry generation")

	// ErrEntryRediscoveryPending means a deleted provider-backed entry is still
	// visible in a provider snapshot without an authoritative post-delete
	// absence having been observed first.
	ErrEntryRediscoveryPending = errors.New("main entry rediscovery requires a post-delete provider absence")
)

// mainEntryLifecycle serializes only the final local read/write section for one
// main-entry key. Provider and network work carries a generation token and
// never holds these locks. CompleteQueuedDeletion is the narrow filesystem
// exception: local cleanup holds the per-key lock to serialize against admin
// snapshot installation. Its callback must not re-enter Storage.
//
// Generations are intentionally process-local because no worker survives a
// restart. Retired keys and authoritative provider absences are persisted
// separately, so a restart cannot reinterpret eventual-consistency presence
// as a new discovery.
type mainEntryLifecycle struct {
	mu       sync.Mutex
	sequence atomic.Uint64
	states   map[string]*mainEntryState
}

type mainEntryState struct {
	mu sync.Mutex

	generation    uint64
	refs          int
	seen          bool
	deleting      bool
	queueDeleting bool
	retired       bool
	retiredAt     uint64

	// absentAt is keyed by normalized provider name. A provider-backed
	// rediscovery is accepted only from a strictly later full snapshot.
	absentAt              map[string]uint64
	durableAbsent         map[string]struct{}
	authorizedQueueIncarn string
}

type mainEntryStateRef struct {
	lifecycle *mainEntryLifecycle
	key       string
	state     *mainEntryState
}

func newMainEntryLifecycle() *mainEntryLifecycle {
	return &mainEntryLifecycle{states: make(map[string]*mainEntryState)}
}

func normalizeMainEntryKey(key string) string {
	return strings.ToLower(strings.TrimSpace(key))
}

func normalizeMainEntryProvider(provider string) string {
	return strings.ToLower(strings.TrimSpace(provider))
}

func (l *mainEntryLifecycle) nextSequence() uint64 {
	next := l.sequence.Add(1)
	if next == 0 {
		// Do not expose the zero value: it means "unbound" on Entry.
		next = l.sequence.Add(1)
	}
	return next
}

func (l *mainEntryLifecycle) beginProviderSnapshot() uint64 {
	return l.nextSequence()
}

func (l *mainEntryLifecycle) acquire(
	key string,
	initial func() (
		seen bool,
		retired bool,
		durableAbsent map[string]struct{},
		authorizedQueueIncarn string,
		queueDeleting bool,
		err error,
	),
) (*mainEntryStateRef, error) {
	key = normalizeMainEntryKey(key)
	if key == "" {
		return nil, fmt.Errorf("main entry key is empty")
	}

	l.mu.Lock()
	state := l.states[key]
	if state == nil {
		seen, retired, durableAbsent, authorizedQueueIncarn, queueDeleting, err := initial()
		if err != nil {
			l.mu.Unlock()
			return nil, err
		}
		if durableAbsent == nil {
			durableAbsent = make(map[string]struct{})
		}
		state = &mainEntryState{
			generation:            l.nextSequence(),
			seen:                  seen,
			retired:               retired,
			queueDeleting:         queueDeleting,
			absentAt:              make(map[string]uint64),
			durableAbsent:         durableAbsent,
			authorizedQueueIncarn: authorizedQueueIncarn,
		}
		l.states[key] = state
	}
	state.refs++
	l.mu.Unlock()

	return &mainEntryStateRef{lifecycle: l, key: key, state: state}, nil
}

// release garbage-collects state created by failed probes of genuinely unseen
// keys. Live entries and unresolved retired keys remain: those are semantic
// state, bounded by the number of durable entries plus deletions awaiting a
// provider absence/reappearance.
func (r *mainEntryStateRef) release() {
	if r == nil || r.lifecycle == nil || r.state == nil {
		return
	}

	r.lifecycle.mu.Lock()
	defer r.lifecycle.mu.Unlock()
	if current := r.lifecycle.states[r.key]; current != r.state {
		return
	}
	r.state.mu.Lock()
	defer r.state.mu.Unlock()
	r.state.refs--
	if r.state.refs == 0 &&
		!r.state.seen &&
		!r.state.deleting &&
		!r.state.queueDeleting &&
		!r.state.retired {
		delete(r.lifecycle.states, r.key)
	}
}

func staleMainEntryError(key string, got, want uint64) error {
	return fmt.Errorf(
		"%w: %s has generation %d, current generation is %d",
		ErrStaleEntryGeneration,
		key,
		got,
		want,
	)
}

func bindMainEntrySnapshot(entry *Entry, generation uint64) {
	if entry == nil {
		return
	}
	entry.MainGeneration = generation
	entry.MainProviderSnapshot = 0
	entry.MainMutationProvider = ""
	entry.MainReimportIncarnation = ""
}

func (s *Storage) acquireMainEntryState(key string) (*mainEntryStateRef, error) {
	if s.mainEntries == nil {
		return nil, fmt.Errorf("main entry lifecycle is not initialized")
	}
	normalized := normalizeMainEntryKey(key)
	return s.mainEntries.acquire(normalized, func() (bool, bool, map[string]struct{}, string, bool, error) {
		tombstone, found, err := s.loadMainEntryTombstone(normalized)
		if err != nil {
			return false, false, nil, "", false, err
		}
		durableAbsent := make(map[string]struct{})
		if found {
			for _, provider := range tombstone.AbsentProviders {
				durableAbsent[normalizeMainEntryProvider(provider)] = struct{}{}
			}
		}
		authorizedQueueIncarn := ""
		if found && tombstone.Phase == mainEntryTombstoneRetired {
			authorizedQueueIncarn = tombstone.QueueIncarnation
		}
		_, queueDeleting, err := s.loadQueueDeletionTombstone(normalized)
		if err != nil {
			return false, false, nil, "", false, err
		}
		return s.entries != nil && s.entries.Exists(normalized),
			found,
			durableAbsent,
			authorizedQueueIncarn,
			queueDeleting,
			nil
	})
}

// BeginProviderSnapshot returns a process-local token that must be captured
// before starting a provider's full-list network request.
func (s *Storage) BeginProviderSnapshot() uint64 {
	if s == nil || s.mainEntries == nil {
		return 0
	}
	return s.mainEntries.beginProviderSnapshot()
}

// PrepareProviderEntry binds a newly discovered provider entry to the current
// generation. If the key was explicitly deleted, the provider must first have
// produced a complete post-delete snapshot in which the key was absent, and
// this presence must come from a strictly later snapshot.
func (s *Storage) PrepareProviderEntry(entry *Entry, provider string, snapshot uint64) error {
	if entry == nil {
		return fmt.Errorf("main entry is nil")
	}
	ref, err := s.acquireMainEntryState(entry.InfoHash)
	if err != nil {
		return err
	}
	defer ref.release()

	state := ref.state
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.deleting {
		return fmt.Errorf("%w: %s", ErrEntryDeleting, ref.key)
	}
	if !state.seen && !state.retired {
		// No durable main incarnation exists yet, so a token left on a payload
		// by an earlier failed first write has no authority to preserve.
		entry.MainGeneration = 0
		entry.MainMutationProvider = ""
		entry.MainProviderSnapshot = 0
		return nil
	}
	if entry.MainGeneration != 0 && entry.MainGeneration != state.generation {
		return staleMainEntryError(ref.key, entry.MainGeneration, state.generation)
	}
	if state.retired {
		provider = normalizeMainEntryProvider(provider)
		absentAt := state.absentAt[provider]
		_, durableAbsence := state.durableAbsent[provider]
		absenceAllowsSnapshot := durableAbsence &&
			(absentAt == 0 || snapshot > absentAt)
		if provider == "" ||
			snapshot == 0 ||
			!absenceAllowsSnapshot {
			return fmt.Errorf(
				"%w: %s from provider %q at snapshot %d",
				ErrEntryRediscoveryPending,
				ref.key,
				provider,
				snapshot,
			)
		}
		entry.MainMutationProvider = provider
		entry.MainProviderSnapshot = snapshot
	}
	entry.MainGeneration = state.generation
	return nil
}

// RediscoveryAwaitingAbsence reports whether a provider appearance for the
// entry would still be rejected by PrepareProviderEntry as an unauthorized
// rediscovery. It mirrors the guard's authorization condition without
// mutating any state, so provider sync can cheaply skip expensive per-torrent
// work (status refreshes, RAR unpacking, batch writes) for entries that are
// already known to be awaiting a post-delete provider absence. The guard
// itself remains the authority: when this check fails open, PrepareProviderEntry
// still rejects the candidate exactly as before.
func (s *Storage) RediscoveryAwaitingAbsence(infoHash, provider string, snapshot uint64) (bool, error) {
	if s == nil || s.mainEntries == nil {
		return false, nil
	}
	normalized := normalizeMainEntryKey(infoHash)
	ref, err := s.acquireMainEntryState(normalized)
	if err != nil {
		return false, err
	}
	defer ref.release()

	state := ref.state
	state.mu.Lock()
	defer state.mu.Unlock()

	if !state.retired {
		return false, nil
	}
	provider = normalizeMainEntryProvider(provider)
	absentAt := state.absentAt[provider]
	_, durableAbsence := state.durableAbsent[provider]
	authorized := provider != "" &&
		snapshot != 0 &&
		durableAbsence &&
		(absentAt == 0 || snapshot > absentAt)
	return !authorized, nil
}

// PrepareQueuedReplacement binds an explicit queue re-import to the current
// main-entry generation. A retired key is authorized only when the candidate
// carries the exact durable incarnation of the queue row created after that
// retirement. Provider sync cannot mint this transient capability.
func (s *Storage) PrepareQueuedReplacement(entry *Entry) error {
	if entry == nil {
		return fmt.Errorf("main entry is nil")
	}
	entry.InfoHash = normalizeMainEntryKey(entry.InfoHash)
	ref, err := s.acquireMainEntryState(entry.InfoHash)
	if err != nil {
		return err
	}
	defer ref.release()

	state := ref.state
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.deleting {
		return fmt.Errorf("%w: %s", ErrEntryDeleting, ref.key)
	}
	if !state.seen && !state.retired {
		entry.MainGeneration = 0
		entry.MainReimportIncarnation = ""
		return nil
	}
	if entry.MainGeneration != 0 && entry.MainGeneration != state.generation {
		return staleMainEntryError(ref.key, entry.MainGeneration, state.generation)
	}
	if !state.retired {
		entry.MainGeneration = state.generation
		entry.MainReimportIncarnation = ""
		return nil
	}

	incarnation := strings.TrimSpace(entry.QueueIncarnation)
	if incarnation == "" || incarnation != state.authorizedQueueIncarn {
		return fmt.Errorf(
			"%w: %s from queue incarnation %q",
			ErrEntryRediscoveryPending,
			ref.key,
			incarnation,
		)
	}
	current, err := s.getQueuedRaw(ref.key)
	if err != nil {
		return fmt.Errorf("verify queued main-entry replacement: %w", err)
	}
	if strings.TrimSpace(current.QueueIncarnation) != incarnation {
		return fmt.Errorf(
			"%w: %s queue incarnation changed from %q to %q",
			ErrEntryRediscoveryPending,
			ref.key,
			incarnation,
			current.QueueIncarnation,
		)
	}

	entry.MainGeneration = state.generation
	entry.MainReimportIncarnation = incarnation
	entry.MainMutationProvider = ""
	entry.MainProviderSnapshot = 0
	return nil
}

// ObserveProviderSnapshot records authoritative absences from a successfully
// completed full-list request. The token must have been captured before that
// request started, ensuring a pre-delete response cannot clear a later delete.
func (s *Storage) ObserveProviderSnapshot(provider string, snapshot uint64, present map[string]struct{}) error {
	if s == nil || s.mainEntries == nil || snapshot == 0 {
		return nil
	}
	provider = normalizeMainEntryProvider(provider)
	if provider == "" {
		return nil
	}

	normalizedPresent := make(map[string]struct{}, len(present))
	for key := range present {
		normalizedPresent[normalizeMainEntryKey(key)] = struct{}{}
	}

	// After a restart, unresolved retirements live only in the durable
	// tombstone store until first touched. Materialize those states so an
	// authoritative absent snapshot can advance their handshake.
	var retiredKeys []string
	if err := s.entryTombstones.ForEach(func(key string, _ []byte) error {
		retiredKeys = append(retiredKeys, key)
		return nil
	}); err != nil {
		return fmt.Errorf("scan retired main entries: %w", err)
	}
	for _, key := range retiredKeys {
		ref, err := s.acquireMainEntryState(key)
		if err != nil {
			return err
		}
		ref.release()
	}

	s.mainEntries.mu.Lock()
	states := make([]struct {
		key   string
		state *mainEntryState
	}, 0, len(s.mainEntries.states))
	for key, state := range s.mainEntries.states {
		states = append(states, struct {
			key   string
			state *mainEntryState
		}{key: key, state: state})
	}
	s.mainEntries.mu.Unlock()

	sort.Slice(states, func(i, j int) bool { return states[i].key < states[j].key })

	locked := states[:0]
	for _, item := range states {
		if _, ok := normalizedPresent[item.key]; ok {
			continue
		}
		item.state.mu.Lock()
		if item.state.retired && snapshot > item.state.retiredAt {
			if _, alreadyDurable := item.state.durableAbsent[provider]; !alreadyDurable {
				locked = append(locked, item)
				continue
			}
		}
		item.state.mu.Unlock()
	}

	if len(locked) == 0 {
		return nil
	}
	unlockAll := func() {
		for _, item := range locked {
			item.state.mu.Unlock()
		}
	}

	for _, item := range locked {
		if err := s.persistMainEntryTombstone(
			item.key,
			item.state,
			provider,
			mainEntryTombstoneRetired,
		); err != nil {
			unlockAll()
			return fmt.Errorf("persist provider absence for %s: %w", item.key, err)
		}
	}
	if err := s.entryTombstones.Sync(); err != nil {
		unlockAll()
		return fmt.Errorf("sync provider absence tombstones: %w", err)
	}
	for _, item := range locked {
		item.state.durableAbsent[provider] = struct{}{}
		item.state.absentAt[provider] = snapshot
	}
	unlockAll()
	return nil
}

const mainEntryTombstoneVersion = 1

type mainEntryTombstonePhase string

const (
	mainEntryTombstoneDeleting           mainEntryTombstonePhase = "deleting"
	mainEntryTombstoneRetired            mainEntryTombstonePhase = "retired"
	mainEntryTombstoneReplacementPending mainEntryTombstonePhase = "replacement_pending"
	mainEntryTombstoneQueuePending       mainEntryTombstonePhase = "queue_replacement_pending"
)

type mainEntryTombstone struct {
	Version          int                     `json:"version"`
	Phase            mainEntryTombstonePhase `json:"phase"`
	AbsentProviders  []string                `json:"absent_providers,omitempty"`
	QueueIncarnation string                  `json:"queue_incarnation,omitempty"`
	// Separate administrative obligation; never folded into a client queue intent.
	PlacementSnapshot []byte `json:"placement_snapshot,omitempty"`
}

func (s *Storage) loadMainEntryTombstone(key string) (*mainEntryTombstone, bool, error) {
	if s.entryTombstones == nil {
		return nil, false, fmt.Errorf("main entry tombstone store is not initialized")
	}
	data, err := s.entryTombstones.Get(normalizeMainEntryKey(key))
	if err != nil {
		if hybrid.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("load main entry tombstone: %w", err)
	}
	var tombstone mainEntryTombstone
	if err := json.Unmarshal(data, &tombstone); err != nil {
		return nil, false, fmt.Errorf("decode main entry tombstone: %w", err)
	}
	if tombstone.Version != mainEntryTombstoneVersion {
		return nil, false, fmt.Errorf(
			"unsupported main entry tombstone version %d",
			tombstone.Version,
		)
	}
	switch tombstone.Phase {
	case mainEntryTombstoneDeleting,
		mainEntryTombstoneRetired,
		mainEntryTombstoneReplacementPending,
		mainEntryTombstoneQueuePending:
	default:
		return nil, false, fmt.Errorf(
			"unsupported main entry tombstone phase %q",
			tombstone.Phase,
		)
	}
	if tombstone.Phase == mainEntryTombstoneQueuePending &&
		strings.TrimSpace(tombstone.QueueIncarnation) == "" {
		return nil, false, fmt.Errorf(
			"main entry tombstone phase %q is missing queue incarnation",
			tombstone.Phase,
		)
	}
	return &tombstone, true, nil
}

func (s *Storage) persistMainEntryTombstone(
	key string,
	state *mainEntryState,
	extraProvider string,
	phase mainEntryTombstonePhase,
	queueIncarnationOverride ...string,
) error {
	providers := make(map[string]struct{}, len(state.durableAbsent)+1)
	if phase != mainEntryTombstoneDeleting {
		for provider := range state.durableAbsent {
			providers[normalizeMainEntryProvider(provider)] = struct{}{}
		}
		extraProvider = normalizeMainEntryProvider(extraProvider)
		if extraProvider != "" {
			providers[extraProvider] = struct{}{}
		}
	}
	names := make([]string, 0, len(providers))
	for provider := range providers {
		if provider != "" {
			names = append(names, provider)
		}
	}
	sort.Strings(names)
	queueIncarnation := state.authorizedQueueIncarn
	if len(queueIncarnationOverride) > 0 {
		queueIncarnation = strings.TrimSpace(queueIncarnationOverride[0])
	}
	if phase == mainEntryTombstoneDeleting {
		queueIncarnation = ""
	}
	previous, _, err := s.loadMainEntryTombstone(key)
	if err != nil {
		return err
	}
	var placementSnapshot []byte
	if previous != nil {
		placementSnapshot = previous.PlacementSnapshot
	}
	data, err := json.Marshal(mainEntryTombstone{
		Version:           mainEntryTombstoneVersion,
		Phase:             phase,
		AbsentProviders:   names,
		QueueIncarnation:  queueIncarnation,
		PlacementSnapshot: placementSnapshot,
	})
	if err != nil {
		return fmt.Errorf("encode main entry tombstone: %w", err)
	}
	return s.entryTombstones.Put(normalizeMainEntryKey(key), data, nil)
}

func (s *Storage) clearMainEntryTombstone(key string) error {
	tombstone, _, err := s.loadMainEntryTombstone(key)
	if err != nil {
		return err
	}
	if tombstone != nil && len(tombstone.PlacementSnapshot) != 0 {
		return fmt.Errorf("main provider cleanup remains pending")
	}
	err = s.entryTombstones.Delete(normalizeMainEntryKey(key))
	if err != nil && !hybrid.IsNotFound(err) {
		return err
	}
	return nil
}

// recoverMainEntryTombstones resolves every durable lifecycle phase before
// callers can observe main entries:
//   - deleting: finish local retirement after an interrupted cleanup;
//   - retired: remove any row that was not authorized through rediscovery;
//   - replacement_pending: keep a durable replacement row, or restore retired
//     state when its row was never made durable.
//   - queue_replacement_pending: authorize only the exact durable queue
//     incarnation whose creation began after retirement.
func (s *Storage) recoverMainEntryTombstones() error {
	if s.entryTombstones == nil || s.entries == nil {
		return fmt.Errorf("main entry recovery stores are not initialized")
	}

	type candidate struct {
		key       string
		tombstone mainEntryTombstone
	}
	var candidates []candidate
	if err := s.entryTombstones.ForEach(func(key string, value []byte) error {
		normalized := normalizeMainEntryKey(key)
		if normalized == "" || normalized != key {
			return fmt.Errorf("invalid main entry tombstone key %q", key)
		}
		var tombstone mainEntryTombstone
		if err := json.Unmarshal(value, &tombstone); err != nil {
			return fmt.Errorf("decode main entry tombstone %s: %w", key, err)
		}
		if tombstone.Version != mainEntryTombstoneVersion {
			return fmt.Errorf(
				"unsupported main entry tombstone version %d for %s",
				tombstone.Version,
				key,
			)
		}
		switch tombstone.Phase {
		case mainEntryTombstoneDeleting,
			mainEntryTombstoneRetired,
			mainEntryTombstoneReplacementPending,
			mainEntryTombstoneQueuePending:
		default:
			return fmt.Errorf(
				"unsupported main entry tombstone phase %q for %s",
				tombstone.Phase,
				key,
			)
		}
		if tombstone.Phase == mainEntryTombstoneQueuePending &&
			strings.TrimSpace(tombstone.QueueIncarnation) == "" {
			return fmt.Errorf(
				"main entry tombstone phase %q is missing queue incarnation for %s",
				tombstone.Phase,
				key,
			)
		}
		candidates = append(candidates, candidate{key: key, tombstone: tombstone})
		return nil
	}); err != nil {
		return err
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].key < candidates[j].key
	})
	var entriesChanged bool
	var tombstonesChanged bool
	var tombstonesRewritten bool
	for _, candidate := range candidates {
		rowExists := s.entries.Exists(candidate.key)
		switch candidate.tombstone.Phase {
		case mainEntryTombstoneQueuePending:
			queued, err := s.getQueuedRaw(candidate.key)
			if err != nil {
				if !IsQueuedEntryNotFound(err) {
					return fmt.Errorf(
						"load pending queued replacement %s: %w",
						candidate.key,
						err,
					)
				}
				data, marshalErr := json.Marshal(mainEntryTombstone{
					Version:         mainEntryTombstoneVersion,
					Phase:           mainEntryTombstoneRetired,
					AbsentProviders: candidate.tombstone.AbsentProviders,
				})
				if marshalErr != nil {
					return marshalErr
				}
				if putErr := s.entryTombstones.Put(candidate.key, data, nil); putErr != nil {
					return putErr
				}
				tombstonesRewritten = true
				continue
			}
			if queued.QueueIncarnation != candidate.tombstone.QueueIncarnation {
				return fmt.Errorf(
					"pending queued replacement %s has incarnation %q, durable row has %q",
					candidate.key,
					candidate.tombstone.QueueIncarnation,
					queued.QueueIncarnation,
				)
			}
			data, marshalErr := json.Marshal(mainEntryTombstone{
				Version:          mainEntryTombstoneVersion,
				Phase:            mainEntryTombstoneRetired,
				AbsentProviders:  candidate.tombstone.AbsentProviders,
				QueueIncarnation: candidate.tombstone.QueueIncarnation,
			})
			if marshalErr != nil {
				return marshalErr
			}
			if putErr := s.entryTombstones.Put(candidate.key, data, nil); putErr != nil {
				return putErr
			}
			tombstonesRewritten = true
			continue
		case mainEntryTombstoneReplacementPending:
			if rowExists {
				if err := s.clearMainEntryTombstone(candidate.key); err != nil {
					return fmt.Errorf(
						"finish clearing rediscovered entry tombstone %s: %w",
						candidate.key,
						err,
					)
				}
				tombstonesChanged = true
				continue
			}
			data, err := json.Marshal(mainEntryTombstone{
				Version:          mainEntryTombstoneVersion,
				Phase:            mainEntryTombstoneRetired,
				AbsentProviders:  candidate.tombstone.AbsentProviders,
				QueueIncarnation: candidate.tombstone.QueueIncarnation,
			})
			if err != nil {
				return err
			}
			if err := s.entryTombstones.Put(candidate.key, data, nil); err != nil {
				return err
			}
			tombstonesRewritten = true
			continue
		case mainEntryTombstoneRetired:
			if !rowExists {
				continue
			}
			// A retired record never permits a row by itself. Remove a row
			// whose replacement did not first persist replacement_pending.
		case mainEntryTombstoneDeleting:
			if !rowExists {
				data, err := json.Marshal(mainEntryTombstone{
					Version:           mainEntryTombstoneVersion,
					Phase:             mainEntryTombstoneRetired,
					PlacementSnapshot: candidate.tombstone.PlacementSnapshot,
				})
				if err != nil {
					return err
				}
				if err := s.entryTombstones.Put(candidate.key, data, nil); err != nil {
					return err
				}
				tombstonesRewritten = true
				continue
			}
		}

		entry, err := s.getMainRaw(candidate.key)
		if err != nil {
			return fmt.Errorf("load interrupted main deletion %s: %w", candidate.key, err)
		}
		if err := s.deleteMainRaw(entry); err != nil {
			return fmt.Errorf("finish interrupted main deletion %s: %w", candidate.key, err)
		}
		entriesChanged = true
	}

	if entriesChanged {
		if err := s.entries.Sync(); err != nil {
			return fmt.Errorf("sync recovered main entry deletions: %w", err)
		}
	}
	if tombstonesChanged || tombstonesRewritten {
		if err := s.entryTombstones.Sync(); err != nil {
			return fmt.Errorf("sync recovered main entry tombstones: %w", err)
		}
	}
	return nil
}
