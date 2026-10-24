package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/workflow"
	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrQueuedEntryDeleting means a durable delete intent owns this queue key.
	// Reads hide the row as not found; mutations return this error explicitly.
	ErrQueuedEntryDeleting = errors.New("queued entry is being deleted")

	// ErrQueuedDeletionIdentityMismatch means a delete intent no longer names
	// the exact durable queue incarnation it originally snapshotted.
	ErrQueuedDeletionIdentityMismatch = errors.New("queued deletion identity mismatch")
)

const queueDeletionTombstoneVersion = 1

const QueuePlacementScopeTransferOnly = "queued_transfer_only"
const QueuePlacementScopeAll = "all_placements"

func (s *Storage) syncQueueDeletionRow() error {
	if s.queueRowSyncForTest != nil {
		return s.queueRowSyncForTest()
	}
	return s.queue.Sync()
}

func (s *Storage) syncQueueDeletionIntent() error {
	if s.queueIntentSyncForTest != nil {
		return s.queueIntentSyncForTest()
	}
	return s.queueTombstones.Sync()
}

type queueDeletionPhase string

const (
	queueDeletionPrepared       queueDeletionPhase = "prepared"
	queueDeletionCleanupStarted queueDeletionPhase = "cleanup_started"
	queueDeletionRowRetired     queueDeletionPhase = "row_retired"
)

type queueDeletionTombstone struct {
	Work                        *workflow.Record   `json:"work,omitempty"`
	PlacementScope              string             `json:"placement_scope,omitempty"`
	Version                     int                `json:"version"`
	Phase                       queueDeletionPhase `json:"phase"`
	QueueIncarnation            string             `json:"queue_incarnation"`
	Snapshot                    []byte             `json:"snapshot"`
	PlacementSnapshots          [][]byte           `json:"placement_snapshots,omitempty"`
	PlacementCleanupPending     bool               `json:"placement_cleanup_pending,omitempty"`
	UnrecoverableCleanupPending bool               `json:"unrecoverable_cleanup_pending,omitempty"`
	PreserveFiles               bool               `json:"preserve_files,omitempty"`
}

// QueueDeletionIntent is a validated, immutable view of a durable queue
// tombstone. Entry is the exact queue incarnation captured before cleanup.
// PlacementEntries are additional same-key snapshots (for example, a main
// entry) needed to retry provider placement cleanup after a restart.
type QueueDeletionIntent struct {
	Work                        *workflow.Record
	PlacementScope              string
	InfoHash                    string
	QueueIncarnation            string
	Phase                       string
	Entry                       *Entry
	PlacementEntries            []*Entry
	PlacementCleanupPending     bool
	UnrecoverableCleanupPending bool
	PreserveFiles               bool
}

func (s *Storage) loadQueueDeletionTombstone(
	key string,
) (*queueDeletionTombstone, bool, error) {
	if s.queueTombstones == nil {
		return nil, false, fmt.Errorf("queue deletion tombstone store is not initialized")
	}
	key = normalizeMainEntryKey(key)
	data, err := s.queueTombstones.Get(key)
	if err != nil {
		if hybrid.IsNotFound(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("load queue deletion tombstone: %w", err)
	}
	tombstone, err := decodeQueueDeletionTombstone(key, data)
	if err != nil {
		return nil, false, err
	}
	return tombstone, true, nil
}

func decodeQueueDeletionTombstone(
	key string,
	data []byte,
) (*queueDeletionTombstone, error) {
	var tombstone queueDeletionTombstone
	if err := json.Unmarshal(data, &tombstone); err != nil {
		return nil, fmt.Errorf("decode queue deletion tombstone %s: %w", key, err)
	}
	if err := tombstone.Work.Validate(); err != nil {
		return nil, err
	}
	if tombstone.Version != queueDeletionTombstoneVersion {
		return nil, fmt.Errorf(
			"unsupported queue deletion tombstone version %d for %s",
			tombstone.Version,
			key,
		)
	}
	switch tombstone.PlacementScope {
	case "", QueuePlacementScopeAll, QueuePlacementScopeTransferOnly:
	default:
		return nil, fmt.Errorf("unsupported queue placement scope %q", tombstone.PlacementScope)
	}
	if tombstone.PlacementScope == QueuePlacementScopeTransferOnly && (len(tombstone.PlacementSnapshots) > 1 || tombstone.UnrecoverableCleanupPending || (tombstone.PlacementCleanupPending && len(tombstone.PlacementSnapshots) != 1)) {
		return nil, fmt.Errorf("invalid transfer-only cleanup intent")
	}
	switch tombstone.Phase {
	case queueDeletionPrepared,
		queueDeletionCleanupStarted,
		queueDeletionRowRetired:
	default:
		return nil, fmt.Errorf(
			"unsupported queue deletion tombstone phase %q for %s",
			tombstone.Phase,
			key,
		)
	}
	if strings.TrimSpace(tombstone.QueueIncarnation) == "" {
		return nil, fmt.Errorf(
			"%w: %s tombstone is missing queue incarnation",
			ErrQueuedDeletionIdentityMismatch,
			key,
		)
	}
	entry, err := decodeQueuedEntry(key, tombstone.Snapshot)
	if err != nil {
		return nil, fmt.Errorf("decode queued deletion snapshot: %w", err)
	}
	if entry.QueueIncarnation != tombstone.QueueIncarnation {
		return nil, fmt.Errorf(
			"%w: %s tombstone names incarnation %q, snapshot contains %q",
			ErrQueuedDeletionIdentityMismatch,
			key,
			tombstone.QueueIncarnation,
			entry.QueueIncarnation,
		)
	}
	for i, snapshot := range tombstone.PlacementSnapshots {
		if _, err := decodeEntryForKey(
			key,
			snapshot,
			ErrQueuedDeletionIdentityMismatch,
			"queue deletion placement",
		); err != nil {
			return nil, fmt.Errorf("decode placement snapshot %d: %w", i, err)
		}
	}
	if tombstone.PlacementScope == QueuePlacementScopeTransferOnly && len(tombstone.PlacementSnapshots) == 1 {
		active, err := decodeQueuedEntry(key, tombstone.PlacementSnapshots[0])
		if err != nil {
			return nil, err
		}
		if !FailedUncachedHandoff(entry) || !FailedUncachedHandoff(active) || len(active.Providers) != 1 {
			return nil, fmt.Errorf("invalid transfer placement proof")
		}
		want, err := handoffFingerprint(entry)
		if err != nil {
			return nil, err
		}
		got, err := handoffFingerprint(active)
		if err != nil {
			return nil, err
		}
		if string(want) != string(got) {
			return nil, fmt.Errorf("transfer snapshot does not match authorized queue incarnation/placement")
		}
	}
	return &tombstone, nil
}

func queueDeletionIntentFromTombstone(
	key string,
	tombstone *queueDeletionTombstone,
) (*QueueDeletionIntent, error) {
	if tombstone == nil {
		return nil, fmt.Errorf("queue deletion tombstone is nil")
	}
	entry, err := decodeQueuedEntry(key, tombstone.Snapshot)
	if err != nil {
		return nil, err
	}
	placementEntries := make([]*Entry, 0, len(tombstone.PlacementSnapshots))
	for _, snapshot := range tombstone.PlacementSnapshots {
		entry, err := decodeEntryForKey(
			key,
			snapshot,
			ErrQueuedDeletionIdentityMismatch,
			"queue deletion placement",
		)
		if err != nil {
			return nil, err
		}
		placementEntries = append(placementEntries, entry)
	}
	return &QueueDeletionIntent{
		InfoHash:                    key,
		Work:                        tombstone.Work,
		QueueIncarnation:            tombstone.QueueIncarnation,
		Phase:                       string(tombstone.Phase),
		Entry:                       entry,
		PlacementEntries:            placementEntries,
		PlacementScope:              tombstone.PlacementScope,
		PlacementCleanupPending:     tombstone.PlacementCleanupPending,
		UnrecoverableCleanupPending: tombstone.UnrecoverableCleanupPending,
		PreserveFiles:               tombstone.PreserveFiles,
	}, nil
}

func marshalQueueDeletionSnapshot(key string, entry *Entry) ([]byte, error) {
	if entry == nil {
		return nil, fmt.Errorf("queue deletion snapshot is nil")
	}
	if normalizeMainEntryKey(entry.InfoHash) != key {
		return nil, fmt.Errorf(
			"%w: key %q cannot snapshot entry %q",
			ErrQueuedDeletionIdentityMismatch,
			key,
			entry.InfoHash,
		)
	}
	data, err := proto.Marshal(EntryToProto(entry))
	if err != nil {
		return nil, fmt.Errorf("encode queue deletion snapshot: %w", err)
	}
	return data, nil
}

func appendQueueDeletionSnapshots(
	key string,
	existing [][]byte,
	entries []*Entry,
) ([][]byte, bool, error) {
	result := make([][]byte, len(existing))
	copy(result, existing)
	seen := make(map[string]struct{}, len(existing))
	for _, snapshot := range existing {
		seen[string(snapshot)] = struct{}{}
	}
	changed := false
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		snapshot, err := marshalQueueDeletionSnapshot(key, entry)
		if err != nil {
			return nil, false, err
		}
		if _, found := seen[string(snapshot)]; found {
			continue
		}
		seen[string(snapshot)] = struct{}{}
		result = append(result, snapshot)
		changed = true
	}
	return result, changed, nil
}

func (s *Storage) persistQueueDeletionTombstone(
	key string,
	tombstone *queueDeletionTombstone,
) error {
	data, err := json.Marshal(tombstone)
	if err != nil {
		return fmt.Errorf("encode queue deletion tombstone: %w", err)
	}
	if err := s.queueTombstones.Put(key, data, nil); err != nil {
		return fmt.Errorf("persist queue deletion tombstone: %w", err)
	}
	return nil
}

// PrepareQueuedDeletion durably captures the exact final queue row before any
// provider or filesystem cleanup starts. Repeated calls resume the same
// incarnation and may add placement snapshots to an already pending intent.
func (s *Storage) PrepareQueuedDeletion(
	infohash string,
	placementCleanup bool,
	placementSnapshots ...*Entry,
) (*QueueDeletionIntent, error) {
	return s.prepareQueuedDeletion(
		infohash,
		placementCleanup,
		false,
		false,
		QueuePlacementScopeAll,
		placementSnapshots...,
	)
}

// PrepareQueuedDeletionPreservingFiles records that queue retirement must not
// delete local data, including when cleanup resumes after a process restart.
func (s *Storage) PrepareQueuedDeletionPreservingFiles(
	infohash string,
) (*QueueDeletionIntent, error) {
	return s.prepareQueuedDeletion(infohash, false, false, true, QueuePlacementScopeAll)
}

// PrepareQBitQueuedDeletion classifies the final drained queue incarnation under
// the storage lock. Routine removal never authorizes provider/main destruction.
func (s *Storage) PrepareQBitQueuedDeletion(infohash string, deleteFiles bool) (*QueueDeletionIntent, error) {
	return s.prepareQueuedDeletion(infohash, false, false, !deleteFiles, QueuePlacementScopeTransferOnly)
}

func (s *Storage) prepareQueuedDeletion(
	infohash string,
	placementCleanup bool,
	unrecoverableCleanup bool,
	preserveFiles bool,
	scope string,
	placementSnapshots ...*Entry,
) (*QueueDeletionIntent, error) {
	key := normalizeMainEntryKey(infohash)
	ref, err := s.acquireMainEntryState(key)
	if err != nil {
		return nil, err
	}
	defer ref.release()

	state := ref.state
	state.mu.Lock()
	defer state.mu.Unlock()

	tombstone, found, err := s.loadQueueDeletionTombstone(key)
	if err != nil {
		return nil, err
	}
	if found {
		if tombstone.Work != nil {
			return nil, workflow.ErrClaim
		}
		if tombstone.PlacementScope == QueuePlacementScopeTransferOnly {
			if scope == QueuePlacementScopeAll && placementCleanup && len(placementSnapshots) == 0 {
				return nil, fmt.Errorf("queue-only administrative cleanup cannot adopt immutable client intent; finish client cleanup and reconcile provider placements manually")
			}
			if scope == QueuePlacementScopeTransferOnly && preserveFiles && !tombstone.PreserveFiles {
				return nil, fmt.Errorf("preserving retry cannot resume pending file deletion; retry original deleteFiles=true operation")
			}
			return queueDeletionIntentFromTombstone(key, tombstone)
		}
		if scope == QueuePlacementScopeTransferOnly {
			return nil, fmt.Errorf("qbit cleanup cannot resume legacy destructive intent")
		}
		state.queueDeleting = true
		changed := false
		if placementCleanup && !tombstone.PlacementCleanupPending {
			tombstone.PlacementCleanupPending = true
			changed = true
		}
		if unrecoverableCleanup && !tombstone.UnrecoverableCleanupPending {
			tombstone.UnrecoverableCleanupPending = true
			changed = true
		}
		if preserveFiles && !tombstone.PreserveFiles {
			tombstone.PreserveFiles = true
			changed = true
		}
		var snapshotsChanged bool
		tombstone.PlacementSnapshots, snapshotsChanged, err =
			appendQueueDeletionSnapshots(
				key,
				tombstone.PlacementSnapshots,
				placementSnapshots,
			)
		if err != nil {
			return nil, err
		}
		if snapshotsChanged {
			changed = true
		}
		if changed {
			if err := s.persistQueueDeletionTombstone(key, tombstone); err != nil {
				return nil, err
			}
			if err := s.queueTombstones.Sync(); err != nil {
				return nil, fmt.Errorf("sync updated queue deletion intent: %w", err)
			}
		}
		return queueDeletionIntentFromTombstone(key, tombstone)
	}

	if state.queueDeleting {
		// Completion may have removed the intent from the memory index before
		// its sync failed. A client retry must finish that barrier before an
		// absent row can count as success or a replacement can be admitted.
		if err := s.syncQueueDeletionIntent(); err != nil {
			return nil, fmt.Errorf("sync cleared queue deletion intent: %w", err)
		}
		state.queueDeleting = false
	}
	entry, err := s.getQueuedRaw(key)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(entry.QueueIncarnation) == "" {
		entry.QueueIncarnation = uuid.NewString()
		if err := s.writeQueueRaw(entry, true); err != nil {
			return nil, fmt.Errorf("assign queued entry incarnation: %w", err)
		}
		if err := s.queue.Sync(); err != nil {
			return nil, fmt.Errorf("sync queued entry incarnation: %w", err)
		}
	}
	if scope == QueuePlacementScopeTransferOnly && !preserveFiles {
		active, err := s.authorizedTransferSnapshot(entry)
		if err != nil {
			return nil, err
		}
		placementCleanup = active != nil
		placementSnapshots = nil
		if active != nil {
			placementSnapshots = []*Entry{active}
		}
	}
	snapshot, err := marshalQueueDeletionSnapshot(key, entry)
	if err != nil {
		return nil, err
	}
	placementData, _, err := appendQueueDeletionSnapshots(
		key,
		nil,
		placementSnapshots,
	)
	if err != nil {
		return nil, err
	}
	tombstone = &queueDeletionTombstone{
		Version:                     queueDeletionTombstoneVersion,
		PlacementScope:              scope,
		Phase:                       queueDeletionPrepared,
		QueueIncarnation:            entry.QueueIncarnation,
		Snapshot:                    snapshot,
		PlacementSnapshots:          placementData,
		PlacementCleanupPending:     placementCleanup,
		UnrecoverableCleanupPending: unrecoverableCleanup,
		PreserveFiles:               preserveFiles,
	}

	// Once Put has been attempted, keep the in-process key closed even when
	// durability reports an error: the caller must not start cleanup, while a
	// later retry or restart can authoritatively inspect the store.
	state.queueDeleting = true
	if err := s.persistQueueDeletionTombstone(key, tombstone); err != nil {
		return nil, err
	}
	if err := s.queueTombstones.Sync(); err != nil {
		return nil, fmt.Errorf("sync queue deletion intent: %w", err)
	}
	return queueDeletionIntentFromTombstone(key, tombstone)
}

func (s *Storage) updateQueuedDeletion(
	infohash string,
	incarnation string,
	update func(*queueDeletionTombstone) error,
	claim ...string,
) error {
	key := normalizeMainEntryKey(infohash)
	ref, err := s.acquireMainEntryState(key)
	if err != nil {
		return err
	}
	defer ref.release()

	state := ref.state
	state.mu.Lock()
	defer state.mu.Unlock()

	tombstone, found, err := s.loadQueueDeletionTombstone(key)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: no durable intent for %s", ErrQueuedEntryNotFound, key)
	}
	state.queueDeleting = true
	if strings.TrimSpace(incarnation) == "" ||
		tombstone.QueueIncarnation != incarnation {
		return fmt.Errorf(
			"%w: %s intent has incarnation %q, caller supplied %q",
			ErrQueuedDeletionIdentityMismatch,
			key,
			tombstone.QueueIncarnation,
			incarnation,
		)
	}
	if err := cleanupToken(tombstone, firstCleanupToken(claim)); err != nil {
		return err
	}
	if err := update(tombstone); err != nil {
		return err
	}
	if err := s.persistQueueDeletionTombstone(key, tombstone); err != nil {
		return err
	}
	if err := s.queueTombstones.Sync(); err != nil {
		return fmt.Errorf("sync queue deletion transition: %w", err)
	}
	return nil
}

// StartQueuedDeletionCleanup durably records that destructive cleanup may have
// started. Recovery never rolls an intent back after this transition.
func (s *Storage) StartQueuedDeletionCleanup(
	infohash string,
	incarnation string,
) error {
	return s.updateQueuedDeletion(
		infohash,
		incarnation,
		func(tombstone *queueDeletionTombstone) error {
			if tombstone.Phase == queueDeletionPrepared {
				tombstone.Phase = queueDeletionCleanupStarted
			}
			return nil
		},
	)
}

func (s *Storage) MarkQueuedDeletionPlacementsClean(
	infohash string,
	incarnation string,
) error {
	return s.updateQueuedDeletion(
		infohash,
		incarnation,
		func(tombstone *queueDeletionTombstone) error {
			if tombstone.Phase == queueDeletionPrepared {
				return fmt.Errorf("queue deletion cleanup has not started")
			}
			tombstone.PlacementCleanupPending = false
			return nil
		},
	)
}

func (s *Storage) markQueuedDeletionOpaqueCleanupComplete(
	infohash string,
	incarnation string,
) error {
	return s.updateQueuedDeletion(
		infohash,
		incarnation,
		func(tombstone *queueDeletionTombstone) error {
			if tombstone.Phase == queueDeletionPrepared {
				return fmt.Errorf("queue deletion cleanup has not started")
			}
			tombstone.UnrecoverableCleanupPending = false
			return nil
		},
	)
}

// RetireQueuedDeletionRow removes only the exact snapshotted incarnation. A
// replacement row can never be consumed by recovery or a stale delete retry.
func (s *Storage) RetireQueuedDeletionRow(infohash, incarnation string) error {
	return s.retireQueuedDeletionRow(infohash, incarnation, "")
}

func (s *Storage) retireQueuedDeletionRow(infohash, incarnation, token string) error {
	key := normalizeMainEntryKey(infohash)
	ref, err := s.acquireMainEntryState(key)
	if err != nil {
		return err
	}
	defer ref.release()

	state := ref.state
	state.mu.Lock()
	defer state.mu.Unlock()

	tombstone, found, err := s.loadQueueDeletionTombstone(key)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: no durable intent for %s", ErrQueuedEntryNotFound, key)
	}
	state.queueDeleting = true
	if err := cleanupToken(tombstone, token); err != nil {
		return err
	}
	if tombstone.QueueIncarnation != incarnation {
		return fmt.Errorf(
			"%w: %s intent has incarnation %q, caller supplied %q",
			ErrQueuedDeletionIdentityMismatch,
			key,
			tombstone.QueueIncarnation,
			incarnation,
		)
	}
	if tombstone.Phase == queueDeletionPrepared {
		return fmt.Errorf("queue deletion cleanup has not started")
	}

	current, err := s.getQueuedRaw(key)
	switch {
	case err == nil:
		if current.QueueIncarnation != incarnation {
			return fmt.Errorf(
				"%w: %s intent has incarnation %q, durable row has %q",
				ErrQueuedDeletionIdentityMismatch,
				key,
				incarnation,
				current.QueueIncarnation,
			)
		}
		if err := s.queue.Delete(key); err != nil {
			return fmt.Errorf("retire queued deletion row: %w", err)
		}
	case IsQueuedEntryNotFound(err):
		// The row may be absent only from memory after a failed sync. Retry
		// the durability barrier before advancing the same exact intent.
	default:
		return fmt.Errorf("inspect queued deletion row: %w", err)
	}
	if err := s.syncQueueDeletionRow(); err != nil {
		return fmt.Errorf("sync retired queued deletion row: %w", err)
	}

	if tombstone.Phase != queueDeletionRowRetired {
		tombstone.Phase = queueDeletionRowRetired
		if err := s.persistQueueDeletionTombstone(key, tombstone); err != nil {
			return err
		}
		if err := s.queueTombstones.Sync(); err != nil {
			return fmt.Errorf("sync retired queue deletion intent: %w", err)
		}
	}
	return nil
}

// CompleteQueuedDeletion clears an intent only after its row is durably absent
// and every representable cleanup obligation has been acknowledged. Optional
// localCleanup callbacks run under the per-hash lifecycle lock, after the admin
// durability gate. They must only perform local file work: never reenter Storage
// or make provider/network calls.
func (s *Storage) CompleteQueuedDeletion(infohash, incarnation string, localCleanup ...func() error) error {
	return s.completeQueuedDeletion(infohash, incarnation, "", localCleanup...)
}

func (s *Storage) completeQueuedDeletion(infohash, incarnation, token string, localCleanup ...func() error) error {
	key := normalizeMainEntryKey(infohash)
	ref, err := s.acquireMainEntryState(key)
	if err != nil {
		return err
	}
	defer ref.release()

	state := ref.state
	state.mu.Lock()
	defer state.mu.Unlock()

	tombstone, found, err := s.loadQueueDeletionTombstone(key)
	if err != nil {
		return err
	}
	if !found {
		if state.queueDeleting {
			if err := s.syncQueueDeletionIntent(); err != nil {
				return fmt.Errorf("sync cleared queue deletion intent: %w", err)
			}
			state.queueDeleting = false
		}
		return nil
	}
	state.queueDeleting = true
	if err := cleanupToken(tombstone, token); err != nil {
		return err
	}
	if tombstone.QueueIncarnation != incarnation {
		return fmt.Errorf(
			"%w: %s intent has incarnation %q, caller supplied %q",
			ErrQueuedDeletionIdentityMismatch,
			key,
			tombstone.QueueIncarnation,
			incarnation,
		)
	}
	if tombstone.Phase != queueDeletionRowRetired {
		return fmt.Errorf("queue deletion row has not been retired")
	}
	if tombstone.PlacementCleanupPending {
		return fmt.Errorf("queue deletion still requires provider placement cleanup")
	}
	if tombstone.UnrecoverableCleanupPending {
		return fmt.Errorf("queue deletion still requires non-recoverable cleanup")
	}
	if _, err := s.getQueuedRaw(key); err == nil {
		return fmt.Errorf("queue deletion row still exists")
	} else if !IsQueuedEntryNotFound(err) {
		return fmt.Errorf("verify retired queue deletion row: %w", err)
	}
	// A cleared in-memory admin snapshot is not proof of durable completion.
	// Sync before consulting it, under the same key lock used to install it.
	// Keep that lock through local cleanup and intent completion so a concurrent
	// administrator cannot install an obligation in the check/use interval.
	if err := s.syncMainCleanupState(); err != nil {
		return err
	}
	admin, adminFound, err := s.loadMainEntryTombstone(key)
	if err != nil {
		return err
	}
	if adminFound && len(admin.PlacementSnapshot) != 0 {
		return fmt.Errorf("queue deletion awaits administrative placement cleanup")
	}
	for _, cleanup := range localCleanup {
		if err := cleanup(); err != nil {
			return err
		}
	}
	if token != "" {
		if err := s.checkpointCleanupReceipt(key, tombstone); err != nil {
			return err
		}
	}
	if err := s.storageState.Delete(handoffAuthorizationPrefix + key); err != nil && !hybrid.IsNotFound(err) {
		return err
	}
	if err := s.syncHandoffState(); err != nil {
		return err
	}
	s.handoffPending.Delete(handoffAuthorizationPrefix + key)
	if err := s.queueTombstones.Delete(key); err != nil &&
		!hybrid.IsNotFound(err) {
		return fmt.Errorf("clear queue deletion intent: %w", err)
	}
	if err := s.syncQueueDeletionIntent(); err != nil {
		return fmt.Errorf("sync cleared queue deletion intent: %w", err)
	}
	state.queueDeleting = false
	return nil
}

// QueuedDeletionIntents returns every validated tombstone in deterministic key
// order. Manager startup consumes this before restoring any queued jobs.
func (s *Storage) QueuedDeletionIntents() ([]*QueueDeletionIntent, error) {
	if s.queueTombstones == nil {
		return nil, fmt.Errorf("queue deletion tombstone store is not initialized")
	}
	var intents []*QueueDeletionIntent
	if err := s.queueTombstones.ForEach(func(key string, value []byte) error {
		normalized := normalizeMainEntryKey(key)
		if normalized == "" || normalized != key {
			return fmt.Errorf("invalid queue deletion tombstone key %q", key)
		}
		tombstone, err := decodeQueueDeletionTombstone(key, value)
		if err != nil {
			return err
		}
		intent, err := queueDeletionIntentFromTombstone(key, tombstone)
		if err != nil {
			return err
		}
		intents = append(intents, intent)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("scan queue deletion tombstones: %w", err)
	}
	sort.Slice(intents, func(i, j int) bool {
		return intents[i].InfoHash < intents[j].InfoHash
	})
	return intents, nil
}

func (s *Storage) queuedEntryVisible(infohash string) (bool, error) {
	ref, err := s.acquireMainEntryState(infohash)
	if err != nil {
		return false, err
	}
	defer ref.release()

	state := ref.state
	state.mu.Lock()
	defer state.mu.Unlock()
	return !state.queueDeleting, nil
}
