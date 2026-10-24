package storage

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Trifocals3537/tessarr/internal/workflow"
	"github.com/google/uuid"
)

func firstCleanupToken(tokens []string) string {
	if len(tokens) == 1 {
		return tokens[0]
	}
	return ""
}
func cleanupToken(t *queueDeletionTombstone, token string) error {
	if t.Work == nil {
		if token == "" {
			return nil
		}
		return workflow.ErrClaim
	}
	return t.Work.Check(token)
}

// mutateCleanupWork publishes only after fsync. The queue incarnation and work
// revision live in the same authoritative record as immutable cleanup scope.
func (s *Storage) mutateCleanupWork(key, incarnation string, edit func(*queueDeletionTombstone) error) error {
	ref, err := s.acquireMainEntryState(key)
	if err != nil {
		return err
	}
	defer ref.release()
	ref.state.mu.Lock()
	defer ref.state.mu.Unlock()
	t, found, err := s.loadQueueDeletionTombstone(ref.key)
	if err != nil {
		return err
	}
	if !found || incarnation == "" || t.QueueIncarnation != incarnation {
		return ErrQueuedDeletionIdentityMismatch
	}
	if err := edit(t); err != nil {
		return err
	}
	if err := t.Work.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(t)
	if err != nil {
		return err
	}
	if err := s.queueTombstones.PutExistingSynced(ref.key, data, nil); err != nil {
		return fmt.Errorf("checkpoint cleanup workflow: %w", err)
	}
	return nil
}

func (s *Storage) ClaimCleanup(key, incarnation, owner string, accounts map[string]string, now time.Time, restart bool) (*QueueDeletionIntent, error) {
	token := uuid.NewString()
	var claimed *QueueDeletionIntent
	err := s.mutateCleanupWork(key, incarnation, func(t *queueDeletionTombstone) error {
		if t.Work == nil {
			t.Work = workflow.NewRecord(now)
			// Old cleanup_started rows may already have sent a provider request.
			t.Work.EffectStarted = t.PlacementCleanupPending && t.Phase != queueDeletionPrepared
			t.Work.Accounts = accounts
		}
		if t.Work.Attempt == 0 && t.Work.Accounts == nil {
			t.Work.Accounts = accounts
		}
		if err := t.Work.Claim(owner, token, now, restart); err != nil {
			return err
		}
		var err error
		claimed, err = queueDeletionIntentFromTombstone(normalizeMainEntryKey(key), t)
		return err
	})
	if err != nil {
		return nil, err
	}
	return claimed, nil
}

func (s *Storage) CleanupEffectStarted(key, incarnation, token string, now time.Time) error {
	return s.mutateCleanupWork(key, incarnation, func(t *queueDeletionTombstone) error {
		if err := cleanupToken(t, token); err != nil {
			return err
		}
		t.Work.EffectStarted = true
		t.Work.Revision++
		t.Work.UpdatedAt = now
		return nil
	})
}
func (s *Storage) ReleaseCleanup(key, incarnation, token, reason string, blocked bool, now time.Time) error {
	return s.mutateCleanupWork(key, incarnation, func(t *queueDeletionTombstone) error { return t.Work.Release(token, reason, blocked, now) })
}
func (s *Storage) ResolveCleanup(key, incarnation string, revision uint64, id, action string, now time.Time) error {
	if err := s.DurabilityError(); err != nil {
		return err
	}
	err := s.mutateCleanupWork(key, incarnation, func(t *queueDeletionTombstone) error {
		if t.Work == nil {
			if revision != 0 {
				return workflow.ErrClaim
			}
			t.Work = workflow.NewRecord(now)
			revision = t.Work.Revision
			t.Work.EffectStarted = t.PlacementCleanupPending && t.Phase != queueDeletionPrepared
		}
		return t.Work.Resolve(revision, id, action, now)
	})
	if err != ErrQueuedDeletionIdentityMismatch {
		return err
	}
	// A retried HTTP request may arrive after the job completed. Its receipt
	// acknowledges only that exact old incarnation/decision, never a replacement.
	digest := sha256.Sum256([]byte(normalizeMainEntryKey(key) + "\x00" + incarnation))
	data, readErr := s.storageState.Get(fmt.Sprintf("cleanup_receipt/%x", digest))
	if readErr != nil {
		return err
	}
	var receipt CleanupReceipt
	if json.Unmarshal(data, &receipt) != nil || receipt.Key != normalizeMainEntryKey(key) || receipt.Incarnation != incarnation || receipt.Work == nil {
		return err
	}
	for _, decision := range receipt.Work.Decisions {
		if decision.ID == id && decision.Action == action {
			return nil
		}
	}
	return err
}

func (s *Storage) CleanupAdminPending(key string) (bool, error) {
	ref, err := s.acquireMainEntryState(key)
	if err != nil {
		return false, err
	}
	defer ref.release()
	ref.state.mu.Lock()
	defer ref.state.mu.Unlock()
	if err := s.syncMainCleanupState(); err != nil {
		return false, err
	}
	t, found, err := s.loadMainEntryTombstone(ref.key)
	if err != nil {
		return false, err
	}
	return found && len(t.PlacementSnapshot) != 0, nil
}
func (s *Storage) StartCleanup(i *QueueDeletionIntent) error {
	return s.updateQueuedDeletion(i.InfoHash, i.QueueIncarnation, func(t *queueDeletionTombstone) error {
		if t.Phase == queueDeletionPrepared {
			t.Phase = queueDeletionCleanupStarted
		}
		return nil
	}, i.Work.Token)
}
func (s *Storage) RetireCleanup(i *QueueDeletionIntent) error {
	return s.retireQueuedDeletionRow(i.InfoHash, i.QueueIncarnation, i.Work.Token)
}
func (s *Storage) CleanupPlacementsClean(i *QueueDeletionIntent) error {
	return s.updateQueuedDeletion(i.InfoHash, i.QueueIncarnation, func(t *queueDeletionTombstone) error { t.PlacementCleanupPending = false; return nil }, i.Work.Token)
}

type CleanupReceipt struct {
	Key          string           `json:"key"`
	Incarnation  string           `json:"incarnation"`
	Work         *workflow.Record `json:"work"`
	CheckpointAt time.Time        `json:"checkpoint_at"`
	// A receipt is written before removing the intent. Callers must inspect
	// IntentPresent; a receipt alone never asserts that completion committed.
	IntentPresent bool `json:"intent_present"`
}

func (s *Storage) checkpointCleanupReceipt(key string, t *queueDeletionTombstone) error {
	receipt := CleanupReceipt{Key: key, Incarnation: t.QueueIncarnation, Work: t.Work, CheckpointAt: time.Now()}
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	id := sha256.Sum256([]byte(key + "\x00" + t.QueueIncarnation))
	if err := s.storageState.Put(fmt.Sprintf("cleanup_receipt/%x", id), data, nil); err != nil {
		return err
	}
	if err := s.storageState.Sync(); err != nil {
		return err
	}
	if s.cleanupReceiptForTest != nil {
		s.cleanupReceiptForTest()
	}
	return nil
}
func (s *Storage) CompleteCleanup(i *QueueDeletionIntent, local func() error) error {
	if local == nil {
		return s.completeQueuedDeletion(i.InfoHash, i.QueueIncarnation, i.Work.Token)
	}
	return s.completeQueuedDeletion(i.InfoHash, i.QueueIncarnation, i.Work.Token, local)
}
func (s *Storage) CleanupReceipts(limit int) ([]CleanupReceipt, error) {
	if limit < 1 || limit > 100 {
		limit = 100
	}
	result := []CleanupReceipt{}
	err := s.storageState.ForEach(func(key string, data []byte) error {
		if len(result) >= limit || len(key) < 16 || key[:16] != "cleanup_receipt/" {
			return nil
		}
		var r CleanupReceipt
		if err := json.Unmarshal(data, &r); err != nil {
			return err
		}
		t, found, err := s.loadQueueDeletionTombstone(r.Key)
		if err != nil {
			return err
		}
		r.IntentPresent = found && t.QueueIncarnation == r.Incarnation
		result = append(result, r)
		return nil
	})
	return result, err
}
