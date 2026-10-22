package storage

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
	"github.com/google/uuid"
)

var ErrProviderSubmissionConflict = errors.New("provider submission requires reconciliation of its original queue incarnation")

type ProviderSubmissionPhase string

const (
	SubmissionPending  ProviderSubmissionPhase = "pending"
	SubmissionCleanup  ProviderSubmissionPhase = "cleanup"
	SubmissionRejected ProviderSubmissionPhase = "rejected"
	SubmissionBound    ProviderSubmissionPhase = "bound"
)

// ProviderSubmission is an intent for one account-scoped create request. It
// carries no credentials, magnets, provider response bodies, or delete grants
// derived from hash matching. A pending intent is never permission to resubmit.
type ProviderSubmission struct {
	Version          int                     `json:"version"`
	InfoHash         string                  `json:"info_hash"`
	QueueIncarnation string                  `json:"queue_incarnation"`
	Attempt          string                  `json:"attempt"`
	Provider         string                  `json:"provider"`
	Account          string                  `json:"account"`
	DownloadUncached bool                    `json:"download_uncached"`
	Phase            ProviderSubmissionPhase `json:"phase"`
	ProviderID       string                  `json:"provider_id,omitempty"`
	Recovered        bool                    `json:"recovered,omitempty"`
	UpdatedAt        time.Time               `json:"updated_at"`
}

func submissionKey(hash string) string {
	return "provider_submission/" + hex.EncodeToString([]byte(normalizeMainEntryKey(hash)))
}

func (s *Storage) submissionRaw(hash string) (*ProviderSubmission, error) {
	data, err := s.storageState.Get(submissionKey(hash))
	if hybrid.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var intent ProviderSubmission
	if err := json.Unmarshal(data, &intent); err != nil {
		return nil, fmt.Errorf("decode provider submission: %w", err)
	}
	if intent.Version != 1 || intent.InfoHash != normalizeMainEntryKey(hash) || intent.QueueIncarnation == "" || intent.Attempt == "" || intent.Provider == "" || intent.Account == "" {
		return nil, fmt.Errorf("invalid provider submission identity")
	}
	switch intent.Phase {
	case SubmissionPending, SubmissionRejected:
	case SubmissionCleanup, SubmissionBound:
		if intent.ProviderID == "" {
			return nil, fmt.Errorf("provider submission phase requires an ID")
		}
	default:
		return nil, fmt.Errorf("invalid provider submission phase")
	}
	return &intent, nil
}

func (s *Storage) saveSubmission(intent *ProviderSubmission) error {
	intent.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(intent)
	if err != nil {
		return err
	}
	if err := s.storageState.Put(submissionKey(intent.InfoHash), data, nil); err != nil {
		return err
	}
	if s.submissionSyncForTest != nil {
		return s.submissionSyncForTest()
	}
	return s.storageState.Sync()
}

// withSubmission holds only the local queue lifecycle gate. No callback may
// perform provider/network work. Explicit deletion and replacement stay fenced.
func (s *Storage) withSubmission(expected *Entry, fn func(*Entry, *ProviderSubmission) error) error {
	if expected == nil || expected.InfoHash == "" || expected.QueueIncarnation == "" {
		return ErrProviderSubmissionConflict
	}
	ref, err := s.acquireMainEntryState(expected.InfoHash)
	if err != nil {
		return err
	}
	defer ref.release()
	ref.state.mu.Lock()
	defer ref.state.mu.Unlock()
	if ref.state.queueDeleting {
		return ErrQueuedEntryDeleting
	}
	current, err := s.getQueuedRaw(ref.key)
	if err != nil {
		return err
	}
	if current.QueueIncarnation != expected.QueueIncarnation || current.IsComplete || (current.State != EntryStateDownloading && current.State != EntryStateError) {
		return ErrProviderSubmissionConflict
	}
	intent, err := s.submissionRaw(ref.key)
	if err != nil {
		return err
	}
	if intent != nil && intent.QueueIncarnation != current.QueueIncarnation {
		if intent.Phase != SubmissionBound && intent.Phase != SubmissionRejected {
			return ErrProviderSubmissionConflict
		}
		intent = nil
	}
	return fn(current, intent)
}

func (s *Storage) GetProviderSubmission(expected *Entry) (*ProviderSubmission, error) {
	var result *ProviderSubmission
	err := s.withSubmission(expected, func(_ *Entry, intent *ProviderSubmission) error {
		result = intent
		return nil
	})
	return result, err
}

// EnsureProviderSubmissionIdentity adopts legacy queue rows before any remote
// mutation. The caller must retain its existing manager generation fence. A
// second legacy snapshot cannot acquire the newly assigned incarnation.
func (s *Storage) EnsureProviderSubmissionIdentity(expected *Entry) (*Entry, error) {
	if expected == nil || expected.InfoHash == "" {
		return nil, ErrProviderSubmissionConflict
	}
	ref, err := s.acquireMainEntryState(expected.InfoHash)
	if err != nil {
		return nil, err
	}
	defer ref.release()
	ref.state.mu.Lock()
	defer ref.state.mu.Unlock()
	if ref.state.queueDeleting {
		return nil, ErrQueuedEntryDeleting
	}
	current, err := s.getQueuedRaw(ref.key)
	if err != nil {
		return nil, err
	}
	if current.QueueIncarnation != expected.QueueIncarnation || current.State != EntryStateDownloading || current.IsComplete {
		return nil, ErrProviderSubmissionConflict
	}
	if current.QueueIncarnation != "" {
		return current, nil
	}
	prior, err := s.submissionRaw(ref.key)
	if err != nil {
		return nil, err
	}
	if prior != nil && prior.Phase != SubmissionRejected && prior.Phase != SubmissionBound {
		return nil, ErrProviderSubmissionConflict
	}
	current.QueueIncarnation = uuid.NewString()
	if err := s.writeQueueRecord(current, s.queue.PutExistingSynced); err != nil {
		return nil, err
	}
	return current, nil
}

// BeginProviderSubmission synchronizes the intent before the caller may create
// remote work. created=false means an existing attempt must be reconciled.
func (s *Storage) BeginProviderSubmission(expected *Entry, provider, account string, uncached bool) (result *ProviderSubmission, created bool, err error) {
	if strings.TrimSpace(provider) == "" || account == "" {
		return nil, false, fmt.Errorf("provider submission account is required")
	}
	err = s.withSubmission(expected, func(current *Entry, intent *ProviderSubmission) error {
		if intent != nil && intent.Phase != SubmissionRejected {
			result = intent
			return nil
		}
		if current.State != EntryStateDownloading {
			return ErrProviderSubmissionConflict
		}
		result = &ProviderSubmission{Version: 1, InfoHash: normalizeMainEntryKey(current.InfoHash), QueueIncarnation: current.QueueIncarnation,
			Attempt: uuid.NewString(), Provider: provider, Account: account, DownloadUncached: uncached, Phase: SubmissionPending}
		if err := s.saveSubmission(result); err != nil {
			return err
		}
		created = true
		return nil
	})
	return
}

func (s *Storage) RecordProviderSubmissionID(expected *Entry, attempt, id string, recovered bool) error {
	return s.withSubmission(expected, func(_ *Entry, intent *ProviderSubmission) error {
		if intent == nil || intent.Attempt != attempt || intent.Phase != SubmissionPending || strings.TrimSpace(id) == "" || (intent.ProviderID != "" && (intent.ProviderID != id || intent.Recovered != recovered)) {
			return ErrProviderSubmissionConflict
		}
		intent.ProviderID, intent.Recovered = id, recovered
		return s.saveSubmission(intent)
	})
}

func (s *Storage) SetProviderSubmissionPhase(expected *Entry, attempt string, phase ProviderSubmissionPhase) error {
	return s.withSubmission(expected, func(_ *Entry, intent *ProviderSubmission) error {
		if intent == nil || intent.Attempt != attempt {
			return ErrProviderSubmissionConflict
		}
		switch phase {
		case SubmissionCleanup:
			if (intent.Phase != SubmissionPending && intent.Phase != SubmissionBound) || intent.ProviderID == "" || intent.Recovered {
				return ErrProviderSubmissionConflict
			}
		case SubmissionRejected:
			if intent.Phase != SubmissionCleanup && (intent.Phase != SubmissionPending || intent.ProviderID != "") {
				return ErrProviderSubmissionConflict
			}
		default:
			return ErrProviderSubmissionConflict
		}
		intent.Phase = phase
		return s.saveSubmission(intent)
	})
}

// BindProviderSubmission persists the exact acknowledged or reconciled ID into
// a fresh queue snapshot before recording the intent as bound. It does not grant
// main-entry publication, file deletion, or replacement authorization.
func (s *Storage) BindProviderSubmission(expected *Entry, attempt string, remote *debridTypes.Torrent) (result *Entry, err error) {
	err = s.withSubmission(expected, func(current *Entry, intent *ProviderSubmission) error {
		if intent == nil || intent.Attempt != attempt || (intent.Phase != SubmissionPending && intent.Phase != SubmissionBound) || remote == nil || intent.ProviderID == "" || remote.Id != intent.ProviderID || remote.Debrid != intent.Provider || !strings.EqualFold(remote.InfoHash, current.InfoHash) {
			return ErrProviderSubmissionConflict
		}
		// Preserve canonical files until normal validation accepts the provider
		// snapshot; only bind the transfer identity here.
		if _, err := current.UpdateTorrentProviderState(remote); err != nil {
			return err
		}
		current.ActiveProvider = intent.Provider
		current.DownloadUncached = intent.DownloadUncached
		current.State = EntryStateDownloading
		// Keep the scheduler out until the admitted worker has consumed this
		// checkpoint. Startup restoration recognizes the recorded provider ID.
		current.Status = debridTypes.TorrentStatusQueued
		current.LastError = ""
		if err := s.writeQueueRecord(current, s.queue.PutExistingSynced); err != nil {
			return err
		}
		intent.Phase = SubmissionBound
		if err := s.saveSubmission(intent); err != nil {
			return err
		}
		result = current
		return nil
	})
	return
}

// RecoverProviderSubmission repairs the queue-first/bound-marker crash window
// and makes failed uncertain attempts eligible for ordinary startup restoration.
func (s *Storage) RecoverProviderSubmission(expected *Entry) (changed bool, err error) {
	err = s.withSubmission(expected, func(current *Entry, intent *ProviderSubmission) error {
		if intent == nil || intent.Phase == SubmissionRejected || intent.Phase == SubmissionBound {
			return nil
		}
		if p := current.GetActiveProvider(); intent.Phase == SubmissionPending && intent.ProviderID != "" && p != nil && current.ActiveProvider == intent.Provider && p.ID == intent.ProviderID {
			if current.State == EntryStateError {
				current.State = EntryStateDownloading
				current.Status = debridTypes.TorrentStatusDownloading
				if err := s.writeQueueRecord(current, s.queue.PutExistingSynced); err != nil {
					return err
				}
				changed = true
			}
			if err := s.SyncQueue(); err != nil {
				return err
			}
			intent.Phase = SubmissionBound
			return s.saveSubmission(intent)
		}
		if current.State == EntryStateError || current.Status != debridTypes.TorrentStatusQueued {
			current.State = EntryStateDownloading
			current.Status = debridTypes.TorrentStatusQueued
			if err := s.writeQueueRecord(current, s.queue.PutExistingSynced); err != nil {
				return err
			}
			changed = true
		}
		return nil
	})
	return
}
