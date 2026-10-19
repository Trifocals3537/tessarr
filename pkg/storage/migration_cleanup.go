package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
)

var ErrMigrationCleanupNotFound = errors.New("migration cleanup intent not found")

const migrationCleanupVersion = 1

// MigrationCleanupIntent is the durable authorization to remove one exact
// source placement after one exact target placement has been committed. The
// provider names are configured account identities, not provider types.
type MigrationCleanupIntent struct {
	ID              string    `json:"id"`
	JobID           string    `json:"job_id,omitempty"`
	InfoHash        string    `json:"info_hash"`
	SourceProvider  string    `json:"source_provider"`
	SourceTorrentID string    `json:"source_torrent_id"`
	TargetProvider  string    `json:"target_provider"`
	TargetTorrentID string    `json:"target_torrent_id"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
	LastAttemptAt   time.Time `json:"last_attempt_at,omitempty"`
	NextAttemptAt   time.Time `json:"next_attempt_at,omitempty"`
	Attempts        int       `json:"attempts"`
	LastError       string    `json:"last_error,omitempty"`
}

type migrationCleanupRecord struct {
	Version int `json:"version"`
	MigrationCleanupIntent
}

func IsMigrationCleanupNotFound(err error) bool {
	return errors.Is(err, ErrMigrationCleanupNotFound)
}

func migrationCleanupID(intent *MigrationCleanupIntent) string {
	hash := sha256.New()
	for _, part := range []string{
		intent.InfoHash,
		intent.SourceProvider,
		intent.SourceTorrentID,
		intent.TargetProvider,
		intent.TargetTorrentID,
	} {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func normalizeMigrationCleanupIntent(
	intent *MigrationCleanupIntent,
) (*MigrationCleanupIntent, error) {
	if intent == nil {
		return nil, fmt.Errorf("migration cleanup intent is nil")
	}
	normalized := *intent
	normalized.InfoHash = normalizeMainEntryKey(normalized.InfoHash)
	normalized.JobID = strings.TrimSpace(normalized.JobID)
	normalized.SourceProvider = strings.TrimSpace(normalized.SourceProvider)
	normalized.SourceTorrentID = strings.TrimSpace(normalized.SourceTorrentID)
	normalized.TargetProvider = strings.TrimSpace(normalized.TargetProvider)
	normalized.TargetTorrentID = strings.TrimSpace(normalized.TargetTorrentID)
	if normalized.InfoHash == "" ||
		normalized.SourceProvider == "" ||
		normalized.SourceTorrentID == "" ||
		normalized.TargetProvider == "" ||
		normalized.TargetTorrentID == "" {
		return nil, fmt.Errorf("migration cleanup intent has an empty identity field")
	}
	if normalized.SourceProvider == normalized.TargetProvider {
		return nil, fmt.Errorf(
			"migration cleanup source and target are both %q",
			normalized.SourceProvider,
		)
	}
	wantID := migrationCleanupID(&normalized)
	if normalized.ID != "" && normalized.ID != wantID {
		return nil, fmt.Errorf(
			"migration cleanup intent id %q does not match identity %q",
			normalized.ID,
			wantID,
		)
	}
	normalized.ID = wantID
	if normalized.Attempts < 0 {
		return nil, fmt.Errorf("migration cleanup attempts cannot be negative")
	}
	return &normalized, nil
}

func decodeMigrationCleanup(
	key string,
	data []byte,
) (*MigrationCleanupIntent, error) {
	var record migrationCleanupRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return nil, fmt.Errorf("decode migration cleanup %s: %w", key, err)
	}
	if record.Version != migrationCleanupVersion {
		return nil, fmt.Errorf(
			"unsupported migration cleanup version %d for %s",
			record.Version,
			key,
		)
	}
	intent, err := normalizeMigrationCleanupIntent(&record.MigrationCleanupIntent)
	if err != nil {
		return nil, fmt.Errorf("validate migration cleanup %s: %w", key, err)
	}
	if intent.ID != key {
		return nil, fmt.Errorf(
			"migration cleanup key %q does not match record id %q",
			key,
			intent.ID,
		)
	}
	return intent, nil
}

func encodeMigrationCleanup(intent *MigrationCleanupIntent) ([]byte, error) {
	data, err := json.Marshal(migrationCleanupRecord{
		Version:                migrationCleanupVersion,
		MigrationCleanupIntent: *intent,
	})
	if err != nil {
		return nil, fmt.Errorf("encode migration cleanup %s: %w", intent.ID, err)
	}
	return data, nil
}

func sameMigrationCleanupIdentity(a, b *MigrationCleanupIntent) bool {
	return a != nil && b != nil &&
		a.ID == b.ID &&
		a.InfoHash == b.InfoHash &&
		a.SourceProvider == b.SourceProvider &&
		a.SourceTorrentID == b.SourceTorrentID &&
		a.TargetProvider == b.TargetProvider &&
		a.TargetTorrentID == b.TargetTorrentID
}

// PrepareMigrationCleanup flushes the intent before any source-provider call
// is allowed. Re-preparing the same identity is idempotent and attaches the
// newest in-memory job without resetting durable retry backoff.
func (s *Storage) PrepareMigrationCleanup(
	intent *MigrationCleanupIntent,
) (*MigrationCleanupIntent, error) {
	if s.migrationCleanup == nil {
		return nil, fmt.Errorf("migration cleanup store is not initialized")
	}
	normalized, err := normalizeMigrationCleanupIntent(intent)
	if err != nil {
		return nil, err
	}

	s.migrationCleanupMu.Lock()
	defer s.migrationCleanupMu.Unlock()

	if data, getErr := s.migrationCleanup.Get(normalized.ID); getErr == nil {
		existing, decodeErr := decodeMigrationCleanup(normalized.ID, data)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if !sameMigrationCleanupIdentity(existing, normalized) {
			return nil, fmt.Errorf("migration cleanup identity collision for %s", normalized.ID)
		}
		if normalized.JobID != "" && existing.JobID != normalized.JobID {
			existing.JobID = normalized.JobID
			existing.UpdatedAt = time.Now().UTC()
			encoded, encodeErr := encodeMigrationCleanup(existing)
			if encodeErr != nil {
				return nil, encodeErr
			}
			if putErr := s.migrationCleanup.PutExisting(existing.ID, encoded, nil); putErr != nil {
				return nil, fmt.Errorf("update migration cleanup job: %w", putErr)
			}
			if syncErr := s.migrationCleanup.Sync(); syncErr != nil {
				return nil, fmt.Errorf("sync updated migration cleanup job: %w", syncErr)
			}
		}
		copy := *existing
		return &copy, nil
	} else if !hybrid.IsNotFound(getErr) {
		return nil, fmt.Errorf("load migration cleanup %s: %w", normalized.ID, getErr)
	}

	now := time.Now().UTC()
	if normalized.CreatedAt.IsZero() {
		normalized.CreatedAt = now
	}
	normalized.UpdatedAt = now
	data, err := encodeMigrationCleanup(normalized)
	if err != nil {
		return nil, err
	}
	if err := s.migrationCleanup.Put(normalized.ID, data, nil); err != nil {
		return nil, fmt.Errorf("persist migration cleanup: %w", err)
	}
	if err := s.migrationCleanup.Sync(); err != nil {
		return nil, fmt.Errorf("sync migration cleanup: %w", err)
	}
	copy := *normalized
	return &copy, nil
}

func (s *Storage) GetMigrationCleanup(
	id string,
) (*MigrationCleanupIntent, error) {
	if s.migrationCleanup == nil {
		return nil, fmt.Errorf("migration cleanup store is not initialized")
	}
	id = strings.TrimSpace(id)
	if id == "" {
		return nil, fmt.Errorf("migration cleanup id is empty")
	}
	s.migrationCleanupMu.Lock()
	defer s.migrationCleanupMu.Unlock()
	data, err := s.migrationCleanup.Get(id)
	if err != nil {
		if hybrid.IsNotFound(err) {
			return nil, fmt.Errorf("%w: %s", ErrMigrationCleanupNotFound, id)
		}
		return nil, fmt.Errorf("load migration cleanup %s: %w", id, err)
	}
	return decodeMigrationCleanup(id, data)
}

func (s *Storage) MigrationCleanups() ([]*MigrationCleanupIntent, error) {
	if s.migrationCleanup == nil {
		return nil, fmt.Errorf("migration cleanup store is not initialized")
	}
	s.migrationCleanupMu.Lock()
	defer s.migrationCleanupMu.Unlock()
	intents := make([]*MigrationCleanupIntent, 0, s.migrationCleanup.Len())
	if err := s.migrationCleanup.ForEach(func(key string, data []byte) error {
		intent, err := decodeMigrationCleanup(key, data)
		if err != nil {
			return err
		}
		intents = append(intents, intent)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("scan migration cleanups: %w", err)
	}
	sort.Slice(intents, func(i, j int) bool {
		if intents[i].CreatedAt.Equal(intents[j].CreatedAt) {
			return intents[i].ID < intents[j].ID
		}
		return intents[i].CreatedAt.Before(intents[j].CreatedAt)
	})
	return intents, nil
}

func (s *Storage) MarkMigrationCleanupFailed(
	id string,
	cause error,
	attemptedAt time.Time,
	nextAttemptAt time.Time,
) error {
	if cause == nil {
		return fmt.Errorf("migration cleanup failure cause is nil")
	}
	if s.migrationCleanup == nil {
		return fmt.Errorf("migration cleanup store is not initialized")
	}
	s.migrationCleanupMu.Lock()
	defer s.migrationCleanupMu.Unlock()
	data, err := s.migrationCleanup.Get(id)
	if err != nil {
		if hybrid.IsNotFound(err) {
			return fmt.Errorf("%w: %s", ErrMigrationCleanupNotFound, id)
		}
		return fmt.Errorf("load failed migration cleanup %s: %w", id, err)
	}
	intent, err := decodeMigrationCleanup(id, data)
	if err != nil {
		return err
	}
	intent.Attempts++
	intent.LastAttemptAt = attemptedAt.UTC()
	intent.NextAttemptAt = nextAttemptAt.UTC()
	intent.UpdatedAt = attemptedAt.UTC()
	intent.LastError = cause.Error()
	if len(intent.LastError) > 4096 {
		intent.LastError = intent.LastError[:4096]
	}
	encoded, err := encodeMigrationCleanup(intent)
	if err != nil {
		return err
	}
	if err := s.migrationCleanup.PutExisting(id, encoded, nil); err != nil {
		return fmt.Errorf("persist migration cleanup failure: %w", err)
	}
	if err := s.migrationCleanup.Sync(); err != nil {
		return fmt.Errorf("sync migration cleanup failure: %w", err)
	}
	return nil
}

func (s *Storage) CompleteMigrationCleanup(id string) error {
	if s.migrationCleanup == nil {
		return fmt.Errorf("migration cleanup store is not initialized")
	}
	s.migrationCleanupMu.Lock()
	defer s.migrationCleanupMu.Unlock()
	if err := s.migrationCleanup.Delete(id); err != nil {
		if hybrid.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("complete migration cleanup %s: %w", id, err)
	}
	if err := s.migrationCleanup.Sync(); err != nil {
		return fmt.Errorf("sync completed migration cleanup %s: %w", id, err)
	}
	return nil
}

func (s *Storage) MigrationCleanupCount() int {
	if s == nil || s.migrationCleanup == nil {
		return 0
	}
	return s.migrationCleanup.Len()
}
