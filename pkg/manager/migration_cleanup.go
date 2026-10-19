package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

const (
	migrationCleanupSweepInterval = time.Minute
	migrationCleanupRetryBase     = time.Minute
	migrationCleanupRetryMaximum  = time.Hour
)

type migrationEntryLock struct {
	mu   sync.Mutex
	refs int
}

// acquireMigrationEntryLock serializes target activation and delayed source
// cleanup for one info hash. Reference counting avoids retaining a mutex for
// every torrent that has ever been migrated.
func (m *Manager) acquireMigrationEntryLock(infoHash string) func() {
	key := strings.ToLower(strings.TrimSpace(infoHash))
	m.migrationLocksMu.Lock()
	if m.migrationLocks == nil {
		m.migrationLocks = make(map[string]*migrationEntryLock)
	}
	lock := m.migrationLocks[key]
	if lock == nil {
		lock = &migrationEntryLock{}
		m.migrationLocks[key] = lock
	}
	lock.refs++
	m.migrationLocksMu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		m.migrationLocksMu.Lock()
		lock.refs--
		if lock.refs == 0 && m.migrationLocks[key] == lock {
			delete(m.migrationLocks, key)
		}
		m.migrationLocksMu.Unlock()
	}
}

func (m *Manager) migrationCleanupClock() time.Time {
	if m != nil && m.migrationCleanupNow != nil {
		return m.migrationCleanupNow().UTC()
	}
	return time.Now().UTC()
}

func migrationCleanupRetryDelay(failedAttempts int) time.Duration {
	if failedAttempts < 0 {
		failedAttempts = 0
	}
	delay := migrationCleanupRetryBase
	for range failedAttempts {
		if delay >= migrationCleanupRetryMaximum/2 {
			return migrationCleanupRetryMaximum
		}
		delay *= 2
	}
	if delay > migrationCleanupRetryMaximum {
		return migrationCleanupRetryMaximum
	}
	return delay
}

// migrationPlacement resolves a configured account identity. Current rows use
// the account name as the map key. A single value-side match keeps old rows
// recoverable, while multiple legacy matches fail closed.
func migrationPlacement(
	entry *storage.Entry,
	configuredProvider string,
) (*storage.ProviderEntry, error) {
	if entry == nil || entry.Providers == nil {
		return nil, nil
	}
	configuredProvider = strings.TrimSpace(configuredProvider)
	if placement := entry.Providers[configuredProvider]; placement != nil {
		return placement, nil
	}

	var match *storage.ProviderEntry
	for _, placement := range entry.Providers {
		if placement == nil || placement.Provider != configuredProvider {
			continue
		}
		if match != nil && match != placement {
			return nil, fmt.Errorf(
				"multiple legacy placements match configured provider %q",
				configuredProvider,
			)
		}
		match = placement
	}
	return match, nil
}

// prepareMigrationCleanup creates the durable authorization only after the
// target placement has been committed. A missing source means there is no
// longer a locally authorized provider object to remove.
func (m *Manager) prepareMigrationCleanup(
	job *storage.SwitcherJob,
) (*storage.MigrationCleanupIntent, error) {
	if m == nil || m.storage == nil {
		return nil, fmt.Errorf("migration cleanup storage is unavailable")
	}
	if job == nil {
		return nil, fmt.Errorf("migration job is nil")
	}
	entry, err := m.GetEntry(job.InfoHash)
	if err != nil {
		return nil, fmt.Errorf("reload durable migration target: %w", err)
	}

	source, err := migrationPlacement(entry, job.SourceProvider)
	if err != nil {
		return nil, fmt.Errorf("resolve migration source placement: %w", err)
	}
	if source == nil || strings.TrimSpace(source.ID) == "" {
		return nil, nil
	}
	target, err := migrationPlacement(entry, job.TargetProvider)
	if err != nil {
		return nil, fmt.Errorf("resolve migration target placement: %w", err)
	}
	if target == nil || strings.TrimSpace(target.ID) == "" {
		return nil, fmt.Errorf(
			"durable target placement %q is missing after activation",
			job.TargetProvider,
		)
	}
	if target.Status != debridTypes.TorrentStatusDownloaded {
		return nil, fmt.Errorf(
			"durable target placement %s/%s is not downloaded: %s",
			job.TargetProvider,
			target.ID,
			target.Status,
		)
	}

	intent, err := m.storage.PrepareMigrationCleanup(&storage.MigrationCleanupIntent{
		JobID:           job.ID,
		InfoHash:        entry.InfoHash,
		SourceProvider:  job.SourceProvider,
		SourceTorrentID: source.ID,
		TargetProvider:  job.TargetProvider,
		TargetTorrentID: target.ID,
	})
	if err != nil {
		return nil, fmt.Errorf("persist migration cleanup intent: %w", err)
	}
	return intent, nil
}

// runMigrationCleanup serializes attempts for one durable identity. A failure
// is recorded before it is returned so restart recovery retains both the exact
// authorization and its provider-friendly exponential backoff.
func (m *Manager) runMigrationCleanup(
	ctx context.Context,
	id string,
) error {
	if m == nil || m.storage == nil {
		return fmt.Errorf("migration cleanup storage is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	_, err, _ := m.migrationCleanupSG.Do(id, func() (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		intent, err := m.storage.GetMigrationCleanup(id)
		if err != nil {
			if storage.IsMigrationCleanupNotFound(err) {
				return nil, nil
			}
			return nil, err
		}

		if err := m.attemptMigrationCleanup(ctx, intent); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
				// Shutdown is not a provider failure. Leave the existing durable
				// schedule untouched for the next manager incarnation.
				return nil, err
			}
			// Start backoff when the failed provider/storage attempt finishes.
			// A slow provider call must not make the retry immediately overdue.
			attemptedAt := m.migrationCleanupClock()
			nextAttemptAt := attemptedAt.Add(
				migrationCleanupRetryDelay(intent.Attempts),
			)
			markErr := m.storage.MarkMigrationCleanupFailed(
				intent.ID,
				err,
				attemptedAt,
				nextAttemptAt,
			)
			if markErr != nil {
				return nil, errors.Join(
					err,
					fmt.Errorf("persist migration cleanup retry: %w", markErr),
				)
			}
			m.logger.Warn().
				Err(err).
				Str("cleanup_id", intent.ID).
				Str("info_hash", intent.InfoHash).
				Str("source", intent.SourceProvider).
				Time("next_attempt_at", nextAttemptAt).
				Msg("Migration source cleanup will be retried")
			return nil, err
		}
		return nil, nil
	})
	return err
}

// authorizedMigrationCleanupEntry verifies that an intent still describes the
// current local lifecycle. A changed or reactivated source supersedes the old
// authorization and clears it without touching a provider. A missing or
// incomplete target fails closed and remains retryable.
func (m *Manager) authorizedMigrationCleanupEntry(
	intent *storage.MigrationCleanupIntent,
) (*storage.Entry, bool, error) {
	entry, err := m.GetEntry(intent.InfoHash)
	if err != nil {
		if storage.IsEntryNotFound(err) {
			return nil, false, m.storage.CompleteMigrationCleanup(intent.ID)
		}
		return nil, false, fmt.Errorf("load migration cleanup entry: %w", err)
	}

	source, err := migrationPlacement(entry, intent.SourceProvider)
	if err != nil {
		return nil, false, fmt.Errorf("resolve migration cleanup source: %w", err)
	}
	if source == nil || source.ID != intent.SourceTorrentID {
		m.logger.Warn().
			Str("cleanup_id", intent.ID).
			Str("info_hash", intent.InfoHash).
			Str("source", intent.SourceProvider).
			Str("expected_torrent_id", intent.SourceTorrentID).
			Msg("Discarding stale migration cleanup without a provider delete")
		return nil, false, m.storage.CompleteMigrationCleanup(intent.ID)
	}
	if entry.ActiveProvider != intent.TargetProvider {
		// A later migration or explicit activation superseded this intent. The
		// captured source may be active again, so silently deleting it would be
		// unsafe even when both old provider IDs still exist.
		m.logger.Warn().
			Str("cleanup_id", intent.ID).
			Str("info_hash", intent.InfoHash).
			Str("expected_active_provider", intent.TargetProvider).
			Str("current_active_provider", entry.ActiveProvider).
			Msg("Discarding superseded migration cleanup without a provider delete")
		return nil, false, m.storage.CompleteMigrationCleanup(intent.ID)
	}

	target, err := migrationPlacement(entry, intent.TargetProvider)
	if err != nil {
		return nil, false, fmt.Errorf("resolve migration cleanup target: %w", err)
	}
	if target == nil || target.ID != intent.TargetTorrentID {
		return nil, false, fmt.Errorf(
			"durable target identity %s/%s is no longer present",
			intent.TargetProvider,
			intent.TargetTorrentID,
		)
	}
	if target.Status != debridTypes.TorrentStatusDownloaded {
		return nil, false, fmt.Errorf(
			"durable target placement %s/%s is not downloaded: %s",
			intent.TargetProvider,
			intent.TargetTorrentID,
			target.Status,
		)
	}
	return entry, true, nil
}

// attemptMigrationCleanup revalidates both sides from fresh durable state. It
// deliberately calls DeleteTorrent synchronously: provider APIs own their
// request deadlines, and an escaped deletion goroutine could otherwise remove
// a later provider object that reused the same ID.
func (m *Manager) attemptMigrationCleanup(
	ctx context.Context,
	intent *storage.MigrationCleanupIntent,
) error {
	if intent == nil {
		return fmt.Errorf("migration cleanup intent is nil")
	}
	release := m.acquireMigrationEntryLock(intent.InfoHash)
	_, authorized, err := m.authorizedMigrationCleanupEntry(intent)
	release()
	if err != nil {
		return err
	}
	if !authorized {
		return nil
	}
	targetClient := m.ProviderClient(intent.TargetProvider)
	if targetClient == nil {
		return fmt.Errorf(
			"target debrid %s is unavailable for live validation",
			intent.TargetProvider,
		)
	}
	remoteTarget, err := targetClient.GetTorrent(intent.TargetTorrentID)
	if err != nil {
		return fmt.Errorf(
			"validate live target placement %s/%s: %w",
			intent.TargetProvider,
			intent.TargetTorrentID,
			err,
		)
	}
	if remoteTarget == nil || remoteTarget.Id != intent.TargetTorrentID {
		remoteID := ""
		if remoteTarget != nil {
			remoteID = remoteTarget.Id
		}
		return fmt.Errorf(
			"live target identity %s/%s resolved as %q",
			intent.TargetProvider,
			intent.TargetTorrentID,
			remoteID,
		)
	}
	if remoteTarget.Status != debridTypes.TorrentStatusDownloaded {
		return fmt.Errorf(
			"live target placement %s/%s is not downloaded: %s",
			intent.TargetProvider,
			intent.TargetTorrentID,
			remoteTarget.Status,
		)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// The live read intentionally runs without the entry lock. Reacquire and
	// reload before deletion so a migration that completed during that network
	// call can supersede this intent without waiting or losing its new source.
	release = m.acquireMigrationEntryLock(intent.InfoHash)
	defer release()
	entry, authorized, err := m.authorizedMigrationCleanupEntry(intent)
	if err != nil {
		return err
	}
	if !authorized {
		return nil
	}

	sourceClient := m.ProviderClient(intent.SourceProvider)
	if sourceClient == nil {
		return fmt.Errorf(
			"source debrid %s is unavailable",
			intent.SourceProvider,
		)
	}
	if err := m.deleteProviderTorrent(sourceClient, intent.SourceTorrentID); err != nil {
		return fmt.Errorf(
			"delete source placement %s/%s: %w",
			intent.SourceProvider,
			intent.SourceTorrentID,
			err,
		)
	}

	entry.RemoveProvider(intent.SourceProvider)
	if err := m.AddOrUpdate(entry, nil); err != nil {
		return fmt.Errorf("persist removal of migration source placement: %w", err)
	}
	if err := m.storage.CompleteMigrationCleanup(intent.ID); err != nil {
		return err
	}
	if m.entry != nil {
		m.RefreshEntries(false)
	}
	m.logger.Info().
		Str("cleanup_id", intent.ID).
		Str("info_hash", intent.InfoHash).
		Str("source", intent.SourceProvider).
		Str("target", intent.TargetProvider).
		Msg("Migration source cleanup completed")
	return nil
}

func (m *Manager) processMigrationCleanups(ctx context.Context) error {
	if m == nil || m.storage == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	intents, err := m.storage.MigrationCleanups()
	if err != nil {
		return err
	}
	var errs []error
	for _, intent := range intents {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			return errors.Join(errs...)
		}
		now := m.migrationCleanupClock()
		if !intent.NextAttemptAt.IsZero() && now.Before(intent.NextAttemptAt) {
			continue
		}
		if err := m.runMigrationCleanup(ctx, intent.ID); err != nil {
			errs = append(errs, fmt.Errorf(
				"migration cleanup %s: %w",
				intent.ID,
				err,
			))
		}
	}
	return errors.Join(errs...)
}
