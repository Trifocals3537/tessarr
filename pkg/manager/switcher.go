package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/google/uuid"
)

// This is in-charge of moving torrents between different debrid services

// SwitchTorrent moves a torrent from one debrid to another
func (m *Manager) SwitchTorrent(ctx context.Context, infohash, target string, keepOld, waitComplete bool) (*storage.SwitcherJob, error) {
	// GetReader the entry
	entry, err := m.GetEntry(infohash)
	if err != nil {
		return nil, fmt.Errorf("failed to get entry: %w", err)
	}

	// Check if already on target debrid
	if entry.ActiveProvider == target {
		return nil, storage.ErrAlreadyOnDebrid
	}

	// Need to actually migrate - create job
	job := &storage.SwitcherJob{
		ID:             uuid.New().String(),
		InfoHash:       infohash,
		SourceProvider: entry.ActiveProvider,
		TargetProvider: target,
		Status:         storage.SwitcherStatusPending,
		Progress:       0,
		CreatedAt:      time.Now(),
		KeepOld:        keepOld,
		WaitComplete:   waitComplete,
	}

	// Store job
	m.migrationJobs.Store(job.ID, job)

	// Start migration in background
	if !m.startBackground("torrent migration", func() {
		m.executeMigration(job, entry)
	}) {
		m.migrationJobs.Delete(job.ID)
		return nil, fmt.Errorf("cannot switch torrent while manager is stopping")
	}

	return job, nil
}

// executeMigration activates a durable target first, then applies keep-old or
// records an exact, restart-safe source cleanup before any source-provider call.
func (m *Manager) executeMigration(job *storage.SwitcherJob, torrent *storage.Entry) {
	m.logger.Info().
		Str("job_id", job.ID).
		Str("torrent", torrent.Name).
		Str("source", job.SourceProvider).
		Str("target", job.TargetProvider).
		Msg("Starting torrent migration")
	job.Status = storage.SwitcherStatusInProgress
	releaseMigration := m.acquireMigrationEntryLock(job.InfoHash)
	migrationLocked := true
	defer func() {
		if migrationLocked {
			releaseMigration()
		}
	}()

	// SwitchTorrent's snapshot may have waited behind another migration. Reload
	// under the per-entry lock so stale work cannot submit a second target or
	// remove a source that is no longer active.
	current, err := m.GetEntry(job.InfoHash)
	if err != nil {
		job.Status = storage.SwitcherStatusFailed
		job.Error = fmt.Sprintf("failed to reload migration source: %v", err)
		job.CompletedAt = new(time.Now())
		return
	}
	if current.ActiveProvider != job.SourceProvider {
		job.Status = storage.SwitcherStatusFailed
		job.Error = fmt.Sprintf(
			"migration was superseded: active provider changed from %s to %s",
			job.SourceProvider,
			current.ActiveProvider,
		)
		job.CompletedAt = new(time.Now())
		return
	}
	torrent = current

	// Verify the target client still exists before starting provider work.
	if m.ProviderClient(job.TargetProvider) == nil {
		job.Status = storage.SwitcherStatusFailed
		job.Error = fmt.Sprintf("target debrid %s not found", job.TargetProvider)
		job.CompletedAt = new(time.Now())
		return
	}
	// Submit to target debrid

	job.Progress = 10

	success, err := m.fixer.MoveTorrent(torrent, job.TargetProvider, false) // false = don't force re-download

	if err != nil || !success {
		job.Status = storage.SwitcherStatusFailed
		job.Error = fmt.Sprintf("failed to move torrent to target debrid: %v", err)
		job.CompletedAt = new(time.Now())
		m.logger.Error().
			Err(err).
			Str("job_id", job.ID).
			Msg("Failed to move torrent to target debrid")
		return
	}

	// MoveTorrent has already flushed the target activation. Keeping the source
	// therefore needs no second entry write.
	if job.KeepOld {
		job.Status = storage.SwitcherStatusCompleted
		job.Progress = 100
		job.CompletedAt = new(time.Now())
		if m.entry != nil {
			m.RefreshEntries(false)
		}
		m.logger.Info().
			Str("job_id", job.ID).
			Str("status", string(job.Status)).
			Msg("Migration completed")
		return
	}

	job.Progress = 75
	intent, err := m.prepareMigrationCleanup(job)
	if err != nil {
		job.Status = storage.SwitcherStatusFailed
		job.Error = fmt.Sprintf(
			"target activated, but source cleanup could not be prepared: %v",
			err,
		)
		job.CompletedAt = new(time.Now())
		m.logger.Error().
			Err(err).
			Str("job_id", job.ID).
			Msg("Target activated, but durable source cleanup could not be prepared")
		return
	}
	if intent == nil {
		job.Status = storage.SwitcherStatusCompleted
		job.Progress = 100
		job.CompletedAt = new(time.Now())
		if m.entry != nil {
			m.RefreshEntries(false)
		}
		m.logger.Info().
			Str("job_id", job.ID).
			Str("status", string(job.Status)).
			Msg("Migration completed without a remaining source placement")
		return
	}

	// Delayed cleanup acquires the same keyed lock. Release the target phase
	// first so a scheduler that already joined this intent cannot deadlock with
	// the immediate attempt through singleflight.
	releaseMigration()
	migrationLocked = false
	job.Progress = 85
	cleanupContext := m.ctx
	if cleanupContext == nil {
		cleanupContext = context.Background()
	}
	if err := m.runMigrationCleanup(cleanupContext, intent.ID); err != nil {
		job.Status = storage.SwitcherStatusFailed
		job.Error = fmt.Sprintf(
			"target activated; durable source cleanup is pending retry: %v",
			err,
		)
		job.CompletedAt = new(time.Now())
		m.logger.Error().
			Err(err).
			Str("job_id", job.ID).
			Str("cleanup_id", intent.ID).
			Msg("Target activated, but source cleanup is pending retry")
		return
	}

	job.Status = storage.SwitcherStatusCompleted
	job.Progress = 100
	job.CompletedAt = new(time.Now())

	m.logger.Info().
		Str("job_id", job.ID).
		Str("status", string(job.Status)).
		Msg("Migration completed")
}
