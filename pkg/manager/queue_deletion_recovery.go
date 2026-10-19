package manager

import (
	"errors"
	"fmt"

	"github.com/Trifocals3537/tessarr/pkg/storage"
)

// recoverQueuedDeletions runs before JobQueue construction and restore. Fatal
// errors mean the exact durable row could not be retired and startup must not
// admit queue work. Residual cleanup errors are safe to report while starting:
// their rows are already durably absent and their tombstones remain retryable.
func (m *Manager) recoverQueuedDeletions() (residual error, fatal error) {
	store := m.storage
	if store == nil && m.queue != nil {
		store = m.queue.storage
	}
	if store == nil {
		return nil, fmt.Errorf("queue deletion storage is unavailable")
	}
	intents, err := store.QueuedDeletionIntents()
	if err != nil {
		return nil, err
	}

	var residuals []error
	var fatals []error
	adminEntries, err := store.MainDeletionPlacementIntents()
	if err != nil {
		return nil, err
	}
	// An administrative placement obligation belongs to one hash. Its failure
	// (including failure to sync completion) must gate that hash's local cleanup,
	// without preventing unrelated queue intents from finishing recovery.
	adminFailures := make(map[string]error)
	for _, entry := range adminEntries {
		if err := m.RemoveTorrentPlacements(entry); err != nil {
			adminFailures[entry.InfoHash] = err
			residuals = append(residuals, fmt.Errorf("resume administrative main placement cleanup %s: %w", entry.InfoHash, err))
		} else if err := store.CompleteMainDeletionPlacements(entry.InfoHash); err != nil {
			adminFailures[entry.InfoHash] = err
			residuals = append(residuals, err)
		}
	}
	for _, intent := range intents {
		if intent == nil || intent.Entry == nil {
			fatals = append(fatals, fmt.Errorf("invalid nil queue deletion intent"))
			continue
		}
		if err := store.StartQueuedDeletionCleanup(
			intent.InfoHash,
			intent.QueueIncarnation,
		); err != nil {
			fatals = append(fatals, fmt.Errorf(
				"resume queue deletion %s: %w",
				intent.InfoHash,
				err,
			))
			continue
		}

		// On restart the durable snapshot, not the live row, owns cleanup.
		// Retiring first makes restoration impossible even if cleanup is only
		// partially successful or the process crashes again.
		if err := store.RetireQueuedDeletionRow(
			intent.InfoHash,
			intent.QueueIncarnation,
		); err != nil {
			fatals = append(fatals, fmt.Errorf(
				"retire interrupted queue deletion %s: %w",
				intent.InfoHash,
				err,
			))
			continue
		}

		// Keep early row retirement so no worker can resume, but retain the
		// queue intent and files until the separate admin obligation is durable.
		if err := adminFailures[intent.InfoHash]; err != nil {
			residuals = append(residuals, fmt.Errorf("queue deletion %s awaits administrative placement cleanup: %w", intent.InfoHash, err))
			continue
		}

		var cleanupErrs []error
		if intent.PlacementCleanupPending {
			entries := make(
				[]*storage.Entry,
				0,
				2+len(intent.PlacementEntries),
			)
			entries = append(entries, intent.Entry)
			entries = append(entries, intent.PlacementEntries...)
			var mainErr error
			if intent.PlacementScope == storage.QueuePlacementScopeTransferOnly {
				entries = intent.PlacementEntries
				for _, entry := range entries {
					if err := store.ReserveTransferCleanup(entry); err != nil {
						mainErr = err
					}
				}
			} else {
				mainEntry, err := store.Get(intent.InfoHash)
				mainErr = err
				switch {
				case mainErr == nil:
					entries = append(entries, mainEntry)
				case storage.IsEntryNotFound(mainErr):
				default:
					cleanupErrs = append(cleanupErrs, fmt.Errorf(
						"load main entry for recovered placement cleanup: %w",
						mainErr,
					))
				}
			}
			if intent.PlacementScope == storage.QueuePlacementScopeTransferOnly && mainErr != nil {
				cleanupErrs = append(cleanupErrs, mainErr)
			}
			if mainErr == nil || storage.IsEntryNotFound(mainErr) {
				if err := m.RemoveTorrentPlacements(entries...); err != nil {
					cleanupErrs = append(cleanupErrs, err)
				} else if err := store.MarkQueuedDeletionPlacementsClean(
					intent.InfoHash,
					intent.QueueIncarnation,
				); err != nil {
					cleanupErrs = append(cleanupErrs, err)
				}
			}
		}
		if intent.UnrecoverableCleanupPending {
			cleanupErrs = append(cleanupErrs, fmt.Errorf(
				"non-recoverable cleanup callback may have been interrupted; "+
					"manual confirmation is required",
			))
		}
		if !intent.PreserveFiles {
			if err := m.queue.deleteEntryFiles(intent.Entry); err != nil {
				cleanupErrs = append(cleanupErrs, fmt.Errorf(
					"finish local queue cleanup: %w",
					err,
				))
			}
		}
		if len(cleanupErrs) == 0 {
			if err := store.CompleteQueuedDeletion(
				intent.InfoHash,
				intent.QueueIncarnation,
			); err != nil {
				cleanupErrs = append(cleanupErrs, err)
			}
		}
		if len(cleanupErrs) > 0 {
			residuals = append(residuals, fmt.Errorf(
				"queue deletion %s remains pending: %w",
				intent.InfoHash,
				errors.Join(cleanupErrs...),
			))
		}
	}
	return errors.Join(residuals...), errors.Join(fatals...)
}
