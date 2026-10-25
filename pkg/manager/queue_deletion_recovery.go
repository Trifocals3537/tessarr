package manager

import (
	"errors"
	"fmt"
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
	if m.storage == nil {
		m.storage = store
	}
	intents, err := store.QueuedDeletionIntents()
	if err != nil {
		return nil, err
	}

	var residuals []error
	var fatals []error
	owned := make(map[string]bool, len(intents))
	for _, intent := range intents {
		owned[intent.InfoHash] = intent.Work != nil
	}
	adminEntries, err := store.MainDeletionPlacementIntents()
	if err != nil {
		return nil, err
	}
	// An administrative placement obligation belongs to one hash. Its failure
	// (including failure to sync completion) must gate that hash's local cleanup,
	// without preventing unrelated queue intents from finishing recovery.

	for _, entry := range adminEntries {
		// A workflow-owned intent may have crashed after a provider effect.
		// Its claim must reconcile the saved admin scope read-only; never replay
		// that DELETE through the older startup-only path.
		if owned[entry.InfoHash] {
			continue
		}
		if err := m.RemoveTorrentPlacements(entry); err != nil {

			residuals = append(residuals, fmt.Errorf("resume administrative main placement cleanup %s: %w", entry.InfoHash, err))
		} else if err := store.CompleteMainDeletionPlacements(entry.InfoHash); err != nil {

			residuals = append(residuals, err)
		}
	}
	for _, intent := range intents {
		if err := m.runCleanupIntent(intent, true); err != nil {
			if errors.Is(err, errCleanupFatal) {
				fatals = append(fatals, err)
			} else {
				residuals = append(residuals, err)
			}
		}
	}
	return errors.Join(residuals...), errors.Join(fatals...)
}
