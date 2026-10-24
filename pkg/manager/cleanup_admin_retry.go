package manager

import (
	"errors"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func (m *Manager) validateCleanupAccounts(i *storage.QueueDeletionIntent, entries []*storage.Entry, requireBound bool) error {
	for _, entry := range entries {
		for _, p := range entry.Providers {
			if p == nil {
				continue
			}
			if p.Provider == "usenet" {
				return cleanupBlocked("usenet_metadata_requires_review")
			}
			client := m.ProviderClient(p.Provider)
			bound := i.Work.Accounts[p.Provider]
			if client == nil || (requireBound && bound == "") || (bound != "" && bound != submissionAccount(client.Config())) {
				return cleanupBlocked("provider_account_changed")
			}
			if err := m.verifyPlacementAccount(client, p.ID); err != nil {
				return cleanupBlocked("provider_account_changed")
			}
		}
	}
	return nil
}

// retrySavedAdministrativeCleanup is an explicit administrative DELETE retry.
// It retains the original independent admin snapshot and the queue's saved
// scope. It cannot turn generic operator retry into provider deletion authority.
func (m *Manager) retrySavedAdministrativeCleanup(i *storage.QueueDeletionIntent, admin *storage.CleanupAdminIntent) error {
	var queueEntries []*storage.Entry
	if i.PlacementCleanupPending {
		queueEntries = cleanupEntries(i)
		if i.PlacementScope == storage.QueuePlacementScopeTransferOnly {
			for _, entry := range queueEntries {
				if err := m.storage.ReserveTransferCleanup(entry); err != nil {
					return cleanupBlocked("placement_is_protected")
				}
			}
		}
		if err := m.validateCleanupAccounts(i, queueEntries, true); err != nil {
			return err
		}
	}
	if err := m.validateCleanupAccounts(i, []*storage.Entry{admin.Entry}, false); err != nil {
		return err
	}
	if i.PlacementCleanupPending {
		if err := m.storage.CleanupEffectStarted(i.InfoHash, i.QueueIncarnation, i.Work.Token, time.Now()); err != nil {
			return errors.Join(errCleanupFatal, err)
		}
		i.Work.EffectStarted = true
	}
	entries := append(queueEntries, admin.Entry)
	if err := m.RemoveTorrentPlacements(entries...); err != nil {
		return &cleanupFailure{reason: "administrative_cleanup_requires_retry", cause: err, blocked: true}
	}
	m.cleanupPhase("administrative_provider_done")
	if err := m.storage.CompleteCleanupAdmin(i, admin); err != nil {
		return errors.Join(errCleanupFatal, err)
	}
	m.cleanupPhase("administrative_checkpoint")
	if i.PlacementCleanupPending {
		if err := m.storage.CleanupPlacementsClean(i); err != nil {
			return errors.Join(errCleanupFatal, err)
		}
		i.PlacementCleanupPending = false
	}
	return nil
}

func (m *Manager) retryAdoptedAdministrativeCleanup(key string) (bool, error) {
	if err := m.workflowAvailable(); err != nil {
		return true, err
	}
	i, err := m.storage.CleanupIntent(strings.ToLower(strings.TrimSpace(key)))
	if err != nil {
		return true, err
	}
	if i == nil || i.Work == nil {
		return false, nil
	}
	return true, m.runCleanupAttempt(i, false, true)
}
