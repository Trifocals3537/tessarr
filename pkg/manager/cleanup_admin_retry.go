package manager

import (
	"errors"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/storage"
)

// cleanupAccountCandidates captures current identities without granting effect
// authority. Late administrative identities must be checkpointed before use.
func (m *Manager) cleanupAccountCandidates(entries []*storage.Entry) (map[string]string, error) {
	accounts := make(map[string]string)
	for _, entry := range entries {
		for _, p := range entry.Providers {
			if p == nil {
				continue
			}
			if p.Provider == "usenet" {
				return nil, cleanupBlocked("usenet_metadata_requires_review")
			}
			client := m.ProviderClient(p.Provider)
			if client == nil {
				return nil, cleanupBlocked("provider_account_changed")
			}
			if err := m.verifyPlacementAccount(client, p.ID); err != nil {
				return nil, cleanupBlocked("provider_account_changed")
			}
			accounts[p.Provider] = submissionAccount(client.Config())
		}
	}
	return accounts, nil
}

func (m *Manager) validateCleanupAccounts(i *storage.QueueDeletionIntent, entries []*storage.Entry) error {
	accounts, err := m.cleanupAccountCandidates(entries)
	if err != nil {
		return err
	}
	for name, account := range accounts {
		if bound := i.Work.Accounts[name]; bound == "" || bound != account {
			return cleanupBlocked("provider_account_changed")
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
		if err := m.validateCleanupAccounts(i, queueEntries); err != nil {
			return err
		}
	}
	adminEntries := []*storage.Entry{admin.Entry}
	accounts, err := m.cleanupAccountCandidates(adminEntries)
	if err != nil {
		return err
	}
	bound, err := m.storage.BindCleanupAdminAccounts(i, admin, accounts, time.Now())
	if errors.Is(err, storage.ErrCleanupAccountConflict) {
		return cleanupBlocked("provider_account_changed")
	}
	if err != nil {
		return errors.Join(errCleanupFatal, err)
	}
	i.Work = bound
	m.cleanupPhase("administrative_accounts_bound")
	if err := m.validateCleanupAccounts(i, adminEntries); err != nil {
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
