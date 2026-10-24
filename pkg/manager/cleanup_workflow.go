package manager

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Trifocals3537/tessarr/internal/workflow"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/google/uuid"
)

var errCleanupFatal = errors.New("cleanup checkpoint failed")

type cleanupFailure struct {
	cause   error
	reason  string
	blocked bool
}

func (e *cleanupFailure) Error() string  { return e.reason }
func (e *cleanupFailure) Unwrap() error  { return e.cause }
func cleanupBlocked(reason string) error { return &cleanupFailure{reason: reason, blocked: true} }
func cleanupRetry(reason string) error   { return &cleanupFailure{reason: reason} }

func (m *Manager) cleanupContext() context.Context {
	if m.ctx != nil {
		return m.ctx
	}
	return context.Background()
}
func cleanupEntries(i *storage.QueueDeletionIntent) []*storage.Entry {
	if i.PlacementScope == storage.QueuePlacementScopeTransferOnly {
		return i.PlacementEntries
	}
	return append([]*storage.Entry{i.Entry}, i.PlacementEntries...)
}
func (m *Manager) cleanupAccounts(i *storage.QueueDeletionIntent) map[string]string {
	accounts := map[string]string{}
	if m.clients == nil {
		return accounts
	}
	for _, e := range cleanupEntries(i) {
		for _, p := range e.Providers {
			if p != nil && p.Provider != "usenet" {
				if c := m.ProviderClient(p.Provider); c != nil {
					accounts[p.Provider] = submissionAccount(c.Config())
				}
			}
		}
	}
	return accounts
}

// runCleanupIntent owns the in-process deletion lease and the durable claim.
// No elapsed-time lease stealing is allowed: Stop drains this worker before
// releasing process/storage ownership to another manager.
func (m *Manager) runCleanupIntent(snapshot *storage.QueueDeletionIntent, restart bool) error {
	return m.runCleanupAttempt(snapshot, restart, false)
}

// explicitAdmin is used only by the original removePlacements=true operation,
// never by the generic operator retry action or the automatic worker.
func (m *Manager) runCleanupAttempt(snapshot *storage.QueueDeletionIntent, restart, explicitAdmin bool) error {
	if m.queue == nil || snapshot == nil || snapshot.Entry == nil {
		return errCleanupFatal
	}
	var i *storage.QueueDeletionIntent
	lease, err := m.queue.lifecycle.beginDeleteChecked(snapshot.InfoHash, func() error {
		if explicitAdmin {
			if snapshot.Work == nil {
				return workflow.ErrClaim
			}
			if err := m.storage.ResolveCleanup(snapshot.InfoHash, snapshot.QueueIncarnation, snapshot.Work.Revision, uuid.NewString(), "retry", time.Now()); err != nil {
				return err
			}
		}
		var claimErr error
		accounts := m.cleanupAccounts(snapshot)
		if snapshot.Work == nil || (snapshot.Work.Attempt == 0 && snapshot.Work.Accounts == nil) {
			admin, err := m.storage.PendingAdministrativeCleanup(snapshot.InfoHash)
			if err != nil {
				return err
			}
			if admin != nil {
				for name, account := range m.cleanupAccounts(&storage.QueueDeletionIntent{Entry: admin.Entry}) {
					accounts[name] = account
				}
			}
		}
		i, claimErr = m.storage.ClaimCleanup(snapshot.InfoHash, snapshot.QueueIncarnation, m.queue.cleanupIdentity(), accounts, time.Now(), restart)
		return claimErr
	})
	if errors.Is(err, workflow.ErrClaim) || errors.Is(err, workflow.ErrNotDue) ||
		errors.Is(err, storage.ErrCleanupIntentObsolete) || errors.Is(err, ErrQueueEntryDeleting) {
		if explicitAdmin {
			return err
		}
		return nil
	}
	if err != nil {
		m.recordWorkflowFault(err)
		return errors.Join(errCleanupFatal, err)
	}
	complete := false
	defer func() { lease.Finish(complete) }()
	ctx := m.cleanupContext()
	m.cleanupPhase("claimed")
	outcome := workflow.Run(ctx, func(ctx context.Context) error {
		timeout := m.queue.deleteDrainTimeout
		if timeout <= 0 {
			timeout = defaultEntryDeleteDrainTimeout
		}
		wait, cancel := context.WithTimeout(ctx, timeout)
		err := lease.Wait(wait)
		cancel()
		if err != nil {
			return err
		}
		if m.queue.removePendingJobs != nil {
			m.queue.removePendingJobs(snapshot.InfoHash)
		}
		if err := m.storage.StartCleanup(i); err != nil {
			return errors.Join(errCleanupFatal, err)
		}
		if err := m.storage.RetireCleanup(i); err != nil {
			return errors.Join(errCleanupFatal, err)
		}
		if i.UnrecoverableCleanupPending {
			return cleanupBlocked("unrecoverable_callback")
		}
		// The independent administrative main obligation remains authoritative.
		admin, err := m.storage.CleanupAdminIntent(i)
		if err != nil {
			return errors.Join(errCleanupFatal, err)
		}
		if admin != nil {
			if explicitAdmin {
				if err := m.retrySavedAdministrativeCleanup(i, admin); err != nil {
					return err
				}
			} else {
				if err := m.validateCleanupAccounts(i, []*storage.Entry{admin.Entry}, true); err != nil {
					return err
				}
				if err := m.reconcileProviderEntries(ctx, []*storage.Entry{admin.Entry}); err != nil {
					return err
				}
				if err := m.storage.CompleteCleanupAdmin(i, admin); err != nil {
					return errors.Join(errCleanupFatal, err)
				}
			}
		}
		if i.PlacementCleanupPending {
			entries := cleanupEntries(i)
			if i.PlacementScope == storage.QueuePlacementScopeTransferOnly {
				for _, entry := range entries {
					if err := m.storage.ReserveTransferCleanup(entry); err != nil {
						return cleanupBlocked("placement_is_protected")
					}
				}
			}
			if err := m.validateCleanupAccounts(i, entries, true); err != nil {
				return err
			}
			if i.Work.EffectStarted || i.Work.Mode == "reconcile" {
				if err := m.reconcileCleanupProviders(ctx, i); err != nil {
					return err
				}
			} else {
				if err := m.storage.CleanupEffectStarted(i.InfoHash, i.QueueIncarnation, i.Work.Token, time.Now()); err != nil {
					return errors.Join(errCleanupFatal, err)
				}
				m.cleanupPhase("provider_intent")
				if err := m.RemoveTorrentPlacements(entries...); err != nil {
					return &cleanupFailure{reason: "provider_result_uncertain", cause: err}
				}
			}
			m.cleanupPhase("provider_done")
			if err := m.storage.CleanupPlacementsClean(i); err != nil {
				return errors.Join(errCleanupFatal, err)
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		err = m.storage.CompleteCleanup(i, func() error {
			if i.PreserveFiles {
				return nil
			}
			if i.Work.Mode == "reconcile" {
				return m.reconcileCleanupFiles(i.Entry)
			}
			err := m.queue.deleteEntryFiles(i.Entry)
			if err == nil {
				m.cleanupPhase("local_done")
			}
			return err
		})
		if err != nil {
			if m.workflowAvailable() != nil {
				return errors.Join(errCleanupFatal, err)
			}
			return cleanupBlocked("local_cleanup_requires_review")
		}
		// Completion is authoritative even if shutdown cancels the context
		// while the local callback or final checkpoints are finishing. Releasing
		// an already removed intent would falsely suspend new admissions.
		complete = true
		return nil
	})
	if complete {
		return nil
	}
	if errors.Is(outcome.Err, errCleanupFatal) {
		m.recordWorkflowFault(outcome.Err)
		return outcome.Err
	}
	reason := "worker_interrupted"
	blocked := false
	var failure *cleanupFailure
	if errors.As(outcome.Err, &failure) {
		reason = failure.reason
		blocked = failure.blocked
	}
	if outcome.Kind == workflow.Panicked {
		reason = "worker_panicked"
	}
	if err := m.storage.ReleaseCleanup(i.InfoHash, i.QueueIncarnation, i.Work.Token, reason, blocked, time.Now()); err != nil {
		m.recordWorkflowFault(err)
		return errors.Join(errCleanupFatal, err)
	}
	return fmt.Errorf("cleanup %s: %w", i.InfoHash, outcome.Err)
}

// A recorded/uncertain DELETE is never blindly replayed. Only a typed exact-ID
// absence can discharge its obligation. A listing miss or HTTP 500 is not proof.
func (m *Manager) reconcileCleanupProviders(ctx context.Context, i *storage.QueueDeletionIntent) error {
	return m.reconcileProviderEntries(ctx, cleanupEntries(i))
}

func (m *Manager) reconcileProviderEntries(ctx context.Context, entries []*storage.Entry) error {
	for _, entry := range entries {
		for _, p := range entry.Providers {
			if p == nil {
				continue
			}
			client := m.ProviderClient(p.Provider)
			if client == nil {
				return cleanupBlocked("provider_account_changed")
			}
			checker, ok := client.(debrid.ContextTorrentPresenceChecker)
			if !ok {
				return cleanupBlocked("provider_absence_requires_review")
			}
			presence, err := checker.TorrentPresenceContext(ctx, p.ID)
			if errors.Is(err, debrid.ErrPresenceUnverifiable) {
				return cleanupBlocked("provider_absence_requires_review")
			}
			if err != nil {
				return cleanupRetry("provider_lookup_unavailable")
			}
			if presence == debrid.TorrentAbsent {
				continue
			}
			if presence != debrid.TorrentPresent {
				return cleanupBlocked("provider_absence_requires_review")
			}
			return cleanupBlocked("provider_effect_requires_review")
		}
	}
	return nil
}

func (m *Manager) retryCleanupWorkflows() {
	if m.storage == nil || m.queue == nil || m.workflowAvailable() != nil {
		return
	}
	intents, err := m.storage.QueuedDeletionIntents()
	if err != nil {
		m.recordWorkflowFault(err)
		return
	}
	count := 0
	for _, i := range intents {
		if m.cleanupContext().Err() != nil || count >= 32 {
			return
		}
		if i.Work != nil && (i.Work.State == workflow.Blocked || i.Work.State == workflow.Running || time.Now().Before(i.Work.NextAt)) {
			continue
		}
		count++
		if err := m.runCleanupIntent(i, false); errors.Is(err, errCleanupFatal) {
			return
		}
	}
}
func (m *Manager) startCleanupWorkflows() {
	m.startBackground("cleanup workflows", func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-m.cleanupContext().Done():
				return
			case <-ticker.C:
				m.retryCleanupWorkflows()
			}
		}
	})
}

// ResolveCleanupWorkflow requests another fenced attempt. It does not execute
// provider/file mutations in the HTTP request and cannot widen saved scope.
func (m *Manager) ResolveCleanupWorkflow(key, incarnation string, revision uint64, id, action string) error {
	if err := m.workflowAvailable(); err != nil {
		return err
	}
	return m.storage.ResolveCleanup(key, incarnation, revision, id, action, time.Now())
}
func (m *Manager) CleanupWorkflows() ([]*storage.QueueDeletionIntent, []storage.CleanupReceipt, error) {
	if err := m.storage.DurabilityError(); err != nil {
		return nil, nil, err
	}
	intents, err := m.storage.QueuedDeletionIntents()
	if err != nil {
		return nil, nil, err
	}
	receipts, err := m.storage.CleanupReceipts(100)
	return intents, receipts, err
}

func (m *Manager) cleanupPhase(stage string) {
	if m.cleanupPhaseForTest != nil {
		m.cleanupPhaseForTest(stage)
	}
}
