package manager

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/workflow"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/storage"
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
	if m.queue == nil || snapshot == nil || snapshot.Entry == nil {
		return errCleanupFatal
	}
	lease, err := m.queue.lifecycle.beginDelete(snapshot.InfoHash)
	if err != nil {
		return nil
	}
	complete := false
	defer func() { lease.Finish(complete) }()
	ctx := m.cleanupContext()
	wait, cancel := context.WithTimeout(ctx, defaultEntryDeleteDrainTimeout)
	err = lease.Wait(wait)
	cancel()
	if err != nil {
		return err
	}
	if m.queue.removePendingJobs != nil {
		m.queue.removePendingJobs(snapshot.InfoHash)
	}
	i, err := m.storage.ClaimCleanup(snapshot.InfoHash, snapshot.QueueIncarnation, m.queue.cleanupIdentity(), m.cleanupAccounts(snapshot), time.Now(), restart)
	if errors.Is(err, workflow.ErrClaim) || errors.Is(err, workflow.ErrNotDue) {
		return nil
	}
	if err != nil {
		m.recordWorkflowFault(err)
		return errors.Join(errCleanupFatal, err)
	}
	m.cleanupPhase("claimed")
	outcome := workflow.Run(ctx, func(ctx context.Context) error {
		if err := m.storage.StartCleanup(i); err != nil {
			return errors.Join(errCleanupFatal, err)
		}
		if err := m.storage.RetireCleanup(i); err != nil {
			return errors.Join(errCleanupFatal, err)
		}
		// The independent administrative main obligation remains authoritative.
		pendingAdmin, err := m.storage.CleanupAdminPending(i.InfoHash)
		if err != nil {
			return errors.Join(errCleanupFatal, err)
		}
		if pendingAdmin {
			return cleanupRetry("awaiting_administrative_cleanup")
		}
		if i.UnrecoverableCleanupPending {
			return cleanupBlocked("unrecoverable_callback")
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
			for _, entry := range entries {
				for _, p := range entry.Providers {
					if p == nil {
						continue
					}
					if p.Provider == "usenet" {
						return cleanupBlocked("usenet_metadata_requires_review")
					}
					client := m.ProviderClient(p.Provider)
					if client == nil || i.Work.Accounts[p.Provider] == "" || i.Work.Accounts[p.Provider] != submissionAccount(client.Config()) {
						return cleanupBlocked("provider_account_changed")
					}
					if err := m.verifyPlacementAccount(client, p.ID); err != nil {
						return cleanupBlocked("provider_account_changed")
					}
				}
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
		return nil
	})
	if outcome.Kind == workflow.Succeeded {
		complete = true
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
	for _, entry := range cleanupEntries(i) {
		for _, p := range entry.Providers {
			if p == nil {
				continue
			}
			client := m.ProviderClient(p.Provider)
			if client == nil {
				return cleanupBlocked("provider_account_changed")
			}
			var err error
			if c, ok := client.(debrid.ContextTorrentGetter); ok {
				_, err = c.GetTorrentContext(ctx, p.ID)
			} else {
				_, err = client.GetTorrent(p.ID)
			}
			if errors.Is(err, customerror.TorrentNotFoundError) {
				continue
			}
			if err != nil {
				return cleanupRetry("provider_lookup_unavailable")
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
