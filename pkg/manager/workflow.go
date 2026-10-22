package manager

import (
	"errors"
	"fmt"

	"github.com/Trifocals3537/tessarr/pkg/storage"
)

// queueOutcomeStore is the first repository boundary for the workflow core.
// A future transactional store must retain the same fencing/durability contract.
type queueOutcomeStore interface {
	CommitQueuedOutcome(*storage.Entry, storage.QueueOutcome) (*storage.Entry, error)
}

var ErrWorkflowUnavailable = errors.New("workflow persistence is unavailable; restart after resolving storage errors")

type workflowFault struct{}

func (q *Queue) commitOutcome(entry *storage.Entry, outcome storage.QueueOutcome) error {
	if q == nil || q.lifecycle == nil {
		return fmt.Errorf("queue outcome repository is unavailable")
	}
	repository := q.outcomes
	if repository == nil && q.storage != nil {
		repository = q.storage
	}
	if repository == nil {
		return fmt.Errorf("queue outcome repository is unavailable")
	}
	return q.lifecycle.withUpdate(entry, func() error {
		updated, err := repository.CommitQueuedOutcome(entry, outcome)
		if err != nil {
			return err
		}
		updated.QueueGeneration = entry.QueueGeneration
		// Queue serialization deliberately excludes these main-entry fencing
		// capabilities. Refreshing persisted metadata must not reset the worker's
		// existing token or grant it an unbound mutation on its next main write.
		updated.MainGeneration = entry.MainGeneration
		updated.MainProviderSnapshot = entry.MainProviderSnapshot
		updated.MainMutationProvider = entry.MainMutationProvider
		updated.MainReimportIncarnation = entry.MainReimportIncarnation
		*entry = *updated
		return nil
	})
}

func obsoleteWorkflow(err error) bool {
	return errors.Is(err, ErrStaleQueueGeneration) || errors.Is(err, ErrQueueEntryDeleting) ||
		errors.Is(err, storage.ErrQueuedEntryDeleting) || storage.IsQueuedEntryNotFound(err) ||
		errors.Is(err, storage.ErrQueueOutcomeConflict)
}

func (m *Manager) workflowAvailable() error {
	if m.workflowFault.Load() != nil {
		return ErrWorkflowUnavailable
	}
	if m.storage != nil {
		if err := m.storage.DurabilityError(); err != nil {
			return errors.Join(ErrWorkflowUnavailable, err)
		}
	}
	return nil
}

func (m *Manager) recordWorkflowFault(err error) {
	if err == nil || obsoleteWorkflow(err) {
		return
	}
	if m.workflowFault.CompareAndSwap(nil, &workflowFault{}) {
		m.logger.Error().Err(err).Msg("Workflow persistence failed; new admissions suspended until restart")
	}
}

func (m *Manager) commitJobOutcome(job *Job, outcome storage.QueueOutcome) error {
	if job.Entry == nil {
		return nil
	}
	if err := m.queue.commitOutcome(job.Entry, outcome); err != nil {
		m.recordWorkflowFault(err)
		return fmt.Errorf("commit job outcome: %w", err)
	}
	return nil
}
