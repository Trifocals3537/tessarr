package manager

import (
	"errors"
	"fmt"
)

type watchedNZBReconcileFunc func() (watchedNZBReconciliationState, error)
type watchedNZBSubmitFunc func() (string, error)
type watchedNZBDurabilityBarrierFunc func() error
type watchedNZBAcceptFunc func() (string, error)
type watchedNZBCleanupFunc func(string)

func runWatchedNZBTransaction(
	identity watchedNZBIdentity,
	reconcile watchedNZBReconcileFunc,
	submit watchedNZBSubmitFunc,
	durabilityBarrier watchedNZBDurabilityBarrierFunc,
	accept watchedNZBAcceptFunc,
	cleanup watchedNZBCleanupFunc,
) error {
	if err := identity.validate(); err != nil {
		return ambiguousWatchedNZBError(err)
	}
	if reconcile == nil ||
		submit == nil ||
		durabilityBarrier == nil ||
		accept == nil ||
		cleanup == nil {
		return ambiguousWatchedNZBError(
			fmt.Errorf("watcher transaction callback is nil"),
		)
	}

	state, err := reconcile()
	if err != nil {
		return err
	}
	if state != watchedNZBReconciliationDurable {
		submittedID, submitErr := submit()
		if (submitErr == nil && submittedID != identity.ID) ||
			(submitErr != nil && submittedID != "" && submittedID != identity.ID) {
			return errors.Join(
				submitErr,
				ambiguousWatchedNZBError(fmt.Errorf(
					"watcher submission returned ID %q, want %q",
					submittedID,
					identity.ID,
				)),
			)
		}

		state, err = reconcile()
		if err != nil {
			return errors.Join(submitErr, err)
		}
		if state != watchedNZBReconciliationDurable {
			if submitErr != nil {
				return submitErr
			}
			return ambiguousWatchedNZBError(
				fmt.Errorf("watcher submission did not produce durable matching state"),
			)
		}
	}

	// Reconciliation reads the live index. A failed append-log Sync can leave a
	// matching row visible in that index even though a crash would lose it.
	// Flush the queue store, then re-read all provenance before consuming the
	// importing source. On a transient failure, the next watcher pass retries
	// this exact barrier without resubmitting the deterministic ID.
	if err := durabilityBarrier(); err != nil {
		return fmt.Errorf("prove watched NZB queue durability: %w", err)
	}
	state, err = reconcile()
	if err != nil {
		return err
	}
	if state != watchedNZBReconciliationDurable {
		return ambiguousWatchedNZBError(
			fmt.Errorf("watched NZB state changed across durability barrier"),
		)
	}

	acceptedPath, err := accept()
	if err != nil {
		return fmt.Errorf("mark watched NZB accepted: %w", err)
	}
	cleanup(acceptedPath)
	return nil
}
