package manager

import (
	"crypto/sha256"
	"fmt"
	"os"

	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/Trifocals3537/tessarr/pkg/usenet"
)

type watchedNZBReconciliationState uint8

const (
	watchedNZBReconciliationAbsent watchedNZBReconciliationState = iota
	watchedNZBReconciliationResumable
	watchedNZBReconciliationDurable
)

func reconcileWatchedNZBState(
	identity watchedNZBIdentity,
	metadataRoot string,
	maxBytes int64,
	queueLookup watchedNZBEntryLookup,
	mainLookup watchedNZBEntryLookup,
	metadataLookup watchedNZBMetadataLookup,
	isQueueNotFound watchedNZBNotFound,
	isMainNotFound watchedNZBNotFound,
	isMetadataNotFound watchedNZBNotFound,
) (watchedNZBReconciliationState, error) {
	if err := identity.validate(); err != nil {
		return watchedNZBReconciliationAbsent, ambiguousWatchedNZBError(err)
	}
	queueEntry, queueFound, err := loadWatchedNZBEntry(
		identity,
		"queue",
		queueLookup,
		isQueueNotFound,
	)
	if err != nil {
		return watchedNZBReconciliationAbsent, err
	}
	mainEntry, mainFound, err := loadWatchedNZBEntry(
		identity,
		"main",
		mainLookup,
		isMainNotFound,
	)
	if err != nil {
		return watchedNZBReconciliationAbsent, err
	}
	metadata, metadataFound, err := loadWatchedNZBMetadata(
		identity,
		metadataLookup,
		isMetadataNotFound,
	)
	if err != nil {
		return watchedNZBReconciliationAbsent, err
	}

	if queueFound {
		if !metadataFound {
			return watchedNZBReconciliationAbsent, ambiguousWatchedNZBError(
				fmt.Errorf("visible watched NZB entry has no metadata"),
			)
		}
		if err := matchWatchedNZBDurableState(
			identity,
			queueEntry,
			metadata,
			metadataRoot,
			maxBytes,
		); err != nil {
			return watchedNZBReconciliationAbsent, ambiguousWatchedNZBError(
				fmt.Errorf("queue state mismatch: %w", err),
			)
		}
		if mainFound {
			if err := matchWatchedNZBEntryMetadata(
				identity,
				mainEntry,
				metadata,
			); err != nil {
				return watchedNZBReconciliationAbsent, ambiguousWatchedNZBError(
					fmt.Errorf("main state mismatch: %w", err),
				)
			}
		}
		return watchedNZBReconciliationDurable, nil
	}
	if mainFound {
		if !metadataFound {
			return watchedNZBReconciliationAbsent, ambiguousWatchedNZBError(
				fmt.Errorf("main-only watched NZB entry has no metadata"),
			)
		}
		if err := matchWatchedNZBMainTerminalState(
			identity,
			mainEntry,
			metadata,
			metadataRoot,
			maxBytes,
		); err != nil {
			return watchedNZBReconciliationAbsent, ambiguousWatchedNZBError(
				fmt.Errorf("main-only terminal state mismatch: %w", err),
			)
		}
		return watchedNZBReconciliationDurable, nil
	}

	if metadataFound {
		if err := matchWatchedNZBPartialMetadata(
			identity,
			metadata,
			metadataRoot,
			maxBytes,
		); err != nil {
			return watchedNZBReconciliationAbsent, ambiguousWatchedNZBError(
				fmt.Errorf("partial metadata mismatch: %w", err),
			)
		}
		return watchedNZBReconciliationResumable, nil
	}

	staged, err := usenet.ReadCanonicalStagedNZBAt(
		metadataRoot,
		identity.ID,
		maxBytes,
	)
	if err != nil {
		if os.IsNotExist(err) {
			return watchedNZBReconciliationAbsent, nil
		}
		return watchedNZBReconciliationAbsent, ambiguousWatchedNZBError(
			fmt.Errorf("inspect orphan staged source: %w", err),
		)
	}
	if digest := sha256.Sum256(staged); digest != identity.ContentDigest {
		return watchedNZBReconciliationAbsent, ambiguousWatchedNZBError(
			fmt.Errorf(
				"orphan staged source digest %x does not match identity digest %x",
				digest,
				identity.ContentDigest,
			),
		)
	}
	return watchedNZBReconciliationResumable, nil
}

func loadWatchedNZBEntry(
	identity watchedNZBIdentity,
	storeName string,
	lookup watchedNZBEntryLookup,
	isNotFound watchedNZBNotFound,
) (*storage.Entry, bool, error) {
	if lookup == nil || isNotFound == nil {
		return nil, false, ambiguousWatchedNZBError(
			fmt.Errorf("%s entry lookup input is nil", storeName),
		)
	}
	entry, err := lookup(identity.ID)
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, ambiguousWatchedNZBError(
			fmt.Errorf("inspect %s entry: %w", storeName, err),
		)
	}
	if err := matchWatchedNZBEntry(identity, entry); err != nil {
		return nil, false, ambiguousWatchedNZBError(
			fmt.Errorf("%s entry mismatch: %w", storeName, err),
		)
	}
	return entry, true, nil
}

func loadWatchedNZBMetadata(
	identity watchedNZBIdentity,
	lookup watchedNZBMetadataLookup,
	isNotFound watchedNZBNotFound,
) (*storage.NZB, bool, error) {
	if lookup == nil || isNotFound == nil {
		return nil, false, ambiguousWatchedNZBError(
			fmt.Errorf("metadata lookup input is nil"),
		)
	}
	metadata, err := lookup(identity.ID)
	if err != nil {
		if isNotFound(err) {
			return nil, false, nil
		}
		return nil, false, ambiguousWatchedNZBError(
			fmt.Errorf("inspect metadata: %w", err),
		)
	}
	if err := matchWatchedNZBMetadata(identity, metadata); err != nil {
		return nil, false, ambiguousWatchedNZBError(err)
	}
	return metadata, true, nil
}

func ambiguousWatchedNZBError(err error) error {
	if err == nil {
		return errWatchedNZBStateAmbiguous
	}
	return fmt.Errorf("%w: %v", errWatchedNZBStateAmbiguous, err)
}
