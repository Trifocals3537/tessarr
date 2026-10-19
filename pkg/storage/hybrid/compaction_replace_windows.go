//go:build windows

package hybrid

import (
	"errors"
	"fmt"
	"os"

	"github.com/Trifocals3537/tessarr/internal/safepath"
)

// Windows does not permit Go's os.Rename to replace an existing destination,
// and an open handle can prevent either rename. Keep the old canonical bytes
// recoverable under .backup until the fully-synced .compact log has been
// promoted and reopened successfully.
func (s *Store) installCompactedLog(
	oldLog *appendLog,
	newLog *appendLog,
	paths compactionPaths,
) compactionInstallResult {
	if err := ensureCanonicalLogIdentity(oldLog, paths.canonical); err != nil {
		cleanupErr := cleanupUncommittedCompact(newLog, paths)
		return compactionInstallResult{
			log: oldLog,
			err: errors.Join(err, cleanupErr),
		}
	}

	if err := newLog.Close(); err != nil {
		// The canonical handle is still live and authoritative.
		cleanupErr := removeClosedCompact(paths)
		return compactionInstallResult{
			log: oldLog,
			err: errors.Join(
				fmt.Errorf("close staged compact log: %w", err),
				cleanupErr,
			),
		}
	}
	if err := oldLog.Close(); err != nil {
		reopened, reopenErr := openExistingAppendLog(paths.canonical)
		cleanupErr := removeClosedCompact(paths)
		return compactionInstallResult{
			log: reopened,
			err: errors.Join(
				fmt.Errorf("close canonical log before Windows replacement: %w", err),
				wrapCompactionError("reopen canonical log after close failure", reopenErr),
				cleanupErr,
			),
		}
	}

	if err := safepath.RenameRegularFile(paths.canonical, paths.backup); err != nil {
		reopened, reopenErr := openExistingAppendLog(paths.canonical)
		cleanupErr := removeClosedCompact(paths)
		return compactionInstallResult{
			log: reopened,
			err: errors.Join(
				fmt.Errorf("move canonical log to recoverable backup: %w", err),
				wrapCompactionError("reopen canonical log after backup failure", reopenErr),
				cleanupErr,
			),
		}
	}
	if err := syncParentDirectory(paths.parent); err != nil {
		return s.rollbackWindowsCompaction(paths, fmt.Errorf("sync canonical backup: %w", err))
	}
	s.reachCompactionPhase(compactionPhaseCanonicalBackedUp)

	if err := safepath.RenameRegularFile(paths.compact, paths.canonical); err != nil {
		return s.rollbackWindowsCompaction(paths, fmt.Errorf("promote compact log: %w", err))
	}
	s.reachCompactionPhase(compactionPhaseCanonicalReplaced)

	syncErr := syncParentDirectory(paths.parent)
	if syncErr == nil {
		s.reachCompactionPhase(compactionPhaseReplacementSynced)
	}

	active, openErr := openExistingAppendLog(paths.canonical)
	if openErr != nil {
		return s.rollbackWindowsPromotedLog(
			paths,
			errors.Join(
				wrapCompactionError("sync promoted compact log", syncErr),
				fmt.Errorf("open promoted compact log: %w", openErr),
			),
		)
	}
	if err := active.ValidateComplete(); err != nil {
		closeErr := active.Close()
		return s.rollbackWindowsPromotedLog(
			paths,
			errors.Join(
				wrapCompactionError("sync promoted compact log", syncErr),
				fmt.Errorf("validate promoted compact log: %w", err),
				wrapCompactionError("close invalid promoted compact log", closeErr),
			),
		)
	}

	removeErr := safepath.RemoveRegularFile(paths.backup)
	var cleanupSyncErr error
	if removeErr == nil {
		cleanupSyncErr = syncParentDirectory(paths.parent)
		if cleanupSyncErr == nil {
			s.reachCompactionPhase(compactionPhaseBackupRemoved)
		}
	}
	return compactionInstallResult{
		log:       active,
		committed: true,
		err: errors.Join(
			wrapCompactionError("sync promoted compact log", syncErr),
			wrapCompactionError("remove canonical backup", removeErr),
			wrapCompactionError("sync canonical backup cleanup", cleanupSyncErr),
		),
	}
}

func (s *Store) rollbackWindowsCompaction(paths compactionPaths, cause error) compactionInstallResult {
	rollbackErr := safepath.RenameRegularFile(paths.backup, paths.canonical)
	var syncErr error
	if rollbackErr == nil {
		syncErr = syncParentDirectory(paths.parent)
	}
	reopened, reopenErr := openExistingAppendLog(paths.canonical)
	cleanupErr := removeClosedCompact(paths)
	return compactionInstallResult{
		log: reopened,
		err: errors.Join(
			cause,
			wrapCompactionError("restore canonical backup", rollbackErr),
			wrapCompactionError("sync restored canonical backup", syncErr),
			wrapCompactionError("reopen restored canonical log", reopenErr),
			cleanupErr,
		),
	}
}

func (s *Store) rollbackWindowsPromotedLog(paths compactionPaths, cause error) compactionInstallResult {
	moveNewErr := safepath.RenameRegularFile(paths.canonical, paths.compact)
	if moveNewErr != nil {
		// The promoted compact generation is still canonical. If it can be
		// reopened, its index must be published despite the rollback failure;
		// pairing it with the old in-memory index would corrupt reads.
		active, reopenErr := openExistingAppendLog(paths.canonical)
		if reopenErr == nil {
			validateErr := active.ValidateComplete()
			if validateErr == nil {
				return compactionInstallResult{
					log:       active,
					committed: true,
					err: errors.Join(
						cause,
						fmt.Errorf("move failed promoted log back to staging: %w", moveNewErr),
					),
				}
			}
			reopenErr = errors.Join(validateErr, active.Close())
		}
		return compactionInstallResult{
			err: errors.Join(
				cause,
				fmt.Errorf("move failed promoted log back to staging: %w", moveNewErr),
				wrapCompactionError("reopen promoted canonical log", reopenErr),
			),
		}
	}

	var restoreErr error
	var syncErr error
	restoreErr = safepath.RenameRegularFile(paths.backup, paths.canonical)
	if restoreErr == nil {
		syncErr = syncParentDirectory(paths.parent)
	}
	reopened, reopenErr := openExistingAppendLog(paths.canonical)
	var cleanupErr error
	if restoreErr == nil {
		cleanupErr = removeClosedCompact(paths)
	}
	return compactionInstallResult{
		log: reopened,
		err: errors.Join(
			cause,
			wrapCompactionError("restore canonical backup", restoreErr),
			wrapCompactionError("sync restored canonical backup", syncErr),
			wrapCompactionError("reopen restored canonical log", reopenErr),
			cleanupErr,
		),
	}
}

func removeClosedCompact(paths compactionPaths) error {
	removeErr := safepath.RemoveRegularFile(paths.compact)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	var syncErr error
	if removeErr == nil {
		syncErr = syncParentDirectory(paths.parent)
	}
	return errors.Join(
		wrapCompactionError("remove uncommitted compact log", removeErr),
		wrapCompactionError("sync uncommitted compact cleanup", syncErr),
	)
}

// Windows has no portable directory-fsync operation. The backup protocol keeps
// every interrupted namespace state recoverable without relying on one.
func syncParentDirectory(string) error {
	return nil
}

func wrapCompactionError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}
