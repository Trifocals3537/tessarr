//go:build !windows

package hybrid

import (
	"errors"
	"fmt"

	"github.com/Trifocals3537/tessarr/internal/safepath"
)

// installCompactedLog atomically replaces the canonical directory entry on
// POSIX systems. Both file descriptors remain open during rename, so a failed
// replacement leaves the running store on the original canonical log.
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

	if err := safepath.RenameRegularFile(paths.compact, paths.canonical); err != nil {
		cleanupErr := cleanupUncommittedCompact(newLog, paths)
		return compactionInstallResult{
			log: oldLog,
			err: errors.Join(fmt.Errorf("atomically replace canonical log: %w", err), cleanupErr),
		}
	}
	newLog.path = paths.canonical
	s.reachCompactionPhase(compactionPhaseCanonicalReplaced)

	// Once rename succeeds, disk authority has changed. Publish the compacted
	// log even if a later durability or old-handle close reports an error; using
	// the old index against the new canonical file would be unsafe.
	syncErr := syncParentDirectory(paths.parent)
	if syncErr == nil {
		s.reachCompactionPhase(compactionPhaseReplacementSynced)
	}
	closeErr := oldLog.Close()
	return compactionInstallResult{
		log:       newLog,
		committed: true,
		err: errors.Join(
			wrapCompactionError("sync atomic replacement directory", syncErr),
			wrapCompactionError("close replaced canonical log", closeErr),
		),
	}
}

func syncParentDirectory(path string) error {
	return safepath.SyncDirectory(path)
}

func wrapCompactionError(operation string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", operation, err)
}
