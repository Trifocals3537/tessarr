package hybrid

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Trifocals3537/tessarr/internal/safepath"
)

const (
	compactionSuffix = ".compact"
	backupSuffix     = ".backup"
)

// compactionPhase marks the durable boundaries of the replacement protocol.
// The callback is deliberately test-only: subprocess tests terminate at these
// boundaries to exercise real startup recovery with operating-system handles
// released exactly as they would be after a process crash.
type compactionPhase string

const (
	compactionPhaseStaged            compactionPhase = "staged"
	compactionPhaseCanonicalBackedUp compactionPhase = "canonical-backed-up"
	compactionPhaseCanonicalReplaced compactionPhase = "canonical-replaced"
	compactionPhaseReplacementSynced compactionPhase = "replacement-synced"
	compactionPhaseBackupRemoved     compactionPhase = "backup-removed"
)

type compactionPaths struct {
	canonical string
	compact   string
	backup    string
	parent    string
}

type compactionInstallResult struct {
	log       *appendLog
	committed bool
	err       error
}

func pathsForCompaction(path string) (compactionPaths, error) {
	if path == "" {
		return compactionPaths{}, fmt.Errorf("append log path is required")
	}
	absolute, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return compactionPaths{}, fmt.Errorf("resolve append log path: %w", err)
	}
	base := filepath.Base(absolute)
	if base == "." || base == string(filepath.Separator) || base == "" {
		return compactionPaths{}, fmt.Errorf("append log path must name a file: %s", path)
	}
	if err := safepath.ValidateIdentifier(base); err != nil {
		return compactionPaths{}, fmt.Errorf("invalid append log file name: %w", err)
	}
	parent, err := resolveCompactionParent(filepath.Dir(absolute))
	if err != nil {
		return compactionPaths{}, fmt.Errorf("resolve append log directory links: %w", err)
	}
	parentRoot, validatedParent, err := safepath.OpenRoot(parent)
	if err != nil {
		return compactionPaths{}, fmt.Errorf("pin append log directory: %w", err)
	}
	parentInfo, statErr := parentRoot.Stat(".")
	closeErr := parentRoot.Close()
	if statErr != nil || closeErr != nil {
		return compactionPaths{}, fmt.Errorf("inspect append log directory: %w", errors.Join(statErr, closeErr))
	}
	if !parentInfo.IsDir() {
		return compactionPaths{}, fmt.Errorf("append log parent is not a directory")
	}
	parent = validatedParent
	absolute = filepath.Join(parent, base)

	return compactionPaths{
		canonical: absolute,
		compact:   absolute + compactionSuffix,
		backup:    absolute + backupSuffix,
		parent:    parent,
	}, nil
}

type artifactState struct {
	exists bool
	info   os.FileInfo
}

func inspectCompactionArtifact(path string) (artifactState, error) {
	info, err := safepath.StatRegularFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return artifactState{}, nil
	}
	if err != nil {
		return artifactState{}, err
	}
	return artifactState{exists: true, info: info}, nil
}

func ensureCleanCompactionWorkspace(paths compactionPaths) error {
	compact, err := inspectCompactionArtifact(paths.compact)
	if err != nil {
		return err
	}
	backup, err := inspectCompactionArtifact(paths.backup)
	if err != nil {
		return err
	}
	if compact.exists || backup.exists {
		return fmt.Errorf(
			"unresolved compaction artifacts beside %s (compact=%t, backup=%t)",
			paths.canonical,
			compact.exists,
			backup.exists,
		)
	}
	return nil
}

// recoverCompactionState resolves only the two exact sibling artifacts used by
// compaction; it never scans a user-controlled directory. Authority is
// deterministic:
//   - a valid canonical log always wins;
//   - with no canonical log, a valid compact log wins because the old
//     canonical is renamed to backup only after compact is fully synced;
//   - backup is restored only when no valid compact log is available.
func recoverCompactionState(paths compactionPaths) error {
	canonical, err := inspectCompactionArtifact(paths.canonical)
	if err != nil {
		return err
	}
	compact, err := inspectCompactionArtifact(paths.compact)
	if err != nil {
		return err
	}
	backup, err := inspectCompactionArtifact(paths.backup)
	if err != nil {
		return err
	}

	if canonical.exists {
		if !compact.exists && !backup.exists {
			return nil
		}
		if err := validateCompactionLog(paths.canonical); err != nil {
			if backup.exists && !compact.exists {
				if backupErr := validateCompactionLog(paths.backup); backupErr != nil {
					return errors.Join(
						fmt.Errorf("canonical append log is invalid: %w", err),
						fmt.Errorf("backup append log is invalid: %w", backupErr),
					)
				}
				return restoreBackupAroundInvalidCanonical(paths, err)
			}
			return fmt.Errorf("canonical append log is invalid; preserving recovery artifacts: %w", err)
		}
		changed := false
		if compact.exists {
			if err := safepath.RemoveRegularFile(paths.compact); err != nil {
				return fmt.Errorf("remove stale compact log: %w", err)
			}
			changed = true
		}
		if backup.exists {
			if err := safepath.RemoveRegularFile(paths.backup); err != nil {
				return fmt.Errorf("remove stale backup log: %w", err)
			}
			changed = true
		}
		if changed {
			if err := syncParentDirectory(paths.parent); err != nil {
				return fmt.Errorf("sync compaction cleanup: %w", err)
			}
		}
		return nil
	}

	if compact.exists {
		if compactErr := validateCompactionLog(paths.compact); compactErr == nil {
			if err := safepath.RenameRegularFile(paths.compact, paths.canonical); err != nil {
				return fmt.Errorf("promote recovered compact log: %w", err)
			}
			if err := syncParentDirectory(paths.parent); err != nil {
				return fmt.Errorf("sync recovered compact log: %w", err)
			}
			if backup.exists {
				if err := safepath.RemoveRegularFile(paths.backup); err != nil {
					return fmt.Errorf("remove recovered backup log: %w", err)
				}
				if err := syncParentDirectory(paths.parent); err != nil {
					return fmt.Errorf("sync recovered backup cleanup: %w", err)
				}
			}
			return nil
		} else if !backup.exists {
			return fmt.Errorf("compact append log is invalid and no backup exists: %w", compactErr)
		}
	}

	if backup.exists {
		if err := validateCompactionLog(paths.backup); err != nil {
			return fmt.Errorf("backup append log is invalid: %w", err)
		}
		if err := safepath.RenameRegularFile(paths.backup, paths.canonical); err != nil {
			return fmt.Errorf("restore recovered backup log: %w", err)
		}
		if err := syncParentDirectory(paths.parent); err != nil {
			return fmt.Errorf("sync restored backup log: %w", err)
		}
		if compact.exists {
			if err := safepath.RemoveRegularFile(paths.compact); err != nil {
				return fmt.Errorf("remove invalid compact log after backup restore: %w", err)
			}
			if err := syncParentDirectory(paths.parent); err != nil {
				return fmt.Errorf("sync invalid compact cleanup: %w", err)
			}
		}
		return nil
	}

	return nil
}

func restoreBackupAroundInvalidCanonical(paths compactionPaths, canonicalErr error) error {
	// Preserve the rejected canonical generation under .compact until the
	// validated backup is canonical and its rename is synced. A crash between
	// either rename is therefore resolved by the ordinary no-canonical recovery
	// matrix on the next startup.
	if err := safepath.RenameRegularFile(paths.canonical, paths.compact); err != nil {
		return errors.Join(
			fmt.Errorf("canonical append log is invalid: %w", canonicalErr),
			fmt.Errorf("preserve invalid canonical log for backup restore: %w", err),
		)
	}
	if err := syncParentDirectory(paths.parent); err != nil {
		return fmt.Errorf("sync preserved invalid canonical log: %w", err)
	}
	if err := safepath.RenameRegularFile(paths.backup, paths.canonical); err != nil {
		return fmt.Errorf("restore validated backup around invalid canonical: %w", err)
	}
	if err := syncParentDirectory(paths.parent); err != nil {
		return fmt.Errorf("sync validated backup restoration: %w", err)
	}
	if err := safepath.RemoveRegularFile(paths.compact); err != nil {
		return fmt.Errorf("remove preserved invalid canonical after backup restore: %w", err)
	}
	if err := syncParentDirectory(paths.parent); err != nil {
		return fmt.Errorf("sync invalid canonical cleanup: %w", err)
	}
	return nil
}

func validateCompactionLog(path string) error {
	log, err := openExistingAppendLog(path)
	if err != nil {
		return err
	}
	validateErr := log.ValidateComplete()
	closeErr := log.Close()
	return errors.Join(validateErr, closeErr)
}

func ensureCanonicalLogIdentity(log *appendLog, canonicalPath string) error {
	openedInfo, err := log.file.Stat()
	if err != nil {
		return fmt.Errorf("inspect open canonical log: %w", err)
	}
	pathInfo, err := safepath.StatRegularFile(canonicalPath)
	if err != nil {
		return fmt.Errorf("inspect canonical log path: %w", err)
	}
	if !os.SameFile(openedInfo, pathInfo) {
		return fmt.Errorf("canonical log path changed while the store was open: %s", canonicalPath)
	}
	return nil
}

func cleanupUncommittedCompact(log *appendLog, paths compactionPaths) error {
	var closeErr error
	if log != nil {
		closeErr = log.Close()
	}
	removeErr := safepath.RemoveRegularFile(paths.compact)
	if errors.Is(removeErr, os.ErrNotExist) {
		removeErr = nil
	}
	var syncErr error
	if removeErr == nil {
		syncErr = syncParentDirectory(paths.parent)
	}
	return errors.Join(closeErr, removeErr, syncErr)
}

func (s *Store) reachCompactionPhase(phase compactionPhase) {
	if s.compactionPhaseForTest != nil {
		s.compactionPhaseForTest(phase)
	}
}
