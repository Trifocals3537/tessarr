package usenet

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	watchedNZBImportingSuffix = ".nzb.importing"
	watchedNZBAcceptedSuffix  = ".nzb.accepted"
	acceptedCleanupReadBatch  = 64

	DefaultAcceptedNZBMaxEntries          = 256
	DefaultAcceptedNZBMaxFiles            = 16
	DefaultAcceptedNZBMaxFileBytes  int64 = DefaultWatchedNZBMaxFileBytes
	DefaultAcceptedNZBMaxTotalBytes int64 = DefaultWatchedNZBMaxTotalBytes
)

var (
	// ErrWatchedNZBHardLinkUnavailable means the watched filesystem refused the
	// no-overwrite hard-link transition used to make acceptance crash-safe.
	// NFS/CIFS and other filesystems may not support it. The importing source is
	// deliberately preserved and must be retried with its deterministic ID.
	ErrWatchedNZBHardLinkUnavailable = errors.New("watched NZB acceptance requires hard-link support")

	// ErrWatchedNZBAcceptedConflict means an importing and accepted name both
	// exist but cannot be proven to represent the same bounded content.
	ErrWatchedNZBAcceptedConflict = errors.New("watched NZB accepted state conflicts with importing source")

	// ErrWatchedNZBClaimChanged means the claimed path, inode, size, or
	// modification time changed while its bounded content snapshot was read.
	ErrWatchedNZBClaimChanged = errors.New("watched NZB claim changed while reading")
)

type ClaimedNZBSnapshot struct {
	Path          string
	Content       []byte
	ContentDigest [sha256.Size]byte
	Size          int64
	ModTime       time.Time
}

// AcceptedNZBCleanupLimits bounds terminal-tombstone cleanup by directory
// entries, removal attempts, and logical file size. MaxFiles must not exceed
// MaxEntries, and MaxTotalBytes must be at least MaxFileBytes.
type AcceptedNZBCleanupLimits struct {
	MaxEntries    int
	MaxFiles      int
	MaxFileBytes  int64
	MaxTotalBytes int64
}

type AcceptedNZBCleanupResult struct {
	Scanned      int
	Matched      int
	Attempted    int
	Removed      int
	Failed       int
	BytesRemoved int64
	More         bool
}

func DefaultAcceptedNZBCleanupLimits() AcceptedNZBCleanupLimits {
	return AcceptedNZBCleanupLimits{
		MaxEntries:    DefaultAcceptedNZBMaxEntries,
		MaxFiles:      DefaultAcceptedNZBMaxFiles,
		MaxFileBytes:  DefaultAcceptedNZBMaxFileBytes,
		MaxTotalBytes: DefaultAcceptedNZBMaxTotalBytes,
	}
}

// AcceptedNZBCleaner retains a rooted directory cursor across scans. This is
// what makes enumeration genuinely bounded without permanently starving
// tombstones that sort or happen to appear after a large metadata population.
type AcceptedNZBCleaner struct {
	mu sync.Mutex

	absoluteRoot string
	rooted       *os.Root
	directory    *os.File
	pending      []os.DirEntry
	reachedEOF   bool
	closed       bool
}

// ReadClaimedNZB reads one rooted, no-follow .nzb.importing source with a hard
// byte limit. ClaimNewNZBs can use this during integration instead of an
// unbounded metadata read.
func (u *Usenet) ReadClaimedNZB(path string, maxBytes int64) ([]byte, error) {
	return ReadClaimedNZBAt(u.metadataDir, path, maxBytes)
}

func ReadClaimedNZBAt(metadataRoot, claimedPath string, maxBytes int64) ([]byte, error) {
	snapshot, err := ReadClaimedNZBSnapshotAt(metadataRoot, claimedPath, maxBytes)
	if err != nil {
		return nil, err
	}
	return snapshot.Content, nil
}

func (u *Usenet) ReadClaimedNZBSnapshot(path string, maxBytes int64) (*ClaimedNZBSnapshot, error) {
	return ReadClaimedNZBSnapshotAt(u.metadataDir, path, maxBytes)
}

func ReadClaimedNZBSnapshotAt(
	metadataRoot, claimedPath string,
	maxBytes int64,
) (*ClaimedNZBSnapshot, error) {
	return readClaimedNZBSnapshotAt(metadataRoot, claimedPath, maxBytes, nil)
}

// AcceptClaimedNZB creates the terminal .nzb.accepted name without overwriting
// an existing file, then removes only the importing link. A crash after Link
// and before Remove leaves two links to the same inode; a retry recognizes that
// exact state and finishes the transition.
func (u *Usenet) AcceptClaimedNZB(
	claimedPath string,
	expectedDigest [sha256.Size]byte,
	maxBytes int64,
) (string, error) {
	return AcceptClaimedNZBAt(u.metadataDir, claimedPath, expectedDigest, maxBytes)
}

func AcceptClaimedNZBAt(
	metadataRoot, claimedPath string,
	expectedDigest [sha256.Size]byte,
	maxBytes int64,
) (string, error) {
	return acceptClaimedNZBAt(
		metadataRoot,
		claimedPath,
		expectedDigest,
		maxBytes,
		func(rooted *os.Root, importingLeaf, acceptedLeaf string) error {
			return rooted.Link(importingLeaf, acceptedLeaf)
		},
	)
}

type watchedNZBLinkFunc func(*os.Root, string, string) error
type watchedNZBDirSyncFunc func(*os.Root) error

func acceptClaimedNZBAt(
	metadataRoot, claimedPath string,
	expectedDigest [sha256.Size]byte,
	maxBytes int64,
	link watchedNZBLinkFunc,
) (string, error) {
	return acceptClaimedNZBAtWithOps(
		metadataRoot,
		claimedPath,
		expectedDigest,
		maxBytes,
		link,
		syncWatchedNZBRoot,
	)
}

func acceptClaimedNZBAtWithOps(
	metadataRoot, claimedPath string,
	expectedDigest [sha256.Size]byte,
	maxBytes int64,
	link watchedNZBLinkFunc,
	syncDirectory watchedNZBDirSyncFunc,
) (string, error) {
	if link == nil {
		return "", fmt.Errorf("watched NZB hard-link operation is nil")
	}
	if syncDirectory == nil {
		return "", fmt.Errorf("watched NZB directory-sync operation is nil")
	}
	absoluteRoot, importingLeaf, err := watchedNZBPath(
		metadataRoot,
		claimedPath,
		watchedNZBImportingSuffix,
	)
	if err != nil {
		return "", err
	}
	if err := validateWatchedNZBMaxBytes(maxBytes); err != nil {
		return "", err
	}
	if expectedDigest == ([sha256.Size]byte{}) {
		return "", fmt.Errorf("watched NZB expected content digest is empty")
	}
	acceptedLeaf := strings.TrimSuffix(importingLeaf, ".importing") + ".accepted"
	acceptedPath := filepath.Join(absoluteRoot, acceptedLeaf)
	if _, _, err := watchedNZBPath(absoluteRoot, acceptedPath, watchedNZBAcceptedSuffix); err != nil {
		return "", err
	}

	rooted, err := os.OpenRoot(absoluteRoot)
	if err != nil {
		return "", fmt.Errorf("open watched NZB metadata root: %w", err)
	}
	defer rooted.Close()

	importingInfo, err := rooted.Lstat(importingLeaf)
	if err != nil {
		if os.IsNotExist(err) {
			acceptedInfo, acceptedErr := rooted.Lstat(acceptedLeaf)
			if acceptedErr != nil {
				return acceptedPath, fmt.Errorf("inspect accepted watched NZB: %w", acceptedErr)
			}
			if err := validateWatchedNZBFileInfo(acceptedLeaf, acceptedInfo, maxBytes); err != nil {
				return acceptedPath, err
			}
			acceptedDigest, digestErr := digestWatchedNZBLeaf(
				rooted,
				acceptedLeaf,
				acceptedInfo,
				maxBytes,
			)
			if digestErr != nil {
				return acceptedPath, digestErr
			}
			if acceptedDigest != expectedDigest {
				return acceptedPath, fmt.Errorf(
					"%w: terminal file %q does not match submitted content",
					ErrWatchedNZBAcceptedConflict,
					acceptedLeaf,
				)
			}
			if err := syncWatchedNZBLeaf(rooted, acceptedLeaf, maxBytes); err != nil {
				return acceptedPath, fmt.Errorf(
					"sync terminal watched NZB content %q: %w",
					acceptedLeaf,
					err,
				)
			}
			if err := syncDirectory(rooted); err != nil {
				return acceptedPath, fmt.Errorf(
					"sync terminal watched NZB state %q: %w",
					acceptedLeaf,
					err,
				)
			}
			return acceptedPath, nil
		}
		return acceptedPath, fmt.Errorf("inspect importing watched NZB: %w", err)
	}
	if err := validateWatchedNZBFileInfo(importingLeaf, importingInfo, maxBytes); err != nil {
		return acceptedPath, err
	}
	importingDigest, err := digestWatchedNZBLeaf(rooted, importingLeaf, importingInfo, maxBytes)
	if err != nil {
		return acceptedPath, err
	}
	if importingDigest != expectedDigest {
		return acceptedPath, fmt.Errorf(
			"%w: importing file %q changed after submission",
			ErrWatchedNZBAcceptedConflict,
			importingLeaf,
		)
	}
	if err := syncWatchedNZBLeaf(rooted, importingLeaf, maxBytes); err != nil {
		return acceptedPath, fmt.Errorf(
			"sync importing watched NZB content %q: %w",
			importingLeaf,
			err,
		)
	}

	linkErr := link(rooted, importingLeaf, acceptedLeaf)
	switch {
	case linkErr == nil:
		if err := validateAcceptedLink(rooted, importingLeaf, acceptedLeaf, importingInfo, maxBytes); err != nil {
			return acceptedPath, err
		}
	case os.IsExist(linkErr):
		same, err := sameWatchedNZBContent(rooted, importingLeaf, acceptedLeaf, maxBytes)
		if err != nil {
			return acceptedPath, fmt.Errorf("%w: %v", ErrWatchedNZBAcceptedConflict, err)
		}
		if !same {
			return acceptedPath, fmt.Errorf(
				"%w: %q and %q contain different data",
				ErrWatchedNZBAcceptedConflict,
				importingLeaf,
				acceptedLeaf,
			)
		}
		if err := syncWatchedNZBLeaf(rooted, acceptedLeaf, maxBytes); err != nil {
			return acceptedPath, fmt.Errorf(
				"sync existing accepted watched NZB content %q: %w",
				acceptedLeaf,
				err,
			)
		}
	default:
		return acceptedPath, errors.Join(
			ErrWatchedNZBHardLinkUnavailable,
			fmt.Errorf("link %q to %q beneath watched root: %w", importingLeaf, acceptedLeaf, linkErr),
		)
	}

	// The accepted directory entry must be durable before the only importing
	// name can be removed. On failure both names remain recoverable.
	if err := syncDirectory(rooted); err != nil {
		return acceptedPath, fmt.Errorf(
			"sync accepted watched NZB %q before removing %q: %w",
			acceptedLeaf,
			importingLeaf,
			err,
		)
	}
	if err := rooted.Remove(importingLeaf); err != nil && !os.IsNotExist(err) {
		return acceptedPath, fmt.Errorf(
			"accepted watched NZB but failed to remove importing link %q: %w",
			importingLeaf,
			err,
		)
	}
	// Make the importing-name removal durable as a separate transition. If it
	// fails, the accepted name remains and a retry can safely reconcile either
	// the one-link or two-link state.
	if err := syncDirectory(rooted); err != nil {
		return acceptedPath, fmt.Errorf(
			"sync watched NZB state after removing importing link %q: %w",
			importingLeaf,
			err,
		)
	}
	return acceptedPath, nil
}

func syncWatchedNZBRoot(rooted *os.Root) error {
	directory, err := rooted.Open(".")
	if err != nil {
		return fmt.Errorf("open watched NZB directory for sync: %w", err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	// Windows does not provide the same portable directory-fsync primitive as
	// Unix. Attempt it, but treat an unsupported directory flush as best effort;
	// the link-before-unlink ordering and terminal recovery checks still apply.
	if runtime.GOOS == "windows" {
		syncErr = nil
	}
	return errors.Join(syncErr, closeErr)
}

// RemoveAcceptedNZB performs the best-effort terminal cleanup after acceptance.
// Failure leaves the ignored .nzb.accepted tombstone for a bounded later scan.
func (u *Usenet) RemoveAcceptedNZB(acceptedPath string, maxBytes int64) error {
	return RemoveAcceptedNZBAt(u.metadataDir, acceptedPath, maxBytes)
}

func RemoveAcceptedNZBAt(metadataRoot, acceptedPath string, maxBytes int64) error {
	absoluteRoot, acceptedLeaf, err := watchedNZBPath(
		metadataRoot,
		acceptedPath,
		watchedNZBAcceptedSuffix,
	)
	if err != nil {
		return err
	}
	if err := validateWatchedNZBMaxBytes(maxBytes); err != nil {
		return err
	}
	rooted, err := os.OpenRoot(absoluteRoot)
	if err != nil {
		return fmt.Errorf("open watched NZB metadata root: %w", err)
	}
	defer rooted.Close()
	return removeAcceptedNZBInRoot(rooted, acceptedLeaf, maxBytes)
}

func (u *Usenet) CleanupAcceptedNZBs(
	limits AcceptedNZBCleanupLimits,
) (AcceptedNZBCleanupResult, error) {
	var result AcceptedNZBCleanupResult
	u.watcherMu.Lock()
	if u.acceptedCleaner == nil {
		cleaner, err := NewAcceptedNZBCleaner(u.metadataDir)
		if err != nil {
			u.watcherMu.Unlock()
			return result, err
		}
		u.acceptedCleaner = cleaner
	}
	cleaner := u.acceptedCleaner
	u.watcherMu.Unlock()
	return cleaner.Cleanup(limits)
}

// CleanupAcceptedNZBsAt performs one bounded pass. Long-running integration
// should retain an AcceptedNZBCleaner so its directory cursor advances across
// calls instead of restarting at the beginning of a large metadata directory.
func CleanupAcceptedNZBsAt(
	metadataRoot string,
	limits AcceptedNZBCleanupLimits,
) (AcceptedNZBCleanupResult, error) {
	var result AcceptedNZBCleanupResult
	cleaner, err := NewAcceptedNZBCleaner(metadataRoot)
	if err != nil {
		return result, err
	}
	defer cleaner.Close()
	return cleaner.Cleanup(limits)
}

func NewAcceptedNZBCleaner(metadataRoot string) (*AcceptedNZBCleaner, error) {
	absoluteRoot, err := filepath.Abs(metadataRoot)
	if err != nil {
		return nil, fmt.Errorf("resolve watched NZB metadata root: %w", err)
	}
	absoluteRoot = filepath.Clean(absoluteRoot)
	if _, _, err := metadataDirectLeaf(absoluteRoot, filepath.Join(absoluteRoot, ".probe")); err != nil {
		return nil, err
	}
	rooted, err := os.OpenRoot(absoluteRoot)
	if err != nil {
		return nil, fmt.Errorf("open watched NZB metadata root: %w", err)
	}
	return &AcceptedNZBCleaner{
		absoluteRoot: absoluteRoot,
		rooted:       rooted,
		pending:      make([]os.DirEntry, 0, acceptedCleanupReadBatch),
	}, nil
}

func (cleaner *AcceptedNZBCleaner) Close() error {
	if cleaner == nil {
		return nil
	}
	cleaner.mu.Lock()
	defer cleaner.mu.Unlock()
	if cleaner.closed {
		return nil
	}
	cleaner.closed = true
	var errs []error
	if cleaner.directory != nil {
		errs = append(errs, cleaner.directory.Close())
		cleaner.directory = nil
	}
	if cleaner.rooted != nil {
		errs = append(errs, cleaner.rooted.Close())
		cleaner.rooted = nil
	}
	cleaner.pending = nil
	return errors.Join(errs...)
}

// Cleanup retries a bounded number of directory entries, terminal files, and
// logical bytes. The root and directory cursor are pinned, every candidate is
// opened no-follow, and corrupt entries are reported without being removed.
func (cleaner *AcceptedNZBCleaner) Cleanup(
	limits AcceptedNZBCleanupLimits,
) (AcceptedNZBCleanupResult, error) {
	var result AcceptedNZBCleanupResult
	if cleaner == nil {
		return result, fmt.Errorf("accepted watched NZB cleaner is nil")
	}
	if err := validateAcceptedCleanupLimits(limits); err != nil {
		return result, err
	}

	cleaner.mu.Lock()
	defer cleaner.mu.Unlock()
	if cleaner.closed || cleaner.rooted == nil {
		return result, fmt.Errorf("accepted watched NZB cleaner is closed")
	}
	if cleaner.directory == nil {
		directory, err := cleaner.rooted.Open(".")
		if err != nil {
			return result, fmt.Errorf("open watched NZB cleanup directory: %w", err)
		}
		cleaner.directory = directory
		cleaner.reachedEOF = false
	}

	var errs []error
	var bytesSpent int64
	for {
		for len(cleaner.pending) > 0 {
			entry := cleaner.pending[0]
			leaf := entry.Name()
			if !strings.HasSuffix(leaf, watchedNZBAcceptedSuffix) {
				cleaner.pending = cleaner.pending[1:]
				continue
			}
			if result.Attempted >= limits.MaxFiles {
				result.More = true
				return result, errors.Join(errs...)
			}
			result.Matched++

			if _, _, err := watchedNZBPath(
				cleaner.absoluteRoot,
				filepath.Join(cleaner.absoluteRoot, leaf),
				watchedNZBAcceptedSuffix,
			); err != nil {
				cleaner.pending = cleaner.pending[1:]
				result.Attempted++
				result.Failed++
				result.More = true
				errs = append(errs, fmt.Errorf("refuse accepted watched NZB %q: %w", leaf, err))
				continue
			}

			info, err := cleaner.rooted.Lstat(leaf)
			if err != nil {
				cleaner.pending = cleaner.pending[1:]
				result.Attempted++
				if os.IsNotExist(err) {
					result.Removed++
					continue
				}
				result.Failed++
				result.More = true
				errs = append(errs, fmt.Errorf("inspect accepted watched NZB %q: %w", leaf, err))
				continue
			}
			if err := validateWatchedNZBFileInfo(leaf, info, limits.MaxFileBytes); err != nil {
				cleaner.pending = cleaner.pending[1:]
				result.Attempted++
				result.Failed++
				result.More = true
				errs = append(errs, err)
				continue
			}
			if info.Size() > limits.MaxTotalBytes-bytesSpent {
				result.More = true
				return result, errors.Join(errs...)
			}

			cleaner.pending = cleaner.pending[1:]
			result.Attempted++
			bytesSpent += info.Size()
			if err := cleaner.rooted.Remove(leaf); err != nil && !os.IsNotExist(err) {
				result.Failed++
				result.More = true
				errs = append(errs, fmt.Errorf("remove accepted watched NZB %q: %w", leaf, err))
				continue
			}
			result.Removed++
			result.BytesRemoved += info.Size()
		}

		if cleaner.reachedEOF {
			if err := cleaner.directory.Close(); err != nil {
				errs = append(errs, fmt.Errorf("close watched NZB cleanup directory: %w", err))
			}
			cleaner.directory = nil
			cleaner.reachedEOF = false
			result.More = result.More || result.Failed > 0
			return result, errors.Join(errs...)
		}
		if result.Scanned >= limits.MaxEntries {
			result.More = true
			return result, errors.Join(errs...)
		}

		batchSize := min(acceptedCleanupReadBatch, limits.MaxEntries-result.Scanned)
		entries, readErr := cleaner.directory.ReadDir(batchSize)
		result.Scanned += len(entries)
		cleaner.pending = append(cleaner.pending, entries...)
		if errors.Is(readErr, io.EOF) {
			cleaner.reachedEOF = true
		} else if readErr != nil {
			return result, errors.Join(
				errors.Join(errs...),
				fmt.Errorf("scan accepted watched NZBs: %w", readErr),
			)
		}
		if len(entries) == 0 && readErr == nil {
			return result, errors.Join(
				errors.Join(errs...),
				fmt.Errorf("scan accepted watched NZBs made no progress"),
			)
		}
	}
}

func watchedNZBPath(metadataRoot, path, requiredSuffix string) (string, string, error) {
	absoluteRoot, leaf, err := metadataDirectLeaf(metadataRoot, path)
	if err != nil {
		return "", "", err
	}
	if !strings.HasSuffix(leaf, requiredSuffix) {
		return "", "", fmt.Errorf("watched NZB path %q does not end in %s", path, requiredSuffix)
	}
	original := strings.TrimSuffix(leaf, strings.TrimPrefix(requiredSuffix, ".nzb"))
	if !strings.HasSuffix(original, ".nzb") {
		return "", "", fmt.Errorf("watched NZB path %q has no original .nzb leaf", path)
	}
	return absoluteRoot, leaf, nil
}

func validateWatchedNZBMaxBytes(maxBytes int64) error {
	const maxInt64 = int64(^uint64(0) >> 1)
	if maxBytes <= 0 || maxBytes == maxInt64 {
		return fmt.Errorf("watched NZB byte limit must be between 1 and %d", maxInt64-1)
	}
	return nil
}

func validateWatchedNZBFileInfo(name string, info os.FileInfo, maxBytes int64) error {
	if err := validateWatchedNZBMaxBytes(maxBytes); err != nil {
		return err
	}
	if info == nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("watched NZB file %q is not a regular no-follow file", name)
	}
	if info.Size() < 0 || info.Size() > maxBytes {
		return fmt.Errorf(
			"watched NZB file %q size %d exceeds limit %d",
			name,
			info.Size(),
			maxBytes,
		)
	}
	return nil
}

func validateAcceptedLink(
	rooted *os.Root,
	importingLeaf, acceptedLeaf string,
	originalInfo os.FileInfo,
	maxBytes int64,
) error {
	acceptedInfo, err := rooted.Lstat(acceptedLeaf)
	if err != nil {
		return fmt.Errorf("inspect linked accepted watched NZB: %w", err)
	}
	if err := validateWatchedNZBFileInfo(acceptedLeaf, acceptedInfo, maxBytes); err != nil {
		return err
	}
	currentImportingInfo, err := rooted.Lstat(importingLeaf)
	if err != nil {
		return fmt.Errorf("reinspect importing watched NZB: %w", err)
	}
	if err := validateWatchedNZBFileInfo(importingLeaf, currentImportingInfo, maxBytes); err != nil {
		return err
	}
	if !os.SameFile(originalInfo, acceptedInfo) ||
		!os.SameFile(currentImportingInfo, acceptedInfo) {
		return fmt.Errorf(
			"%w: importing file changed during terminal transition",
			ErrWatchedNZBAcceptedConflict,
		)
	}
	return nil
}

func sameWatchedNZBContent(
	rooted *os.Root,
	importingLeaf, acceptedLeaf string,
	maxBytes int64,
) (bool, error) {
	importingInfo, err := rooted.Lstat(importingLeaf)
	if err != nil {
		return false, fmt.Errorf("inspect importing watched NZB: %w", err)
	}
	if err := validateWatchedNZBFileInfo(importingLeaf, importingInfo, maxBytes); err != nil {
		return false, err
	}
	acceptedInfo, err := rooted.Lstat(acceptedLeaf)
	if err != nil {
		return false, fmt.Errorf("inspect accepted watched NZB: %w", err)
	}
	if err := validateWatchedNZBFileInfo(acceptedLeaf, acceptedInfo, maxBytes); err != nil {
		return false, err
	}
	if os.SameFile(importingInfo, acceptedInfo) {
		return true, nil
	}
	if importingInfo.Size() != acceptedInfo.Size() {
		return false, nil
	}

	importingDigest, err := digestWatchedNZBLeaf(rooted, importingLeaf, importingInfo, maxBytes)
	if err != nil {
		return false, err
	}
	acceptedDigest, err := digestWatchedNZBLeaf(rooted, acceptedLeaf, acceptedInfo, maxBytes)
	if err != nil {
		return false, err
	}
	return importingDigest == acceptedDigest, nil
}

func digestWatchedNZBLeaf(
	rooted *os.Root,
	leaf string,
	expectedInfo os.FileInfo,
	maxBytes int64,
) ([sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	file, err := rooted.Open(leaf)
	if err != nil {
		return digest, fmt.Errorf("open watched NZB %q: %w", leaf, err)
	}
	openedInfo, statErr := file.Stat()
	if statErr != nil {
		_ = file.Close()
		return digest, fmt.Errorf("inspect opened watched NZB %q: %w", leaf, statErr)
	}
	if err := validateWatchedNZBFileInfo(leaf, openedInfo, maxBytes); err != nil {
		_ = file.Close()
		return digest, err
	}
	if !os.SameFile(expectedInfo, openedInfo) {
		_ = file.Close()
		return digest, fmt.Errorf("watched NZB %q changed while opening", leaf)
	}

	hash := sha256.New()
	n, readErr := io.Copy(hash, io.LimitReader(file, maxBytes+1))
	afterInfo, statErr := file.Stat()
	pathInfo, pathErr := rooted.Lstat(leaf)
	closeErr := file.Close()
	if readErr != nil {
		return digest, fmt.Errorf("hash watched NZB %q: %w", leaf, readErr)
	}
	if statErr != nil {
		return digest, fmt.Errorf("reinspect opened watched NZB %q: %w", leaf, statErr)
	}
	if pathErr != nil {
		return digest, fmt.Errorf("reinspect watched NZB path %q: %w", leaf, pathErr)
	}
	if closeErr != nil {
		return digest, fmt.Errorf("close watched NZB %q: %w", leaf, closeErr)
	}
	if n > maxBytes {
		return digest, fmt.Errorf("watched NZB %q grew beyond byte limit %d", leaf, maxBytes)
	}
	if !os.SameFile(expectedInfo, afterInfo) ||
		!os.SameFile(expectedInfo, pathInfo) ||
		expectedInfo.Size() != afterInfo.Size() ||
		expectedInfo.Size() != pathInfo.Size() ||
		expectedInfo.Size() != n ||
		!expectedInfo.ModTime().Equal(afterInfo.ModTime()) ||
		!expectedInfo.ModTime().Equal(pathInfo.ModTime()) {
		return digest, fmt.Errorf("watched NZB %q changed while hashing", leaf)
	}
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

func readClaimedNZBSnapshotAt(
	root, path string,
	maxBytes int64,
	afterRead func(),
) (*ClaimedNZBSnapshot, error) {
	absoluteRoot, leaf, err := watchedNZBPath(root, path, watchedNZBImportingSuffix)
	if err != nil {
		return nil, err
	}
	if err := validateWatchedNZBMaxBytes(maxBytes); err != nil {
		return nil, err
	}
	canonicalPath := filepath.Join(absoluteRoot, leaf)
	file, err := openMetadataFile(absoluteRoot, canonicalPath)
	if err != nil {
		return nil, err
	}
	beforeInfo, statErr := file.Stat()
	if statErr != nil {
		_ = file.Close()
		return nil, statErr
	}
	if err := validateWatchedNZBFileInfo(canonicalPath, beforeInfo, maxBytes); err != nil {
		_ = file.Close()
		return nil, err
	}
	data, readErr := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if readErr != nil {
		_ = file.Close()
		return nil, readErr
	}
	if int64(len(data)) > maxBytes {
		_ = file.Close()
		return nil, fmt.Errorf("watched NZB %q exceeds byte limit %d", path, maxBytes)
	}
	if afterRead != nil {
		afterRead()
	}
	afterInfo, statErr := file.Stat()
	if statErr != nil {
		_ = file.Close()
		return nil, fmt.Errorf("restat opened watched NZB %q: %w", path, statErr)
	}
	pathInfo, pathErr := statMetadataFile(absoluteRoot, canonicalPath)
	closeErr := file.Close()
	if pathErr != nil {
		return nil, fmt.Errorf("%w: restat claimed path %q: %v", ErrWatchedNZBClaimChanged, path, pathErr)
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if !os.SameFile(beforeInfo, afterInfo) ||
		!os.SameFile(beforeInfo, pathInfo) ||
		beforeInfo.Size() != afterInfo.Size() ||
		beforeInfo.Size() != pathInfo.Size() ||
		beforeInfo.Size() != int64(len(data)) ||
		!beforeInfo.ModTime().Equal(afterInfo.ModTime()) ||
		!beforeInfo.ModTime().Equal(pathInfo.ModTime()) {
		return nil, fmt.Errorf(
			"%w: path %q inode, size, or modification time changed",
			ErrWatchedNZBClaimChanged,
			path,
		)
	}
	return &ClaimedNZBSnapshot{
		Path:          canonicalPath,
		Content:       data,
		ContentDigest: sha256.Sum256(data),
		Size:          beforeInfo.Size(),
		ModTime:       beforeInfo.ModTime(),
	}, nil
}

func removeAcceptedNZBInRoot(rooted *os.Root, acceptedLeaf string, maxBytes int64) error {
	info, err := rooted.Lstat(acceptedLeaf)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("inspect accepted watched NZB %q: %w", acceptedLeaf, err)
	}
	if err := validateWatchedNZBFileInfo(acceptedLeaf, info, maxBytes); err != nil {
		return err
	}
	if err := rooted.Remove(acceptedLeaf); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove accepted watched NZB %q: %w", acceptedLeaf, err)
	}
	return nil
}

func validateAcceptedCleanupLimits(limits AcceptedNZBCleanupLimits) error {
	if limits.MaxEntries <= 0 {
		return fmt.Errorf("accepted watched NZB cleanup MaxEntries must be positive")
	}
	if limits.MaxFiles <= 0 {
		return fmt.Errorf("accepted watched NZB cleanup MaxFiles must be positive")
	}
	if limits.MaxFiles > limits.MaxEntries {
		return fmt.Errorf(
			"accepted watched NZB cleanup MaxFiles %d exceeds MaxEntries %d",
			limits.MaxFiles,
			limits.MaxEntries,
		)
	}
	if err := validateWatchedNZBMaxBytes(limits.MaxFileBytes); err != nil {
		return err
	}
	if limits.MaxTotalBytes < limits.MaxFileBytes {
		return fmt.Errorf(
			"accepted watched NZB cleanup MaxTotalBytes %d is below MaxFileBytes %d",
			limits.MaxTotalBytes,
			limits.MaxFileBytes,
		)
	}
	return nil
}
