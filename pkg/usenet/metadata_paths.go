package usenet

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/google/uuid"
)

type metadataRemovalLock struct {
	mutex sync.Mutex
	refs  int
}

var metadataRemovalLocks = struct {
	sync.Mutex
	leaves map[string]*metadataRemovalLock
}{
	leaves: make(map[string]*metadataRemovalLock),
}

// lockMetadataRemoval serializes deletion of one exact metadata leaf. Windows
// can reject concurrent root-relative Lstat/Remove operations with a sharing
// violation even after another caller has successfully deleted the file.
// Keeping the lock leaf-scoped avoids serializing unrelated NZB cleanup, and
// reference counting prevents an unbounded registry of completed downloads.
func lockMetadataRemoval(target string) func() {
	key, err := filepath.Abs(target)
	if err != nil {
		key = target
	}
	key = filepath.Clean(key)
	if runtime.GOOS == "windows" {
		key = strings.ToLower(key)
	}

	metadataRemovalLocks.Lock()
	lock := metadataRemovalLocks.leaves[key]
	if lock == nil {
		lock = &metadataRemovalLock{}
		metadataRemovalLocks.leaves[key] = lock
	}
	lock.refs++
	metadataRemovalLocks.Unlock()

	lock.mutex.Lock()
	return func() {
		lock.mutex.Unlock()
		metadataRemovalLocks.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(metadataRemovalLocks.leaves, key)
		}
		metadataRemovalLocks.Unlock()
	}
}

type metadataFileSuffix string

const (
	maxNZBMetadataFileBytes int64 = 256 << 20

	nzbSourceSuffix     metadataFileSuffix = ".nzb"
	nzbStagedSuffix     metadataFileSuffix = ".queued"
	nzbStagedTempSuffix metadataFileSuffix = ".queued.tmp"
	nzbProcessingSuffix metadataFileSuffix = ".nzb.processing"
	nzbProcessedSuffix  metadataFileSuffix = ".nzb.processed"
	nzbFailedSuffix     metadataFileSuffix = ".nzb.failed"
	nzbMetaSuffix       metadataFileSuffix = ".meta"
	nzbMetaTempSuffix   metadataFileSuffix = ".meta.tmp"
	nzbMetaV2TempSuffix metadataFileSuffix = ".meta.v2tmp"
)

// canonicalNZBID accepts only the canonical UUID form produced by Tessarr.
// Keeping the identifier fixed-width also bounds every derived filename.
func canonicalNZBID(id string) (string, error) {
	parsed, err := uuid.Parse(id)
	if err != nil {
		return "", fmt.Errorf("invalid NZB ID %q: %w", id, err)
	}
	canonical := parsed.String()
	if id != canonical {
		return "", fmt.Errorf("invalid NZB ID %q: canonical form is %q", id, canonical)
	}
	return canonical, nil
}

func metadataFilePath(root, id string, suffix metadataFileSuffix) (string, error) {
	canonical, err := canonicalNZBID(id)
	if err != nil {
		return "", err
	}
	switch suffix {
	case nzbSourceSuffix,
		nzbStagedSuffix,
		nzbStagedTempSuffix,
		nzbProcessingSuffix,
		nzbProcessedSuffix,
		nzbFailedSuffix,
		nzbMetaSuffix,
		nzbMetaTempSuffix,
		nzbMetaV2TempSuffix:
	default:
		return "", fmt.Errorf("unsupported NZB metadata suffix %q", suffix)
	}
	return safepath.JoinIdentifiers(root, canonical+string(suffix))
}

// validatePersistedMetadataPath requires persisted state to name the one exact
// file derived from its NZB ID. Merely being beneath metadataDir is not enough:
// cross-entry deletion must also be impossible.
func validatePersistedMetadataPath(root, id, persisted string, suffix metadataFileSuffix) (string, error) {
	if persisted == "" {
		return "", nil
	}
	expected, err := metadataFilePath(root, id, suffix)
	if err != nil {
		return "", err
	}
	actual, err := filepath.Abs(persisted)
	if err != nil {
		return "", fmt.Errorf("resolve persisted NZB path: %w", err)
	}
	actual = filepath.Clean(actual)
	same := actual == expected
	if runtime.GOOS == "windows" {
		same = strings.EqualFold(actual, expected)
	}
	if !same {
		return "", fmt.Errorf(
			"persisted NZB path %q does not match expected path %q",
			persisted,
			expected,
		)
	}
	if _, err := safepath.ValidateUnderRoot(root, actual); err != nil {
		return "", err
	}
	return expected, nil
}

func metadataDirectLeaf(root, target string) (string, string, error) {
	absoluteRoot, err := safepath.ValidateRoot(root)
	if err != nil {
		return "", "", err
	}
	absoluteTarget, err := safepath.ValidateUnderRoot(absoluteRoot, target)
	if err != nil {
		return "", "", err
	}
	relative, err := filepath.Rel(absoluteRoot, absoluteTarget)
	if err != nil {
		return "", "", fmt.Errorf("make NZB metadata path relative: %w", err)
	}
	if filepath.Dir(relative) != "." {
		return "", "", fmt.Errorf("NZB metadata path %q is not directly beneath %q", absoluteTarget, absoluteRoot)
	}
	if err := safepath.ValidateIdentifier(relative); err != nil {
		return "", "", err
	}
	return absoluteRoot, relative, nil
}

func openMetadataFile(root, target string) (*os.File, error) {
	absoluteRoot, leaf, err := metadataDirectLeaf(root, target)
	if err != nil {
		return nil, err
	}
	rooted, err := os.OpenRoot(absoluteRoot)
	if err != nil {
		return nil, fmt.Errorf("open NZB metadata root: %w", err)
	}
	info, err := rooted.Lstat(leaf)
	if err != nil {
		_ = rooted.Close()
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		_ = rooted.Close()
		return nil, fmt.Errorf("NZB metadata file %q is not a regular file", target)
	}
	file, openErr := rooted.Open(leaf)
	closeErr := rooted.Close()
	if openErr != nil {
		return nil, openErr
	}
	if closeErr != nil {
		_ = file.Close()
		return nil, fmt.Errorf("close NZB metadata root: %w", closeErr)
	}
	openedInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("inspect opened NZB metadata file %q: %w", target, err)
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) {
		_ = file.Close()
		return nil, fmt.Errorf("NZB metadata file %q changed while opening", target)
	}
	return file, nil
}

func readMetadataFile(root, target string) ([]byte, error) {
	return readStableMetadataContent(
		root,
		target,
		maxNZBMetadataFileBytes,
	)
}

func writeMetadataFile(root, target string, data []byte, perm os.FileMode) error {
	return writeMetadataFileWithMode(root, target, data, perm, false)
}

// writeMetadataFilePreservingMode restores the exact permission bits of an
// existing offline-migration record, independently of the process umask.
// Runtime writes continue to honor the umask through writeMetadataFile.
func writeMetadataFilePreservingMode(root, target string, data []byte, perm os.FileMode) error {
	return writeMetadataFileWithMode(root, target, data, perm, true)
}

func writeMetadataFileWithMode(root, target string, data []byte, perm os.FileMode, preserveMode bool) error {
	absoluteRoot, leaf, err := metadataDirectLeaf(root, target)
	if err != nil {
		return err
	}
	rooted, err := os.OpenRoot(absoluteRoot)
	if err != nil {
		return fmt.Errorf("open NZB metadata root: %w", err)
	}

	if err := rooted.Remove(leaf); err != nil && !os.IsNotExist(err) {
		closeErr := rooted.Close()
		return errors.Join(
			fmt.Errorf("remove existing NZB metadata file %q: %w", target, err),
			closeErr,
		)
	}
	file, err := rooted.OpenFile(
		leaf,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		perm,
	)
	if err != nil {
		syncErr := syncWatchedNZBRoot(rooted)
		closeErr := rooted.Close()
		return errors.Join(
			fmt.Errorf("create NZB metadata file %q: %w", target, err),
			syncErr,
			closeErr,
		)
	}

	writeErr := writeFull(file, data)
	var chmodErr error
	if preserveMode {
		chmodErr = file.Chmod(perm.Perm())
	}
	syncErr := file.Sync()
	fileCloseErr := file.Close()
	if err := errors.Join(writeErr, chmodErr, syncErr, fileCloseErr); err != nil {
		removeErr := rooted.Remove(leaf)
		if os.IsNotExist(removeErr) {
			removeErr = nil
		}
		directorySyncErr := syncWatchedNZBRoot(rooted)
		rootCloseErr := rooted.Close()
		return errors.Join(
			fmt.Errorf("persist NZB metadata file %q: %w", target, err),
			removeErr,
			directorySyncErr,
			rootCloseErr,
		)
	}

	directorySyncErr := syncWatchedNZBRoot(rooted)
	rootCloseErr := rooted.Close()
	return errors.Join(directorySyncErr, rootCloseErr)
}

func statMetadataFile(root, target string) (os.FileInfo, error) {
	absoluteRoot, leaf, err := metadataDirectLeaf(root, target)
	if err != nil {
		return nil, err
	}
	rooted, err := os.OpenRoot(absoluteRoot)
	if err != nil {
		return nil, fmt.Errorf("open NZB metadata root: %w", err)
	}
	info, statErr := rooted.Lstat(leaf)
	closeErr := rooted.Close()
	if statErr != nil {
		return nil, statErr
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close NZB metadata root: %w", closeErr)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("NZB metadata file %q is not a regular file", target)
	}
	return info, nil
}

// removeMetadataFile durably removes an exact leaf, including retrying an
// already-absent leaf. Root validation and sync/close failures remain errors.
func removeMetadataFile(root, target string) error {
	// Lock before validating the target because Windows can also reject the
	// validation Lstat while another root-relative removal is in flight.
	unlock := lockMetadataRemoval(target)
	defer unlock()
	return removeMetadataFileLocked(root, target)
}

// removeMetadataFileLocked performs the complete security validation and
// durable removal while its caller holds the target's metadata removal lock.
func removeMetadataFileLocked(root, target string) error {
	absoluteRoot, leaf, err := metadataDirectLeaf(root, target)
	if err != nil {
		return err
	}
	rooted, err := os.OpenRoot(absoluteRoot)
	if err != nil {
		return fmt.Errorf("open NZB metadata root: %w", err)
	}
	syncRoot := func() error { return syncWatchedNZBRoot(rooted) }
	info, err := rooted.Lstat(leaf)
	if err != nil {
		return finishMetadataRemoval(err, syncRoot, rooted.Close)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return finishMetadataRemoval(
			fmt.Errorf("NZB metadata file %q is not a regular file", target),
			syncRoot,
			rooted.Close,
		)
	}
	return finishMetadataRemoval(rooted.Remove(leaf), syncRoot, rooted.Close)
}

func finishMetadataRemoval(operationErr error, syncRoot, closeRoot func() error) error {
	// Normalize only the leaf syscall's error before joining independent
	// failures. An ENOENT from sync or close must never make cleanup succeed.
	if os.IsNotExist(operationErr) {
		operationErr = nil
	}
	var syncErr error
	if operationErr == nil {
		// Also sync an absent leaf: a prior attempt may have removed it but
		// failed before the directory change became durable.
		syncErr = syncRoot()
	}
	closeErr := closeRoot()
	return errors.Join(operationErr, syncErr, closeErr)
}

func removeMetadataFileIfExists(root, target string) error {
	if target == "" {
		return nil
	}
	return removeMetadataFile(root, target)
}

func renameMetadataFile(root, oldTarget, newTarget string) error {
	absoluteRoot, oldLeaf, err := metadataDirectLeaf(root, oldTarget)
	if err != nil {
		return err
	}
	newRoot, newLeaf, err := metadataDirectLeaf(root, newTarget)
	if err != nil {
		return err
	}
	sameRoot := absoluteRoot == newRoot
	if runtime.GOOS == "windows" {
		sameRoot = strings.EqualFold(absoluteRoot, newRoot)
	}
	if !sameRoot {
		return fmt.Errorf("NZB metadata rename crosses roots")
	}
	rooted, err := os.OpenRoot(absoluteRoot)
	if err != nil {
		return fmt.Errorf("open NZB metadata root: %w", err)
	}
	info, err := rooted.Lstat(oldLeaf)
	if err != nil {
		closeErr := rooted.Close()
		return errors.Join(err, closeErr)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		closeErr := rooted.Close()
		return errors.Join(
			fmt.Errorf("NZB metadata source %q is not a regular file", oldTarget),
			closeErr,
		)
	}
	renameErr := rooted.Rename(oldLeaf, newLeaf)
	if renameErr != nil {
		closeErr := rooted.Close()
		return errors.Join(renameErr, closeErr)
	}
	syncErr := syncWatchedNZBRoot(rooted)
	closeErr := rooted.Close()
	return errors.Join(syncErr, closeErr)
}
