package manager

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

const (
	usenetOwnerMarkerName     = ".tessarr-nzb-owner-v1"
	usenetOwnershipLockName   = ".tessarr-nzb-ownership.lock"
	usenetQuarantinePrefix    = ".tessarr-nzb-quarantine-"
	usenetOwnerMarkerMaxBytes = 512
	usenetDirectoryReadBatch  = 128
	usenetDirectoryMaxEntries = 100_000
)

var usenetOwnerMarkerNames = []string{usenetOwnerMarkerName, legacyUsenetOwnerMarkerName}

// usenetOwnershipMu makes portable-alias discovery plus creation atomic within
// this process on case-sensitive filesystems. os.Root supplies the filesystem
// containment; this lock supplies the cross-name invariant that Linux itself
// does not enforce for names that alias on Windows.
var usenetOwnershipMu sync.Mutex

var (
	usenetOwnershipLockTimeout = 5 * time.Second
	usenetOwnershipRetryDelay  = 25 * time.Millisecond
	usenetSyncFile             = func(file *os.File) error { return file.Sync() }
	usenetSyncDirectory        = syncUsenetDirectoryDefault
)

// usenetEntryPaths builds the only permitted SavePath and DownloadPath shape
// for an NZB: <download root>/<optional category>/<NZB name>. Category and name
// are identifiers, not caller-controlled relative paths.
func usenetEntryPaths(downloadRoot, category, name string) (savePath, downloadPath string, err error) {
	root, err := safepath.ValidateRoot(downloadRoot)
	if err != nil {
		return "", "", fmt.Errorf("invalid usenet download root: %w", err)
	}

	savePath = root
	components := make([]string, 0, 2)
	if category != "" {
		if err := safepath.ValidateIdentifier(category); err != nil {
			return "", "", fmt.Errorf("invalid usenet category: %w", err)
		}
		if isReservedUsenetPrivateName(category) {
			return "", "", fmt.Errorf("usenet category %q is reserved for internal ownership state", category)
		}
		savePath, err = safepath.JoinIdentifiers(root, category)
		if err != nil {
			return "", "", fmt.Errorf("resolve usenet category path: %w", err)
		}
		components = append(components, category)
	}

	entryName := utils.RemoveExtension(name)
	if err := safepath.ValidateIdentifier(entryName); err != nil {
		return "", "", fmt.Errorf("invalid NZB name: %w", err)
	}
	entryName, err = safepath.CompactIdentifier(entryName, safepath.PortableIdentifierMaxBytes)
	if err != nil {
		return "", "", fmt.Errorf("compact NZB name: %w", err)
	}
	if isReservedUsenetPrivateName(entryName) {
		return "", "", fmt.Errorf("NZB name %q is reserved for internal ownership state", entryName)
	}
	components = append(components, entryName)
	downloadPath, err = safepath.JoinIdentifiers(root, components...)
	if err != nil {
		return "", "", fmt.Errorf("resolve usenet download path: %w", err)
	}
	return savePath, downloadPath, nil
}

func requireConfiguredUsenetDownloadRoot(configuredRoot, requestedRoot string) (string, error) {
	configured, err := safepath.ValidateRoot(configuredRoot)
	if err != nil {
		return "", fmt.Errorf("invalid configured usenet download root: %w", err)
	}
	requested, err := safepath.ValidateRoot(requestedRoot)
	if err != nil {
		return "", fmt.Errorf("invalid requested usenet download root: %w", err)
	}
	if !sameFilesystemPath(requested, configured) {
		return "", fmt.Errorf("custom NZB download root %q is not supported; configured root is %q", requested, configured)
	}
	return configured, nil
}

// safeUsenetEntryDownloadPath re-derives an NZB path from trusted configuration
// and identifiers, then rejects stale or tampered persisted SavePath values.
func safeUsenetEntryDownloadPath(downloadRoot string, entry *storage.Entry) (string, error) {
	if entry == nil || !entry.IsNZB() {
		return "", fmt.Errorf("entry is not an NZB")
	}
	expectedSave, expectedDownload, err := usenetEntryPaths(downloadRoot, entry.Category, entry.Name)
	if err != nil {
		return "", err
	}
	if !sameFilesystemPath(entry.SavePath, expectedSave) {
		return "", fmt.Errorf("NZB save path %q is outside configured path %q", entry.SavePath, expectedSave)
	}
	if !sameFilesystemPath(entry.DownloadPath(), expectedDownload) {
		return "", fmt.Errorf("NZB download path %q is outside configured path %q", entry.DownloadPath(), expectedDownload)
	}
	return expectedDownload, nil
}

func safeUsenetFilePath(downloadRoot string, entry *storage.Entry, fileName string) (string, error) {
	entryPath, err := safeUsenetEntryDownloadPath(downloadRoot, entry)
	if err != nil {
		return "", err
	}
	if isReservedUsenetPrivateName(fileName) {
		return "", fmt.Errorf("NZB file name %q is reserved for internal ownership state", fileName)
	}
	filePath, err := safepath.JoinIdentifiers(entryPath, fileName)
	if err != nil {
		return "", fmt.Errorf("invalid NZB file name: %w", err)
	}
	return filePath, nil
}

// claimUsenetEntryDirectory establishes per-entry ownership of the visible
// release directory. A same-ID retry is idempotent. A different owner, a
// portable case alias, or a non-empty unowned directory is preserved and
// rejected before an action can write files.
func claimUsenetEntryDirectory(downloadRoot string, entry *storage.Entry) (entryPath string, newlyClaimed bool, err error) {
	usenetOwnershipMu.Lock()
	defer usenetOwnershipMu.Unlock()

	ownership, _, err := acquireUsenetOwnershipLock(downloadRoot, true)
	if err != nil {
		return "", false, err
	}
	entryPath, newlyClaimed, err = claimUsenetEntryDirectoryLocked(ownership.root, ownership.absolute, entry)
	if closeErr := ownership.close(); closeErr != nil && err == nil {
		err = fmt.Errorf("close NZB ownership root: %w", closeErr)
	}
	return entryPath, newlyClaimed, err
}

func claimUsenetEntryDirectoryLocked(rooted *os.Root, absoluteRoot string, entry *storage.Entry) (entryPath string, newlyClaimed bool, err error) {
	entryPath, err = safeUsenetEntryDownloadPath(absoluteRoot, entry)
	if err != nil {
		return "", false, err
	}
	ownerID, err := canonicalUsenetOwnerID(entry.InfoHash)
	if err != nil {
		return "", false, err
	}
	relativeEntry, err := filepath.Rel(absoluteRoot, entryPath)
	if err != nil {
		return "", false, fmt.Errorf("make NZB release path relative: %w", err)
	}

	if err := recoverUsenetQuarantineForClaim(rooted, relativeEntry, ownerID); err != nil {
		return "", false, err
	}

	parentRelative := filepath.Dir(relativeEntry)
	if parentRelative != "." {
		parentName := filepath.Base(parentRelative)
		parentParent := filepath.Dir(parentRelative)
		if err := rejectPortableSiblingAlias(rooted, parentParent, parentName); err != nil {
			return "", false, fmt.Errorf("claim NZB category directory: %w", err)
		}
		if err := rooted.MkdirAll(parentRelative, 0o755); err != nil {
			return "", false, fmt.Errorf("create NZB category directory: %w", err)
		}
	}

	if err := rejectPortableSiblingAlias(rooted, parentRelative, filepath.Base(relativeEntry)); err != nil {
		return "", false, fmt.Errorf("claim NZB release directory: %w", err)
	}

	createdDirectory := false
	if err := rooted.Mkdir(relativeEntry, 0o755); err != nil {
		if !os.IsExist(err) {
			return "", false, fmt.Errorf("create NZB release directory: %w", err)
		}
		info, statErr := rooted.Lstat(relativeEntry)
		if statErr != nil {
			return "", false, fmt.Errorf("inspect existing NZB release directory: %w", statErr)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return "", false, fmt.Errorf("NZB release path %q is not a regular directory", entryPath)
		}
	} else {
		createdDirectory = true
	}

	entryRoot, err := rooted.OpenRoot(relativeEntry)
	if err != nil {
		if createdDirectory {
			_ = rooted.Remove(relativeEntry)
		}
		return "", false, fmt.Errorf("open NZB release directory: %w", err)
	}
	newlyClaimed, err = claimUsenetOwnerMarker(entryRoot, ownerID)
	closeErr := entryRoot.Close()
	if err != nil {
		if createdDirectory {
			_ = rooted.Remove(relativeEntry)
		}
		return "", false, err
	}
	if closeErr != nil {
		return "", newlyClaimed, fmt.Errorf("close NZB release directory: %w", closeErr)
	}
	return entryPath, newlyClaimed, nil
}

func claimUsenetOwnerMarker(entryRoot *os.Root, ownerID string) (bool, error) {
	markerOwner, err := readUsenetOwnerMarker(entryRoot)
	switch {
	case err == nil:
		if markerOwner != ownerID {
			return false, fmt.Errorf("NZB release directory is owned by %q, not %q", markerOwner, ownerID)
		}
		return false, nil
	case !os.IsNotExist(err):
		return false, err
	}

	onlyMarker, err := usenetDirectoryContainsOnlyMarker(entryRoot, false)
	if err != nil {
		return false, err
	}
	if !onlyMarker {
		return false, fmt.Errorf("refusing to claim non-empty unowned NZB release directory")
	}

	err = writeDurableExclusiveUsenetFile(entryRoot, usenetOwnerMarkerName, ownerID+"\n", 0o600)
	if err != nil {
		if os.IsExist(err) {
			markerOwner, readErr := readUsenetOwnerMarker(entryRoot)
			if readErr != nil {
				return false, readErr
			}
			if markerOwner == ownerID {
				return false, nil
			}
			return false, fmt.Errorf("NZB release directory is owned by %q, not %q", markerOwner, ownerID)
		}
		return false, fmt.Errorf("create NZB ownership marker: %w", err)
	}

	onlyMarker, err = usenetDirectoryContainsOnlyMarker(entryRoot, true)
	if err != nil || !onlyMarker {
		if markerOwner, readErr := readUsenetOwnerMarker(entryRoot); readErr == nil && markerOwner == ownerID {
			_ = entryRoot.Remove(usenetOwnerMarkerName)
		}
		if err != nil {
			return false, err
		}
		return false, fmt.Errorf("NZB release directory changed while ownership was claimed")
	}
	return true, nil
}

func writeDurableExclusiveUsenetFile(rooted *os.Root, name, contents string, perm os.FileMode) error {
	file, err := rooted.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return err
	}

	cleanup := func() {
		_ = file.Close()
		_ = rooted.Remove(name)
		_ = usenetSyncDirectory(rooted)
	}
	if n, writeErr := io.WriteString(file, contents); writeErr != nil || n != len(contents) {
		cleanup()
		if writeErr != nil {
			return fmt.Errorf("write durable file %q: %w", name, writeErr)
		}
		return fmt.Errorf("write durable file %q: short write", name)
	}
	if err := usenetSyncFile(file); err != nil {
		cleanup()
		return fmt.Errorf("sync durable file %q: %w", name, err)
	}
	if err := file.Close(); err != nil {
		_ = rooted.Remove(name)
		_ = usenetSyncDirectory(rooted)
		return fmt.Errorf("close durable file %q: %w", name, err)
	}
	if err := usenetSyncDirectory(rooted); err != nil {
		_ = rooted.Remove(name)
		_ = usenetSyncDirectory(rooted)
		return fmt.Errorf("sync durable file parent for %q: %w", name, err)
	}
	return nil
}

func syncUsenetDirectoryDefault(rooted *os.Root) error {
	// Windows does not provide a portable directory fsync through os.File.
	// The marker file itself is still flushed before close there.
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := rooted.Open(".")
	if err != nil {
		return fmt.Errorf("open directory for sync: %w", err)
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return fmt.Errorf("sync directory: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close synced directory: %w", closeErr)
	}
	return nil
}

// removeOwnedUsenetEntryDirectory recursively removes an existing release only
// after proving its marker matches the queue entry. A missing release is a
// clean no-op so pre-action failures remain deletable.
func removeOwnedUsenetEntryDirectory(downloadRoot string, entry *storage.Entry) error {
	usenetOwnershipMu.Lock()
	defer usenetOwnershipMu.Unlock()

	ownership, rootExists, err := acquireUsenetOwnershipLock(downloadRoot, false)
	if err != nil {
		return err
	}
	if !rootExists {
		return nil
	}
	removeErr := quarantineAndRemoveUsenetEntryLocked(ownership.root, ownership.absolute, entry, false)
	if closeErr := ownership.close(); closeErr != nil && removeErr == nil {
		removeErr = fmt.Errorf("close NZB ownership root: %w", closeErr)
	}
	return removeErr
}

func rollbackUsenetEntryClaim(downloadRoot string, entry *storage.Entry) error {
	usenetOwnershipMu.Lock()
	defer usenetOwnershipMu.Unlock()

	ownership, rootExists, err := acquireUsenetOwnershipLock(downloadRoot, false)
	if err != nil {
		return err
	}
	if !rootExists {
		return nil
	}
	rollbackErr := quarantineAndRemoveUsenetEntryLocked(ownership.root, ownership.absolute, entry, true)
	if closeErr := ownership.close(); closeErr != nil && rollbackErr == nil {
		rollbackErr = fmt.Errorf("close NZB ownership root: %w", closeErr)
	}
	return rollbackErr
}

func acquireUsenetOwnershipLock(downloadRoot string, createRoot bool) (*ownershipRoot, bool, error) {
	ownership, exists, err := acquireCompatibleOwnershipRoot(
		downloadRoot,
		[]string{legacyUsenetOwnershipLockName, usenetOwnershipLockName},
		createRoot,
		usenetOwnershipLockTimeout,
		usenetOwnershipRetryDelay,
	)
	if err != nil {
		return nil, false, fmt.Errorf("acquire NZB ownership root: %w", err)
	}
	return ownership, exists, nil
}

func quarantineAndRemoveUsenetEntryLocked(rooted *os.Root, absoluteRoot string, entry *storage.Entry, markerOnly bool) error {
	entryPath, err := safeUsenetEntryDownloadPath(absoluteRoot, entry)
	if err != nil {
		return err
	}
	ownerID, err := canonicalUsenetOwnerID(entry.InfoHash)
	if err != nil {
		return err
	}
	relativeEntry, err := filepath.Rel(absoluteRoot, entryPath)
	if err != nil {
		return fmt.Errorf("make NZB release path relative: %w", err)
	}
	info, err := rooted.Lstat(relativeEntry)
	visibleExists := err == nil
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("inspect NZB release directory: %w", err)
		}
	}
	quarantines, err := findUsenetQuarantines(rooted, relativeEntry, ownerID)
	if err != nil {
		return err
	}
	if len(quarantines) > 0 {
		if visibleExists {
			return fmt.Errorf("refusing ambiguous cleanup with both visible and quarantined NZB release directories")
		}
		return removeUsenetQuarantines(rooted, quarantines, ownerID, markerOnly, nil)
	}
	if !visibleExists {
		return nil
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("NZB release path %q is not a regular directory", entryPath)
	}
	entryRoot, err := rooted.OpenRoot(relativeEntry)
	if err != nil {
		return fmt.Errorf("open NZB release before quarantine: %w", err)
	}
	pinnedInfo, statErr := entryRoot.Stat(".")
	markerOwner, precheckErr := readUsenetOwnerMarker(entryRoot)
	if statErr != nil {
		precheckErr = fmt.Errorf("stat opened NZB release directory: %w", statErr)
	} else if !os.SameFile(info, pinnedInfo) {
		precheckErr = fmt.Errorf("NZB release directory changed during ownership verification")
	}
	if precheckErr == nil && markerOwner != ownerID {
		precheckErr = fmt.Errorf("NZB release directory is owned by %q, not %q", markerOwner, ownerID)
	}
	if precheckErr == nil && markerOnly {
		onlyMarker, inspectErr := usenetDirectoryContainsOnlyMarker(entryRoot, true)
		if inspectErr != nil {
			precheckErr = inspectErr
		} else if !onlyMarker {
			precheckErr = fmt.Errorf("refusing to roll back non-empty NZB release directory %q", entryPath)
		}
	}
	closeErr := entryRoot.Close()
	if precheckErr != nil {
		return precheckErr
	}
	if closeErr != nil {
		return fmt.Errorf("close NZB release before quarantine: %w", closeErr)
	}
	quarantineRelative, err := newUsenetQuarantinePath(rooted, relativeEntry, ownerID)
	if err != nil {
		return err
	}
	if err := rooted.Rename(relativeEntry, quarantineRelative); err != nil {
		return fmt.Errorf("quarantine NZB release directory: %w", err)
	}
	if err := usenetSyncDirectory(rooted); err != nil {
		return fmt.Errorf("sync NZB release quarantine: %w", err)
	}
	return removeUsenetQuarantines(
		rooted,
		[]string{quarantineRelative},
		ownerID,
		markerOnly,
		pinnedInfo,
	)
}

func recoverUsenetQuarantineForClaim(rooted *os.Root, relativeEntry, ownerID string) error {
	quarantines, err := findUsenetQuarantines(rooted, relativeEntry, ownerID)
	if err != nil {
		return err
	}
	if len(quarantines) == 0 {
		return nil
	}
	if len(quarantines) > 1 {
		return fmt.Errorf("multiple crash-remnant quarantines exist for NZB owner %q", ownerID)
	}
	if _, err := rooted.Lstat(relativeEntry); err == nil {
		return fmt.Errorf("both visible and quarantined NZB release directories exist for owner %q", ownerID)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect visible NZB release during recovery: %w", err)
	}
	info, err := rooted.Lstat(quarantines[0])
	if err != nil {
		return fmt.Errorf("inspect quarantined NZB release during recovery: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("quarantined NZB release %q is not a regular directory", quarantines[0])
	}
	quarantineRoot, err := rooted.OpenRoot(quarantines[0])
	if err != nil {
		return fmt.Errorf("open quarantined NZB release during recovery: %w", err)
	}
	markerOwner, markerErr := readUsenetOwnerMarker(quarantineRoot)
	closeErr := quarantineRoot.Close()
	if markerErr != nil {
		return fmt.Errorf("verify quarantined NZB release during recovery: %w", markerErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close quarantined NZB release during recovery: %w", closeErr)
	}
	if markerOwner != ownerID {
		return fmt.Errorf("quarantined NZB release is owned by %q, not %q", markerOwner, ownerID)
	}
	return fmt.Errorf("matching quarantined NZB release requires cleanup before retry")
}

func removeUsenetQuarantines(
	rooted *os.Root,
	quarantines []string,
	ownerID string,
	markerOnly bool,
	expectedInfo os.FileInfo,
) error {
	if len(quarantines) > 1 {
		return fmt.Errorf("refusing ambiguous cleanup of %d NZB quarantines", len(quarantines))
	}
	for _, quarantineRelative := range quarantines {
		info, err := rooted.Lstat(quarantineRelative)
		if err != nil {
			return fmt.Errorf("inspect crash-remnant NZB quarantine %q: %w", quarantineRelative, err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("crash-remnant NZB quarantine %q is not a regular directory", quarantineRelative)
		}
		quarantineRoot, err := rooted.OpenRoot(quarantineRelative)
		if err != nil {
			return fmt.Errorf("open crash-remnant NZB quarantine %q: %w", quarantineRelative, err)
		}
		pinned, statErr := quarantineRoot.Stat(".")
		if statErr != nil {
			_ = quarantineRoot.Close()
			return fmt.Errorf("stat crash-remnant NZB quarantine %q: %w", quarantineRelative, statErr)
		}
		if !os.SameFile(info, pinned) ||
			(expectedInfo != nil && !os.SameFile(expectedInfo, pinned)) {
			_ = quarantineRoot.Close()
			return fmt.Errorf("crash-remnant NZB quarantine %q changed during verification", quarantineRelative)
		}
		markerOwner, verifyErr := readUsenetOwnerMarker(quarantineRoot)
		if verifyErr == nil && markerOwner != ownerID {
			verifyErr = fmt.Errorf("quarantine %q is owned by %q, not %q", quarantineRelative, markerOwner, ownerID)
		}
		if verifyErr == nil && markerOnly {
			onlyMarker, inspectErr := usenetDirectoryContainsOnlyMarker(quarantineRoot, true)
			if inspectErr != nil {
				verifyErr = inspectErr
			} else if !onlyMarker {
				verifyErr = fmt.Errorf("refusing to remove non-empty rollback quarantine %q", quarantineRelative)
			}
		}
		if verifyErr != nil {
			_ = quarantineRoot.Close()
			return fmt.Errorf("refusing crash-remnant NZB quarantine cleanup: %w", verifyErr)
		}
		if err := safepath.RemovePinnedTreeContents(
			quarantineRoot,
			safepath.PinnedTreeRemovalOptions{
				MaxEntries:       usenetDirectoryMaxEntries,
				MaxDepth:         64,
				ReadBatch:        usenetDirectoryReadBatch,
				PreserveTopLevel: usenetOwnerMarkerNames,
			},
		); err != nil {
			_ = quarantineRoot.Close()
			return fmt.Errorf("empty pinned NZB quarantine %q: %w", quarantineRelative, err)
		}
		afterContents, err := rooted.Lstat(quarantineRelative)
		if err != nil || !os.SameFile(pinned, afterContents) {
			_ = quarantineRoot.Close()
			if err != nil {
				return fmt.Errorf("reinspect emptied NZB quarantine %q: %w", quarantineRelative, err)
			}
			return fmt.Errorf("NZB quarantine %q changed before marker removal", quarantineRelative)
		}
		markerOwner, err = readUsenetOwnerMarker(quarantineRoot)
		if err != nil || markerOwner != ownerID {
			_ = quarantineRoot.Close()
			if err != nil {
				return fmt.Errorf("reverify emptied NZB quarantine %q: %w", quarantineRelative, err)
			}
			return fmt.Errorf("emptied NZB quarantine %q is owned by %q, not %q", quarantineRelative, markerOwner, ownerID)
		}
		if err := removeUsenetOwnerMarkers(quarantineRoot); err != nil {
			_ = quarantineRoot.Close()
			return fmt.Errorf("remove NZB quarantine ownership marker %q: %w", quarantineRelative, err)
		}
		if err := usenetSyncDirectory(quarantineRoot); err != nil {
			_ = quarantineRoot.Close()
			return fmt.Errorf("sync emptied NZB quarantine %q: %w", quarantineRelative, err)
		}
		if err := quarantineRoot.Close(); err != nil {
			return fmt.Errorf("close emptied NZB quarantine %q: %w", quarantineRelative, err)
		}
		afterClose, err := rooted.Lstat(quarantineRelative)
		if err != nil || !os.SameFile(pinned, afterClose) {
			if err != nil {
				return fmt.Errorf("reinspect closed NZB quarantine %q: %w", quarantineRelative, err)
			}
			return fmt.Errorf("NZB quarantine %q changed before final unlink", quarantineRelative)
		}
		if err := rooted.Remove(quarantineRelative); err != nil {
			return fmt.Errorf("remove emptied NZB quarantine %q: %w", quarantineRelative, err)
		}
		if err := usenetSyncDirectory(rooted); err != nil {
			return fmt.Errorf("sync removed NZB quarantine %q: %w", quarantineRelative, err)
		}
	}
	return nil
}

func findUsenetQuarantines(rooted *os.Root, relativeEntry, ownerID string) ([]string, error) {
	parentRelative := filepath.Dir(relativeEntry)
	dir, err := rooted.Open(parentRelative)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open NZB release parent for quarantine recovery: %w", err)
	}
	prefixes := usenetQuarantinePrefixesForEntry(ownerID, relativeEntry)
	quarantines := make([]string, 0, 1)
	readErr := scanBoundedUsenetDirectory(dir, func(existing os.DirEntry) (bool, error) {
		lowerName := strings.ToLower(existing.Name())
		for _, prefix := range prefixes {
			if strings.HasPrefix(lowerName, prefix) {
				quarantines = append(quarantines, filepath.Join(parentRelative, existing.Name()))
				break
			}
		}
		// Every caller rejects an ambiguous multiple-quarantine state, so two
		// matches are sufficient proof and bound the retained result.
		return len(quarantines) > 1, nil
	})
	closeErr := dir.Close()
	if readErr != nil {
		return nil, fmt.Errorf("inspect NZB release parent for quarantines: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close NZB quarantine inspection: %w", closeErr)
	}

	return quarantines, nil
}

func newUsenetQuarantinePath(rooted *os.Root, relativeEntry, ownerID string) (string, error) {
	parentRelative := filepath.Dir(relativeEntry)
	prefix := usenetQuarantinePrefixForEntry(ownerID, relativeEntry)
	for range 100 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", fmt.Errorf("generate NZB quarantine name: %w", err)
		}
		name := prefix + hex.EncodeToString(random[:])
		relative := filepath.Join(parentRelative, name)
		if _, err := rooted.Lstat(relative); os.IsNotExist(err) {
			return relative, nil
		} else if err != nil {
			return "", fmt.Errorf("inspect NZB quarantine candidate: %w", err)
		}
	}
	return "", fmt.Errorf("generate NZB quarantine name: exhausted unique names")
}

func usenetQuarantinePrefixForEntry(ownerID, relativeEntry string) string {
	digest := sha256.Sum256([]byte(ownerID + "\x00" + filepath.ToSlash(relativeEntry)))
	return usenetQuarantinePrefix + hex.EncodeToString(digest[:16]) + "-"
}

func usenetQuarantinePrefixesForEntry(ownerID, relativeEntry string) []string {
	digest := sha256.Sum256([]byte(ownerID + "\x00" + filepath.ToSlash(relativeEntry)))
	suffix := hex.EncodeToString(digest[:16]) + "-"
	return []string{usenetQuarantinePrefix + suffix, legacyUsenetQuarantinePrefix + suffix}
}

func readUsenetOwnerMarker(entryRoot *os.Root) (string, error) {
	owner := ""
	found := false
	for _, name := range usenetOwnerMarkerNames {
		markerOwner, err := readUsenetOwnerMarkerNamed(entryRoot, name)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		if found && markerOwner != owner {
			return "", fmt.Errorf("NZB ownership markers disagree")
		}
		owner = markerOwner
		found = true
	}
	if !found {
		return "", os.ErrNotExist
	}
	return owner, nil
}

func readUsenetOwnerMarkerNamed(entryRoot *os.Root, name string) (string, error) {
	info, err := entryRoot.Lstat(name)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", fmt.Errorf("NZB ownership marker is not a regular file")
	}
	file, err := entryRoot.Open(name)
	if err != nil {
		return "", err
	}
	contents, readErr := io.ReadAll(io.LimitReader(file, usenetOwnerMarkerMaxBytes+1))
	closeErr := file.Close()
	if readErr != nil {
		return "", fmt.Errorf("read NZB ownership marker: %w", readErr)
	}
	if closeErr != nil {
		return "", fmt.Errorf("close NZB ownership marker: %w", closeErr)
	}
	if len(contents) > usenetOwnerMarkerMaxBytes {
		return "", fmt.Errorf("NZB ownership marker is too large")
	}
	if len(contents) < 2 || contents[len(contents)-1] != '\n' {
		return "", fmt.Errorf("NZB ownership marker is malformed")
	}
	ownerID := string(contents[:len(contents)-1])
	if strings.ContainsRune(ownerID, '\n') {
		return "", fmt.Errorf("NZB ownership marker is malformed")
	}
	return ownerID, nil
}

func removeUsenetOwnerMarkers(rooted *os.Root) error {
	var removeErr error
	for _, name := range usenetOwnerMarkerNames {
		if err := rooted.Remove(name); err != nil && !os.IsNotExist(err) {
			removeErr = errors.Join(removeErr, err)
		}
	}
	return removeErr
}

func usenetDirectoryContainsOnlyMarker(entryRoot *os.Root, requireMarker bool) (bool, error) {
	dir, err := entryRoot.Open(".")
	if err != nil {
		return false, fmt.Errorf("open NZB release directory for inspection: %w", err)
	}
	count := 0
	markerCount := 0
	readErr := scanBoundedUsenetDirectory(dir, func(entry os.DirEntry) (bool, error) {
		count++
		if isUsenetOwnerMarkerName(entry.Name()) {
			markerCount++
		}
		return count > len(usenetOwnerMarkerNames), nil
	})
	closeErr := dir.Close()
	if readErr != nil {
		return false, fmt.Errorf("inspect NZB release directory: %w", readErr)
	}
	if closeErr != nil {
		return false, fmt.Errorf("close NZB release directory inspection: %w", closeErr)
	}
	if requireMarker {
		return markerCount > 0 && count == markerCount, nil
	}
	return count == 0, nil
}

func rejectPortableSiblingAlias(rooted *os.Root, parentRelative, desiredName string) error {
	if parentRelative == "" {
		parentRelative = "."
	}
	dir, err := rooted.Open(parentRelative)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open parent directory: %w", err)
	}
	readErr := scanBoundedUsenetDirectory(dir, func(existing os.DirEntry) (bool, error) {
		if existing.Name() != desiredName && portableUsenetNameAlias(existing.Name(), desiredName) {
			return true, fmt.Errorf("path name %q collides with portable alias %q", desiredName, existing.Name())
		}
		return false, nil
	})
	closeErr := dir.Close()
	if readErr != nil {
		return fmt.Errorf("inspect parent directory: %w", readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close parent directory: %w", closeErr)
	}
	return nil
}

// scanBoundedUsenetDirectory keeps directory inspection memory-bounded and
// fails closed if an unexpectedly large parent would otherwise make ownership
// or portable-alias proof unbounded. The visitor may stop early once it has
// enough evidence (for example, two conflicting quarantines).
func scanBoundedUsenetDirectory(
	dir *os.File,
	visit func(os.DirEntry) (stop bool, err error),
) error {
	if dir == nil || visit == nil {
		return fmt.Errorf("NZB directory scanner input is nil")
	}
	scanned := 0
	for scanned < usenetDirectoryMaxEntries {
		remaining := usenetDirectoryMaxEntries - scanned
		entries, readErr := dir.ReadDir(min(usenetDirectoryReadBatch, remaining))
		scanned += len(entries)
		for _, entry := range entries {
			stop, err := visit(entry)
			if err != nil {
				return err
			}
			if stop {
				return nil
			}
		}
		if errors.Is(readErr, io.EOF) {
			return nil
		}
		if readErr != nil {
			return readErr
		}
		if len(entries) == 0 {
			return fmt.Errorf("NZB directory scan made no progress")
		}
	}
	return fmt.Errorf(
		"NZB directory inspection exceeded %d entries",
		usenetDirectoryMaxEntries,
	)
}

func portableUsenetNameAlias(a, b string) bool {
	return strings.EqualFold(strings.TrimRight(a, " ."), strings.TrimRight(b, " ."))
}

func isReservedUsenetPrivateName(name string) bool {
	trimmed := strings.TrimRight(name, " .")
	return portableUsenetNameAlias(trimmed, usenetOwnerMarkerName) ||
		portableUsenetNameAlias(trimmed, legacyUsenetOwnerMarkerName) ||
		portableUsenetNameAlias(trimmed, usenetOwnershipLockName) ||
		portableUsenetNameAlias(trimmed, legacyUsenetOwnershipLockName) ||
		portableUsenetNameAlias(trimmed, usenetLegacyAdoptionCheckpointName) ||
		portableUsenetNameAlias(trimmed, legacyUsenetAdoptionCheckpointName) ||
		strings.HasPrefix(strings.ToLower(trimmed), usenetQuarantinePrefix) ||
		strings.HasPrefix(strings.ToLower(trimmed), legacyUsenetQuarantinePrefix)
}

func canonicalUsenetOwnerID(infoHash string) (string, error) {
	if infoHash == "" {
		return "", fmt.Errorf("NZB owner ID is empty")
	}
	if len(infoHash) > usenetOwnerMarkerMaxBytes-1 {
		return "", fmt.Errorf("NZB owner ID is too long")
	}
	if infoHash != strings.TrimSpace(infoHash) {
		return "", fmt.Errorf("NZB owner ID has leading or trailing whitespace")
	}
	if strings.IndexByte(infoHash, 0) >= 0 || strings.IndexFunc(infoHash, unicode.IsControl) >= 0 {
		return "", fmt.Errorf("NZB owner ID contains a control character")
	}
	return strings.ToLower(infoHash), nil
}

func sameFilesystemPath(a, b string) bool {
	absoluteA, errA := filepath.Abs(a)
	absoluteB, errB := filepath.Abs(b)
	if errA != nil || errB != nil {
		return false
	}
	absoluteA = filepath.Clean(absoluteA)
	absoluteB = filepath.Clean(absoluteB)
	if runtime.GOOS == "windows" {
		return strings.EqualFold(absoluteA, absoluteB)
	}
	return absoluteA == absoluteB
}
