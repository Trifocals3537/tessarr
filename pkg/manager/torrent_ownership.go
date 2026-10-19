package manager

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

const (
	torrentOwnerMarkerName      = ".tessarr-torrent-owner-v1"
	torrentOwnershipLockName    = ".tessarr-torrent-ownership.lock"
	torrentQuarantinePrefix     = ".tessarr-torrent-quarantine-"
	torrentPartialPrefix        = ".tessarr-torrent-part-"
	torrentOwnerMarkerMaxBytes  = 256
	torrentOwnershipMaxEntries  = 100_000
	torrentOwnershipReadBatch   = 256
	torrentOwnershipMaxDepth    = 64
	torrentOwnershipLockTimeout = 5 * time.Second
)

var (
	torrentOwnershipMu sync.Mutex

	torrentSyncFile = func(file *os.File) error {
		return file.Sync()
	}
	torrentSyncDirectory = syncTorrentDirectory

	// torrentAfterQuarantine is a test seam for crash and path-swap coverage.
	// Production leaves it nil.
	torrentAfterQuarantine         func(absoluteVisiblePath string) error
	torrentAfterQuarantineVerified func(quarantineRelative string) error
	torrentAfterMarkerLstat        func(root *os.Root) error
)

var torrentOwnerMarkerNames = []string{torrentOwnerMarkerName, legacyTorrentOwnerMarkerName}

type torrentFileLayout struct {
	file     *storage.File
	relative string
	key      string
}

type torrentLegacyProof struct {
	mountPath string
	strmURL   string
}

type ownedTorrentPart struct {
	root             *os.Root
	file             *os.File
	entryPath        string
	finalRelative    string
	partRelative     string
	partAbsolutePath string
	initialInfo      os.FileInfo
	expectedSize     int64
	recovered        bool
	closed           bool
}

// safeTorrentEntryDownloadPath validates the persisted torrent output path
// against the configured root. Unlike the NZB layout, torrent SavePath may be
// a caller-selected descendant, so durable ownership is what authorizes later
// destructive cleanup.
func safeTorrentEntryDownloadPath(downloadRoot string, entry *storage.Entry) (string, error) {
	if entry == nil || !entry.IsTorrent() {
		return "", fmt.Errorf("entry is not a torrent")
	}
	root, err := safepath.ValidateRoot(downloadRoot)
	if err != nil {
		return "", fmt.Errorf("invalid torrent download root: %w", err)
	}
	savePath, err := validateTorrentDownloadFolder(root, entry.SavePath)
	if err != nil {
		return "", err
	}
	// Output identity does not make an unsafe display title safe to use as a
	// mount/source path. Preserve the independent title traversal check.
	if err := validateTorrentRootName(entry.Name, false); err != nil {
		return "", err
	}
	rootName := entry.OutputComponent()
	if err := safepath.ValidateIdentifier(rootName); err != nil {
		return "", fmt.Errorf("invalid torrent output component %q: %w", rootName, err)
	}
	if isReservedTorrentPrivateName(rootName) {
		return "", fmt.Errorf("torrent root name %q is reserved for internal ownership state", rootName)
	}
	expected := filepath.Join(savePath, rootName)
	safe, err := safepath.ValidateUnderRoot(root, expected)
	if err != nil {
		return "", fmt.Errorf("invalid torrent output path: %w", err)
	}
	if !sameFilesystemPath(entry.DownloadPath(), safe) {
		return "", fmt.Errorf("torrent output path %q does not match validated path %q", entry.DownloadPath(), safe)
	}
	return safe, nil
}

func isReservedTorrentPrivateName(name string) bool {
	lower := strings.ToLower(name)
	return lower == torrentOwnerMarkerName ||
		lower == legacyTorrentOwnerMarkerName ||
		lower == torrentOwnershipLockName ||
		lower == legacyTorrentOwnershipLockName ||
		strings.HasPrefix(lower, torrentQuarantinePrefix) ||
		strings.HasPrefix(lower, legacyTorrentQuarantinePrefix) ||
		strings.HasPrefix(lower, torrentPartialPrefix) ||
		strings.HasPrefix(lower, legacyTorrentPartialPrefix)
}

// torrentEntryFileLayouts preserves safe nested provider paths. Both slash
// styles are interpreted as separators so a record cannot become unsafe when
// storage moves between Linux and Windows.
func torrentEntryFileLayouts(entry *storage.Entry) ([]torrentFileLayout, error) {
	if entry == nil || !entry.IsTorrent() {
		return nil, fmt.Errorf("entry is not a torrent")
	}
	if len(entry.Files) > torrentOwnershipMaxEntries {
		return nil, fmt.Errorf("torrent contains %d file records, maximum is %d", len(entry.Files), torrentOwnershipMaxEntries)
	}
	layouts := make([]torrentFileLayout, 0, len(entry.Files))
	seenPaths := make(map[string]string, len(entry.Files))
	fileKeys := make(map[string]string, len(entry.Files))
	directoryKeys := make(map[string]string, len(entry.Files))
	logicalNames := make(map[string]string, len(entry.Files))

	for _, file := range entry.GetActiveFiles() {
		if file == nil {
			return nil, fmt.Errorf("torrent contains a nil file")
		}
		if err := validateTorrentFileTransferMetadata(file); err != nil {
			return nil, err
		}
		if effectiveTorrentAction(entry.Action) == config.DownloadActionDownload && torrentTransferSize(file) <= 0 {
			return nil, fmt.Errorf("torrent download file %q has no bounded positive transfer size", file.Name)
		}
		relative, err := torrentFileRelativePath(entry, file)
		if err != nil {
			return nil, err
		}
		logicalName, err := normalizeTorrentFileOutputPath(entry, strings.TrimSpace(file.Name))
		if err != nil {
			return nil, fmt.Errorf("invalid torrent file name %q: %w", file.Name, err)
		}
		logicalKey := portableTorrentRelativeKey(logicalName)
		if previous, exists := logicalNames[logicalKey]; exists {
			return nil, fmt.Errorf("torrent files have duplicate logical name %q (%q and %q)", file.Name, previous, file.Name)
		}
		logicalNames[logicalKey] = file.Name
		key := portableTorrentRelativeKey(relative)
		if previous, exists := seenPaths[key]; exists {
			return nil, fmt.Errorf("torrent files %q and %q collide at portable path %q", previous, file.Name, relative)
		}
		seenPaths[key] = file.Name

		if directoryOwner, conflict := directoryKeys[key]; conflict {
			return nil, fmt.Errorf("torrent file %q conflicts with directory required by %q", file.Name, directoryOwner)
		}
		fileKeys[key] = file.Name
		parent := filepath.Dir(relative)
		for parent != "." {
			parentKey := portableTorrentRelativeKey(parent)
			if fileOwner, conflict := fileKeys[parentKey]; conflict {
				return nil, fmt.Errorf("torrent directory for %q conflicts with file %q", file.Name, fileOwner)
			}
			directoryKeys[parentKey] = file.Name
			parent = filepath.Dir(parent)
		}

		layouts = append(layouts, torrentFileLayout{
			file:     file,
			relative: relative,
			key:      key,
		})
	}
	sort.Slice(layouts, func(i, j int) bool {
		return layouts[i].key < layouts[j].key
	})
	return layouts, nil
}

func validateTorrentFileTransferMetadata(file *storage.File) error {
	if file == nil {
		return fmt.Errorf("torrent file is nil")
	}
	if file.Size < 0 {
		return fmt.Errorf("torrent file %q has negative size %d", file.Name, file.Size)
	}
	if file.ByteRange == nil {
		return nil
	}
	start, end := file.ByteRange[0], file.ByteRange[1]
	if start < 0 || end < start {
		return fmt.Errorf("torrent file %q has invalid byte range %d-%d", file.Name, start, end)
	}
	if end-start == math.MaxInt64 {
		return fmt.Errorf("torrent file %q byte range length overflows", file.Name)
	}
	return nil
}

func torrentFileRelativePath(entry *storage.Entry, file *storage.File) (string, error) {
	if file == nil {
		return "", fmt.Errorf("torrent file is nil")
	}
	raw := strings.TrimSpace(file.Path)
	if raw == "" {
		raw = strings.TrimSpace(file.Name)
	}
	relative, err := normalizeTorrentFileOutputPath(entry, raw)
	if err != nil {
		return "", fmt.Errorf("invalid torrent file path %q: %w", raw, err)
	}

	logicalName, err := normalizeTorrentFileOutputPath(entry, strings.TrimSpace(file.Name))
	if err != nil {
		return "", fmt.Errorf("invalid torrent file name %q: %w", file.Name, err)
	}
	if !strings.EqualFold(filepath.Base(relative), filepath.Base(logicalName)) {
		return "", fmt.Errorf("torrent file name %q does not match provider path %q", file.Name, file.Path)
	}
	return relative, nil
}

// Strip only an identified provider release root, never an arbitrary unsafe
// path component. New output names let a display root such as "Movie: Part Two"
// be represented on disk without relaxing validation of nested directories or
// media filenames. Legacy records keep their previous strict interpretation.
func normalizeTorrentFileOutputPath(entry *storage.Entry, raw string) (string, error) {
	if entry != nil && entry.OutputName != "" {
		parts := strings.Split(strings.ReplaceAll(raw, `\`, "/"), "/")
		if len(parts) > 1 && entry.TorrentPathFirstComponentIsEntryRoot(parts[0]) && validateTorrentRootName(parts[0], false) == nil {
			raw = strings.Join(parts[1:], "/")
		}
		return normalizeTorrentRelativePath(raw)
	}
	relative, err := normalizeTorrentRelativePath(raw)
	if err != nil {
		return "", err
	}
	parts := strings.Split(filepath.ToSlash(relative), "/")
	if len(parts) > 1 && entry.TorrentPathFirstComponentIsEntryRoot(parts[0]) {
		return normalizeTorrentRelativePath(strings.Join(parts[1:], "/"))
	}
	return relative, nil
}

func normalizeTorrentRelativePath(value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("path is empty")
	}
	if strings.IndexByte(value, 0) >= 0 {
		return "", fmt.Errorf("path contains a NUL byte")
	}
	if strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return "", fmt.Errorf("path contains a control character")
	}
	value = strings.ReplaceAll(value, `\`, "/")
	if path.IsAbs(value) || filepath.IsAbs(value) || filepath.VolumeName(value) != "" ||
		(len(value) >= 2 && value[1] == ':') {
		return "", fmt.Errorf("path is absolute")
	}
	clean := path.Clean(value)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("path traverses outside the release")
	}
	parts := strings.Split(clean, "/")
	for _, component := range parts {
		if err := safepath.ValidateIdentifier(component); err != nil {
			return "", err
		}
		if isReservedTorrentPrivateName(component) {
			return "", fmt.Errorf("path component %q is reserved for internal ownership state", component)
		}
	}
	return filepath.Join(parts...), nil
}

func portableTorrentRelativeKey(relative string) string {
	parts := strings.Split(filepath.ToSlash(relative), "/")
	for i := range parts {
		parts[i] = strings.ToLower(strings.TrimRight(parts[i], " ."))
	}
	return strings.Join(parts, "/")
}

// normalizeTorrentProviderSourcePath validates a path only for lookup inside a
// pinned provider mount. Provider-native punctuation is allowed here because
// it never becomes an output name; traversal, separators inside components,
// NUL/control bytes, and absolute host paths remain forbidden.
func normalizeTorrentProviderSourcePath(value string) (string, error) {
	if value == "" {
		return "", fmt.Errorf("path is empty")
	}
	if strings.IndexByte(value, 0) >= 0 {
		return "", fmt.Errorf("path contains a NUL byte")
	}
	if strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return "", fmt.Errorf("path contains a control character")
	}
	value = strings.ReplaceAll(value, `\`, "/")
	if path.IsAbs(value) || filepath.IsAbs(value) || filepath.VolumeName(value) != "" {
		return "", fmt.Errorf("path is absolute")
	}
	clean := path.Clean(value)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("path traverses outside the provider release")
	}
	parts := strings.Split(clean, "/")
	for _, component := range parts {
		if err := validateTorrentProviderSourceIdentifier(component); err != nil {
			return "", err
		}
	}
	return filepath.Join(parts...), nil
}

func validateTorrentProviderSourceIdentifier(name string) error {
	if name == "" || name == "." || name == ".." {
		return fmt.Errorf("path component is empty or traversal")
	}
	if strings.IndexByte(name, 0) >= 0 || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return fmt.Errorf("path component contains a NUL or control character")
	}
	if strings.ContainsAny(name, `/\`) {
		return fmt.Errorf("path component contains a separator")
	}
	return nil
}

func torrentProviderSourceKey(relative string) string {
	return strings.ToLower(path.Clean(filepath.ToSlash(relative)))
}

func safeTorrentFilePath(downloadRoot string, entry *storage.Entry, relative, suffix string) (string, error) {
	entryPath, err := safeTorrentEntryDownloadPath(downloadRoot, entry)
	if err != nil {
		return "", err
	}
	relative, err = normalizeTorrentRelativePath(relative)
	if err != nil {
		return "", err
	}
	if suffix != "" {
		relative, err = normalizeTorrentRelativePath(relative + suffix)
		if err != nil {
			return "", err
		}
	}
	target := filepath.Join(entryPath, relative)
	safe, err := safepath.ValidateUnderRoot(entryPath, target)
	if err != nil {
		return "", err
	}
	return safe, nil
}

func openOwnedTorrentEntryRoot(downloadRoot string, entry *storage.Entry) (string, *os.Root, error) {
	entryPath, err := safeTorrentEntryDownloadPath(downloadRoot, entry)
	if err != nil {
		return "", nil, err
	}
	ownerID, err := canonicalTorrentOwnerID(entry.InfoHash)
	if err != nil {
		return "", nil, err
	}
	info, err := os.Lstat(entryPath)
	if err != nil {
		return "", nil, fmt.Errorf("inspect owned torrent release directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", nil, fmt.Errorf("torrent release path %q is not a regular directory", entryPath)
	}
	root, err := os.OpenRoot(entryPath)
	if err != nil {
		return "", nil, fmt.Errorf("open owned torrent release directory: %w", err)
	}
	pinned, err := root.Stat(".")
	if err != nil {
		_ = root.Close()
		return "", nil, fmt.Errorf("stat owned torrent release directory: %w", err)
	}
	if !os.SameFile(info, pinned) {
		_ = root.Close()
		return "", nil, fmt.Errorf("torrent release directory changed while opening")
	}
	markerOwner, err := readTorrentOwnerMarker(root)
	if err != nil {
		_ = root.Close()
		return "", nil, fmt.Errorf("verify torrent ownership marker: %w", err)
	}
	if markerOwner != ownerID {
		_ = root.Close()
		return "", nil, fmt.Errorf("torrent release directory is owned by %q, not %q", markerOwner, ownerID)
	}
	return entryPath, root, nil
}

func ensureOwnedTorrentParent(root *os.Root, relative string) error {
	relative, err := normalizeTorrentRelativePath(relative)
	if err != nil {
		return err
	}
	parent := filepath.Dir(relative)
	if parent == "." {
		return nil
	}
	parts := strings.Split(filepath.ToSlash(parent), "/")
	current := "."
	for _, component := range parts {
		if err := rejectPortableTorrentSiblingAlias(root, current, component); err != nil {
			return err
		}
		current = filepath.Join(current, component)
		if err := root.Mkdir(current, 0o755); err != nil {
			if !os.IsExist(err) {
				return fmt.Errorf("create torrent artifact directory %q: %w", current, err)
			}
			info, statErr := root.Lstat(current)
			if statErr != nil {
				return fmt.Errorf("inspect torrent artifact directory %q: %w", current, statErr)
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				return fmt.Errorf("torrent artifact path component %q is not a regular directory", current)
			}
		}
	}
	return nil
}

func createOwnedTorrentSymlink(downloadRoot string, entry *storage.Entry, relative, target string, relativeTarget bool) (string, error) {
	relative, err := normalizeTorrentRelativePath(relative)
	if err != nil {
		return "", err
	}
	entryPath, root, err := openOwnedTorrentEntryRoot(downloadRoot, entry)
	if err != nil {
		return "", err
	}
	defer root.Close()
	if err := ensureOwnedTorrentParent(root, relative); err != nil {
		return "", err
	}
	linkPath := filepath.Join(entryPath, relative)
	storedTarget, err := symlinkTarget(linkPath, target, relativeTarget)
	if err != nil {
		return "", err
	}
	if err := root.Symlink(storedTarget, relative); err != nil {
		if os.IsExist(err) {
			before, statErr := root.Lstat(relative)
			if statErr == nil && before.Mode()&os.ModeSymlink != 0 {
				existingTarget, readErr := root.Readlink(relative)
				after, afterErr := root.Lstat(relative)
				if readErr == nil && afterErr == nil && os.SameFile(before, after) &&
					sameSymlinkTarget(linkPath, existingTarget, storedTarget) {
					return filepath.Join(entryPath, relative), nil
				}
			}
		}
		return "", fmt.Errorf("create torrent symlink %q -> %q: %w", relative, target, err)
	}
	before, err := root.Lstat(relative)
	if err != nil || before.Mode()&os.ModeSymlink == 0 {
		if err != nil {
			return "", fmt.Errorf("inspect created torrent symlink %q: %w", relative, err)
		}
		return "", fmt.Errorf("created torrent artifact %q is not a symlink", relative)
	}
	actualTarget, err := root.Readlink(relative)
	if err != nil {
		return "", fmt.Errorf("read created torrent symlink %q: %w", relative, err)
	}
	after, err := root.Lstat(relative)
	if err != nil || !os.SameFile(before, after) || !sameSymlinkTarget(linkPath, actualTarget, storedTarget) {
		if err != nil {
			return "", fmt.Errorf("reinspect created torrent symlink %q: %w", relative, err)
		}
		return "", fmt.Errorf("created torrent symlink %q changed during verification", relative)
	}
	return filepath.Join(entryPath, relative), nil
}

func writeOwnedTorrentFile(downloadRoot string, entry *storage.Entry, relative string, contents []byte, perm os.FileMode) error {
	relative, err := normalizeTorrentRelativePath(relative)
	if err != nil {
		return err
	}
	_, root, err := openOwnedTorrentEntryRoot(downloadRoot, entry)
	if err != nil {
		return err
	}
	defer root.Close()
	if err := ensureOwnedTorrentParent(root, relative); err != nil {
		return err
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return fmt.Errorf("generate torrent artifact temporary name: %w", err)
	}
	tempRelative := filepath.Join(
		filepath.Dir(relative),
		torrentPartialPrefix+hex.EncodeToString(random[:]),
	)
	file, err := root.OpenFile(tempRelative, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return fmt.Errorf("create torrent artifact temporary file: %w", err)
	}
	tempExists := true
	defer func() {
		if tempExists {
			_ = root.Remove(tempRelative)
		}
	}()
	if err := writeAllTorrentBytes(file, contents); err != nil {
		_ = file.Close()
		return fmt.Errorf("write torrent artifact %q: %w", relative, err)
	}
	if err := torrentSyncFile(file); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync torrent artifact %q: %w", relative, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close torrent artifact %q: %w", relative, err)
	}
	tempInfo, err := root.Lstat(tempRelative)
	if err != nil || tempInfo.Mode()&os.ModeSymlink != 0 || !tempInfo.Mode().IsRegular() {
		if err != nil {
			return fmt.Errorf("inspect torrent artifact temporary file: %w", err)
		}
		return fmt.Errorf("torrent artifact temporary path changed before publish")
	}
	tempLinkCount, err := rootedTorrentRegularFileLinkCount(root, tempRelative, tempInfo)
	if err != nil {
		return fmt.Errorf("inspect torrent artifact temporary link count: %w", err)
	}
	if tempLinkCount != 1 {
		return fmt.Errorf("torrent artifact temporary file has unsafe hard-link count %d", tempLinkCount)
	}
	published := false
	var expectedFinalInfo os.FileInfo
	if err := root.Link(tempRelative, relative); err != nil {
		if !os.IsExist(err) {
			return fmt.Errorf("publish torrent artifact without overwrite: %w", err)
		}
		equal, compareErr := rootedTorrentFilesEqual(root, tempRelative, relative)
		if compareErr != nil {
			return compareErr
		}
		if !equal {
			return fmt.Errorf("refusing to replace existing torrent artifact %q", relative)
		}
		if err := syncRootedTorrentRegularFile(root, relative); err != nil {
			return fmt.Errorf("sync idempotent torrent artifact: %w", err)
		}
		expectedFinalInfo, err = rootedTorrentRegularFileInfo(root, relative)
		if err != nil {
			return fmt.Errorf("reinspect idempotent torrent artifact: %w", err)
		}
		finalLinkCount, countErr := rootedTorrentRegularFileLinkCount(root, relative, expectedFinalInfo)
		if countErr != nil {
			return fmt.Errorf("inspect idempotent torrent artifact link count: %w", countErr)
		}
		if finalLinkCount != 1 {
			return fmt.Errorf("idempotent torrent artifact has unsafe hard-link count %d", finalLinkCount)
		}
	} else {
		published = true
		finalInfo, err := root.Lstat(relative)
		if err != nil || !os.SameFile(tempInfo, finalInfo) {
			if err != nil {
				return fmt.Errorf("verify published torrent artifact: %w", err)
			}
			return fmt.Errorf("published torrent artifact changed during verification")
		}
		expectedFinalInfo = finalInfo
	}
	parent, err := root.Open(filepath.Dir(relative))
	if err != nil {
		return fmt.Errorf("open torrent artifact parent for sync: %w", err)
	}
	if published {
		if err := torrentSyncDirectory(parent); err != nil {
			_ = parent.Close()
			return fmt.Errorf("sync published torrent artifact parent: %w", err)
		}
	}
	if err := root.Remove(tempRelative); err != nil {
		_ = parent.Close()
		return fmt.Errorf("remove torrent artifact temporary file: %w", err)
	}
	tempExists = false
	if err := torrentSyncDirectory(parent); err != nil {
		_ = parent.Close()
		return fmt.Errorf("sync torrent artifact temporary removal: %w", err)
	}
	finalInfo, err := root.Lstat(relative)
	if err != nil || expectedFinalInfo == nil || !os.SameFile(expectedFinalInfo, finalInfo) {
		_ = parent.Close()
		if err != nil {
			return fmt.Errorf("reinspect published torrent artifact: %w", err)
		}
		return fmt.Errorf("published torrent artifact changed before completion")
	}
	finalLinkCount, countErr := rootedTorrentRegularFileLinkCount(root, relative, finalInfo)
	if countErr != nil {
		_ = parent.Close()
		return fmt.Errorf("inspect published torrent artifact link count: %w", countErr)
	}
	if finalLinkCount != 1 {
		_ = parent.Close()
		return fmt.Errorf("published torrent artifact has unsafe hard-link count %d", finalLinkCount)
	}
	return parent.Close()
}

func writeAllTorrentBytes(file *os.File, contents []byte) error {
	for len(contents) > 0 {
		written, err := file.Write(contents)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		contents = contents[written:]
	}
	return nil
}

func openOwnedTorrentPart(downloadRoot string, entry *storage.Entry, relative string, expectedSize int64) (*ownedTorrentPart, error) {
	relative, err := normalizeTorrentRelativePath(relative)
	if err != nil {
		return nil, err
	}
	entryPath, root, err := openOwnedTorrentEntryRoot(downloadRoot, entry)
	if err != nil {
		return nil, err
	}
	if err := ensureOwnedTorrentParent(root, relative); err != nil {
		_ = root.Close()
		return nil, err
	}

	digest := sha256.Sum256([]byte(portableTorrentRelativeKey(relative)))
	partName := torrentPartialPrefix + hex.EncodeToString(digest[:16])
	partRelative := filepath.Join(filepath.Dir(relative), partName)
	legacyPartRelative := filepath.Join(
		filepath.Dir(relative),
		legacyTorrentPartialPrefix+hex.EncodeToString(digest[:16]),
	)
	if _, currentErr := root.Lstat(partRelative); currentErr == nil {
		if _, legacyErr := root.Lstat(legacyPartRelative); legacyErr == nil {
			_ = root.Close()
			return nil, fmt.Errorf("both Tessarr and legacy torrent partial files exist")
		} else if !os.IsNotExist(legacyErr) {
			_ = root.Close()
			return nil, fmt.Errorf("inspect legacy torrent partial file: %w", legacyErr)
		}
	} else if os.IsNotExist(currentErr) {
		if _, legacyErr := root.Lstat(legacyPartRelative); legacyErr == nil {
			partRelative = legacyPartRelative
		} else if !os.IsNotExist(legacyErr) {
			_ = root.Close()
			return nil, fmt.Errorf("inspect legacy torrent partial file: %w", legacyErr)
		}
	} else {
		_ = root.Close()
		return nil, fmt.Errorf("inspect torrent partial file: %w", currentErr)
	}
	file, err := root.OpenFile(partRelative, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	created := err == nil
	if err != nil && !os.IsExist(err) {
		_ = root.Close()
		return nil, fmt.Errorf("create rooted torrent partial file: %w", err)
	}
	if !created {
		before, statErr := root.Lstat(partRelative)
		if statErr != nil {
			_ = root.Close()
			return nil, fmt.Errorf("inspect existing torrent partial file: %w", statErr)
		}
		if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
			_ = root.Close()
			return nil, fmt.Errorf("existing torrent partial path is not a regular file")
		}
		file, err = root.OpenFile(partRelative, os.O_RDWR, 0)
		if err != nil {
			_ = root.Close()
			return nil, fmt.Errorf("open existing rooted torrent partial file: %w", err)
		}
		after, statErr := file.Stat()
		if statErr != nil || !os.SameFile(before, after) {
			_ = file.Close()
			_ = root.Close()
			if statErr != nil {
				return nil, fmt.Errorf("stat opened torrent partial file: %w", statErr)
			}
			return nil, fmt.Errorf("torrent partial file changed while opening")
		}
	}
	initialInfo, err := file.Stat()
	if err != nil {
		_ = file.Close()
		_ = root.Close()
		return nil, fmt.Errorf("stat torrent partial file: %w", err)
	}
	pathInfo, err := root.Lstat(partRelative)
	if err != nil || pathInfo.Mode()&os.ModeSymlink != 0 || !pathInfo.Mode().IsRegular() ||
		!os.SameFile(initialInfo, pathInfo) {
		_ = file.Close()
		_ = root.Close()
		if err != nil {
			return nil, fmt.Errorf("reinspect torrent partial path: %w", err)
		}
		return nil, fmt.Errorf("torrent partial path changed during open")
	}
	linkCount, err := torrentOpenFileLinkCount(file)
	if err != nil {
		_ = file.Close()
		_ = root.Close()
		return nil, fmt.Errorf("inspect torrent partial hard-link count: %w", err)
	}
	if created && linkCount != 1 {
		_ = file.Close()
		_ = root.Close()
		return nil, fmt.Errorf("new torrent partial file has unsafe hard-link count %d", linkCount)
	}
	if !created && linkCount != 1 {
		finalInfo, finalErr := root.Lstat(relative)
		if expectedSize <= 0 || finalErr != nil || linkCount != 2 ||
			finalInfo.Mode()&os.ModeSymlink != 0 || !finalInfo.Mode().IsRegular() ||
			!os.SameFile(initialInfo, finalInfo) {
			_ = file.Close()
			_ = root.Close()
			return nil, fmt.Errorf("existing torrent partial file has %d unsafe or unverifiable hard links", linkCount)
		}
	}
	if expectedSize > 0 && initialInfo.Size() > expectedSize {
		_ = file.Close()
		_ = root.Remove(partRelative)
		_ = root.Close()
		return nil, fmt.Errorf("torrent partial file is larger than expected")
	}
	if _, err := file.Seek(initialInfo.Size(), io.SeekStart); err != nil {
		_ = file.Close()
		_ = root.Close()
		return nil, fmt.Errorf("seek torrent partial file: %w", err)
	}
	return &ownedTorrentPart{
		root:             root,
		file:             file,
		entryPath:        entryPath,
		finalRelative:    relative,
		partRelative:     partRelative,
		partAbsolutePath: filepath.Join(entryPath, partRelative),
		initialInfo:      initialInfo,
		expectedSize:     expectedSize,
		recovered:        !created,
	}, nil
}

func (part *ownedTorrentPart) Size() (int64, error) {
	if part == nil || part.file == nil || part.closed {
		return 0, fmt.Errorf("torrent partial file is closed")
	}
	info, err := part.file.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (part *ownedTorrentPart) Reset() error {
	if part == nil || part.file == nil || part.closed {
		return fmt.Errorf("torrent partial file is closed")
	}
	if err := part.file.Truncate(0); err != nil {
		return err
	}
	_, err := part.file.Seek(0, io.SeekStart)
	return err
}

func (part *ownedTorrentPart) Commit() error {
	if part == nil || part.file == nil || part.root == nil || part.closed {
		return fmt.Errorf("torrent partial file is closed")
	}
	currentInfo, err := part.file.Stat()
	if err != nil {
		return fmt.Errorf("stat completed torrent partial file: %w", err)
	}
	if part.expectedSize > 0 && currentInfo.Size() != part.expectedSize {
		return fmt.Errorf("completed torrent file has size %d, expected %d", currentInfo.Size(), part.expectedSize)
	}
	if err := torrentSyncFile(part.file); err != nil {
		return fmt.Errorf("sync completed torrent partial file: %w", err)
	}
	openLinkCount, err := torrentOpenFileLinkCount(part.file)
	if err != nil {
		return fmt.Errorf("inspect completed torrent partial hard-link count: %w", err)
	}
	if err := part.file.Close(); err != nil {
		return fmt.Errorf("close completed torrent partial file: %w", err)
	}
	part.file = nil

	pathInfo, err := part.root.Lstat(part.partRelative)
	if err != nil {
		return fmt.Errorf("inspect completed torrent partial path: %w", err)
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 || !pathInfo.Mode().IsRegular() ||
		!os.SameFile(currentInfo, pathInfo) {
		return fmt.Errorf("torrent partial path changed before commit")
	}
	pathLinkCount, err := rootedTorrentRegularFileLinkCount(part.root, part.partRelative, pathInfo)
	if err != nil {
		return fmt.Errorf("reinspect completed torrent partial hard-link count: %w", err)
	}
	if pathLinkCount != openLinkCount {
		return fmt.Errorf("torrent partial hard-link count changed from %d to %d before commit", openLinkCount, pathLinkCount)
	}
	if !part.recovered {
		if pathLinkCount != 1 {
			return fmt.Errorf("new torrent partial file gained %d hard links before commit", pathLinkCount)
		}
	} else if pathLinkCount != 1 {
		finalInfo, finalErr := part.root.Lstat(part.finalRelative)
		if finalErr != nil || pathLinkCount != 2 ||
			finalInfo.Mode()&os.ModeSymlink != 0 || !finalInfo.Mode().IsRegular() ||
			!os.SameFile(pathInfo, finalInfo) {
			return fmt.Errorf("torrent partial file gained an unsafe hard link before commit")
		}
	}
	published := false
	var expectedFinalInfo os.FileInfo
	if err := part.root.Link(part.partRelative, part.finalRelative); err != nil {
		if !os.IsExist(err) {
			return fmt.Errorf("publish completed torrent file without overwrite: %w", err)
		}
		equal, compareErr := rootedTorrentFilesEqual(part.root, part.partRelative, part.finalRelative)
		if compareErr != nil {
			return compareErr
		}
		if !equal {
			return fmt.Errorf("refusing to replace existing torrent artifact %q", part.finalRelative)
		}
		if err := syncRootedTorrentRegularFile(part.root, part.finalRelative); err != nil {
			return fmt.Errorf("sync idempotent torrent artifact: %w", err)
		}
		expectedFinalInfo, err = rootedTorrentRegularFileInfo(part.root, part.finalRelative)
		if err != nil {
			return fmt.Errorf("reinspect idempotent torrent artifact: %w", err)
		}
		finalLinkCount, countErr := rootedTorrentRegularFileLinkCount(part.root, part.finalRelative, expectedFinalInfo)
		if countErr != nil {
			return fmt.Errorf("inspect idempotent torrent artifact link count: %w", countErr)
		}
		sameAsPartial := os.SameFile(pathInfo, expectedFinalInfo)
		if (!sameAsPartial && finalLinkCount != 1) || (sameAsPartial && finalLinkCount != 2) {
			return fmt.Errorf("idempotent torrent artifact has unsafe hard-link count %d", finalLinkCount)
		}
	} else {
		published = true
		finalInfo, err := part.root.Lstat(part.finalRelative)
		if err != nil || !os.SameFile(currentInfo, finalInfo) {
			if err != nil {
				return fmt.Errorf("verify published torrent file: %w", err)
			}
			return fmt.Errorf("published torrent file changed during commit")
		}
		expectedFinalInfo = finalInfo
	}
	parent, err := part.root.Open(filepath.Dir(part.finalRelative))
	if err != nil {
		return fmt.Errorf("open published torrent parent for sync: %w", err)
	}
	if published {
		if err := torrentSyncDirectory(parent); err != nil {
			_ = parent.Close()
			return fmt.Errorf("sync published torrent parent: %w", err)
		}
	}
	if err := part.root.Remove(part.partRelative); err != nil {
		_ = parent.Close()
		return fmt.Errorf("remove published torrent partial link: %w", err)
	}
	if err := torrentSyncDirectory(parent); err != nil {
		_ = parent.Close()
		return fmt.Errorf("sync torrent partial removal: %w", err)
	}
	finalInfo, err := part.root.Lstat(part.finalRelative)
	if err != nil || expectedFinalInfo == nil || !os.SameFile(expectedFinalInfo, finalInfo) {
		_ = parent.Close()
		if err != nil {
			return fmt.Errorf("reinspect committed torrent artifact: %w", err)
		}
		return fmt.Errorf("committed torrent artifact changed before completion")
	}
	finalLinkCount, countErr := rootedTorrentRegularFileLinkCount(part.root, part.finalRelative, finalInfo)
	if countErr != nil {
		_ = parent.Close()
		return fmt.Errorf("inspect committed torrent artifact link count: %w", countErr)
	}
	if finalLinkCount != 1 {
		_ = parent.Close()
		return fmt.Errorf("committed torrent artifact has unsafe hard-link count %d", finalLinkCount)
	}
	if err := parent.Close(); err != nil {
		return fmt.Errorf("close published torrent parent: %w", err)
	}
	if err := part.root.Close(); err != nil {
		return fmt.Errorf("close owned torrent root: %w", err)
	}
	part.root = nil
	part.closed = true
	return nil
}

func rootedTorrentFilesEqual(root *os.Root, left, right string) (bool, error) {
	leftBefore, err := rootedTorrentRegularFileInfo(root, left)
	if err != nil {
		return false, fmt.Errorf("inspect torrent publish source: %w", err)
	}
	rightBefore, err := rootedTorrentRegularFileInfo(root, right)
	if err != nil {
		return false, fmt.Errorf("inspect existing torrent artifact: %w", err)
	}
	if leftBefore.Size() != rightBefore.Size() {
		return false, nil
	}
	leftDigest, leftInfo, err := rootedTorrentFileDigest(root, left)
	if err != nil {
		return false, fmt.Errorf("inspect torrent publish source: %w", err)
	}
	rightDigest, rightInfo, err := rootedTorrentFileDigest(root, right)
	if err != nil {
		return false, fmt.Errorf("inspect existing torrent artifact: %w", err)
	}
	if leftInfo.Size() != rightInfo.Size() {
		return false, nil
	}
	return leftDigest == rightDigest, nil
}

func rootedTorrentRegularFileInfo(root *os.Root, relative string) (os.FileInfo, error) {
	before, err := root.Lstat(relative)
	if err != nil {
		return nil, err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return nil, fmt.Errorf("path %q is not a regular file", relative)
	}
	file, err := root.Open(relative)
	if err != nil {
		return nil, err
	}
	opened, statErr := file.Stat()
	closeErr := file.Close()
	if statErr != nil {
		return nil, statErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	after, err := root.Lstat(relative)
	if err != nil {
		return nil, err
	}
	if !os.SameFile(before, opened) || !os.SameFile(opened, after) {
		return nil, fmt.Errorf("path %q changed while inspecting", relative)
	}
	return opened, nil
}

func rootedTorrentRegularFileLinkCount(root *os.Root, relative string, expected os.FileInfo) (uint64, error) {
	before, err := root.Lstat(relative)
	if err != nil {
		return 0, err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return 0, fmt.Errorf("path %q is not a regular file", relative)
	}
	if expected != nil && !os.SameFile(expected, before) {
		return 0, fmt.Errorf("path %q changed before link-count inspection", relative)
	}
	file, err := root.Open(relative)
	if err != nil {
		return 0, err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		_ = file.Close()
		if err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("path %q changed while opening for link-count inspection", relative)
	}
	count, countErr := torrentOpenFileLinkCount(file)
	closeErr := file.Close()
	if countErr != nil {
		return 0, countErr
	}
	if closeErr != nil {
		return 0, closeErr
	}
	after, err := root.Lstat(relative)
	if err != nil || !os.SameFile(opened, after) {
		if err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("path %q changed during link-count inspection", relative)
	}
	return count, nil
}

func syncRootedTorrentRegularFile(root *os.Root, relative string) error {
	before, err := root.Lstat(relative)
	if err != nil {
		return err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return fmt.Errorf("path %q is not a regular file", relative)
	}
	// Windows requires a writable handle for FlushFileBuffers. Owned torrent
	// artifacts are created writable, so reopen read-write while retaining the
	// rooted, no-follow identity checks around the sync.
	file, err := root.OpenFile(relative, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		_ = file.Close()
		if err != nil {
			return err
		}
		return fmt.Errorf("path %q changed while opening for sync", relative)
	}
	if err := torrentSyncFile(file); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	after, err := root.Lstat(relative)
	if err != nil || !os.SameFile(opened, after) {
		if err != nil {
			return err
		}
		return fmt.Errorf("path %q changed while syncing", relative)
	}
	return nil
}

func rootedTorrentFileDigest(root *os.Root, relative string) ([sha256.Size]byte, os.FileInfo, error) {
	var zero [sha256.Size]byte
	before, err := root.Lstat(relative)
	if err != nil {
		return zero, nil, err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return zero, nil, fmt.Errorf("path %q is not a regular file", relative)
	}
	file, err := root.Open(relative)
	if err != nil {
		return zero, nil, err
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		_ = file.Close()
		if err != nil {
			return zero, nil, err
		}
		return zero, nil, fmt.Errorf("path %q changed while opening", relative)
	}
	hasher := sha256.New()
	if _, err := io.Copy(hasher, file); err != nil {
		_ = file.Close()
		return zero, nil, err
	}
	if err := file.Close(); err != nil {
		return zero, nil, err
	}
	after, err := root.Lstat(relative)
	if err != nil || !os.SameFile(opened, after) {
		if err != nil {
			return zero, nil, err
		}
		return zero, nil, fmt.Errorf("path %q changed while reading", relative)
	}
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	return digest, opened, nil
}

func (part *ownedTorrentPart) Close() error {
	if part == nil || part.closed {
		return nil
	}
	var errs []error
	if part.file != nil {
		errs = append(errs, part.file.Close())
		part.file = nil
	}
	if part.root != nil {
		errs = append(errs, part.root.Close())
		part.root = nil
	}
	part.closed = true
	return errorsJoinNonNil(errs...)
}

func errorsJoinNonNil(errs ...error) error {
	var messages []string
	for _, err := range errs {
		if err != nil {
			messages = append(messages, err.Error())
		}
	}
	if len(messages) == 0 {
		return nil
	}
	return fmt.Errorf("%s", strings.Join(messages, "; "))
}

func claimTorrentEntryDirectory(downloadRoot string, entry *storage.Entry, proof torrentLegacyProof) (entryPath string, newlyClaimed bool, err error) {
	torrentOwnershipMu.Lock()
	defer torrentOwnershipMu.Unlock()

	ownership, err := acquireTorrentOwnershipLock(downloadRoot)
	if err != nil {
		return "", false, err
	}
	entryPath, newlyClaimed, err = claimTorrentEntryDirectoryLocked(ownership.root, ownership.absolute, entry, proof)
	if closeErr := ownership.close(); closeErr != nil && err == nil {
		err = fmt.Errorf("close torrent ownership root: %w", closeErr)
	}
	return entryPath, newlyClaimed, err
}

func claimTorrentEntryDirectoryLocked(rooted *os.Root, absoluteRoot string, entry *storage.Entry, proof torrentLegacyProof) (string, bool, error) {
	entryPath, err := safeTorrentEntryDownloadPath(absoluteRoot, entry)
	if err != nil {
		return "", false, err
	}
	ownerID, err := canonicalTorrentOwnerID(entry.InfoHash)
	if err != nil {
		return "", false, err
	}
	relativeEntry, err := filepath.Rel(absoluteRoot, entryPath)
	if err != nil {
		return "", false, fmt.Errorf("make torrent release path relative: %w", err)
	}
	if err := recoverTorrentQuarantineForClaim(rooted, relativeEntry, ownerID); err != nil {
		return "", false, err
	}
	createdDirectory, err := ensureTorrentReleaseDirectory(rooted, relativeEntry)
	if err != nil {
		return "", false, err
	}
	entryRoot, err := rooted.OpenRoot(relativeEntry)
	if err != nil {
		if createdDirectory {
			_ = rooted.Remove(relativeEntry)
		}
		return "", false, fmt.Errorf("open torrent release directory: %w", err)
	}
	newlyClaimed, err := claimTorrentOwnerMarker(entryRoot, entry, ownerID, proof)
	closeErr := entryRoot.Close()
	if err != nil {
		if createdDirectory {
			_ = rooted.Remove(relativeEntry)
		}
		return "", false, err
	}
	if closeErr != nil {
		return "", newlyClaimed, fmt.Errorf("close torrent release directory: %w", closeErr)
	}
	return entryPath, newlyClaimed, nil
}

func ensureTorrentReleaseDirectory(rooted *os.Root, relativeEntry string) (bool, error) {
	parts := strings.Split(filepath.ToSlash(relativeEntry), "/")
	current := "."
	createdFinal := false
	for i, component := range parts {
		if err := rejectPortableTorrentSiblingAlias(rooted, current, component); err != nil {
			return false, err
		}
		current = filepath.Join(current, component)
		if err := rooted.Mkdir(current, 0o755); err != nil {
			if !os.IsExist(err) {
				return false, fmt.Errorf("create torrent release directory %q: %w", current, err)
			}
			info, statErr := rooted.Lstat(current)
			if statErr != nil {
				return false, fmt.Errorf("inspect torrent release directory %q: %w", current, statErr)
			}
			if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
				return false, fmt.Errorf("torrent release path component %q is not a regular directory", current)
			}
			continue
		}
		if i == len(parts)-1 {
			createdFinal = true
		}
	}
	return createdFinal, nil
}

func claimTorrentOwnerMarker(entryRoot *os.Root, entry *storage.Entry, ownerID string, proof torrentLegacyProof) (bool, error) {
	markerOwner, err := readTorrentOwnerMarker(entryRoot)
	switch {
	case err == nil:
		if markerOwner != ownerID {
			return false, fmt.Errorf("torrent release directory is owned by %q, not %q", markerOwner, ownerID)
		}
		return false, nil
	case !os.IsNotExist(err):
		return false, err
	}

	empty, err := torrentDirectoryIsEmpty(entryRoot)
	if err != nil {
		return false, err
	}
	if !empty {
		if err := verifyLegacyTorrentArtifacts(entryRoot, entry, proof); err != nil {
			return false, fmt.Errorf("manual review required for unowned torrent release directory: %w", err)
		}
	}
	if err := writeDurableTorrentMarker(entryRoot, ownerID); err != nil {
		if os.IsExist(err) {
			markerOwner, readErr := readTorrentOwnerMarker(entryRoot)
			if readErr == nil && markerOwner == ownerID {
				return false, nil
			}
		}
		return false, err
	}
	return true, nil
}

func torrentDirectoryIsEmpty(root *os.Root) (bool, error) {
	dir, err := root.Open(".")
	if err != nil {
		return false, fmt.Errorf("open torrent release directory for inspection: %w", err)
	}
	entries, readErr := dir.ReadDir(1)
	closeErr := dir.Close()
	if readErr != nil && readErr != io.EOF {
		return false, fmt.Errorf("inspect torrent release directory: %w", readErr)
	}
	if closeErr != nil {
		return false, fmt.Errorf("close torrent release directory: %w", closeErr)
	}
	return len(entries) == 0, nil
}

func writeDurableTorrentMarker(root *os.Root, ownerID string) error {
	file, err := root.OpenFile(torrentOwnerMarkerName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create torrent ownership marker: %w", err)
	}
	removeOnFailure := true
	defer func() {
		if removeOnFailure {
			_ = root.Remove(torrentOwnerMarkerName)
		}
	}()
	if err := writeAllTorrentBytes(file, []byte(ownerID+"\n")); err != nil {
		_ = file.Close()
		return fmt.Errorf("write torrent ownership marker: %w", err)
	}
	if err := torrentSyncFile(file); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync torrent ownership marker: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close torrent ownership marker: %w", err)
	}
	dir, err := root.Open(".")
	if err != nil {
		return fmt.Errorf("open torrent release directory for sync: %w", err)
	}
	if err := torrentSyncDirectory(dir); err != nil {
		_ = dir.Close()
		return fmt.Errorf("sync torrent release directory: %w", err)
	}
	if err := dir.Close(); err != nil {
		return fmt.Errorf("close torrent release directory after sync: %w", err)
	}
	removeOnFailure = false
	return nil
}

func readTorrentOwnerMarker(root *os.Root) (string, error) {
	owner := ""
	found := false
	for _, name := range torrentOwnerMarkerNames {
		markerOwner, err := readTorrentOwnerMarkerNamed(root, name)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", err
		}
		if found && markerOwner != owner {
			return "", fmt.Errorf("torrent ownership markers disagree")
		}
		owner = markerOwner
		found = true
	}
	if !found {
		return "", os.ErrNotExist
	}
	return owner, nil
}

func readTorrentOwnerMarkerNamed(root *os.Root, name string) (string, error) {
	before, err := root.Lstat(name)
	if err != nil {
		return "", err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return "", fmt.Errorf("torrent ownership marker is not a regular file")
	}
	if torrentAfterMarkerLstat != nil {
		if err := torrentAfterMarkerLstat(root); err != nil {
			return "", err
		}
	}
	file, err := root.Open(name)
	if err != nil {
		return "", err
	}
	opened, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return "", fmt.Errorf("stat opened torrent ownership marker: %w", err)
	}
	if !os.SameFile(before, opened) {
		_ = file.Close()
		return "", fmt.Errorf("torrent ownership marker changed while opening")
	}
	linkCount, err := torrentOpenFileLinkCount(file)
	if err != nil {
		_ = file.Close()
		return "", fmt.Errorf("inspect torrent ownership marker hard-link count: %w", err)
	}
	if linkCount != 1 {
		_ = file.Close()
		return "", fmt.Errorf("torrent ownership marker has %d hard links", linkCount)
	}
	contents, readErr := io.ReadAll(io.LimitReader(file, torrentOwnerMarkerMaxBytes+1))
	afterReadLinkCount, countErr := torrentOpenFileLinkCount(file)
	closeErr := file.Close()
	if readErr != nil {
		return "", fmt.Errorf("read torrent ownership marker: %w", readErr)
	}
	if countErr != nil {
		return "", fmt.Errorf("reinspect torrent ownership marker hard-link count: %w", countErr)
	}
	if afterReadLinkCount != 1 {
		return "", fmt.Errorf("torrent ownership marker has %d hard links after reading", afterReadLinkCount)
	}
	if closeErr != nil {
		return "", fmt.Errorf("close torrent ownership marker: %w", closeErr)
	}
	after, err := root.Lstat(name)
	if err != nil || !os.SameFile(opened, after) {
		if err != nil {
			return "", fmt.Errorf("reinspect torrent ownership marker: %w", err)
		}
		return "", fmt.Errorf("torrent ownership marker changed while reading")
	}
	pathLinkCount, err := rootedTorrentRegularFileLinkCount(root, name, after)
	if err != nil {
		return "", fmt.Errorf("reinspect torrent ownership marker path link count: %w", err)
	}
	if pathLinkCount != 1 {
		return "", fmt.Errorf("torrent ownership marker path has %d hard links after reading", pathLinkCount)
	}
	if len(contents) > torrentOwnerMarkerMaxBytes {
		return "", fmt.Errorf("torrent ownership marker is too large")
	}
	if len(contents) < 2 || contents[len(contents)-1] != '\n' {
		return "", fmt.Errorf("torrent ownership marker is malformed")
	}
	ownerID := string(contents[:len(contents)-1])
	if strings.ContainsRune(ownerID, '\n') {
		return "", fmt.Errorf("torrent ownership marker is malformed")
	}
	return canonicalTorrentOwnerID(ownerID)
}

func canonicalTorrentOwnerID(infoHash string) (string, error) {
	ownerID := strings.ToLower(strings.TrimSpace(infoHash))
	if ownerID == "" {
		return "", fmt.Errorf("torrent owner ID is empty")
	}
	if len(ownerID) > torrentOwnerMarkerMaxBytes-1 {
		return "", fmt.Errorf("torrent owner ID is too long")
	}
	if err := safepath.ValidateIdentifier(ownerID); err != nil {
		return "", fmt.Errorf("invalid torrent owner ID: %w", err)
	}
	return ownerID, nil
}

func verifyLegacyTorrentArtifacts(root *os.Root, entry *storage.Entry, proof torrentLegacyProof) error {
	layouts, err := torrentEntryFileLayouts(entry)
	if err != nil {
		return err
	}
	if len(layouts) == 0 {
		return fmt.Errorf("entry has no active files")
	}

	expectedFiles := make(map[string]torrentFileLayout, len(layouts))
	expectedDirs := make(map[string]struct{})
	for _, layout := range layouts {
		relative := layout.relative
		if effectiveTorrentAction(entry.Action) == config.DownloadActionStrm {
			relative += ".strm"
		}
		relative, err = normalizeTorrentRelativePath(relative)
		if err != nil {
			return err
		}
		expectedFiles[portableTorrentRelativeKey(relative)] = torrentFileLayout{
			file:     layout.file,
			relative: relative,
			key:      portableTorrentRelativeKey(relative),
		}
		for parent := filepath.Dir(relative); parent != "."; parent = filepath.Dir(parent) {
			expectedDirs[portableTorrentRelativeKey(parent)] = struct{}{}
		}
	}

	actualFiles, actualDirs, err := inspectTorrentArtifactTree(root)
	if err != nil {
		return err
	}
	if len(actualFiles) != len(expectedFiles) {
		return fmt.Errorf("artifact count is %d, expected %d", len(actualFiles), len(expectedFiles))
	}
	for key := range actualDirs {
		if _, ok := expectedDirs[key]; !ok {
			return fmt.Errorf("unexpected directory %q", actualDirs[key])
		}
	}
	for key, expected := range expectedFiles {
		actual, ok := actualFiles[key]
		if !ok {
			return fmt.Errorf("expected artifact %q is missing", expected.relative)
		}
		if actual.relative != expected.relative {
			return fmt.Errorf("artifact %q has a non-portable case alias %q", expected.relative, actual.relative)
		}
		if err := verifyLegacyTorrentArtifact(root, entry, expected, actual.info, proof); err != nil {
			return err
		}
	}
	return nil
}

type inspectedTorrentArtifact struct {
	relative string
	info     os.FileInfo
}

func inspectTorrentArtifactTree(root *os.Root) (map[string]inspectedTorrentArtifact, map[string]string, error) {
	files := make(map[string]inspectedTorrentArtifact)
	dirs := make(map[string]string)
	type pendingDir struct {
		relative string
		depth    int
	}
	pending := []pendingDir{{relative: ".", depth: 0}}
	seen := 0

	for len(pending) > 0 {
		current := pending[len(pending)-1]
		pending = pending[:len(pending)-1]
		if current.depth > torrentOwnershipMaxDepth {
			return nil, nil, fmt.Errorf("torrent artifact tree exceeds maximum depth")
		}
		dir, err := root.Open(current.relative)
		if err != nil {
			return nil, nil, fmt.Errorf("open torrent artifact directory %q: %w", current.relative, err)
		}
		for {
			entries, readErr := dir.ReadDir(torrentOwnershipReadBatch)
			for _, entry := range entries {
				seen++
				if seen > torrentOwnershipMaxEntries {
					_ = dir.Close()
					return nil, nil, fmt.Errorf("torrent artifact tree exceeds %d entries", torrentOwnershipMaxEntries)
				}
				if err := safepath.ValidateIdentifier(entry.Name()); err != nil {
					_ = dir.Close()
					return nil, nil, fmt.Errorf("invalid artifact name %q: %w", entry.Name(), err)
				}
				if isReservedTorrentPrivateName(entry.Name()) {
					_ = dir.Close()
					return nil, nil, fmt.Errorf("unexpected reserved artifact %q", entry.Name())
				}
				relative := filepath.Join(current.relative, entry.Name())
				relative = strings.TrimPrefix(relative, "."+string(filepath.Separator))
				info, statErr := root.Lstat(relative)
				if statErr != nil {
					_ = dir.Close()
					return nil, nil, fmt.Errorf("inspect torrent artifact %q: %w", relative, statErr)
				}
				key := portableTorrentRelativeKey(relative)
				if info.Mode()&os.ModeSymlink != 0 {
					if previous, exists := files[key]; exists {
						_ = dir.Close()
						return nil, nil, fmt.Errorf("artifact %q collides with %q", relative, previous.relative)
					}
					files[key] = inspectedTorrentArtifact{relative: relative, info: info}
					continue
				}
				if info.IsDir() {
					if previous, exists := dirs[key]; exists {
						_ = dir.Close()
						return nil, nil, fmt.Errorf("directory %q collides with %q", relative, previous)
					}
					dirs[key] = relative
					pending = append(pending, pendingDir{relative: relative, depth: current.depth + 1})
					continue
				}
				if !info.Mode().IsRegular() {
					_ = dir.Close()
					return nil, nil, fmt.Errorf("artifact %q is not a regular file or symlink", relative)
				}
				if previous, exists := files[key]; exists {
					_ = dir.Close()
					return nil, nil, fmt.Errorf("artifact %q collides with %q", relative, previous.relative)
				}
				files[key] = inspectedTorrentArtifact{relative: relative, info: info}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				_ = dir.Close()
				return nil, nil, fmt.Errorf("read torrent artifact directory %q: %w", current.relative, readErr)
			}
		}
		if err := dir.Close(); err != nil {
			return nil, nil, fmt.Errorf("close torrent artifact directory %q: %w", current.relative, err)
		}
	}
	return files, dirs, nil
}

func verifyLegacyTorrentArtifact(root *os.Root, entry *storage.Entry, expected torrentFileLayout, info os.FileInfo, proof torrentLegacyProof) error {
	switch effectiveTorrentAction(entry.Action) {
	case config.DownloadActionDownload:
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("legacy download artifact %q is not a regular file", expected.relative)
		}
		verified, err := rootedTorrentRegularFileInfo(root, expected.relative)
		if err != nil || !os.SameFile(info, verified) {
			if err != nil {
				return fmt.Errorf("verify legacy download artifact %q: %w", expected.relative, err)
			}
			return fmt.Errorf("legacy download artifact %q changed during verification", expected.relative)
		}
		expectedSize := torrentTransferSize(expected.file)
		if verified.Size() != expectedSize {
			return fmt.Errorf("legacy download artifact %q has size %d, expected %d", expected.relative, verified.Size(), expectedSize)
		}
		linkCount, err := rootedTorrentRegularFileLinkCount(root, expected.relative, verified)
		if err != nil {
			return fmt.Errorf("inspect legacy download artifact %q hard-link count: %w", expected.relative, err)
		}
		if linkCount != 1 {
			return fmt.Errorf("legacy download artifact %q has %d hard links", expected.relative, linkCount)
		}
		return nil
	case config.DownloadActionStrm:
		if proof.strmURL == "" {
			return fmt.Errorf("STRM base URL is unavailable")
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("legacy STRM artifact %q is not a regular file", expected.relative)
		}
		want, err := torrentSTRMURL(proof.strmURL, entry, expected.file)
		if err != nil {
			return err
		}
		file, err := root.Open(expected.relative)
		if err != nil {
			return fmt.Errorf("open legacy STRM artifact %q: %w", expected.relative, err)
		}
		opened, statErr := file.Stat()
		if statErr != nil || !os.SameFile(info, opened) {
			_ = file.Close()
			if statErr != nil {
				return fmt.Errorf("stat legacy STRM artifact %q: %w", expected.relative, statErr)
			}
			return fmt.Errorf("legacy STRM artifact %q changed while opening", expected.relative)
		}
		linkCount, countErr := torrentOpenFileLinkCount(file)
		if countErr != nil {
			_ = file.Close()
			return fmt.Errorf("inspect legacy STRM artifact %q hard-link count: %w", expected.relative, countErr)
		}
		if linkCount != 1 {
			_ = file.Close()
			return fmt.Errorf("legacy STRM artifact %q has %d hard links", expected.relative, linkCount)
		}
		contents, readErr := io.ReadAll(io.LimitReader(file, int64(len(want)+1)))
		afterReadLinkCount, countErr := torrentOpenFileLinkCount(file)
		closeErr := file.Close()
		if readErr != nil {
			return fmt.Errorf("read legacy STRM artifact %q: %w", expected.relative, readErr)
		}
		if countErr != nil {
			return fmt.Errorf("reinspect legacy STRM artifact %q hard-link count: %w", expected.relative, countErr)
		}
		if afterReadLinkCount != 1 {
			return fmt.Errorf("legacy STRM artifact %q gained %d hard links", expected.relative, afterReadLinkCount)
		}
		if closeErr != nil {
			return fmt.Errorf("close legacy STRM artifact %q: %w", expected.relative, closeErr)
		}
		after, statErr := root.Lstat(expected.relative)
		if statErr != nil || !os.SameFile(opened, after) {
			if statErr != nil {
				return fmt.Errorf("reinspect legacy STRM artifact %q: %w", expected.relative, statErr)
			}
			return fmt.Errorf("legacy STRM artifact %q changed while reading", expected.relative)
		}
		pathLinkCount, countErr := rootedTorrentRegularFileLinkCount(root, expected.relative, after)
		if countErr != nil {
			return fmt.Errorf("reinspect legacy STRM artifact %q path link count: %w", expected.relative, countErr)
		}
		if pathLinkCount != 1 {
			return fmt.Errorf("legacy STRM artifact %q path has %d hard links", expected.relative, pathLinkCount)
		}
		if string(contents) != want {
			return fmt.Errorf("legacy STRM artifact %q does not match this entry", expected.relative)
		}
		return nil
	default:
		if proof.mountPath == "" {
			return fmt.Errorf("torrent mount path is unavailable")
		}
		if info.Mode()&os.ModeSymlink == 0 {
			return fmt.Errorf("legacy symlink artifact %q is not a symlink", expected.relative)
		}
		before, err := root.Lstat(expected.relative)
		if err != nil || !os.SameFile(info, before) {
			if err != nil {
				return fmt.Errorf("reinspect legacy symlink artifact %q: %w", expected.relative, err)
			}
			return fmt.Errorf("legacy symlink artifact %q changed before target verification", expected.relative)
		}
		target, err := root.Readlink(expected.relative)
		if err != nil {
			return fmt.Errorf("read legacy symlink artifact %q: %w", expected.relative, err)
		}
		after, err := root.Lstat(expected.relative)
		if err != nil || !os.SameFile(before, after) {
			if err != nil {
				return fmt.Errorf("reinspect legacy symlink artifact %q: %w", expected.relative, err)
			}
			return fmt.Errorf("legacy symlink artifact %q changed while reading its target", expected.relative)
		}
		want := filepath.Join(proof.mountPath, expected.relative)
		if !sameFilesystemPath(target, want) {
			return fmt.Errorf("legacy symlink artifact %q targets %q, expected %q", expected.relative, target, want)
		}
		return nil
	}
}

func effectiveTorrentAction(action config.DownloadAction) config.DownloadAction {
	switch action {
	case config.DownloadActionDownload, config.DownloadActionStrm, config.DownloadActionSymlink:
		return action
	default:
		return config.DownloadActionSymlink
	}
}

func torrentSTRMURL(base string, entry *storage.Entry, file *storage.File) (string, error) {
	if entry == nil || file == nil {
		return "", fmt.Errorf("entry and file are required")
	}
	return url.JoinPath(
		base,
		"webdav",
		"stream",
		EntryAllFolder,
		url.PathEscape(entry.GetFolder()),
		url.PathEscape(file.Name),
	)
}

func removeOwnedTorrentEntryDirectory(downloadRoot string, entry *storage.Entry) (err error) {
	torrentOwnershipMu.Lock()
	defer torrentOwnershipMu.Unlock()

	ownership, err := acquireTorrentOwnershipLock(downloadRoot)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := ownership.close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close torrent ownership root: %w", closeErr)
		}
	}()
	absoluteRoot := ownership.absolute
	rooted := ownership.root

	entryPath, err := safeTorrentEntryDownloadPath(absoluteRoot, entry)
	if err != nil {
		return err
	}
	ownerID, err := canonicalTorrentOwnerID(entry.InfoHash)
	if err != nil {
		return err
	}
	relativeEntry, err := filepath.Rel(absoluteRoot, entryPath)
	if err != nil {
		return fmt.Errorf("make torrent release path relative: %w", err)
	}
	visibleInfo, visibleErr := rooted.Lstat(relativeEntry)
	visibleExists := visibleErr == nil
	if visibleErr != nil && !os.IsNotExist(visibleErr) {
		return fmt.Errorf("inspect torrent release directory: %w", visibleErr)
	}
	quarantines, err := findTorrentQuarantines(rooted, relativeEntry, ownerID)
	if err != nil {
		return err
	}
	if len(quarantines) > 0 {
		if visibleExists {
			return fmt.Errorf("manual review required: both visible and quarantined torrent release directories exist")
		}
		return removeTorrentQuarantines(rooted, quarantines, ownerID, nil)
	}
	if !visibleExists {
		return nil
	}
	if visibleInfo.Mode()&os.ModeSymlink != 0 || !visibleInfo.IsDir() {
		return fmt.Errorf("torrent release path %q is not a regular directory", entryPath)
	}
	entryRoot, err := rooted.OpenRoot(relativeEntry)
	if err != nil {
		return fmt.Errorf("open torrent release before quarantine: %w", err)
	}
	pinnedInfo, statErr := entryRoot.Stat(".")
	markerOwner, markerErr := readTorrentOwnerMarker(entryRoot)
	closeErr := entryRoot.Close()
	if statErr != nil {
		return fmt.Errorf("stat opened torrent release directory: %w", statErr)
	}
	if !os.SameFile(visibleInfo, pinnedInfo) {
		return fmt.Errorf("torrent release directory changed during ownership verification")
	}
	if markerErr != nil {
		return fmt.Errorf("refusing to delete unowned torrent release directory: %w", markerErr)
	}
	if markerOwner != ownerID {
		return fmt.Errorf("torrent release directory is owned by %q, not %q", markerOwner, ownerID)
	}
	if closeErr != nil {
		return fmt.Errorf("close torrent release before quarantine: %w", closeErr)
	}

	quarantineRelative, err := newTorrentQuarantinePath(rooted, relativeEntry, ownerID)
	if err != nil {
		return err
	}
	if err := rooted.Rename(relativeEntry, quarantineRelative); err != nil {
		return fmt.Errorf("quarantine torrent release directory: %w", err)
	}
	if err := syncTorrentRootDirectory(rooted, filepath.Dir(relativeEntry)); err != nil {
		return fmt.Errorf("sync torrent release quarantine: %w", err)
	}
	if torrentAfterQuarantine != nil {
		if err := torrentAfterQuarantine(entryPath); err != nil {
			return fmt.Errorf("torrent cleanup interrupted after quarantine: %w", err)
		}
	}
	return removeTorrentQuarantines(rooted, []string{quarantineRelative}, ownerID, pinnedInfo)
}

func removeTorrentQuarantines(rooted *os.Root, quarantines []string, ownerID string, expectedInfo os.FileInfo) error {
	if len(quarantines) != 1 {
		return fmt.Errorf("manual review required: expected one torrent quarantine, found %d", len(quarantines))
	}
	quarantine := quarantines[0]
	info, err := rooted.Lstat(quarantine)
	if err != nil {
		return fmt.Errorf("inspect quarantined torrent release: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("quarantined torrent release %q is not a regular directory", quarantine)
	}
	quarantineRoot, err := rooted.OpenRoot(quarantine)
	if err != nil {
		return fmt.Errorf("open quarantined torrent release: %w", err)
	}
	pinned, statErr := quarantineRoot.Stat(".")
	markerOwner, markerErr := readTorrentOwnerMarker(quarantineRoot)
	if statErr != nil {
		_ = quarantineRoot.Close()
		return fmt.Errorf("stat quarantined torrent release: %w", statErr)
	}
	if !os.SameFile(info, pinned) || (expectedInfo != nil && !os.SameFile(expectedInfo, pinned)) {
		_ = quarantineRoot.Close()
		return fmt.Errorf("quarantined torrent release changed during verification")
	}
	if markerErr != nil {
		_ = quarantineRoot.Close()
		return fmt.Errorf("verify quarantined torrent ownership: %w", markerErr)
	}
	if markerOwner != ownerID {
		_ = quarantineRoot.Close()
		return fmt.Errorf("quarantined torrent release is owned by %q, not %q", markerOwner, ownerID)
	}
	if torrentAfterQuarantineVerified != nil {
		if err := torrentAfterQuarantineVerified(quarantine); err != nil {
			_ = quarantineRoot.Close()
			return fmt.Errorf("torrent cleanup interrupted after quarantine verification: %w", err)
		}
	}
	if err := safepath.RemovePinnedTreeContents(quarantineRoot, safepath.PinnedTreeRemovalOptions{
		MaxEntries:       torrentOwnershipMaxEntries,
		MaxDepth:         torrentOwnershipMaxDepth,
		ReadBatch:        torrentOwnershipReadBatch,
		PreserveTopLevel: torrentOwnerMarkerNames,
	}); err != nil {
		_ = quarantineRoot.Close()
		return fmt.Errorf("empty pinned torrent quarantine: %w", err)
	}
	afterContents, err := rooted.Lstat(quarantine)
	if err != nil || !os.SameFile(pinned, afterContents) {
		_ = quarantineRoot.Close()
		if err != nil {
			return fmt.Errorf("reinspect emptied torrent quarantine: %w", err)
		}
		return fmt.Errorf("torrent quarantine changed before marker removal")
	}
	markerOwner, err = readTorrentOwnerMarker(quarantineRoot)
	if err != nil || markerOwner != ownerID {
		_ = quarantineRoot.Close()
		if err != nil {
			return fmt.Errorf("reverify emptied torrent quarantine ownership: %w", err)
		}
		return fmt.Errorf("emptied torrent quarantine is owned by %q, not %q", markerOwner, ownerID)
	}
	if err := removeTorrentOwnerMarkers(quarantineRoot); err != nil {
		_ = quarantineRoot.Close()
		return fmt.Errorf("remove torrent quarantine ownership marker: %w", err)
	}
	if err := syncTorrentRootDirectory(quarantineRoot, "."); err != nil {
		_ = quarantineRoot.Close()
		return fmt.Errorf("sync emptied torrent quarantine: %w", err)
	}
	if err := quarantineRoot.Close(); err != nil {
		return fmt.Errorf("close emptied torrent quarantine: %w", err)
	}
	afterClose, err := rooted.Lstat(quarantine)
	if err != nil || !os.SameFile(pinned, afterClose) {
		if err != nil {
			return fmt.Errorf("reinspect closed torrent quarantine: %w", err)
		}
		return fmt.Errorf("torrent quarantine changed before final unlink")
	}
	if err := rooted.Remove(quarantine); err != nil {
		return fmt.Errorf("remove emptied torrent quarantine: %w", err)
	}
	if err := syncTorrentRootDirectory(rooted, filepath.Dir(quarantine)); err != nil {
		return fmt.Errorf("sync removed torrent quarantine: %w", err)
	}
	return nil
}

func recoverTorrentQuarantineForClaim(rooted *os.Root, relativeEntry, ownerID string) error {
	quarantines, err := findTorrentQuarantines(rooted, relativeEntry, ownerID)
	if err != nil {
		return err
	}
	if len(quarantines) == 0 {
		return nil
	}
	if len(quarantines) > 1 {
		return fmt.Errorf("manual review required: multiple torrent cleanup quarantines exist")
	}
	if _, err := rooted.Lstat(relativeEntry); err == nil {
		return fmt.Errorf("manual review required: both visible and quarantined torrent releases exist")
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect torrent release during quarantine recovery: %w", err)
	}
	return fmt.Errorf("matching quarantined torrent release requires cleanup before retry")
}

func findTorrentQuarantines(rooted *os.Root, relativeEntry, ownerID string) ([]string, error) {
	parent := filepath.Dir(relativeEntry)
	dir, err := rooted.Open(parent)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("open torrent release parent for quarantine scan: %w", err)
	}
	defer dir.Close()
	prefixes := torrentQuarantinePrefixesForEntry(ownerID, relativeEntry)
	quarantines := make([]string, 0, 1)
	seen := 0
	for {
		entries, readErr := dir.ReadDir(torrentOwnershipReadBatch)
		for _, entry := range entries {
			seen++
			if seen > torrentOwnershipMaxEntries {
				return nil, fmt.Errorf("torrent release parent exceeds bounded quarantine scan")
			}
			lowerName := strings.ToLower(entry.Name())
			for _, prefix := range prefixes {
				if strings.HasPrefix(lowerName, prefix) {
					quarantines = append(quarantines, filepath.Join(parent, entry.Name()))
					break
				}
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("scan torrent release quarantines: %w", readErr)
		}
	}
	sort.Strings(quarantines)
	return quarantines, nil
}

func newTorrentQuarantinePath(rooted *os.Root, relativeEntry, ownerID string) (string, error) {
	parent := filepath.Dir(relativeEntry)
	prefix := torrentQuarantinePrefixForEntry(ownerID, relativeEntry)
	for range 100 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", fmt.Errorf("generate torrent quarantine name: %w", err)
		}
		relative := filepath.Join(parent, prefix+hex.EncodeToString(random[:]))
		if _, err := rooted.Lstat(relative); os.IsNotExist(err) {
			return relative, nil
		} else if err != nil {
			return "", fmt.Errorf("inspect torrent quarantine candidate: %w", err)
		}
	}
	return "", fmt.Errorf("generate torrent quarantine name: exhausted unique names")
}

func torrentQuarantinePrefixForEntry(ownerID, relativeEntry string) string {
	digest := sha256.Sum256([]byte(ownerID + "\x00" + filepath.ToSlash(relativeEntry)))
	return torrentQuarantinePrefix + hex.EncodeToString(digest[:16]) + "-"
}

func torrentQuarantinePrefixesForEntry(ownerID, relativeEntry string) []string {
	digest := sha256.Sum256([]byte(ownerID + "\x00" + filepath.ToSlash(relativeEntry)))
	suffix := hex.EncodeToString(digest[:16]) + "-"
	return []string{torrentQuarantinePrefix + suffix, legacyTorrentQuarantinePrefix + suffix}
}

func removeTorrentOwnerMarkers(rooted *os.Root) error {
	var removeErr error
	for _, name := range torrentOwnerMarkerNames {
		if err := rooted.Remove(name); err != nil && !os.IsNotExist(err) {
			removeErr = errors.Join(removeErr, err)
		}
	}
	return removeErr
}

func rejectPortableTorrentSiblingAlias(rooted *os.Root, parent, wanted string) error {
	dir, err := rooted.Open(parent)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("open torrent path parent %q: %w", parent, err)
	}
	defer dir.Close()
	seen := 0
	for {
		entries, readErr := dir.ReadDir(torrentOwnershipReadBatch)
		for _, entry := range entries {
			seen++
			if seen > torrentOwnershipMaxEntries {
				return fmt.Errorf("torrent path parent exceeds bounded alias scan")
			}
			if entry.Name() != wanted && strings.EqualFold(strings.TrimRight(entry.Name(), " ."), strings.TrimRight(wanted, " .")) {
				return fmt.Errorf("torrent path component %q aliases existing name %q", wanted, entry.Name())
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return fmt.Errorf("scan torrent path parent %q: %w", parent, readErr)
		}
	}
}

func acquireTorrentOwnershipLock(downloadRoot string) (*ownershipRoot, error) {
	ownership, _, err := acquireCompatibleOwnershipRoot(
		downloadRoot,
		[]string{legacyTorrentOwnershipLockName, torrentOwnershipLockName},
		true,
		torrentOwnershipLockTimeout,
		25*time.Millisecond,
	)
	if err != nil {
		return nil, fmt.Errorf("acquire torrent ownership root: %w", err)
	}
	return ownership, nil
}

func syncTorrentRootDirectory(root *os.Root, relative string) error {
	dir, err := root.Open(relative)
	if err != nil {
		return err
	}
	if err := torrentSyncDirectory(dir); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}
