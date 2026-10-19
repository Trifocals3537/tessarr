package manager

import (
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

const (
	usenetLegacyAdoptionCheckpointName = ".tessarr-nzb-legacy-adoption-v1.done"
	legacyUsenetAdoptionCheckpointName = ".decypharr-nzb-legacy-adoption-v1.done"
	// Marker payloads are durable schema identifiers; the migration renames the
	// file without invalidating already completed adoption work.
	usenetLegacyAdoptionCheckpointData = "decypharr NZB legacy ownership adoption v1\n"
	usenetLegacyAdoptionMaxFiles       = 100_000
)

var usenetLegacyAdoptionCheckpointNames = []string{
	usenetLegacyAdoptionCheckpointName,
	legacyUsenetAdoptionCheckpointName,
}

var ErrLegacyUsenetManualReview = errors.New("legacy NZB adoption requires manual review")

type legacyUsenetHeaderLookup func(string) (*storage.NZB, error)
type legacyUsenetManualReviewWriter func(*storage.Entry) error

type legacyUsenetAdopter struct {
	downloadRoot string
	mountRoot    string
	strmURL      string
	folderNaming config.WebDavFolderNaming
	header       legacyUsenetHeaderLookup
	markManual   legacyUsenetManualReviewWriter
}

func (m *Manager) adoptLegacyUsenetOwnership() error {
	if m.queue == nil || m.queue.storage == nil {
		return fmt.Errorf("queue storage is unavailable")
	}
	entries, err := m.queue.storage.FilterQueued(func(entry *storage.Entry) bool {
		return entry != nil && entry.IsNZB()
	})
	if err != nil {
		return fmt.Errorf("snapshot queued NZBs for legacy adoption: %w", err)
	}
	for _, entry := range entries {
		if err := m.queue.lifecycle.bindEntry(entry); err != nil {
			return fmt.Errorf("bind queued NZB %q for legacy adoption: %w", entry.InfoHash, err)
		}
	}

	var strmURL string
	if m.downloader != nil {
		strmURL = m.downloader.strmURL
	}
	adopter := &legacyUsenetAdopter{
		downloadRoot: m.config.DownloadFolder,
		mountRoot:    m.config.Mount.MountPath,
		strmURL:      strmURL,
		folderNaming: m.config.FolderNaming,
		header: func(id string) (*storage.NZB, error) {
			if m.usenet == nil {
				return nil, fmt.Errorf("usenet metadata service is unavailable")
			}
			return m.usenet.GetNZBHeader(id)
		},
		markManual: m.queue.Update,
	}
	return adopter.run(entries)
}

// run adopts only the queue snapshot supplied by the caller. The caller takes
// that snapshot before this function acquires the ownership lock, so new intake
// can never become eligible midway through a one-time migration.
func (a *legacyUsenetAdopter) run(entries []*storage.Entry) (runErr error) {
	usenetOwnershipMu.Lock()
	defer usenetOwnershipMu.Unlock()

	ownership, _, err := acquireUsenetOwnershipLock(a.downloadRoot, true)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := ownership.close(); closeErr != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close NZB ownership root after legacy adoption: %w", closeErr))
		}
	}()
	rooted := ownership.root
	absoluteRoot := ownership.absolute

	if err := rejectPortableSiblingAlias(rooted, ".", usenetLegacyAdoptionCheckpointName); err != nil {
		return fmt.Errorf("inspect legacy adoption checkpoint aliases: %w", err)
	}
	checkpointExists, err := legacyUsenetCheckpointExists(rooted)
	if err != nil {
		return err
	}
	if checkpointExists {
		return nil
	}

	entries = append([]*storage.Entry(nil), entries...)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i] == nil {
			return false
		}
		if entries[j] == nil {
			return true
		}
		return strings.ToLower(entries[i].InfoHash) < strings.ToLower(entries[j].InfoHash)
	})

	var unresolved []error
	for _, entry := range entries {
		if entry == nil || !entry.IsNZB() {
			continue
		}
		candidate, adoptionErr := a.adoptEntry(rooted, absoluteRoot, entry)
		if !candidate {
			continue
		}
		if adoptionErr == nil {
			continue
		}
		if err := a.persistManualReview(entry, adoptionErr); err != nil {
			unresolved = append(unresolved, err)
		}
	}
	if len(unresolved) > 0 {
		return errors.Join(unresolved...)
	}

	if err := writeDurableExclusiveUsenetFile(
		rooted,
		usenetLegacyAdoptionCheckpointName,
		usenetLegacyAdoptionCheckpointData,
		0o600,
	); err != nil {
		if !os.IsExist(err) {
			return fmt.Errorf("write legacy adoption checkpoint: %w", err)
		}
	}
	exists, err := legacyUsenetCheckpointExists(rooted)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("legacy adoption checkpoint disappeared after creation")
	}
	return nil
}

// adoptEntry returns candidate=false only when the exact configured path is
// genuinely absent and has no portable alias. Such staged/pending jobs have no
// legacy directory to adopt and must remain untouched.
func (a *legacyUsenetAdopter) adoptEntry(rooted *os.Root, absoluteRoot string, entry *storage.Entry) (candidate bool, err error) {
	entryPath, ownerID, err := validateLegacyUsenetQueueShape(absoluteRoot, entry)
	if err != nil {
		return true, err
	}
	relativeEntry, err := filepath.Rel(absoluteRoot, entryPath)
	if err != nil {
		return true, fmt.Errorf("make legacy NZB path relative: %w", err)
	}

	parentRelative := filepath.Dir(relativeEntry)
	releaseParent := rooted
	releaseName := filepath.Base(relativeEntry)
	if entry.Category != "" {
		if aliasErr := rejectPortableSiblingAlias(rooted, ".", entry.Category); aliasErr != nil {
			return true, aliasErr
		}
		parentInfo, statErr := rooted.Lstat(parentRelative)
		if statErr != nil {
			if os.IsNotExist(statErr) {
				return false, nil
			}
			return true, fmt.Errorf("inspect legacy NZB category directory: %w", statErr)
		}
		if parentInfo.Mode()&os.ModeSymlink != 0 || !parentInfo.IsDir() {
			return true, fmt.Errorf("legacy NZB category path %q is not a regular directory", filepath.Join(absoluteRoot, parentRelative))
		}
		categoryRoot, openErr := openStableLegacyDirectory(rooted, parentRelative, parentInfo)
		if openErr != nil {
			return true, fmt.Errorf("open legacy NZB category directory: %w", openErr)
		}
		defer categoryRoot.Close()
		releaseParent = categoryRoot
	}
	if aliasErr := rejectPortableSiblingAlias(releaseParent, ".", releaseName); aliasErr != nil {
		return true, aliasErr
	}

	info, err := releaseParent.Lstat(releaseName)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return true, fmt.Errorf("inspect legacy NZB release directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return true, fmt.Errorf("legacy NZB release path %q is not a regular directory", entryPath)
	}

	entryRoot, err := openStableLegacyDirectory(releaseParent, releaseName, info)
	if err != nil {
		return true, fmt.Errorf("open legacy NZB release directory: %w", err)
	}
	defer entryRoot.Close()

	markerOwner, markerErr := readUsenetOwnerMarker(entryRoot)
	switch {
	case markerErr == nil:
		if markerOwner != ownerID {
			return true, fmt.Errorf("legacy NZB release is owned by %q, not %q", markerOwner, ownerID)
		}
		for _, markerName := range usenetOwnerMarkerNames {
			_, statErr := entryRoot.Lstat(markerName)
			if os.IsNotExist(statErr) {
				continue
			}
			if statErr != nil {
				return true, fmt.Errorf("inspect existing NZB owner marker: %w", statErr)
			}
			if err := requireSingleLegacyNamedRegularLink(entryRoot, markerName); err != nil {
				return true, fmt.Errorf("verify existing NZB owner marker: %w", err)
			}
		}
	case !os.IsNotExist(markerErr):
		return true, markerErr
	}

	if a.header == nil {
		return true, fmt.Errorf("stored NZB metadata is unavailable")
	}
	header, err := a.header(entry.InfoHash)
	if err != nil {
		return true, fmt.Errorf("load stored NZB metadata: %w", err)
	}
	expectedFiles, err := validateLegacyUsenetProvenance(absoluteRoot, entry, header)
	if err != nil {
		return true, err
	}
	markerAlreadyExists := markerErr == nil
	if err := a.validateArtifacts(entryRoot, entry, expectedFiles, markerAlreadyExists); err != nil {
		return true, err
	}
	if markerAlreadyExists {
		// A same-owner marker without the root checkpoint is a possible crash
		// remnant, but it is not trusted blindly: current queue/header identity
		// and action artifacts were revalidated above.
		return true, nil
	}

	createdMarker := false
	if err := writeDurableExclusiveUsenetFile(entryRoot, usenetOwnerMarkerName, ownerID+"\n", 0o600); err != nil {
		if !os.IsExist(err) {
			return true, fmt.Errorf("write adopted NZB owner marker: %w", err)
		}
		markerOwner, readErr := readUsenetOwnerMarker(entryRoot)
		if readErr != nil {
			return true, fmt.Errorf("verify concurrently created NZB owner marker: %w", readErr)
		}
		if markerOwner != ownerID {
			return true, fmt.Errorf("concurrently created NZB owner marker belongs to %q", markerOwner)
		}
	} else {
		createdMarker = true
	}

	removeCreatedMarker := func() {
		if createdMarker {
			_ = entryRoot.Remove(usenetOwnerMarkerName)
			_ = usenetSyncDirectory(entryRoot)
		}
	}
	markerOwner, err = readUsenetOwnerMarker(entryRoot)
	if err != nil || markerOwner != ownerID {
		removeCreatedMarker()
		if err != nil {
			return true, fmt.Errorf("reread adopted NZB owner marker: %w", err)
		}
		return true, fmt.Errorf("adopted NZB owner marker changed to %q", markerOwner)
	}
	if err := requireSingleLegacyNamedRegularLink(entryRoot, usenetOwnerMarkerName); err != nil {
		removeCreatedMarker()
		return true, fmt.Errorf("verify adopted NZB owner marker: %w", err)
	}
	if err := a.validateArtifacts(entryRoot, entry, expectedFiles, true); err != nil {
		var removeErr, syncErr error
		if createdMarker {
			removeErr = entryRoot.Remove(usenetOwnerMarkerName)
			syncErr = usenetSyncDirectory(entryRoot)
		}
		return true, errors.Join(
			fmt.Errorf("legacy NZB directory changed while ownership was adopted: %w", err),
			removeErr,
			syncErr,
		)
	}
	return true, nil
}

func validateLegacyUsenetQueueShape(downloadRoot string, entry *storage.Entry) (entryPath, ownerID string, err error) {
	if entry == nil || !entry.IsNZB() {
		return "", "", fmt.Errorf("queue entry is not an NZB")
	}
	entryPath, err = safeUsenetEntryDownloadPath(downloadRoot, entry)
	if err != nil {
		return "", "", err
	}
	if !sameFilesystemPath(entry.ContentPath, entryPath) {
		return "", "", fmt.Errorf("NZB content path %q does not match configured path %q", entry.ContentPath, entryPath)
	}
	ownerID, err = canonicalUsenetOwnerID(entry.InfoHash)
	if err != nil {
		return "", "", err
	}
	if entry.Size < 0 || entry.Bytes < 0 {
		return "", "", fmt.Errorf("NZB queue entry has a negative size")
	}
	if entry.ActiveProvider != "usenet" {
		return "", "", fmt.Errorf("active provider is not usenet")
	}
	provider := entry.Providers["usenet"]
	if provider == nil || provider.Provider != "usenet" {
		return "", "", fmt.Errorf("usenet provider provenance is missing")
	}
	providerID, err := canonicalUsenetOwnerID(provider.ID)
	if err != nil || providerID != ownerID {
		return "", "", fmt.Errorf("usenet provider ID does not match queue owner")
	}
	switch entry.Action {
	case "", config.DownloadActionSymlink, config.DownloadActionDownload, config.DownloadActionStrm, config.DownloadActionNone:
	default:
		return "", "", fmt.Errorf("unsupported legacy NZB action %q", entry.Action)
	}
	if len(entry.Files) > usenetLegacyAdoptionMaxFiles || len(provider.Files) > usenetLegacyAdoptionMaxFiles {
		return "", "", fmt.Errorf("NZB queue entry exceeds the legacy adoption file limit")
	}
	portable := make(map[string]string)
	for key, file := range entry.Files {
		if file == nil || file.Deleted {
			continue
		}
		if key != file.Name {
			return "", "", fmt.Errorf("queue file key %q does not match file name %q", key, file.Name)
		}
		if err := validateLegacyLogicalName(file.Name, portable); err != nil {
			return "", "", err
		}
		fileID, err := canonicalUsenetOwnerID(file.InfoHash)
		if err != nil || fileID != ownerID {
			return "", "", fmt.Errorf("queue logical file %q has the wrong owner", file.Name)
		}
		if file.Size < 0 {
			return "", "", fmt.Errorf("queue logical file %q has a negative size", file.Name)
		}
	}
	return entryPath, ownerID, nil
}

func (a *legacyUsenetAdopter) persistManualReview(entry *storage.Entry, reason error) error {
	manualErr := fmt.Errorf("%w for %q: %v", ErrLegacyUsenetManualReview, entry.InfoHash, reason)
	if entry.State != storage.EntryStateError || entry.LastError != manualErr.Error() {
		entry.MarkAsError(manualErr)
	}
	if a.markManual == nil {
		return fmt.Errorf("persist manual-review state for %q: writer is unavailable", entry.InfoHash)
	}
	if err := a.markManual(entry); err != nil {
		return fmt.Errorf("persist manual-review state for %q: %w", entry.InfoHash, err)
	}
	return nil
}

func legacyUsenetCheckpointExists(rooted *os.Root) (bool, error) {
	found := false
	for _, name := range usenetLegacyAdoptionCheckpointNames {
		exists, err := legacyUsenetCheckpointNamedExists(rooted, name)
		if err != nil {
			return false, err
		}
		found = found || exists
	}
	return found, nil
}

func legacyUsenetCheckpointNamedExists(rooted *os.Root, name string) (bool, error) {
	info, err := rooted.Lstat(name)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("inspect legacy adoption checkpoint: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return false, fmt.Errorf("legacy adoption checkpoint is not a regular file")
	}
	file, _, err := openStableLegacyRegularFile(rooted, name)
	if err != nil {
		return false, fmt.Errorf("open legacy adoption checkpoint: %w", err)
	}
	defer file.Close()
	if err := requireSingleLegacyLink(file); err != nil {
		return false, fmt.Errorf("verify legacy adoption checkpoint: %w", err)
	}
	contents, err := io.ReadAll(io.LimitReader(file, int64(len(usenetLegacyAdoptionCheckpointData)+1)))
	if err != nil {
		return false, fmt.Errorf("read legacy adoption checkpoint: %w", err)
	}
	if string(contents) != usenetLegacyAdoptionCheckpointData {
		return false, fmt.Errorf("legacy adoption checkpoint contents are invalid")
	}
	return true, nil
}

func validateLegacyUsenetProvenance(downloadRoot string, entry *storage.Entry, header *storage.NZB) (map[string]int64, error) {
	if entry == nil || header == nil {
		return nil, fmt.Errorf("queue entry or NZB metadata is missing")
	}
	entryID, err := canonicalUsenetOwnerID(entry.InfoHash)
	if err != nil {
		return nil, err
	}
	headerID, err := canonicalUsenetOwnerID(header.ID)
	if err != nil || headerID != entryID {
		return nil, fmt.Errorf("stored NZB ID does not match queue owner")
	}
	if entry.Name != header.Name || entry.Category != header.Category {
		return nil, fmt.Errorf("stored NZB name/category does not match queue entry")
	}
	if entry.OriginalFilename != "" && entry.OriginalFilename != header.Name {
		return nil, fmt.Errorf("stored NZB original filename does not match queue entry")
	}
	if header.TotalSize < 0 || entry.Size != header.TotalSize || entry.Bytes != header.TotalSize {
		return nil, fmt.Errorf("stored NZB size does not match queue entry")
	}
	expectedPath, err := safeUsenetEntryDownloadPath(downloadRoot, entry)
	if err != nil {
		return nil, err
	}
	if !sameFilesystemPath(entry.ContentPath, expectedPath) {
		return nil, fmt.Errorf("NZB content path %q does not match configured path %q", entry.ContentPath, expectedPath)
	}
	if entry.ActiveProvider != "usenet" {
		return nil, fmt.Errorf("active provider is not usenet")
	}
	provider := entry.Providers["usenet"]
	if provider == nil || provider.Provider != "usenet" {
		return nil, fmt.Errorf("usenet provider provenance is missing")
	}
	providerID, err := canonicalUsenetOwnerID(provider.ID)
	if err != nil || providerID != entryID {
		return nil, fmt.Errorf("usenet provider ID does not match queue owner")
	}

	expected := make(map[string]int64)
	portable := make(map[string]string)
	for _, file := range header.Files {
		if file.IsDeleted {
			continue
		}
		if err := validateLegacyLogicalName(file.Name, portable); err != nil {
			return nil, err
		}
		if file.Size < 0 {
			return nil, fmt.Errorf("NZB logical file %q has a negative size", file.Name)
		}
		if _, exists := expected[file.Name]; exists {
			return nil, fmt.Errorf("stored NZB contains duplicate logical file %q", file.Name)
		}
		expected[file.Name] = file.Size
		if len(expected) > usenetLegacyAdoptionMaxFiles {
			return nil, fmt.Errorf("stored NZB exceeds the legacy adoption file limit")
		}
	}
	if len(expected) == 0 {
		return nil, fmt.Errorf("stored NZB has no logical files")
	}

	queueFiles := make(map[string]*storage.File)
	for key, file := range entry.Files {
		if file == nil || file.Deleted {
			continue
		}
		if key != file.Name {
			return nil, fmt.Errorf("queue file key %q does not match file name %q", key, file.Name)
		}
		queueFiles[key] = file
	}
	if len(queueFiles) != len(expected) {
		return nil, fmt.Errorf("queue logical file set does not match stored NZB")
	}
	if len(provider.Files) != len(expected) {
		return nil, fmt.Errorf("provider logical file set does not match stored NZB")
	}
	for name, size := range expected {
		file := queueFiles[name]
		if file == nil || file.Size != size {
			return nil, fmt.Errorf("queue logical file %q does not match stored NZB", name)
		}
		fileID, err := canonicalUsenetOwnerID(file.InfoHash)
		if err != nil || fileID != entryID {
			return nil, fmt.Errorf("queue logical file %q has the wrong owner", name)
		}
		providerFile := provider.Files[name]
		if providerFile == nil || providerFile.Id != name {
			return nil, fmt.Errorf("provider logical file %q is missing or invalid", name)
		}
		expectedProviderPath := path.Join(entry.MountPath, name)
		if providerFile.Path != expectedProviderPath || providerFile.Link != expectedProviderPath {
			return nil, fmt.Errorf("provider logical file %q path does not match queue provenance", name)
		}
	}

	switch entry.Action {
	case "", config.DownloadActionSymlink, config.DownloadActionDownload, config.DownloadActionStrm, config.DownloadActionNone:
	default:
		return nil, fmt.Errorf("unsupported legacy NZB action %q", entry.Action)
	}
	return expected, nil
}

func validateLegacyLogicalName(name string, portable map[string]string) error {
	if err := safepath.ValidateIdentifier(name); err != nil {
		return fmt.Errorf("invalid NZB logical file name %q: %w", name, err)
	}
	if isReservedUsenetPrivateName(name) {
		return fmt.Errorf("NZB logical file name %q is reserved", name)
	}
	key, err := safepath.PortableNameKey(name)
	if err != nil {
		return err
	}
	if existing, ok := portable[key]; ok && existing != name {
		return fmt.Errorf("NZB logical files %q and %q are portable aliases", existing, name)
	}
	portable[key] = name
	return nil
}

type legacyArtifactKind uint8

const (
	legacyArtifactDownload legacyArtifactKind = iota
	legacyArtifactStrm
	legacyArtifactSymlink
)

type legacyArtifact struct {
	kind     legacyArtifactKind
	size     int64
	exact    bool
	contents string
	target   string
}

func (a *legacyUsenetAdopter) expectedArtifacts(entry *storage.Entry, files map[string]int64) (map[string]legacyArtifact, error) {
	expected := make(map[string]legacyArtifact)
	portable := make(map[string]string)
	action := entry.Action
	if action == "" {
		action = config.DownloadActionSymlink
	}
	if action == config.DownloadActionNone {
		return expected, nil
	}

	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		artifactName := name
		artifact := legacyArtifact{
			size:  files[name],
			exact: entry.IsComplete || entry.State == storage.EntryStatePausedUP,
		}
		switch action {
		case config.DownloadActionDownload:
			artifact.kind = legacyArtifactDownload
		case config.DownloadActionStrm:
			artifact.kind = legacyArtifactStrm
			artifactName += ".strm"
			streamURL, err := url.JoinPath(
				a.strmURL,
				"webdav",
				"stream",
				EntryAllFolder,
				url.PathEscape(storage.GetTorrentFolder(a.folderNaming, entry)),
				url.PathEscape(name),
			)
			if err != nil {
				return nil, fmt.Errorf("build expected STRM URL for %q: %w", name, err)
			}
			artifact.contents = streamURL
		case config.DownloadActionSymlink:
			artifact.kind = legacyArtifactSymlink
			mountRoot, err := safepath.ValidateRoot(a.mountRoot)
			if err != nil {
				return nil, fmt.Errorf("validate configured mount path: %w", err)
			}
			folder := storage.GetTorrentFolder(a.folderNaming, entry)
			artifact.target, err = safepath.JoinIdentifiers(mountRoot, EntryAllFolder, folder, name)
			if err != nil {
				return nil, fmt.Errorf("derive expected mount target for %q: %w", name, err)
			}
		default:
			return nil, fmt.Errorf("unsupported legacy NZB action %q", action)
		}
		if err := validateLegacyLogicalName(artifactName, portable); err != nil {
			return nil, err
		}
		expected[artifactName] = artifact
	}
	return expected, nil
}

func (a *legacyUsenetAdopter) validateArtifacts(entryRoot *os.Root, entry *storage.Entry, files map[string]int64, allowMarker bool) error {
	expected, err := a.expectedArtifacts(entry, files)
	if err != nil {
		return err
	}
	maxEntries := len(expected) + 1
	if allowMarker {
		maxEntries++
	}
	if maxEntries > usenetLegacyAdoptionMaxFiles+1 {
		return fmt.Errorf("legacy NZB artifact set exceeds inspection limit")
	}
	dir, err := entryRoot.Open(".")
	if err != nil {
		return fmt.Errorf("open legacy NZB directory for inspection: %w", err)
	}
	entries, readErr := dir.ReadDir(maxEntries)
	closeErr := dir.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return fmt.Errorf("read legacy NZB directory: %w", readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close legacy NZB directory inspection: %w", closeErr)
	}
	allowedCount := len(expected)
	if allowMarker {
		allowedCount += len(usenetOwnerMarkerNames)
	}
	if len(entries) > allowedCount {
		return fmt.Errorf("legacy NZB directory contains extra artifacts")
	}

	actualPortable := make(map[string]string)
	found := make(map[string]struct{})
	for _, dirEntry := range entries {
		name := dirEntry.Name()
		if allowMarker && isUsenetOwnerMarkerName(name) {
			continue
		}
		if err := safepath.ValidateIdentifier(name); err != nil {
			return fmt.Errorf("invalid legacy NZB artifact name %q: %w", name, err)
		}
		if isReservedUsenetPrivateName(name) {
			return fmt.Errorf("unexpected reserved artifact %q in legacy NZB directory", name)
		}
		key, err := safepath.PortableNameKey(name)
		if err != nil {
			return err
		}
		if existing, ok := actualPortable[key]; ok && existing != name {
			return fmt.Errorf("legacy NZB artifacts %q and %q are portable aliases", existing, name)
		}
		actualPortable[key] = name

		artifact, ok := expected[name]
		if !ok {
			for expectedName := range expected {
				expectedKey, _ := safepath.PortableNameKey(expectedName)
				if expectedKey == key {
					return fmt.Errorf("legacy NZB artifact %q is a portable alias of expected %q", name, expectedName)
				}
			}
			return fmt.Errorf("unexpected legacy NZB artifact %q", name)
		}
		if err := validateLegacyArtifact(entryRoot, name, artifact); err != nil {
			return err
		}
		found[name] = struct{}{}
	}

	completed := entry.IsComplete || entry.State == storage.EntryStatePausedUP
	if completed && len(found) != len(expected) {
		return fmt.Errorf("completed legacy NZB action is missing expected artifacts")
	}
	return nil
}

func isUsenetOwnerMarkerName(name string) bool {
	for _, markerName := range usenetOwnerMarkerNames {
		if name == markerName {
			return true
		}
	}
	return false
}

func validateLegacyArtifact(rooted *os.Root, name string, artifact legacyArtifact) error {
	switch artifact.kind {
	case legacyArtifactDownload:
		file, info, err := openStableLegacyRegularFile(rooted, name)
		if err != nil {
			return fmt.Errorf("inspect downloaded legacy artifact %q: %w", name, err)
		}
		defer file.Close()
		if err := requireSingleLegacyLink(file); err != nil {
			return fmt.Errorf("inspect downloaded legacy artifact %q: %w", name, err)
		}
		if info.Size() > artifact.size {
			return fmt.Errorf("downloaded legacy artifact %q is larger than expected", name)
		}
		if artifact.exact && info.Size() != artifact.size {
			return fmt.Errorf("completed downloaded legacy artifact %q has size %d, expected %d", name, info.Size(), artifact.size)
		}
	case legacyArtifactStrm:
		file, _, err := openStableLegacyRegularFile(rooted, name)
		if err != nil {
			return fmt.Errorf("inspect STRM legacy artifact %q: %w", name, err)
		}
		defer file.Close()
		if err := requireSingleLegacyLink(file); err != nil {
			return fmt.Errorf("inspect STRM legacy artifact %q: %w", name, err)
		}
		contents, err := io.ReadAll(io.LimitReader(file, int64(len(artifact.contents)+1)))
		if err != nil {
			return fmt.Errorf("read STRM legacy artifact %q: %w", name, err)
		}
		if string(contents) != artifact.contents {
			return fmt.Errorf("STRM legacy artifact %q has unexpected contents", name)
		}
	case legacyArtifactSymlink:
		before, err := rooted.Lstat(name)
		if err != nil {
			return fmt.Errorf("inspect symlink legacy artifact %q: %w", name, err)
		}
		if before.Mode()&os.ModeSymlink == 0 {
			return fmt.Errorf("legacy artifact %q is not a symlink", name)
		}
		if err := requireSingleLegacySymlinkLink(rooted, name, before); err != nil {
			return fmt.Errorf("inspect symlink legacy artifact %q: %w", name, err)
		}
		target, err := rooted.Readlink(name)
		if err != nil {
			return fmt.Errorf("read legacy symlink %q: %w", name, err)
		}
		after, err := rooted.Lstat(name)
		if err != nil || !os.SameFile(before, after) || after.Mode()&os.ModeSymlink == 0 {
			return fmt.Errorf("legacy symlink %q changed during inspection", name)
		}
		if !filepath.IsAbs(target) || !sameFilesystemPath(target, artifact.target) {
			return fmt.Errorf("legacy symlink %q targets %q, expected %q", name, target, artifact.target)
		}
	default:
		return fmt.Errorf("unknown legacy artifact type for %q", name)
	}
	return nil
}

func openStableLegacyRegularFile(rooted *os.Root, name string) (*os.File, os.FileInfo, error) {
	before, err := rooted.Lstat(name)
	if err != nil {
		return nil, nil, err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("artifact is not a regular file")
	}
	file, err := rooted.Open(name)
	if err != nil {
		return nil, nil, err
	}
	opened, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	current, err := rooted.Lstat(name)
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	if current.Mode()&os.ModeSymlink != 0 ||
		!current.Mode().IsRegular() ||
		!os.SameFile(before, opened) ||
		!os.SameFile(opened, current) {
		_ = file.Close()
		return nil, nil, fmt.Errorf("artifact changed or became a symlink during inspection")
	}
	return file, opened, nil
}

func openStableLegacyDirectory(rooted *os.Root, name string, before os.FileInfo) (*os.Root, error) {
	if before == nil || before.Mode()&os.ModeSymlink != 0 || !before.IsDir() {
		return nil, fmt.Errorf("artifact is not a regular directory")
	}
	directory, err := rooted.OpenRoot(name)
	if err != nil {
		return nil, err
	}
	openedFile, err := directory.Open(".")
	if err != nil {
		_ = directory.Close()
		return nil, err
	}
	opened, statErr := openedFile.Stat()
	closeErr := openedFile.Close()
	if statErr != nil {
		_ = directory.Close()
		return nil, statErr
	}
	if closeErr != nil {
		_ = directory.Close()
		return nil, closeErr
	}
	current, err := rooted.Lstat(name)
	if err != nil {
		_ = directory.Close()
		return nil, err
	}
	if current.Mode()&os.ModeSymlink != 0 ||
		!current.IsDir() ||
		!os.SameFile(before, opened) ||
		!os.SameFile(opened, current) {
		_ = directory.Close()
		return nil, fmt.Errorf("directory changed or became a symlink during inspection")
	}
	return directory, nil
}

func requireSingleLegacyLink(file *os.File) error {
	links, err := legacyUsenetLinkCount(file)
	if err != nil {
		return err
	}
	if links != 1 {
		return fmt.Errorf("artifact has %d hard links", links)
	}
	return nil
}

func requireSingleLegacyNamedRegularLink(rooted *os.Root, name string) error {
	file, _, err := openStableLegacyRegularFile(rooted, name)
	if err != nil {
		return err
	}
	return errors.Join(requireSingleLegacyLink(file), file.Close())
}

func requireSingleLegacySymlinkLink(rooted *os.Root, name string, info os.FileInfo) error {
	links, err := legacyUsenetSymlinkLinkCount(rooted, name, info)
	if err != nil {
		return err
	}
	if links != 1 {
		return fmt.Errorf("artifact has %d hard links", links)
	}
	return nil
}
