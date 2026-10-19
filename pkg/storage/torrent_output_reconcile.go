package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Trifocals3537/tessarr/internal/safepath"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

const maxMaterializedOutputNameAttempts = 16

type torrentOutputNamespace struct {
	entry        *Entry
	files        map[string]string
	directories  map[string]string
	dirOwners    map[string]string
	logicalNames map[string]string
}

// reserveMaterializedTorrentOutputPaths keeps the on-disk namespace stable
// when a complete provider refresh discovers additional files. The provider
// output planner is intentionally stateless, so a newly visible file can make
// it choose a different name for a directory that has already been
// materialized. Existing paths remain authoritative; only genuinely new files
// are assigned a deterministic alternate component when their planned path
// would make a file and directory occupy the same portable location.
func reserveMaterializedTorrentOutputPaths(
	entry *Entry,
	canonical map[string]*File,
	remoteFiles map[string]debridTypes.File,
	canonicalNames map[string]string,
	canonicalToRemote map[string]string,
	unmatchedRemoteNames []string,
) error {
	namespace := torrentOutputNamespace{
		entry:        entry,
		files:        make(map[string]string, len(canonical)),
		directories:  make(map[string]string, len(canonical)),
		dirOwners:    make(map[string]string, len(canonical)),
		logicalNames: make(map[string]string, len(canonical)),
	}
	for canonicalName, file := range canonical {
		if file == nil {
			return fmt.Errorf("canonical torrent file %q is nil", canonicalName)
		}
		localPath := strings.TrimSpace(file.Path)
		if localPath == "" {
			localPath = strings.TrimSpace(file.Name)
		}
		localPath = materializedTorrentRelativePath(entry, localPath)
		nativePath := ""
		if remoteName := canonicalToRemote[canonicalName]; remoteName != "" {
			nativePath = remoteFiles[remoteName].Path
		}
		if err := namespace.reserve(localPath, canonicalName, nativePath); err != nil {
			return fmt.Errorf("reserve canonical torrent file %q: %w", canonicalName, err)
		}
	}

	for _, remoteName := range unmatchedRemoteNames {
		remoteFile := remoteFiles[remoteName]
		if relative := materializedTorrentRelativePath(entry, remoteFile.LocalPath()); relative != remoteFile.LocalPath() {
			remoteFile.OutputPath = relative
		}
		resolved, canonicalName, err := namespace.resolve(remoteFile, canonicalNames[remoteName])
		if err != nil {
			return fmt.Errorf("reserve new provider file %q: %w", remoteName, err)
		}
		remoteFiles[remoteName] = resolved
		canonicalNames[remoteName] = canonicalName
	}
	return nil
}

// Match the release-root handling used by torrentEntryFileLayouts. Provider
// source paths are not changed; only their managed output paths are planned.
func materializedTorrentRelativePath(entry *Entry, raw string) string {
	if entry == nil {
		return raw
	}
	parts := strings.Split(strings.ReplaceAll(raw, `\`, "/"), "/")
	if len(parts) < 2 || !entry.TorrentPathFirstComponentIsEntryRoot(parts[0]) {
		return raw
	}
	root := strings.TrimSpace(parts[0])
	if entry.OutputName == "" {
		if safepath.ValidateIdentifier(root) != nil {
			return raw
		}
	} else if !utf8.ValidString(root) || strings.IndexFunc(root, unicode.IsControl) >= 0 ||
		strings.Trim(root, " .") == "" || (len(root) >= 2 && root[1] == ':') {
		return raw
	}
	return strings.Join(parts[1:], "/")
}

// CanApplyTorrentTitle keeps mutable provider display metadata from changing
// the interpretation of an already materialized output tree. New files can be
// given pinned local paths, but existing files must retain their old paths.
func (entry *Entry) CanApplyTorrentTitle(remote *debridTypes.Torrent) bool {
	if entry == nil || remote == nil || !torrentArtifactsMayExist(entry) {
		return true
	}
	projected := *entry
	if remote.Name != "" {
		projected.Name = remote.Name
	}
	if remote.OriginalFilename != "" {
		projected.OriginalFilename = remote.OriginalFilename
	}
	// A materialized title can be the WebDAV folder embedded in existing STRM
	// URLs (or the source folder for symlinks). OutputName only pins local
	// artifacts, so freeze both title fields after either may be exposed.
	if entry.Name != projected.Name || entry.OriginalFilename != projected.OriginalFilename {
		return false
	}
	if entry.OutputComponent() != projected.OutputComponent() {
		return false
	}
	for _, file := range entry.Files {
		if file == nil {
			continue
		}
		localPath := strings.TrimSpace(file.Path)
		if localPath == "" {
			localPath = strings.TrimSpace(file.Name)
		}
		if materializedTorrentRelativePath(entry, localPath) != materializedTorrentRelativePath(&projected, localPath) ||
			materializedTorrentRelativePath(entry, file.Name) != materializedTorrentRelativePath(&projected, file.Name) {
			return false
		}
	}
	return true
}

func (namespace *torrentOutputNamespace) resolve(
	file debridTypes.File,
	canonicalName string,
) (debridTypes.File, string, error) {
	localPath := strings.TrimSpace(strings.ReplaceAll(file.LocalPath(), `\`, "/"))
	parts := strings.Split(localPath, "/")
	if len(parts) == 0 || localPath == "" || path.Clean(localPath) != localPath {
		return file, "", fmt.Errorf("planned output path %q is not a clean relative path", file.LocalPath())
	}
	for _, component := range parts {
		if err := safepath.ValidateIdentifier(component); err != nil {
			return file, "", fmt.Errorf("planned output path %q: %w", file.LocalPath(), err)
		}
	}

	baseParts := append([]string(nil), parts...)
	attempts := make(map[int]int, len(parts))
	generatedLeaf := false
	for {
		conflictDepth, conflict := namespace.conflictDepth(parts, file.Path)
		if !conflict && generatedLeaf {
			logicalKey := portableTorrentStoragePathKey(materializedTorrentRelativePath(namespace.entry, canonicalName))
			if _, occupied := namespace.logicalNames[logicalKey]; occupied {
				conflictDepth, conflict = len(parts)-1, true
			}
		}
		if !conflict {
			resolvedPath := strings.Join(parts, "/")
			file.OutputPath = resolvedPath
			file.Name = canonicalName
			if err := namespace.reserve(resolvedPath, canonicalName, file.Path); err != nil {
				return file, "", err
			}
			return file, canonicalName, nil
		}

		attempt := attempts[conflictDepth]
		if attempt >= maxMaterializedOutputNameAttempts {
			return file, "", fmt.Errorf(
				"cannot assign a unique output path after %d attempts at component %q",
				maxMaterializedOutputNameAttempts,
				baseParts[conflictDepth],
			)
		}
		generated, err := disambiguateMaterializedTorrentComponent(
			baseParts[conflictDepth],
			file.Path,
			conflictDepth,
			attempt,
		)
		if err != nil {
			return file, "", err
		}
		parts[conflictDepth] = generated
		attempts[conflictDepth] = attempt + 1
		if conflictDepth == len(parts)-1 {
			canonicalName = generated
			generatedLeaf = true
		}
	}
}

func (namespace *torrentOutputNamespace) conflictDepth(parts []string, nativePath string) (int, bool) {
	for depth := 0; depth < len(parts)-1; depth++ {
		prefixKey := portableTorrentStoragePathKey(strings.Join(parts[:depth+1], "/"))
		if _, exists := namespace.files[prefixKey]; exists {
			return depth, true
		}
		if owner, exists := namespace.dirOwners[prefixKey]; exists {
			incoming := namespace.nativeDirectoryOwner(nativePath, depth, len(parts))
			if owner == "" || incoming == "" || owner != incoming {
				return depth, true
			}
		}
	}
	fileKey := portableTorrentStoragePathKey(strings.Join(parts, "/"))
	if _, exists := namespace.files[fileKey]; exists {
		return len(parts) - 1, true
	}
	if _, exists := namespace.directories[fileKey]; exists {
		return len(parts) - 1, true
	}
	return 0, false
}

func (namespace *torrentOutputNamespace) reserve(localPath, logicalName, nativePath string) error {
	parts := strings.Split(strings.ReplaceAll(strings.TrimSpace(localPath), `\`, "/"), "/")
	if len(parts) == 0 || strings.TrimSpace(localPath) == "" || path.Clean(strings.Join(parts, "/")) != strings.Join(parts, "/") {
		return fmt.Errorf("output path %q is not a clean relative path", localPath)
	}
	for _, component := range parts {
		if err := safepath.ValidateIdentifier(component); err != nil {
			return fmt.Errorf("output path %q: %w", localPath, err)
		}
	}
	fileKey := portableTorrentStoragePathKey(strings.Join(parts, "/"))
	if previous, exists := namespace.files[fileKey]; exists {
		return fmt.Errorf("output file %q collides with %q", localPath, previous)
	}
	if previous, exists := namespace.directories[fileKey]; exists {
		return fmt.Errorf("output file %q conflicts with directory required by %q", localPath, previous)
	}
	for depth := 0; depth < len(parts)-1; depth++ {
		prefix := strings.Join(parts[:depth+1], "/")
		prefixKey := portableTorrentStoragePathKey(prefix)
		if previous, exists := namespace.files[prefixKey]; exists {
			return fmt.Errorf("output directory %q conflicts with file %q", prefix, previous)
		}
		namespace.directories[prefixKey] = localPath
		owner := namespace.nativeDirectoryOwner(nativePath, depth, len(parts))
		if previous, exists := namespace.dirOwners[prefixKey]; !exists {
			namespace.dirOwners[prefixKey] = owner
		} else if previous != owner {
			// Existing materialized files are immutable. An ambiguous owner
			// may remain readable, but no new native directory may join it.
			namespace.dirOwners[prefixKey] = ""
		}
	}
	logicalKey := portableTorrentStoragePathKey(materializedTorrentRelativePath(namespace.entry, logicalName))
	if previous, exists := namespace.logicalNames[logicalKey]; exists {
		return fmt.Errorf("logical file name %q collides with %q", logicalName, previous)
	}
	namespace.files[fileKey] = localPath
	namespace.logicalNames[logicalKey] = logicalName
	return nil
}

func (namespace *torrentOutputNamespace) nativeDirectoryOwner(nativePath string, depth, localParts int) string {
	raw := strings.ReplaceAll(nativePath, `\`, "/")
	if raw == "" {
		return ""
	}
	parts := strings.Split(materializedTorrentRelativePath(namespace.entry, raw), "/")
	if len(parts) != localParts {
		// A lossy native release root can remain as a sanitized local
		// directory. In that case stripping only the native root loses a
		// component that still exists in the materialized output layout.
		parts = strings.Split(raw, "/")
	}
	if len(parts) != localParts || depth >= len(parts)-1 {
		return ""
	}
	return path.Clean(strings.Join(parts[:depth+1], "/"))
}

func disambiguateMaterializedTorrentComponent(
	component, providerPath string,
	depth, attempt int,
) (string, error) {
	extension := path.Ext(component)
	stem := strings.TrimSuffix(component, extension)
	identity := strings.TrimSpace(strings.ReplaceAll(providerPath, `\`, "/"))
	providerParts := strings.Split(identity, "/")
	if depth >= 0 && depth < len(providerParts)-1 {
		identity = strings.Join(providerParts[:depth+1], "/")
	}
	digest := sha256.Sum256([]byte(
		identity + "\x00materialized-output\x00" + strconv.Itoa(depth) + "\x00" + strconv.Itoa(attempt),
	))
	name := stem + "~" + hex.EncodeToString(digest[:]) + extension
	return safepath.CompactIdentifier(name, safepath.PortableIdentifierMaxBytes)
}

func portableTorrentStoragePathKey(value string) string {
	parts := strings.Split(strings.ReplaceAll(strings.TrimSpace(value), `\`, "/"), "/")
	for index := range parts {
		parts[index] = strings.ToLower(strings.TrimRight(parts[index], " ."))
	}
	return strings.Join(parts, "/")
}
