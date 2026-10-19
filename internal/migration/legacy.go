// Package migration contains offline, one-way state migration helpers.
package migration

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/Trifocals3537/tessarr/pkg/usenet"
)

const (
	receiptName            = ".tessarr-migration.json"
	receiptVersion         = 3
	legacyReceiptVersionV2 = 2
	receiptMaxSize         = 4096
)

var exactStateNames = map[string]string{
	".decypharr":                             ".tessarr",
	".decypharr-cache-cleanup.lock":          ".tessarr-cache-cleanup.lock",
	".decypharr-cache-instance":              ".tessarr-cache-instance",
	".decypharr-cache-owner":                 ".tessarr-cache-owner",
	".decypharr-nzb-legacy-adoption-v1.done": ".tessarr-nzb-legacy-adoption-v1.done",
	".decypharr-nzb-owner-v1":                ".tessarr-nzb-owner-v1",
	".decypharr-nzb-ownership.lock":          ".tessarr-nzb-ownership.lock",
	".decypharr-stream-cache-v1":             ".tessarr-stream-cache-v1",
	".decypharr-strm-entry":                  ".tessarr-strm-entry",
	".decypharr-strm-root":                   ".tessarr-strm-root",
	".decypharr-torrent-owner-v1":            ".tessarr-torrent-owner-v1",
	".decypharr-torrent-ownership.lock":      ".tessarr-torrent-ownership.lock",
	"decypharr.log":                          "tessarr.log",
}

var prefixedStateNames = []struct {
	old string
	new string
}{
	{old: ".decypharr-cache-quarantine-", new: ".tessarr-cache-quarantine-"},
	{old: ".decypharr-nzb-quarantine-", new: ".tessarr-nzb-quarantine-"},
	{old: ".decypharr-strm-", new: ".tessarr-strm-"},
	{old: ".decypharr-torrent-part-", new: ".tessarr-torrent-part-"},
	{old: ".decypharr-torrent-quarantine-", new: ".tessarr-torrent-quarantine-"},
}

// Options controls a Decypharr-to-Tessarr state migration.
type Options struct {
	Source string
	Target string
	DryRun bool
}

// Result summarizes a completed or planned migration.
type Result struct {
	Files           int
	Directories     int
	Bytes           int64
	RenamedEntries  int
	AlreadyMigrated bool
	DryRun          bool
}

type manifestEntry struct {
	SourcePath string
	TargetPath string
	Mode       fs.FileMode
	Size       int64
	ModTime    time.Time
	Digest     string
}

type receipt struct {
	SchemaVersion int    `json:"schema_version"`
	SourceDigest  string `json:"source_digest"`
	TargetDigest  string `json:"target_digest"`
}

// Migrate copies an offline Decypharr state tree into a new Tessarr state
// tree. The source is never mutated, symlinks and special files are rejected,
// and the fully verified staging tree is promoted with one rename.
func Migrate(options Options) (Result, error) {
	var result Result
	result.DryRun = options.DryRun

	source, target, err := validateRoots(options.Source, options.Target)
	if err != nil {
		return result, err
	}

	entries, err := buildManifest(source)
	if err != nil {
		return result, err
	}
	for _, entry := range entries {
		if entry.Mode.IsDir() {
			result.Directories++
			continue
		}
		result.Files++
		result.Bytes += entry.Size
		if entry.SourcePath != entry.TargetPath {
			result.RenamedEntries++
		}
	}
	sourceDigest := digestManifest(entries)

	if _, err := os.Lstat(target); err == nil {
		if err := verifyExistingTarget(target, entries); err != nil {
			return result, err
		}
		result.AlreadyMigrated = true
		return result, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return result, fmt.Errorf("inspect migration target: %w", err)
	}
	if options.DryRun {
		return result, nil
	}

	parent := filepath.Dir(target)
	stage, err := os.MkdirTemp(parent, ".tessarr-migration-")
	if err != nil {
		return result, fmt.Errorf("create migration staging directory: %w", err)
	}
	promoted := false
	defer func() {
		if !promoted {
			_ = os.RemoveAll(stage)
		}
	}()

	if err := copyManifest(source, stage, entries); err != nil {
		return result, err
	}
	if err := verifyTree(stage, entries); err != nil {
		return result, fmt.Errorf("verify staged Tessarr state: %w", err)
	}
	if err := applyStatePathRebase(stage, source, target); err != nil {
		return result, err
	}
	targetEntries, err := scanTarget(stage)
	if err != nil {
		return result, fmt.Errorf("inspect rebased Tessarr state: %w", err)
	}
	targetDigest := digestManifest(targetEntries)

	currentEntries, err := buildManifest(source)
	if err != nil {
		return result, fmt.Errorf("reinspect source after copy: %w", err)
	}
	if digestManifest(currentEntries) != sourceDigest {
		return result, fmt.Errorf("source state changed during migration; stop the service and retry")
	}
	if err := syncNestedStagedDirectories(stage); err != nil {
		return result, fmt.Errorf("sync staged state directories: %w", err)
	}

	if err := writeReceipt(stage, sourceDigest, targetDigest); err != nil {
		return result, err
	}
	promoted, err = promoteStagedState(stage, target, safepath.SyncDirectory)
	if err != nil {
		return result, err
	}
	return result, nil
}

// Files are synced when copied or rebased. Sync nested directory entries too
// so a durable receipt never points at a tree with uncommitted child names.
func syncNestedStagedDirectories(stage string) error {
	var directories []string
	err := filepath.WalkDir(stage, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == stage {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("staged state contains symlink %q", path)
		}
		if entry.IsDir() {
			directories = append(directories, path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	for index := len(directories) - 1; index >= 0; index-- {
		if err := safepath.SyncDirectory(directories[index]); err != nil {
			return fmt.Errorf("sync %s: %w", directories[index], err)
		}
	}
	return nil
}

func promoteStagedState(stage, target string, syncDirectory func(string) error) (bool, error) {
	if err := syncDirectory(stage); err != nil {
		return false, fmt.Errorf("sync migration staging directory: %w", err)
	}
	if err := os.Rename(stage, target); err != nil {
		return false, fmt.Errorf("promote verified Tessarr state: %w", err)
	}
	if err := syncDirectory(filepath.Dir(target)); err != nil {
		return true, fmt.Errorf("sync promoted Tessarr state directory: %w", err)
	}
	return true, nil
}

func validateRoots(source, target string) (string, string, error) {
	if strings.TrimSpace(source) == "" || strings.TrimSpace(target) == "" {
		return "", "", fmt.Errorf("source and target are required")
	}
	source, err := filepath.Abs(source)
	if err != nil {
		return "", "", fmt.Errorf("resolve migration source: %w", err)
	}
	target, err = filepath.Abs(target)
	if err != nil {
		return "", "", fmt.Errorf("resolve migration target: %w", err)
	}
	source = filepath.Clean(source)
	target = filepath.Clean(target)
	if samePath(source, target) {
		return "", "", fmt.Errorf("source and target must be different directories")
	}
	if pathContains(source, target) || pathContains(target, source) {
		return "", "", fmt.Errorf("source and target may not contain one another")
	}
	info, err := os.Lstat(source)
	if err != nil {
		return "", "", fmt.Errorf("inspect migration source: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", "", fmt.Errorf("migration source must be a real directory")
	}
	if err := safepath.RejectSymlinks(source); err != nil {
		return "", "", fmt.Errorf("validate migration source: %w", err)
	}
	targetParent := filepath.Dir(target)
	if err := safepath.RejectSymlinks(targetParent); err != nil {
		return "", "", fmt.Errorf("validate migration target parent: %w", err)
	}
	parentInfo, err := os.Lstat(targetParent)
	if err != nil {
		return "", "", fmt.Errorf("inspect migration target parent: %w", err)
	}
	if parentInfo.Mode()&os.ModeSymlink != 0 || !parentInfo.IsDir() {
		return "", "", fmt.Errorf("migration target parent must be a real directory")
	}
	return source, target, nil
}

func buildManifest(source string) ([]manifestEntry, error) {
	entries := make([]manifestEntry, 0)
	portablePaths := make(map[string]string)
	err := filepath.WalkDir(source, func(path string, dirEntry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if samePath(path, source) {
			return nil
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing symlink in migration source: %s", path)
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return fmt.Errorf("refusing special file in migration source: %s", path)
		}
		relative, err := filepath.Rel(source, path)
		if err != nil || !filepath.IsLocal(relative) {
			return fmt.Errorf("source entry escapes migration root: %s", path)
		}
		targetPath := transformRelativePath(relative)
		if filepath.Dir(targetPath) == "." && portablePathKey(targetPath) == portablePathKey(receiptName) {
			return fmt.Errorf("migration source reserves target name %q for its receipt", receiptName)
		}
		key := portablePathKey(targetPath)
		if previous, exists := portablePaths[key]; exists {
			return fmt.Errorf("migration name collision: %q and %q both map to %q", previous, relative, targetPath)
		}
		portablePaths[key] = relative

		entry := manifestEntry{
			SourcePath: relative,
			TargetPath: targetPath,
			Mode:       info.Mode(),
			Size:       info.Size(),
			ModTime:    info.ModTime(),
		}
		if info.Mode().IsRegular() {
			entry.Digest, err = digestFile(path)
			if err != nil {
				return err
			}
		}
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("inspect migration source: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].TargetPath < entries[j].TargetPath
	})
	return entries, nil
}

func transformRelativePath(relative string) string {
	parts := strings.Split(filepath.Clean(relative), string(filepath.Separator))
	for index, part := range parts {
		parts[index] = transformStateName(part)
	}
	return filepath.Join(parts...)
}

func transformStateName(name string) string {
	if replacement, exists := exactStateNames[name]; exists {
		return replacement
	}
	for _, replacement := range prefixedStateNames {
		if strings.HasPrefix(name, replacement.old) {
			return replacement.new + strings.TrimPrefix(name, replacement.old)
		}
	}
	return name
}

func portablePathKey(path string) string {
	parts := strings.Split(filepath.Clean(path), string(filepath.Separator))
	for index, part := range parts {
		parts[index] = strings.ToLower(strings.TrimRight(part, " ."))
	}
	return strings.Join(parts, "/")
}

func copyManifest(source, stage string, entries []manifestEntry) error {
	for _, entry := range entries {
		sourcePath := filepath.Join(source, entry.SourcePath)
		targetPath := filepath.Join(stage, entry.TargetPath)
		if entry.Mode.IsDir() {
			if err := os.Mkdir(targetPath, 0o700); err != nil {
				return fmt.Errorf("create staged directory %q: %w", entry.TargetPath, err)
			}
			continue
		}
		if err := copyFile(sourcePath, targetPath, entry.Mode.Perm()); err != nil {
			return fmt.Errorf("copy state file %q: %w", entry.SourcePath, err)
		}
		if err := os.Chtimes(targetPath, entry.ModTime, entry.ModTime); err != nil {
			return fmt.Errorf("preserve timestamp for %q: %w", entry.TargetPath, err)
		}
	}
	for index := len(entries) - 1; index >= 0; index-- {
		entry := entries[index]
		if !entry.Mode.IsDir() {
			continue
		}
		if err := os.Chmod(filepath.Join(stage, entry.TargetPath), entry.Mode.Perm()); err != nil {
			return fmt.Errorf("preserve directory mode for %q: %w", entry.TargetPath, err)
		}
	}
	return nil
}

func copyFile(source, target string, mode fs.FileMode) error {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	copyErr := error(nil)
	if _, err := io.Copy(output, input); err != nil {
		copyErr = err
	} else if err := output.Chmod(mode); err != nil {
		copyErr = err
	} else if err := output.Sync(); err != nil {
		copyErr = err
	}
	if err := output.Close(); copyErr == nil {
		copyErr = err
	}
	return copyErr
}

func verifyTree(root string, expected []manifestEntry) error {
	actual, err := scanTarget(root)
	if err != nil {
		return err
	}
	if len(actual) != len(expected) {
		return fmt.Errorf("entry count is %d, expected %d", len(actual), len(expected))
	}
	for index := range expected {
		want := expected[index]
		got := actual[index]
		metadataMismatch := want.TargetPath != got.TargetPath ||
			want.Mode.Type() != got.Mode.Type() ||
			want.Mode.Perm() != got.Mode.Perm()
		contentMismatch := want.Mode.IsRegular() &&
			(want.Size != got.Size || want.Digest != got.Digest)
		if metadataMismatch || contentMismatch {
			return fmt.Errorf("staged entry %q does not match source", want.TargetPath)
		}
	}
	return nil
}

func scanTarget(root string) ([]manifestEntry, error) {
	entries := make([]manifestEntry, 0)
	err := filepath.WalkDir(root, func(path string, dirEntry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if samePath(path, root) {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil || !filepath.IsLocal(relative) {
			return fmt.Errorf("target entry escapes migration root: %s", path)
		}
		info, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if relative == receiptName {
			if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
				return fmt.Errorf("migration receipt is not a regular file")
			}
			return nil
		}
		if info.Mode()&os.ModeSymlink != 0 || (!info.IsDir() && !info.Mode().IsRegular()) {
			return fmt.Errorf("unexpected target entry type: %s", path)
		}
		entry := manifestEntry{TargetPath: relative, Mode: info.Mode(), Size: info.Size()}
		if info.Mode().IsRegular() {
			entry.Digest, err = digestFile(path)
			if err != nil {
				return err
			}
		}
		entries = append(entries, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].TargetPath < entries[j].TargetPath
	})
	return entries, nil
}

func writeReceipt(stage, sourceDigest, targetDigest string) error {
	data, err := json.MarshalIndent(receipt{
		SchemaVersion: receiptVersion,
		SourceDigest:  sourceDigest,
		TargetDigest:  targetDigest,
	}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode migration receipt: %w", err)
	}
	data = append(data, '\n')
	path := filepath.Join(stage, receiptName)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create migration receipt: %w", err)
	}
	_, writeErr := file.Write(data)
	syncErr := file.Sync()
	closeErr := file.Close()
	if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
		return fmt.Errorf("persist migration receipt: %w", err)
	}
	return nil
}

func verifyExistingTarget(target string, sourceEntries []manifestEntry) error {
	info, err := os.Lstat(target)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("migration target exists and is not a real directory")
	}
	data, err := readVerifiedReceipt(target, info)
	if err != nil {
		return fmt.Errorf("migration target already exists without a valid Tessarr receipt: %w", err)
	}
	var completed receipt
	if err := json.Unmarshal(data, &completed); err != nil {
		return fmt.Errorf("decode migration receipt: %w", err)
	}
	if completed.TargetDigest == "" {
		return fmt.Errorf("migration target belongs to different source state")
	}
	actual, err := scanTarget(target)
	if err != nil {
		return fmt.Errorf("inspect existing Tessarr state: %w", err)
	}
	switch completed.SchemaVersion {
	case receiptVersion:
		if completed.SourceDigest != digestManifest(sourceEntries) {
			return fmt.Errorf("migration target belongs to different source state")
		}
		if digestManifest(actual) != completed.TargetDigest {
			return fmt.Errorf("existing Tessarr state failed receipt verification")
		}
		return nil
	case legacyReceiptVersionV2:
		// Version 2 included filesystem-dependent directory allocation sizes.
		// Only its exact recorded hashes provide provenance. Directory allocation
		// sizes cannot be reconstructed safely after they drift, so require an
		// explicit recovery instead of accepting a content-only reconstruction.
		if completed.SourceDigest == digestManifestV2(sourceEntries) &&
			completed.TargetDigest == digestManifestV2(actual) {
			return nil
		}
		return fmt.Errorf(
			"version-two migration receipt cannot be safely verified after filesystem metadata drift; recover or remigrate explicitly",
		)
	default:
		return fmt.Errorf("migration target uses unsupported receipt version %d", completed.SchemaVersion)
	}
}

func applyStatePathRebase(stage, source, target string) error {
	configPath := filepath.Join(stage, "config.json")
	if info, err := os.Lstat(configPath); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("staged configuration is not a regular file")
		}
		data, err := os.ReadFile(configPath)
		if err != nil {
			return fmt.Errorf("read staged configuration: %w", err)
		}
		var document any
		if err := json.Unmarshal(data, &document); err != nil {
			return fmt.Errorf("decode staged configuration: %w", err)
		}
		if rebaseJSONStatePaths(document, source, target) > 0 {
			encoded, err := json.MarshalIndent(document, "", "  ")
			if err != nil {
				return fmt.Errorf("encode rebased configuration: %w", err)
			}
			encoded = append(encoded, '\n')
			file, err := os.OpenFile(configPath, os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
			if err != nil {
				return fmt.Errorf("open rebased configuration: %w", err)
			}
			_, writeErr := file.Write(encoded)
			syncErr := file.Sync()
			closeErr := file.Close()
			if err := errors.Join(writeErr, syncErr, closeErr); err != nil {
				return fmt.Errorf("persist rebased configuration: %w", err)
			}
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect staged configuration: %w", err)
	}

	dbPath := filepath.Join(stage, "db")
	if info, err := os.Lstat(dbPath); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("staged storage path is not a regular directory")
		}
		if _, err := storage.RebaseStatePaths(dbPath, source, target); err != nil {
			return fmt.Errorf("rebase persisted entry paths: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect staged storage path: %w", err)
	}
	metaPath := filepath.Join(stage, "usenet", "meta")
	if info, err := os.Lstat(metaPath); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("staged NZB metadata path is not a regular directory")
		}
		if _, err := usenet.RebaseNZBMetadataPaths(metaPath, source, target); err != nil {
			return fmt.Errorf("rebase persisted NZB source paths: %w", err)
		}
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect staged NZB metadata path: %w", err)
	}
	return nil
}

func rebaseJSONStatePaths(value any, source, target string) int {
	changed := 0
	switch current := value.(type) {
	case map[string]any:
		for key, child := range current {
			if text, ok := child.(string); ok {
				if rebased, changedPath := rebaseStatePath(text, source, target); changedPath {
					current[key] = rebased
					changed++
				}
				continue
			}
			changed += rebaseJSONStatePaths(child, source, target)
		}
	case []any:
		for index, child := range current {
			if text, ok := child.(string); ok {
				if rebased, changedPath := rebaseStatePath(text, source, target); changedPath {
					current[index] = rebased
					changed++
				}
				continue
			}
			changed += rebaseJSONStatePaths(child, source, target)
		}
	}
	return changed
}

func rebaseStatePath(value, source, target string) (string, bool) {
	if !filepath.IsAbs(value) {
		return value, false
	}
	relative, err := filepath.Rel(source, filepath.Clean(value))
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return value, false
	}
	return filepath.Join(target, relative), true
}

func readVerifiedReceipt(target string, targetInfo os.FileInfo) ([]byte, error) {
	rooted, err := os.OpenRoot(target)
	if err != nil {
		return nil, fmt.Errorf("open migration target: %w", err)
	}
	defer rooted.Close()
	openedRoot, err := rooted.Stat(".")
	if err != nil || !os.SameFile(targetInfo, openedRoot) {
		return nil, fmt.Errorf("migration target changed while opening")
	}

	before, err := rooted.Lstat(receiptName)
	if err != nil {
		return nil, err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
		return nil, fmt.Errorf("migration receipt is not a regular file")
	}
	if before.Size() < 1 || before.Size() > receiptMaxSize {
		return nil, fmt.Errorf("migration receipt size is outside the allowed range")
	}
	file, err := rooted.Open(receiptName)
	if err != nil {
		return nil, err
	}
	opened, statErr := file.Stat()
	if statErr != nil || !os.SameFile(before, opened) {
		_ = file.Close()
		return nil, fmt.Errorf("migration receipt changed while opening")
	}
	data, readErr := io.ReadAll(io.LimitReader(file, receiptMaxSize+1))
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(data) > receiptMaxSize {
		return nil, fmt.Errorf("migration receipt exceeds %d bytes", receiptMaxSize)
	}
	after, err := rooted.Lstat(receiptName)
	if err != nil || !os.SameFile(opened, after) {
		return nil, fmt.Errorf("migration receipt changed while reading")
	}
	return data, nil
}

func digestManifest(entries []manifestEntry) string {
	return digestManifestWithDirectorySizes(entries, false)
}

func digestManifestV2(entries []manifestEntry) string {
	return digestManifestWithDirectorySizes(entries, true)
}

func digestManifestWithDirectorySizes(entries []manifestEntry, includeDirectorySizes bool) string {
	hash := sha256.New()
	for _, entry := range entries {
		size := entry.Size
		if entry.Mode.IsDir() && !includeDirectorySizes {
			size = 0
		}
		_, _ = fmt.Fprintf(hash, "%s\x00%s\x00%o\x00%d\x00%s\n", entry.SourcePath, entry.TargetPath, entry.Mode, size, entry.Digest)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func digestFile(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func pathContains(root, target string) bool {
	relative, err := filepath.Rel(root, target)
	return err == nil && relative != "." && filepath.IsLocal(relative)
}

func samePath(left, right string) bool {
	return strings.EqualFold(filepath.Clean(left), filepath.Clean(right))
}
