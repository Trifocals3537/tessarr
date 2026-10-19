package manager

import (
	"fmt"
	"path/filepath"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/safepath"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

// validateTorrentDownloadFolder accepts the configured download root itself or
// a symlink-free strict descendant. Arbitrary API-supplied save paths must
// never redirect provider results outside the application-owned tree.
func validateTorrentDownloadFolder(configuredRoot, requested string) (string, error) {
	root, err := safepath.ValidateRoot(configuredRoot)
	if err != nil {
		return "", fmt.Errorf("invalid configured download root: %w", err)
	}
	if requested == "" {
		return "", fmt.Errorf("download folder is empty")
	}
	absolute, err := filepath.Abs(requested)
	if err != nil {
		return "", fmt.Errorf("resolve download folder: %w", err)
	}
	absolute = filepath.Clean(absolute)
	relative, err := filepath.Rel(root, absolute)
	if err != nil {
		return "", fmt.Errorf("compare download folder with configured root: %w", err)
	}
	if relative == "." {
		if err := safepath.RejectSymlinks(absolute); err != nil {
			return "", err
		}
		return root, nil
	}
	validated, err := safepath.ValidateUnderRoot(root, absolute)
	if err != nil {
		return "", fmt.Errorf("download folder is outside configured root: %w", err)
	}
	return validated, nil
}

func validateTorrentRootName(name string, allowEmpty bool) error {
	// Output names can represent display punctuation, but never accept
	// traversal, absolute paths, controls or malformed text as provider titles.
	// Symlink imports must also validate their independently named mount source.
	if !utf8.ValidString(name) || strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return fmt.Errorf("invalid torrent title encoding or control character")
	}
	name = strings.TrimSpace(name)
	if name == "" && allowEmpty {
		return nil
	}
	if strings.Trim(name, " .") == "" || strings.ContainsAny(name, `/\`) ||
		(len(name) >= 2 && name[1] == ':') {
		return fmt.Errorf("invalid torrent display title %q", name)
	}
	return nil
}

func validateTorrentSourceFolder(entry *storage.Entry, naming config.WebDavFolderNaming, allowEmpty bool) error {
	switch entry.Action {
	case config.DownloadActionDownload, config.DownloadActionStrm, config.DownloadActionNone:
		return nil // These actions do not read a mounted source directory.
	}
	// Unknown/empty actions default to symlinks in Downloader.process.
	name := entry.Name
	switch naming {
	case config.WebDavUseOriginalName, config.WebDavUseOriginalNameNoExt:
		name = entry.OriginalFilename
	case config.WebdavUseHash:
		name = entry.InfoHash
	}
	if allowEmpty && strings.TrimSpace(name) == "" {
		return nil // A magnet may not have its provider-resolved title yet.
	}
	// Check the raw value before GetTorrentFolder can clean away traversal.
	if err := validateTorrentRootName(name, false); err != nil {
		return fmt.Errorf("invalid torrent mount source: %w", err)
	}
	if err := safepath.ValidateIdentifier(storage.GetTorrentFolder(naming, entry)); err != nil {
		return fmt.Errorf("torrent mount source is not portable for symlink import: %w", err)
	}
	return nil
}

func (m *Manager) validateResolvedTorrentNames(torrent *debridTypes.Torrent, action config.DownloadAction) error {
	if err := validateTorrentRootName(torrent.Name, false); err != nil {
		return err
	}
	return validateTorrentSourceFolder(&storage.Entry{
		Name: torrent.Name, OriginalFilename: torrent.OriginalFilename,
		InfoHash: torrent.InfoHash, Action: action,
	}, m.config.FolderNaming, false)
}

func (m *Manager) validateTorrentImportRequest(req *ImportRequest) error {
	if req == nil || req.Magnet == nil {
		return fmt.Errorf("magnet is required")
	}
	if req.Arr == nil {
		return fmt.Errorf("arr is required")
	}
	downloadFolder, err := validateTorrentDownloadFolder(m.config.DownloadFolder, req.DownloadFolder)
	if err != nil {
		return err
	}
	if strings.TrimSpace(req.Arr.Name) == "" {
		req.Arr.Name = "uncategorized"
	}
	if err := safepath.ValidateIdentifier(req.Arr.Name); err != nil {
		return fmt.Errorf("invalid category %q: %w", req.Arr.Name, err)
	}
	if err := validateTorrentRootName(req.Magnet.Name, true); err != nil {
		return err
	}
	if err := validateTorrentSourceFolder(&storage.Entry{
		Name: req.Magnet.Name, OriginalFilename: req.Magnet.Name,
		InfoHash: req.Magnet.InfoHash, Action: req.Action,
	}, m.config.FolderNaming, true); err != nil {
		return err
	}
	req.DownloadFolder = downloadFolder
	return nil
}
