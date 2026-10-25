package manager

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/Trifocals3537/tessarr/pkg/usenet"
)

// reconcileCleanupFiles is reachable only through an explicitly requested,
// durably recorded reconciliation. It never adopts or renames media paths.
// A nonportable legacy leaf may be inspected for absence, but never removed.
func (m *Manager) reconcileCleanupFiles(entry *storage.Entry) error {
	root, err := safepath.ValidateRoot(m.config.DownloadFolder)
	if err != nil {
		return err
	}
	save, err := validateTorrentDownloadFolder(root, entry.SavePath)
	if err != nil {
		return err
	}
	leaf := entry.OutputComponent()
	portable := strings.TrimRight(leaf, " .")
	if leaf == "" || filepath.Base(leaf) != leaf || strings.ContainsAny(leaf, `/\`) {
		return fmt.Errorf("unsafe saved output component")
	}
	if err := safepath.ValidateIdentifier(portable); err != nil {
		return err
	}
	if isReservedTorrentPrivateName(portable) || isReservedUsenetPrivateName(portable) {
		return fmt.Errorf("reserved output component")
	}
	relative, err := filepath.Rel(root, filepath.Join(save, leaf))
	if err != nil {
		return err
	}
	rooted, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	defer rooted.Close()
	parent, err := rooted.Open(filepath.Dir(relative))
	if err != nil {
		return err
	}
	defer parent.Close()
	count := 0
	for {
		names, readErr := parent.ReadDir(256)
		for _, name := range names {
			count++
			if count > 100000 {
				return fmt.Errorf("parent exceeds bounded resolution scan")
			}
			if strings.EqualFold(strings.TrimRight(name.Name(), " ."), portable) && name.Name() != leaf {
				return fmt.Errorf("portable output alias remains")
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	info, inspectErr := rooted.Lstat(relative)
	if inspectErr != nil && !os.IsNotExist(inspectErr) {
		return inspectErr
	}
	if !entry.IsNZB() {
		if inspectErr == nil {
			return fmt.Errorf("torrent output still exists; normal ownership cleanup is required")
		}
		owner, err := canonicalTorrentOwnerID(entry.InfoHash)
		if err != nil {
			return err
		}
		quarantines, err := findTorrentQuarantines(rooted, relative, owner)
		if err != nil {
			return err
		}
		if len(quarantines) != 0 {
			return fmt.Errorf("torrent quarantine requires review")
		}
		return nil
	}
	if leaf != portable {
		return fmt.Errorf("nonportable NZB output requires review")
	}
	if inspectErr == nil && (info.Mode()&os.ModeSymlink != 0 || !info.IsDir()) {
		return fmt.Errorf("NZB output is not a regular directory")
	}
	metadata := filepath.Join(config.GetMainPath(), "usenet", "nzbs")
	if err := usenet.ValidateStagedNZBAt(metadata, entry.InfoHash, entry.Magnet); err != nil {
		return err
	}
	// Pin the original persisted output component while correcting only the
	// display-name derivation for this operation. UUID ownership remains exact.
	adjusted := *entry
	adjusted.Name = leaf
	ownership, exists, err := acquireUsenetOwnershipLock(root, false)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("configured staging root disappeared")
	}
	err = quarantineAndRemoveUsenetEntryLocked(ownership.root, ownership.absolute, &adjusted, true)
	err = errors.Join(err, ownership.close())
	if err != nil {
		return err
	}
	return usenet.RemoveStagedNZBAt(metadata, entry.InfoHash, entry.Magnet)
}
