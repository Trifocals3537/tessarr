package manager

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func (m *Manager) persistTorrentSource(importReq *ImportRequest) error {
	if importReq == nil || importReq.Magnet == nil || !importReq.Magnet.IsTorrent() {
		return nil
	}
	return m.storage.SaveTorrentSource(importReq.Magnet.InfoHash, importReq.Magnet.File)
}

func (m *Manager) pruneTorrentSourcesAfterFailedAdmission(importReq *ImportRequest) error {
	if importReq == nil || importReq.Magnet == nil || !importReq.Magnet.IsTorrent() {
		return nil
	}
	if err := m.storage.PruneTorrentSources(); err != nil {
		return fmt.Errorf("prune torrent source after failed admission: %w", err)
	}
	return nil
}

func (m *Manager) torrentMagnetForEntry(entry *storage.Entry) (*utils.Magnet, error) {
	if entry == nil {
		return nil, fmt.Errorf("torrent entry is nil")
	}

	data, err := m.storage.LoadTorrentSource(entry.InfoHash)
	switch {
	case err == nil:
		magnet, parseErr := utils.GetMagnetFromBytes(data, m.config.AlwaysRmTrackerUrls)
		if parseErr != nil {
			return nil, fmt.Errorf("parse persisted torrent source for %s: %w", entry.InfoHash, parseErr)
		}
		return magnet, nil
	case !errors.Is(err, os.ErrNotExist):
		return nil, fmt.Errorf("load persisted torrent source for %s: %w", entry.InfoHash, err)
	}

	magnet, err := utils.GetMagnetInfo(entry.Magnet, m.config.AlwaysRmTrackerUrls)
	if err != nil {
		return utils.ConstructMagnet(entry.InfoHash, entry.Name), nil
	}
	if !strings.EqualFold(magnet.InfoHash, entry.InfoHash) {
		return nil, fmt.Errorf("persisted magnet infohash does not match entry %s", entry.InfoHash)
	}
	return magnet, nil
}
