package manager

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func (m *Manager) RemoveFromProvider(providerEntry *storage.ProviderEntry) error {
	if providerEntry == nil {
		return nil
	}
	if providerEntry.Provider == "usenet" {
		if m.usenet != nil {
			return m.usenet.Delete(providerEntry.ID)
		}
		return fmt.Errorf("usenet client is not configured")
	}

	client := m.ProviderClient(providerEntry.Provider)
	if client == nil {
		return fmt.Errorf("provider client %q is not configured", providerEntry.Provider)
	}
	err := client.DeleteTorrent(providerEntry.ID)
	if providerPlacementAlreadyAbsent(err) {
		return nil
	}
	return err
}

func providerPlacementAlreadyAbsent(err error) bool {
	return err == nil || errors.Is(err, customerror.TorrentNotFoundError)
}

// RemoveTorrentPlacements removes all unique placements synchronously and
// returns every failure. Callers keep durable state on error so a later delete
// can retry safely; already-absent provider objects are treated as success.
func (m *Manager) RemoveTorrentPlacements(entries ...*storage.Entry) error {
	type keyedPlacement struct {
		key       string
		placement *storage.ProviderEntry
	}
	seen := make(map[string]*storage.ProviderEntry)
	var errs []error
	for _, entry := range entries {
		if entry == nil {
			continue
		}
		for _, placement := range entry.Providers {
			if placement == nil {
				continue
			}
			key := strings.ToLower(placement.Provider) + "\x00" + placement.ID
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = placement
		}
	}

	placements := make([]keyedPlacement, 0, len(seen))
	for key, placement := range seen {
		placements = append(placements, keyedPlacement{key: key, placement: placement})
	}
	sort.Slice(placements, func(i, j int) bool {
		return placements[i].key < placements[j].key
	})
	for _, item := range placements {
		placement := item.placement
		if err := m.RemoveFromProvider(placement); err != nil {
			errs = append(errs, fmt.Errorf(
				"delete placement %s/%s: %w",
				placement.Provider,
				placement.ID,
				err,
			))
		}
	}
	return errors.Join(errs...)
}

// deleteProviderTorrent performs rollback synchronously. Provider HTTP clients
// own their request deadlines; returning before a contextless DeleteTorrent
// call finishes would let escaped cleanup delete a later same-ID replacement.
func (m *Manager) deleteProviderTorrent(client common.Client, torrentID string) error {
	if client == nil || torrentID == "" {
		return nil
	}
	err := client.DeleteTorrent(torrentID)
	if providerPlacementAlreadyAbsent(err) {
		return nil
	}
	return err
}
