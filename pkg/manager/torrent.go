package manager

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/utils"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func (m *Manager) syncTorrents(ctx context.Context) {
	// First time syncTorrents debrid -> storage
	m.logger.Info().
		Int("debrids", m.clients.Size()).
		Msg("Performing initial sync of torrents from debrid clients...")
	var wg sync.WaitGroup
	m.clients.Range(func(name string, client debrid.Client) bool {
		wg.Go(func() {
			if err := m.refreshTorrents(ctx, name, client); err != nil && ctx.Err() == nil {
				m.logger.Error().Err(err).Str("debrid", name).Msg("Initial torrent sync failed")
			}
			if ctx.Err() == nil {
				m.RefreshEntries(false)
			}
		})
		return true
	})
	wg.Wait()
	m.logger.Info().
		Msg("Initial sync of torrents from debrid clients completed")
}

// Refresh configuration constants
const (
	refreshBatchSize       = 500
	refreshWriteBatchSize  = 50
	refreshFlushInterval   = 3 * time.Second
	refreshMaxWorkers      = 50 // Capped to avoid overwhelming debrid APIs
	refreshMinWorkers      = 5
	refreshDeleteWorkers   = 10
	refreshWorkChanBuffer  = 100
	refreshBatchChanBuffer = 50
)

// refreshTorrents refreshes torrents from a specific debrid service.
// Returns an error if the refresh fails.
func (m *Manager) refreshTorrents(ctx context.Context, provider string, debridClient debrid.Client) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Use singleflight to prevent concurrent refreshes for the same debrid
	_, err, _ := m.refreshSG.Do(provider, func() (any, error) {
		return nil, m.doRefreshTorrents(ctx, provider, debridClient)
	})

	return err
}

// doRefreshTorrents performs the actual refresh logic
func (m *Manager) doRefreshTorrents(ctx context.Context, provider string, debridClient debrid.Client) error {
	providerSnapshot := m.storage.BeginProviderSnapshot()

	remote, err := getProviderTorrents(ctx, debridClient)
	if err != nil {
		m.logger.Error().Err(err).Str("debrid", provider).Msg("Failed to get remote")
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	remoteIndex := indexRemoteTorrents(remote)

	// Detect changes by streaming through cached entries
	newTorrents, torrentsToUpdate, torrentsToDelete, present, err := m.detectTorrentChanges(provider, remoteIndex)
	if err != nil {
		return err
	}

	// Handle deletions
	m.handleTorrentDeletions(torrentsToDelete)

	// Batch update torrents with changed placements (run concurrently)
	var updateWg sync.WaitGroup
	if len(torrentsToUpdate) > 0 {
		updateWg.Add(1)
		go func(torrents []*storage.Entry) {
			defer updateWg.Done()
			if err := m.storage.BatchAddOrUpdate(torrents); err != nil {
				m.logger.Error().Err(err).Msg("Failed to batch update remote")
			}
		}(torrentsToUpdate)
	}

	// Process new torrents
	if len(newTorrents) > 0 {
		if err := m.processNewTorrents(provider, providerSnapshot, newTorrents); err != nil {
			m.logger.Error().Err(err).Str("debrid", provider).Msg("Failed to process new torrents")
		}
	}

	// Wait for concurrent update to finish
	updateWg.Wait()

	if err := ctx.Err(); err != nil {
		return err
	}
	if err := m.storage.ObserveProviderSnapshot(provider, providerSnapshot, present); err != nil {
		return fmt.Errorf("record provider snapshot: %w", err)
	}

	if len(remote) == 0 {
		m.logger.Debug().Str("debrid", provider).Msg("No remote found")
	}
	return nil
}

func getProviderTorrents(ctx context.Context, client debrid.Client) ([]*types.Torrent, error) {
	if contextual, ok := client.(debrid.ContextTorrentLister); ok {
		return contextual.GetTorrentsContext(ctx)
	}

	// Compatibility path for an out-of-tree provider that only implements the
	// original interface. The buffered result prevents a late return from
	// retaining manager state after cancellation.
	type result struct {
		torrents []*types.Torrent
		err      error
	}
	resultCh := make(chan result, 1)
	go func() {
		torrents, err := client.GetTorrents()
		resultCh <- result{torrents: torrents, err: err}
	}()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case providerResult := <-resultCh:
		return providerResult.torrents, providerResult.err
	}
}

type remoteTorrentIndex struct {
	byHash map[string]*types.Torrent
	byID   map[string]*types.Torrent
}

func normalizeInfoHash(infoHash string) string {
	return strings.ToLower(strings.TrimSpace(infoHash))
}

// indexRemoteTorrents indexes both portable torrent identity (infohash) and
// provider-local identity. Some providers omit an infohash from list results,
// so the provider ID is needed to safely recognize an already-managed
// placement. Empty hashes are deliberately excluded from byHash: they cannot
// identify a new torrent on their own.
func indexRemoteTorrents(remote []*types.Torrent) remoteTorrentIndex {
	index := remoteTorrentIndex{
		byHash: make(map[string]*types.Torrent, len(remote)),
		byID:   make(map[string]*types.Torrent, len(remote)),
	}
	for _, torrent := range remote {
		if torrent == nil {
			continue
		}
		if infoHash := normalizeInfoHash(torrent.InfoHash); infoHash != "" {
			if old := index.byHash[infoHash]; old == nil || torrent.Added.After(old.Added) {
				index.byHash[infoHash] = torrent
			}
		}
		if torrent.Id != "" {
			if old := index.byID[torrent.Id]; old == nil || torrent.Added.After(old.Added) {
				index.byID[torrent.Id] = torrent
			}
		}
	}
	return index
}

// match returns a remote record for an existing placement. Hash identity wins.
// Provider-ID fallback is accepted only when the remote record omits its hash;
// a different explicit hash must never be rebound to the stored entry.
func (index remoteTorrentIndex) match(infoHash string, placement *storage.ProviderEntry) (*types.Torrent, bool) {
	if remote, ok := index.byHash[normalizeInfoHash(infoHash)]; ok {
		return remote, true
	}
	if placement == nil || placement.ID == "" {
		return nil, false
	}
	remote, ok := index.byID[placement.ID]
	if !ok || normalizeInfoHash(remote.InfoHash) != "" {
		return nil, false
	}

	// Never mutate the provider's shared list result. The rebound hash is used
	// only to update the already-managed entry associated with this placement.
	rebound := remote.Copy()
	rebound.InfoHash = infoHash
	return rebound, true
}

// detectTorrentChanges streams through cached entries and detects what changed
func (m *Manager) detectTorrentChanges(provider string, remote remoteTorrentIndex) (
	newTorrents []*types.Torrent,
	torrentsToUpdate []*storage.Entry,
	torrentsToDelete []*storage.Entry,
	present map[string]struct{},
	err error,
) {
	newTorrents = make([]*types.Torrent, 0, 100)
	torrentsToUpdate = make([]*storage.Entry, 0, 100)
	torrentsToDelete = make([]*storage.Entry, 0, 10)
	cachedInfoHashes := make(map[string]bool, len(remote.byHash))
	present = make(map[string]struct{}, len(remote.byHash))
	for infoHash := range remote.byHash {
		present[infoHash] = struct{}{}
	}

	err = m.storage.ForEachBatch(refreshBatchSize, func(batch []*storage.Entry) error {
		for _, entry := range batch {
			infoHash := normalizeInfoHash(entry.InfoHash)
			cachedInfoHashes[infoHash] = true

			oldPlacement, placementOnDebrid := entry.Providers[provider]
			currentTorrent, onRemote := remote.match(entry.InfoHash, oldPlacement)
			if onRemote {
				present[infoHash] = struct{}{}
			}

			if placementOnDebrid {
				if !onRemote {
					entry.RemoveProvider(provider)
					if len(entry.Providers) == 0 {
						torrentsToDelete = append(torrentsToDelete, entry)
					} else {
						torrentsToUpdate = append(torrentsToUpdate, entry)
					}
				} else if oldPlacement.NeedsUpdate(currentTorrent) {
					// currentTorrent has changes for this provider - update placement info
					// But the issue is that currentTorrent may not have all the metadata we need to update the placement (e.g. downloadedAt, files etc)
					// So we need to fetch the full torrent info from debrid to ensure we have all the metadata to update the placement correctly
					// So let's just add it to the newTorrents list and let processNewTorrents handle the update logic - it will be smart enough to only update the placement info without overwriting other metadata
					newTorrents = append(newTorrents, currentTorrent)
				}
			} else if onRemote {
				newTorrents = append(newTorrents, currentTorrent)
			}
		}
		return nil
	})

	if err != nil {
		m.logger.Error().Err(err).Msg("Failed to stream cached remote")
		return nil, nil, nil, nil, err
	}

	// Check for brand new torrents (not in cache at all)
	for infohash, t := range remote.byHash {
		if !cachedInfoHashes[infohash] {
			newTorrents = append(newTorrents, t)
		}
	}

	return newTorrents, torrentsToUpdate, torrentsToDelete, present, nil
}

// handleTorrentDeletions processes torrent deletions concurrently
func (m *Manager) handleTorrentDeletions(torrentsToDelete []*storage.Entry) {
	if len(torrentsToDelete) == 0 {
		return
	}

	var deleteWg sync.WaitGroup
	deleteChan := make(chan *storage.Entry, len(torrentsToDelete))

	deleteWorkers := min(refreshDeleteWorkers, len(torrentsToDelete))
	for range deleteWorkers {
		deleteWg.Go(func() {
			for entry := range deleteChan {
				if err := m.storage.DeleteSnapshot(entry); err != nil {
					m.logger.Error().Err(err).Str("infohash", entry.InfoHash).Msg("Failed to delete torrent")
				}
			}
		})
	}

	for _, entry := range torrentsToDelete {
		deleteChan <- entry
	}
	close(deleteChan)
	deleteWg.Wait()
}

// processNewTorrents processes new torrents with worker pool and batch writing
func (m *Manager) processNewTorrents(provider string, providerSnapshot uint64, newTorrents []*types.Torrent) error {
	workChan := make(chan *types.Torrent, min(refreshWorkChanBuffer, len(newTorrents)))
	batchChan := make(chan *storage.Entry, refreshBatchChanBuffer)
	errChan := make(chan error, 1) // Buffer for first error

	var processWg sync.WaitGroup
	var batchWg sync.WaitGroup
	var processed atomic.Int64
	totalTorrents := len(newTorrents)

	// Batch writer goroutine
	batchWg.Go(func() {
		m.runBatchWriter(batchChan, errChan)
	})

	// Scale workers based on torrent count, but cap to avoid overwhelming APIs
	workers := min(refreshMaxWorkers, max(refreshMinWorkers, len(newTorrents)/10))

	for range workers {
		processWg.Go(func() {
			for t := range workChan {
				if mt, err := m.processSyncTorrent(t, providerSnapshot); err != nil {
					m.logger.Error().Err(err).Str("debrid", provider).Msgf("Failed to process torrent %s", t.Id)
				} else if mt != nil {
					batchChan <- mt
				}
				count := processed.Add(1)
				if count%50 == 0 {
					m.logger.Debug().Str("debrid", provider).Msgf("Processed %d / %d new torrents", count, totalTorrents)
				}
			}
		})
	}

	// Send torrents to workers
	for _, t := range newTorrents {
		workChan <- t
	}

	close(workChan)
	processWg.Wait()
	close(batchChan)
	batchWg.Wait()

	// Check if batch writer encountered an error
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

// runBatchWriter collects entries and writes them in batches
func (m *Manager) runBatchWriter(batchChan <-chan *storage.Entry, errChan chan<- error) {
	batch := make([]*storage.Entry, 0, refreshWriteBatchSize)
	ticker := time.NewTicker(refreshFlushInterval)
	defer ticker.Stop()

	var writeErr error
	flushBatch := func() {
		if len(batch) == 0 || writeErr != nil {
			return
		}
		if err := m.storage.BatchAddOrUpdate(batch); err != nil {
			m.logger.Error().Err(err).Msg("Failed to batch write remote")
			writeErr = err
			// Send first error to channel (non-blocking)
			select {
			case errChan <- err:
			default:
			}
		}
		// Clear slice
		for i := range batch {
			batch[i] = nil
		}
		batch = batch[:0]
	}

	for {
		select {
		case t, ok := <-batchChan:
			if !ok {
				flushBatch()
				return
			}
			batch = append(batch, t)
			if len(batch) >= refreshWriteBatchSize {
				flushBatch()
			}
		case <-ticker.C:
			flushBatch()
		}
	}
}

// processSyncTorrent processes a single torrent and returns it for batched writing
func (m *Manager) processSyncTorrent(t *types.Torrent, providerSnapshots ...uint64) (*storage.Entry, error) {
	var providerSnapshot uint64
	if len(providerSnapshots) > 0 {
		providerSnapshot = providerSnapshots[0]
	}

	// GetReader the debrid client
	client := m.ProviderClient(t.Debrid)
	if client == nil {
		return nil, nil
	}

	// Entries deleted locally but still returned by the provider are rejected
	// by the rediscovery guard until an authoritative post-delete provider
	// absence authorizes rediscovery. Re-processing them on every sync cycle
	// wasted provider API calls and RAR work only to fail at the guard, and
	// re-logged the same rejection thousands of times per day. Skip the
	// expensive work up front; the guard itself remains the authority when the
	// check fails open.
	if m.storage != nil {
		if awaiting, err := m.storage.RediscoveryAwaitingAbsence(t.InfoHash, t.Debrid, providerSnapshot); err == nil {
			if awaiting {
				m.noteRediscoveryPending(t.Debrid, t.InfoHash)
				return nil, nil
			}
			m.clearRediscoveryPending(t.Debrid, t.InfoHash)
		}
	}

	// Check if files are complete - only make API call if needed
	needsUpdate := len(t.Files) == 0 || !isComplete(t.Files)
	if needsUpdate {
		// This is the main bottleneck - API call per torrent
		// Consider: Could we batch UpdateTorrent calls? Depends on debrid API
		if err := client.UpdateTorrent(t); err != nil {
			return nil, err
		}

		// Re-check completion after update
		if !isComplete(t.Files) {
			return nil, nil
		}
	}

	addedOn := t.Added
	if addedOn.IsZero() {
		addedOn = time.Now()
	}

	// Check if we have an existing managed torrent
	// Note: This is a database read per torrent - could be optimized with batch reads
	// or an in-memory cache, but storage.GetReader is likely fast (indexed by InfoHash)
	mt, err := m.storage.Get(t.InfoHash)
	if err != nil && !storage.IsEntryNotFound(err) {
		return nil, err
	}
	if storage.IsEntryNotFound(err) {
		// Create new managed torrent
		var magnet *utils.Magnet
		if t.Magnet == nil || t.Magnet.Link == "" {
			magnet = utils.ConstructMagnet(t.InfoHash, t.Name)
		} else {
			magnet = t.Magnet
		}
		size := t.Size
		if size == 0 {
			size = t.Bytes
		}
		mt = &storage.Entry{
			Protocol:         config.ProtocolTorrent,
			InfoHash:         t.InfoHash,
			Name:             t.Name,
			OriginalFilename: t.OriginalFilename,
			Size:             size,
			Bytes:            size,
			Magnet:           magnet.Link,
			ActiveProvider:   t.Debrid,
			Providers:        make(map[string]*storage.ProviderEntry),
			Files:            make(map[string]*storage.File),
			Status:           t.Status,
			Progress:         t.Progress,
			Speed:            t.Speed,
			Seeders:          t.Seeders,
			IsComplete:       len(t.Files) > 0,
			Bad:              false,
			AddedOn:          addedOn,
			CreatedAt:        addedOn,
			UpdatedAt:        time.Now(),
		}
	}
	if err := m.storage.PrepareProviderEntry(mt, t.Debrid, providerSnapshot); err != nil {
		return nil, err
	}

	// Populate global Files metadata (only if empty)
	if len(mt.Files) == 0 {
		for _, f := range t.GetFiles() {
			mt.Files[f.Name] = &storage.File{
				Name:      f.Name,
				Path:      f.LocalPath(),
				Size:      f.Size,
				ByteRange: f.ByteRange,
				Deleted:   f.Deleted,
				InfoHash:  t.InfoHash,
				AddedOn:   addedOn,
			}
		}
	}

	// AddOrUpdate or update placement
	placement, err := mt.AddTorrentProvider(t)
	if err != nil {
		return nil, fmt.Errorf("reconcile provider file identities: %w", err)
	}
	placement.Progress = t.Progress
	if t.Status == types.TorrentStatusDownloaded {
		downloadedAt := addedOn
		placement.DownloadedAt = &downloadedAt
	}

	// If this is the first placement or the only one, make it active
	if mt.ActiveProvider == "" || len(mt.Providers) == 1 {
		if t.Status == types.TorrentStatusDownloaded {
			_ = mt.ActivatePlacement(t.Debrid)
		}
	}

	// confirm everything is complete
	if err := mt.Validate(); err != nil {
		m.logger.Warn().Err(err).Str("infohash", t.InfoHash).Str("name", mt.Name).Msg("Validation failed for torrent, marking as bad")
	}

	return mt, nil
}

// refreshTorrent refreshes a single torrent from its active debrid
func (m *Manager) refreshTorrent(infohash string) (*storage.Entry, error) {
	torrent, err := m.storage.Get(infohash)
	if err != nil {
		return nil, err
	}

	if torrent.ActiveProvider == "" {
		return torrent, nil
	}

	client := m.ProviderClient(torrent.ActiveProvider)
	if client == nil {
		return torrent, nil
	}

	placement := torrent.GetActiveProvider()
	if placement == nil {
		return torrent, nil
	}

	// GetReader updated torrent info from debrid
	debridTorrent, err := client.GetTorrent(placement.ID)
	if err != nil {
		return nil, err
	}

	entry, err := m.processSyncTorrent(debridTorrent)
	if err != nil {
		return nil, err
	}
	// Store updated entry in storage
	if entry != nil {
		if err := m.storage.AddOrUpdate(entry); err != nil {
			return nil, err
		}
	}
	return entry, nil
}

// refreshDebridDownloadLinks refreshes download links for a specific debrid service
func (m *Manager) refreshDebridDownloadLinks(ctx context.Context, debridName string, client debrid.Client) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	if client == nil {
		m.logger.Warn().Str("debrid", debridName).Msg("Provider client is nil, skipping download link refresh")
		return
	}

	if err := refreshProviderDownloadLinks(ctx, client); err != nil && ctx.Err() == nil {
		m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to refresh download links")
	}
}

// isComplete checks if all files in a torrent have download links
func isComplete(files map[string]types.File) bool {
	if len(files) == 0 {
		return false
	}
	for _, file := range files {
		if file.Link == "" {
			return false
		}
	}
	return true
}
