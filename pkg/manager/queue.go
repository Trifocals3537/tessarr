package manager

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/Trifocals3537/tessarr/pkg/usenet"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type ImportType string

const (
	ImportTypeQBit    ImportType = "qbit"
	ImportTypeAPI     ImportType = "api"
	ImportTypeSABnzbd ImportType = "sabnzbd"
	ImportTypeWatch   ImportType = "watch"
	ImportSwitcher    ImportType = "switcher"
)

type ImportRequest struct {
	Name             string                `json:"name"`
	NZBContent       []byte                `json:"-"`
	Id               string                `json:"id"`
	DownloadFolder   string                `json:"downloadFolder"`
	SelectedDebrid   string                `json:"debrid"`
	Magnet           *utils.Magnet         `json:"magnet"`
	Arr              *arr.Arr              `json:"arr"`
	Action           config.DownloadAction `json:"action"`
	DownloadUncached *bool                 `json:"downloadUncached"`
	CallBackUrl      string                `json:"callBackUrl"`
	SkipMultiSeason  bool                  `json:"skip_multi_season"`
	ClientEndpoint   string                `json:"-"`

	Status      string    `json:"status"`
	CompletedAt time.Time `json:"completedAt"`
	Error       string    `json:"error,omitempty"`

	Type  ImportType `json:"type"`
	Async bool       `json:"async"`
}

func NewTorrentRequest(debrid string, downloadFolder string, magnet *utils.Magnet, arr *arr.Arr, action config.DownloadAction, downloadUncached *bool, callBackUrl string, importType ImportType, skipMultiSeason bool) *ImportRequest {

	return &ImportRequest{
		Id:               uuid.New().String(),
		Status:           "started",
		DownloadFolder:   downloadFolder,
		SelectedDebrid:   cmp.Or(arr.SelectedDebrid, debrid), // Use debrid from arr if available
		Magnet:           magnet,
		Arr:              arr,
		Action:           action,
		DownloadUncached: downloadUncached,
		CallBackUrl:      callBackUrl,
		Type:             importType,
		SkipMultiSeason:  skipMultiSeason,
	}
}

func NewNZBRequest(name, downloadFolder string, nzbContent []byte, arr *arr.Arr, action config.DownloadAction, callBackUrl string, importType ImportType, skipMultiSeason bool) *ImportRequest {
	return &ImportRequest{
		Name:            name,
		Id:              uuid.New().String(),
		Status:          "started",
		DownloadFolder:  downloadFolder,
		SelectedDebrid:  "usenet", // NZB imports always use usenet
		NZBContent:      nzbContent,
		Arr:             arr,
		Action:          action,
		CallBackUrl:     callBackUrl,
		Type:            importType,
		SkipMultiSeason: skipMultiSeason,
	}
}

type Queue struct {
	storage            *storage.Storage
	logger             zerolog.Logger
	removeStalledAfter time.Duration
	lifecycle          *entryLifecycle
	removePendingJobs  func(string) int
	deleteDrainTimeout time.Duration
}

const defaultEntryDeleteDrainTimeout = 30 * time.Second

func newQueue(storage *storage.Storage, removeStalledAfterStr string, lifecycles ...*entryLifecycle) *Queue {
	var lifecycle *entryLifecycle
	if len(lifecycles) > 0 {
		lifecycle = lifecycles[0]
	}
	if lifecycle == nil {
		lifecycle = newEntryLifecycle()
	}
	q := &Queue{
		storage:            storage,
		logger:             logger.New("queue"),
		lifecycle:          lifecycle,
		deleteDrainTimeout: defaultEntryDeleteDrainTimeout,
	}

	if removeStalledAfterStr != "" {
		removeStalledAfter, err := utils.ParseDuration(removeStalledAfterStr)
		if err == nil {
			q.removeStalledAfter = removeStalledAfter
		}
	}

	return q
}

func (q *Queue) Add(torrent *storage.Entry) error {
	return q.lifecycle.withAdd(torrent, func() error {
		return q.storage.AddQueue(torrent)
	})
}

func (q *Queue) GetTorrent(infohash string) (*storage.Entry, error) {
	return q.lifecycle.withRead(infohash, func() (*storage.Entry, error) {
		return q.storage.GetQueued(infohash)
	})
}

func (q *Queue) deleteEntryFiles(entry *storage.Entry) error {
	if entry.IsNZB() {
		downloadRoot := config.Get().DownloadFolder
		metadataRoot := filepath.Join(config.GetMainPath(), "usenet", "nzbs")
		return q.deleteNZBEntryFiles(downloadRoot, metadataRoot, entry)
	}

	downloadedPath := entry.DownloadPath()
	if downloadedPath == "" {
		return nil
	}
	downloadRoot := config.Get().DownloadFolder
	if err := removeOwnedTorrentEntryDirectory(downloadRoot, entry); err != nil {
		return fmt.Errorf("delete torrent download path %q: %w", downloadedPath, err)
	}
	return nil
}

func (q *Queue) deleteNZBEntryFiles(downloadRoot, metadataRoot string, entry *storage.Entry) error {
	if _, err := safeUsenetEntryDownloadPath(downloadRoot, entry); err != nil {
		return fmt.Errorf("refusing unsafe NZB cleanup: %w", err)
	}

	// Validate the staged metadata path before removing either artifact. The
	// persisted path must identify this entry's exact <UUID>.queued file, not
	// merely any descendant under the metadata root.
	if err := usenet.ValidateStagedNZBAt(metadataRoot, entry.InfoHash, entry.Magnet); err != nil {
		return fmt.Errorf("refusing unsafe staged NZB cleanup: %w", err)
	}

	if err := removeOwnedUsenetEntryDirectory(downloadRoot, entry); err != nil {
		return fmt.Errorf("delete NZB download path: %w", err)
	}
	if err := usenet.RemoveStagedNZBAt(metadataRoot, entry.InfoHash, entry.Magnet); err != nil {
		return fmt.Errorf("delete staged NZB: %w", err)
	}
	return nil
}

func (q *Queue) Delete(infohash string, cleanup func(t *storage.Entry) error) error {
	_, err := q.deleteWithResult(infohash, cleanup)
	return err
}

// DeleteEntryOnly removes an active-download queue record without removing its
// downloaded data. qBittorrent clients use this when deleteFiles=false.
// Lifecycle cancellation and the durable deletion tombstone still apply, so a
// late worker cannot recreate or mutate the retired record.
func (q *Queue) DeleteEntryOnly(infohash string) error {
	_, err := q.deleteWithResultAndSnapshotsOptions(infohash, nil, false)
	return err
}

// deleteWithResult reports whether this call loaded and deleted a durable
// queue row. A nil error with deleted=false is an authoritative, idempotent
// miss; callers that also own main-storage state must still finish that work.
func (q *Queue) deleteWithResult(infohash string, cleanup func(t *storage.Entry) error) (deleted bool, err error) {
	return q.deleteWithResultAndSnapshots(infohash, cleanup)
}

func (q *Queue) deleteWithResultAndSnapshots(
	infohash string,
	cleanup func(t *storage.Entry) error,
	placementSnapshots ...*storage.Entry,
) (deleted bool, err error) {
	return q.deleteWithResultAndSnapshotsOptions(
		infohash,
		cleanup,
		true,
		placementSnapshots...,
	)
}

func (q *Queue) deleteWithResultAndSnapshotsOptions(
	infohash string,
	cleanup func(t *storage.Entry) error,
	deleteFiles bool,
	placementSnapshots ...*storage.Entry,
) (deleted bool, err error) {
	return q.deleteWithPreparation(infohash, cleanup, deleteFiles, func() (*storage.QueueDeletionIntent, error) {
		if deleteFiles {
			return q.storage.PrepareQueuedDeletion(infohash, cleanup != nil, placementSnapshots...)
		}
		return q.storage.PrepareQueuedDeletionPreservingFiles(infohash)
	})
}

func (q *Queue) deleteWithPreparation(infohash string, cleanup func(*storage.Entry) error, deleteFiles bool, prepare func() (*storage.QueueDeletionIntent, error), beforeFiles ...func() error) (deleted bool, err error) {
	deletion, err := q.lifecycle.beginDelete(infohash)
	if err != nil {
		return false, err
	}
	deleteSucceeded := false
	defer func() {
		deletion.Finish(deleteSucceeded)
	}()

	if q.removePendingJobs != nil {
		q.removePendingJobs(infohash)
	}

	timeout := q.deleteDrainTimeout
	if timeout <= 0 {
		timeout = defaultEntryDeleteDrainTimeout
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), timeout)
	waitErr := deletion.Wait(waitCtx)
	cancel()
	if waitErr != nil {
		return false, fmt.Errorf("cancel active queue work for %s: %w", infohash, waitErr)
	}

	// Snapshot and sync the exact final incarnation only after all work has
	// drained. From this point onward every queue read/update/add fails closed,
	// and restart recovery can finish without resurrecting this job.
	intent, err := prepare()
	if err != nil {
		if storage.IsQueuedEntryNotFound(err) {
			deleteSucceeded = true
			return false, nil
		}
		return false, err
	}
	if err := q.storage.StartQueuedDeletionCleanup(
		intent.InfoHash,
		intent.QueueIncarnation,
	); err != nil {
		return false, err
	}
	if intent.UnrecoverableCleanupPending {
		return false, fmt.Errorf(
			"queued entry %s retains non-recoverable cleanup intent",
			intent.InfoHash,
		)
	}
	if intent.PlacementCleanupPending {
		if cleanup == nil {
			return false, fmt.Errorf(
				"queued entry %s requires provider placement cleanup",
				intent.InfoHash,
			)
		}
		entries := []*storage.Entry{intent.Entry}
		if intent.PlacementScope == storage.QueuePlacementScopeTransferOnly {
			entries = intent.PlacementEntries
		}
		for _, entry := range entries {
			if intent.PlacementScope == storage.QueuePlacementScopeTransferOnly {
				if err := q.storage.ReserveTransferCleanup(entry); err != nil {
					return false, err
				}
			}
			if err := cleanup(entry); err != nil {
				return false, err
			}
		}
		if err := q.storage.MarkQueuedDeletionPlacementsClean(
			intent.InfoHash,
			intent.QueueIncarnation,
		); err != nil {
			return false, err
		}
	}
	// Independent administrative main obligations run even for benign client
	// intents, without widening their immutable provider scope or PreserveFiles.
	for _, finish := range beforeFiles {
		if err := finish(); err != nil {
			return false, err
		}
	}
	if err := q.storage.RetireQueuedDeletionRow(
		intent.InfoHash,
		intent.QueueIncarnation,
	); err != nil {
		return false, err
	}
	if err := q.storage.CompleteQueuedDeletion(
		intent.InfoHash,
		intent.QueueIncarnation,
		func() error {
			if deleteFiles && !intent.PreserveFiles {
				return q.deleteEntryFiles(intent.Entry)
			}
			return nil
		},
	); err != nil {
		return false, err
	}
	deleteSucceeded = true
	return true, nil
}

func (q *Queue) DeleteWhere(category string, protocol config.Protocol, state storage.TorrentState, hashes []string, cleanup func(t *storage.Entry) error) error {
	return q.deleteWhere(q.ListFilterFunc(category, protocol, state, hashes), cleanup)
}

func (q *Queue) DeleteStalled() error {
	cutoff := time.Now().Add(-q.removeStalledAfter)
	return q.deleteWhere(func(t *storage.Entry) bool {
		// The uncached watchdog persists stalledDL as a durable handoff request.
		// Keep it until the owning Arr acknowledges the blocklist/research action
		// and deletes the exact queue item through the qBittorrent API. Removing it
		// here would lose retry evidence during a transient Arr outage.
		if t.DownloadUncached && t.State == storage.EntryStateStalledDL {
			return false
		}
		if !t.AddedOn.Before(cutoff) {
			return false
		}
		if t.Status == debridTypes.TorrentStatusQueued {
			return false
		}
		// Torrent entries: not downloading, no seeders, no progress
		if t.Status != debridTypes.TorrentStatusDownloading && t.Seeders == 0 && t.Progress == 0 {
			return true
		}
		// NZB entries stuck in error state with no progress
		if t.State == storage.EntryStateError && t.Progress == 0 {
			return true
		}
		return false
	}, nil)
}

func (q *Queue) deleteWhere(predicate func(*storage.Entry) bool, cleanup func(*storage.Entry) error) error {
	entries, err := q.storage.FilterQueued(predicate)
	if err != nil {
		return fmt.Errorf("scan queued entries for deletion: %w", err)
	}

	var errs []error
	for _, entry := range entries {
		if err := q.Delete(entry.InfoHash, cleanup); err != nil {
			errs = append(errs, fmt.Errorf("delete queued entry %s: %w", entry.InfoHash, err))
		}
	}
	return errors.Join(errs...)
}

func (q *Queue) Update(torrent *storage.Entry) error {
	return q.lifecycle.withUpdate(torrent, func() error {
		return q.storage.UpdateQueueExisting(torrent)
	})
}

func (q *Queue) ListFilterFunc(category string, protocol config.Protocol, state storage.TorrentState, hashes []string) func(*storage.Entry) bool {
	hashSet := make(map[string]struct{}, len(hashes))
	allHashes := len(hashes) == 1 && strings.EqualFold(strings.TrimSpace(hashes[0]), "all")
	if len(hashes) > 0 && !allHashes {
		for _, h := range hashes {
			hashSet[strings.ToLower(strings.TrimSpace(h))] = struct{}{}
		}
	}

	var filterFunc func(*storage.Entry) bool
	if category != "" || (len(hashes) != 0 && !allHashes) || state != "" || protocol != config.ProtocolAll {
		filterFunc = func(t *storage.Entry) bool {
			if category != "" && t.Category != category {
				return false
			}
			if state != "" && t.State != state {
				return false
			}
			if len(hashSet) > 0 {
				if _, ok := hashSet[strings.ToLower(t.InfoHash)]; !ok {
					return false
				}
			}
			if protocol != config.ProtocolAll && t.Protocol != protocol {
				return false
			}
			return true
		}
	}
	return filterFunc
}

func (q *Queue) ListFilter(category string, protocol config.Protocol, state storage.TorrentState, hashes []string, sortBy string, reverse bool) []*storage.Entry {
	filterFunc := q.ListFilterFunc(category, protocol, state, hashes)
	snapshots, err := q.storage.FilterQueued(filterFunc)
	if err != nil {
		// return empty list on error
		return []*storage.Entry{}
	}
	torrents := make([]*storage.Entry, 0, len(snapshots))
	for _, snapshot := range snapshots {
		torrent, err := q.GetTorrent(snapshot.InfoHash)
		if err != nil {
			if !storage.IsQueuedEntryNotFound(err) && !errors.Is(err, ErrQueueEntryDeleting) {
				q.logger.Error().Err(err).Str("entry", snapshot.InfoHash).Msg("Failed to re-read queue entry under lifecycle gate")
			}
			continue
		}
		// A delete/re-add between the scan and authoritative re-read may have
		// replaced the payload. Reapply the filter to the current incarnation.
		if filterFunc == nil || filterFunc(torrent) {
			torrents = append(torrents, torrent)
		}
	}

	if sortBy != "" {
		sort.Slice(torrents, func(i, j int) bool {
			// If ascending is false, swap i and j to get descending order
			if !reverse {
				i, j = j, i
			}

			switch sortBy {
			case "name":
				return torrents[i].Name < torrents[j].Name
			case "size":
				return torrents[i].Size < torrents[j].Size
			case "added_on":
				return torrents[i].AddedOn.Before(torrents[j].AddedOn)
			case "completed", "downloaded":
				return queueCompletedBefore(torrents[i].CompletedAt, torrents[j].CompletedAt)
			case "progress":
				return torrents[i].Progress < torrents[j].Progress
			case "category":
				return torrents[i].Category < torrents[j].Category
			case "seeders":
				return torrents[i].Seeders < torrents[j].Seeders
			default:
				// Default sort by added_on
				return torrents[i].AddedOn.Before(torrents[j].AddedOn)
			}
		})
	}
	return torrents
}

func queueCompletedBefore(left, right *time.Time) bool {
	switch {
	case left == nil:
		return right != nil
	case right == nil:
		return false
	default:
		return left.Before(*right)
	}
}

func (q *Queue) UpdateWhere(predicate func(*storage.Entry) bool, updateFunc func(*storage.Entry) bool) error {
	entries, err := q.storage.FilterQueued(predicate)
	if err != nil {
		return err
	}

	var errs []error
	for _, entry := range entries {
		if err := q.lifecycle.bindEntry(entry); err != nil {
			errs = append(errs, err)
			continue
		}
		if updateFunc != nil && !updateFunc(entry) {
			continue
		}
		if err := q.Update(entry); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
