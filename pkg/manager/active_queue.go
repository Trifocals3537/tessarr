package manager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/Trifocals3537/tessarr/internal/config"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/Trifocals3537/tessarr/pkg/usenet"
)

// recoverInterruptedDownloads clears process-local work left in the durable
// queue by a stopped process. It must run synchronously before constructing the
// JobQueue or starting any scheduler/intake: a background reset could clear a
// flag that a new worker has already claimed. Provider placements, progress and
// download states remain intact; normal restoration resumes the existing work.
func (m *Manager) recoverInterruptedDownloads(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.queue == nil || m.queue.storage == nil {
		return fmt.Errorf("download queue storage is unavailable")
	}
	interrupted := func(entry *storage.Entry) bool {
		return entry.State == storage.EntryStateDownloading && entry.IsDownloading
	}
	// Unlike ListFilter, propagate a failed scan instead of starting workers
	// with a partially recovered (or apparently empty) queue.
	entries, err := m.queue.storage.FilterQueued(interrupted)
	if err != nil {
		return err
	}
	recovered := 0
	for _, snapshot := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Re-read through the lifecycle gate; never bind a scanned payload to a
		// newer generation or resurrect a row hidden by an explicit deletion.
		entry, err := m.queue.GetTorrent(snapshot.InfoHash)
		if storage.IsQueuedEntryNotFound(err) || errors.Is(err, ErrQueueEntryDeleting) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read interrupted download %s: %w", snapshot.InfoHash, err)
		}
		if !interrupted(entry) {
			continue
		}
		entry.IsDownloading = false
		if err := m.queue.Update(entry); err != nil {
			return fmt.Errorf("reset interrupted download %s: %w", entry.InfoHash, err)
		}
		recovered++
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if recovered > 0 {
		if err := m.queue.Sync(); err != nil {
			return fmt.Errorf("sync recovered download queue: %w", err)
		}
		m.logger.Info().Int("count", recovered).Msg("Recovered interrupted local downloads")
	}
	return nil
}

func (m *Manager) restoreActiveDownloadJobs(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}

	entries := m.queue.ListFilter("", config.ProtocolAll, storage.EntryStateDownloading, nil, "", false)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].AddedOn.Before(entries[j].AddedOn)
	})

	// Existing active downloads reserve slots before queued imports are resumed.
	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		if entry.Status == debridTypes.TorrentStatusQueued || m.nzbNeedsReprocessing(entry) {
			continue
		}
		if err := m.submitRestoredJob(ctx, &Job{
			ID:    entry.InfoHash,
			Type:  jobTypeForEntry(entry),
			Entry: entry,
		}); err != nil {
			if ctx.Err() != nil {
				return
			}
			entry.MarkAsError(err)
			_ = m.queue.Update(entry)
		}
	}

	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		if entry.Status != debridTypes.TorrentStatusQueued && !m.nzbNeedsReprocessing(entry) {
			continue
		}
		job, err := m.rebuildQueuedJob(ctx, entry)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			entry.MarkAsError(err)
			_ = m.queue.Update(entry)
			continue
		}
		if job.DebridTorrent == nil && job.NZBMeta == nil {
			entry.Status = debridTypes.TorrentStatusQueued
		}
		_ = m.queue.Update(entry)
		if err := m.submitRestoredJob(ctx, job); err != nil {
			if ctx.Err() != nil {
				return
			}
			entry.MarkAsError(err)
			_ = m.queue.Update(entry)
		}
	}
}

func jobTypeForEntry(entry *storage.Entry) JobType {
	if entry != nil && entry.IsNZB() {
		return JobTypeNZB
	}
	return JobTypeTorrent
}

func (m *Manager) nzbNeedsReprocessing(entry *storage.Entry) bool {
	if entry == nil || !entry.IsNZB() || m.usenet == nil {
		return false
	}
	meta, err := m.usenet.GetNZBHeader(entry.InfoHash)
	return err == nil && meta != nil && (meta.Status == usenet.NZBStatusParsing || meta.Status == usenet.NZBStatusDownloading)
}

func (m *Manager) rebuildQueuedJob(ctx context.Context, entry *storage.Entry) (*Job, error) {
	if entry.IsNZB() {
		return m.rebuildQueuedNZBJob(ctx, entry)
	}
	return m.rebuildQueuedTorrentJob(entry)
}

func (m *Manager) rebuildQueuedTorrentJob(entry *storage.Entry) (*Job, error) {
	if entry.ActiveProvider != "" && entry.GetActiveProvider() != nil {
		return &Job{
			ID:             entry.InfoHash,
			Type:           JobTypeTorrent,
			Entry:          entry,
			ResumeExisting: true,
		}, nil
	}

	magnet, err := m.torrentMagnetForEntry(entry)
	if err != nil {
		return nil, err
	}

	downloadUncached := entry.DownloadUncached
	req := NewTorrentRequest(
		entry.ActiveProvider,
		downloadFolderForEntry(m.config.DownloadFolder, entry),
		magnet,
		m.arr.GetOrCreate(entry.Category),
		entry.Action,
		&downloadUncached,
		entry.CallbackURL,
		ImportTypeAPI,
		entry.SkipMultiSeason,
	)
	req.Id = entry.InfoHash
	job := NewJob(JobTypeTorrent, req)
	job.ID = entry.InfoHash
	job.Entry = entry
	return job, nil
}

func (m *Manager) rebuildQueuedNZBJob(ctx context.Context, entry *storage.Entry) (*Job, error) {
	if m.usenet == nil {
		return nil, fmt.Errorf("usenet is not configured")
	}
	var (
		content []byte
		err     error
	)
	meta, metaErr := m.usenet.GetNZBHeader(entry.InfoHash)
	if metaErr == nil && meta != nil && meta.Path != "" {
		content, err = m.usenet.ReadNZBSource(entry.InfoHash, meta.Path)
	} else {
		if metaErr != nil && !usenet.IsNZBNotFound(metaErr) {
			return nil, fmt.Errorf("inspect queued NZB metadata: %w", metaErr)
		}
		content, err = m.usenet.ReadStagedNZB(entry.InfoHash, entry.Magnet)
	}
	if err != nil {
		return nil, fmt.Errorf("read queued NZB source: %w", err)
	}

	name := entry.OriginalFilename
	if name == "" {
		name = entry.Name
	}
	meta, groups, err := m.usenet.ParseWithID(ctx, entry.InfoHash, name, content, entry.Category)
	if err != nil {
		return nil, fmt.Errorf("usenet parse failed: %w", err)
	}
	// A previous attempt may have parsed successfully and then failed before
	// removing its staged source. Always retry the exact ID-bound removal even
	// when this attempt preferred the persisted .nzb source.
	if entry.Magnet != "" {
		if err := m.usenet.RemoveStagedNZB(entry.InfoHash, entry.Magnet); err != nil {
			return nil, fmt.Errorf("remove staged NZB source: %w", err)
		}
	}

	entry.Magnet = ""
	entry.Name = meta.Name
	entry.OriginalFilename = meta.Name
	entry.Size = meta.TotalSize
	entry.Bytes = meta.TotalSize
	entry.Status = debridTypes.TorrentStatusDownloading
	entry.ActiveProvider = "usenet"
	_ = entry.AddUsenetProvider(meta)

	req := NewNZBRequest(
		meta.Name,
		m.config.DownloadFolder,
		content,
		m.arr.GetOrCreate(entry.Category),
		entry.Action,
		entry.CallbackURL,
		ImportTypeSABnzbd,
		entry.SkipMultiSeason,
	)
	req.Id = entry.InfoHash
	job := NewJob(JobTypeNZB, req)
	job.ID = entry.InfoHash
	job.Entry = entry
	job.NZBMeta = meta
	job.NZBGroups = groups
	return job, nil
}

func downloadFolderForEntry(fallback string, entry *storage.Entry) string {
	if entry != nil && entry.SavePath != "" {
		return filepath.Dir(entry.SavePath)
	}
	return fallback
}
