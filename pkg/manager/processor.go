package manager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/Trifocals3537/tessarr/pkg/usenet"
)

var errDeleteQueueEntryOnJobFinish = errors.New("delete queue entry after job finishes")

// AdmitNewTorrent durably records a torrent before provider work begins, then
// hands submission and status checks to the bounded active-download worker
// pool. qBittorrent callers therefore wait only for validation and durable
// admission, not for provider latency. A process restart can rebuild the job
// from the queued entry if it stops after the record is committed.
func (m *Manager) AdmitNewTorrent(ctx context.Context, importReq *ImportRequest) error {
	if err := m.validateTorrentImportRequest(importReq); err != nil {
		return err
	}

	reservation, err := m.reserveJob(ctx, importReq.Magnet.InfoHash)
	if err != nil {
		return err
	}
	defer reservation.release()

	torrent := newTorrentQueueEntry(importReq, debridTypes.TorrentStatusQueued)
	if err := m.persistTorrentSource(importReq); err != nil {
		return fmt.Errorf("persist torrent source before admission: %w", err)
	}
	if err := m.queue.Add(torrent); err != nil {
		return errors.Join(
			fmt.Errorf("failed to add torrent to queue: %w", err),
			m.pruneTorrentSourcesAfterFailedAdmission(importReq),
		)
	}

	importReq.Status = "queued"
	importReq.Async = true
	importReq.CompletedAt = time.Time{}
	importReq.Error = ""
	job := NewJob(JobTypeTorrent, importReq)
	job.ID = torrent.InfoHash
	job.Entry = torrent
	if err := m.submitReservedJob(reservation, job); err != nil {
		importReq.Status = "error"
		importReq.Error = err.Error()
		torrent.MarkAsError(err)
		_ = m.queue.Update(torrent)
		return fmt.Errorf("failed to queue torrent: %w", err)
	}
	return nil
}

// AddNewTorrent submits a torrent to debrid before entering the active-download queue.
func (m *Manager) AddNewTorrent(ctx context.Context, importReq *ImportRequest) error {
	if importReq == nil || importReq.Magnet == nil {
		return fmt.Errorf("magnet is required")
	}
	if importReq.Arr == nil {
		return fmt.Errorf("arr is required")
	}
	reservation, err := m.reserveJob(ctx, importReq.Magnet.InfoHash)
	if err != nil {
		return err
	}
	defer reservation.release()

	debridTorrent, err := m.SendToDebrid(reservation.Context(), importReq)
	if err != nil {
		if isTooManyActiveDownloads(err) {
			m.logger.Warn().Msgf("Too many active downloads, marking as queued: %s", importReq.Magnet.Name)
			return m.queueTorrentRetry(importReq, reservation)
		}
		return fmt.Errorf("failed to submit torrent to debrid: %w", err)
	}

	torrent := newTorrentQueueEntry(importReq, debridTypes.TorrentStatusQueued)
	torrent.DownloadUncached = debridTorrent.DownloadUncached
	if err := applyDebridTorrentToEntry(torrent, debridTorrent); err != nil {
		return fmt.Errorf("apply submitted provider state: %w", err)
	}

	if err := m.persistTorrentSource(importReq); err != nil {
		rollbackErr := m.deleteProviderTorrent(
			m.ProviderClient(debridTorrent.Debrid),
			debridTorrent.Id,
		)
		return errors.Join(
			fmt.Errorf("persist torrent source before queueing: %w", err),
			rollbackErr,
		)
	}
	if err := m.queue.Add(torrent); err != nil {
		return errors.Join(
			fmt.Errorf("failed to add torrent to queue: %w", err),
			m.pruneTorrentSourcesAfterFailedAdmission(importReq),
		)
	}

	job := NewJob(JobTypeTorrent, importReq)
	job.ID = torrent.InfoHash
	job.Entry = torrent
	job.DebridTorrent = debridTorrent
	if err := m.submitReservedJob(reservation, job); err != nil {
		torrent.MarkAsError(err)
		_ = m.queue.Update(torrent)
		return fmt.Errorf("failed to queue torrent: %w", err)
	}
	return nil
}

func (m *Manager) processTorrentJob(ctx context.Context, job *Job) error {
	if job == nil || job.Entry == nil {
		return fmt.Errorf("invalid torrent job")
	}
	if _, err := m.queue.GetTorrent(job.Entry.InfoHash); err != nil {
		return nil
	}
	if job.ResumeExisting {
		job.Entry.Status = debridTypes.TorrentStatusDownloading
		job.Entry.IsDownloading = false
		_ = m.queue.Update(job.Entry)
		m.processingEntries.Store(job.Entry.InfoHash, struct{}{})
		return m.processQueuedTorrent(ctx, job.Entry)
	}
	if job.DebridTorrent == nil {
		if job.Request == nil {
			m.waitForDownloadCompletion(ctx, job.Entry)
			return nil
		}
		debridTorrent, err := m.SendToDebrid(ctx, job.Request)
		if err != nil {
			return fmt.Errorf("failed to submit torrent to debrid: %w", err)
		}
		job.DebridTorrent = debridTorrent
	}

	job.Entry.Status = debridTypes.TorrentStatusDownloading
	job.Entry.DownloadUncached = job.DebridTorrent.DownloadUncached
	if job.Request != nil {
		job.Request.Status = "started"
	}
	return m.processNewTorrent(ctx, job.Entry, job.DebridTorrent)
}

func (m *Manager) queueTorrentRetry(importReq *ImportRequest, reservation *jobReservation) error {
	torrent := newTorrentQueueEntry(importReq, debridTypes.TorrentStatusQueued)
	if err := m.persistTorrentSource(importReq); err != nil {
		return fmt.Errorf("persist torrent source before retry queueing: %w", err)
	}
	if err := m.queue.Add(torrent); err != nil {
		return errors.Join(
			fmt.Errorf("failed to add torrent to queue: %w", err),
			m.pruneTorrentSourcesAfterFailedAdmission(importReq),
		)
	}

	importReq.Status = "queued"
	importReq.CompletedAt = time.Time{}
	importReq.Error = ""
	job := NewJob(JobTypeTorrent, importReq)
	job.ID = torrent.InfoHash
	job.Entry = torrent
	if err := m.submitReservedJob(reservation, job); err != nil {
		torrent.MarkAsError(err)
		_ = m.queue.Update(torrent)
		return fmt.Errorf("failed to queue torrent: %w", err)
	}
	return nil
}

func newTorrentQueueEntry(importReq *ImportRequest, status debridTypes.TorrentStatus) *storage.Entry {
	now := time.Now()
	torrent := &storage.Entry{
		InfoHash:         importReq.Magnet.InfoHash,
		Name:             importReq.Magnet.Name,
		OriginalFilename: importReq.Magnet.Name,
		Protocol:         config.ProtocolTorrent,
		Size:             importReq.Magnet.Size,
		Bytes:            importReq.Magnet.Size,
		Magnet:           importReq.Magnet.Link,
		Category:         importReq.Arr.Name,
		SavePath:         filepath.Join(importReq.DownloadFolder, importReq.Arr.Name),
		ClientEndpoint:   importReq.ClientEndpoint,
		Status:           status,
		State:            storage.EntryStateDownloading,
		Progress:         0,
		Action:           importReq.Action,
		CallbackURL:      importReq.CallBackUrl,
		SkipMultiSeason:  importReq.SkipMultiSeason,
		CreatedAt:        now,
		UpdatedAt:        now,
		AddedOn:          now,
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Tags:             []string{},
	}
	// Persist the admission policy before provider work starts. ActiveProvider
	// doubles as the requested provider while Providers is still empty; once a
	// placement succeeds applyDebridTorrentToEntry replaces it with the actual
	// provider. This preserves explicit qBittorrent routing across a restart.
	torrent.ActiveProvider = importReq.SelectedDebrid
	if importReq.DownloadUncached != nil {
		torrent.DownloadUncached = *importReq.DownloadUncached
	}
	torrent.OutputName = storage.NewTorrentOutputName(torrent.Name, torrent.InfoHash)
	torrent.ContentPath = torrent.DownloadPath()
	return torrent
}

func isTooManyActiveDownloads(err error) bool {
	customErr, ok := errors.AsType[*customerror.Error](err)
	return ok && customErr.Code == "too_many_active_downloads"
}

func (m *Manager) processQueuedEntries(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	queueEntries := m.queue.ListFilter("", config.ProtocolAll, storage.EntryStateDownloading, nil, "", true)
	if len(queueEntries) == 0 {
		return
	}
	for _, entry := range queueEntries {
		if ctx.Err() != nil {
			return
		}
		// Parse only active downloading torrents
		if entry.State != storage.EntryStateDownloading {
			continue
		}
		if entry.Status == debridTypes.TorrentStatusQueued {
			continue
		}
		// Skip entries that are actively being downloading
		if entry.IsDownloading {
			continue
		}
		// Skip if a previous tick's goroutine hasn't finished yet for this hash.
		if _, loaded := m.processingEntries.LoadOrStore(entry.InfoHash, struct{}{}); loaded {
			continue
		}
		if entry.IsTorrent() {
			if entry.ActiveProvider != "" {
				if !m.startEntryBackground(ctx, "queued torrent processing", entry, func(workCtx context.Context) error {
					return m.processQueuedTorrent(workCtx, entry)
				}) {
					m.processingEntries.Delete(entry.InfoHash)
				}
			} else {
				m.processingEntries.Delete(entry.InfoHash)
			}
		} else if entry.IsNZB() {
			if !m.startEntryBackground(ctx, "queued NZB processing", entry, func(workCtx context.Context) error {
				return m.processQueuedNZB(workCtx, entry)
			}) {
				m.processingEntries.Delete(entry.InfoHash)
			}
		} else {
			m.processingEntries.Delete(entry.InfoHash)
		}
	}
}

func (m *Manager) processQueuedNZB(ctx context.Context, entry *storage.Entry) error {
	defer m.processingEntries.Delete(entry.InfoHash)
	if err := ctx.Err(); err != nil {
		return err
	}
	// Check if the nzb is already processed. Only header fields (status, file
	// list) are needed here; processNZB does not touch the segment map.
	metadata, err := m.usenet.GetNZBHeader(entry.InfoHash)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err != nil {
		m.logger.Error().Err(err).Str("name", entry.Name).Msg("Error getting NZB metadata")
		entry.MarkAsError(err)
		_ = m.queue.Update(entry)
		return nil
	}
	if metadata == nil {
		m.logger.Error().Str("name", entry.Name).Msg("NZB metadata not found")
		entry.MarkAsError(fmt.Errorf("nzb metadata not found"))
		_ = m.queue.Update(entry)
		return nil
	}
	switch metadata.Status {
	case usenet.NZBStatusFailed:
		m.logger.Error().Str("name", entry.Name).Msg("NZB processing failed")
		entry.MarkAsError(fmt.Errorf("nzb processing failed"))
		_ = m.queue.Update(entry)
		return nil
	case usenet.NZBStatusParsing, usenet.NZBStatusDownloading:
		// Still processing, skip for now
		return nil
	case usenet.NZBStatusCompleted:
		if err := m.processNZB(ctx, entry, metadata); err != nil {
			if errors.Is(err, errDeleteQueueEntryOnJobFinish) || errors.Is(err, context.Canceled) {
				return err
			}
			m.logger.Error().Err(err).Str("name", entry.Name).Msg("Error processing queued NZB")
			entry.MarkAsError(err)
			_ = m.queue.Update(entry)
			return nil
		}
	default:
		m.logger.Error().Str("name", entry.Name).Msgf("Unknown NZB status: %s", metadata.Status)
		entry.MarkAsError(fmt.Errorf("unknown nzb status: %s", metadata.Status))
		_ = m.queue.Update(entry)
		return nil
	}
	return nil
}

func providerReportedTransferError(torrent *debridTypes.Torrent) bool {
	return torrent != nil && torrent.Status == debridTypes.TorrentStatusError
}

func providerConfirmedTerminalTransfer(torrent *debridTypes.Torrent, err error) bool {
	return providerReportedTransferError(torrent) &&
		errors.Is(err, debridTypes.ErrTerminalProviderTorrent)
}

// freshUncachedProviderStatusProbe prevents a failed cached observation from being
// mistaken for fresh terminal evidence when the confirmation request itself
// fails before the provider can update the torrent. Provider identity and
// transfer policy stay intact; only observation fields are cleared.
func freshUncachedProviderStatusProbe(torrent *debridTypes.Torrent) *debridTypes.Torrent {
	if torrent == nil {
		return nil
	}
	probe := torrent.Copy()
	probe.Status = ""
	probe.ProviderState = ""
	probe.DownloadUncached = true
	return probe
}

func (m *Manager) processQueuedTorrent(ctx context.Context, entry *storage.Entry) error {
	defer m.processingEntries.Delete(entry.InfoHash)
	if err := ctx.Err(); err != nil {
		return err
	}
	placement := entry.GetActiveProvider()
	if placement == nil {
		m.logger.Error().Str("name", entry.Name).Msg("No active placement found for queued entry")
		entry.MarkAsError(fmt.Errorf("no active placement found"))
		_ = m.queue.Update(entry)
		return nil
	}
	stallKey := uncachedStallCandidateKey(entry)

	client := m.ProviderClient(entry.ActiveProvider)
	if client == nil {
		m.logger.Error().Str("debrid", entry.ActiveProvider).Msg("Provider client not found")
		entry.MarkAsError(fmt.Errorf("debrid client not found: %s", entry.ActiveProvider))
		_ = m.queue.Update(entry)
		return nil
	}

	magnet, err := utils.GetMagnetInfo(entry.Magnet, m.config.AlwaysRmTrackerUrls)
	if err != nil {
		magnet = utils.ConstructMagnet(entry.InfoHash, entry.Name)
	}

	arr := m.arr.GetOrCreate(entry.Category)

	debridTorrent := &debridTypes.Torrent{
		Id:               placement.ID,
		InfoHash:         entry.InfoHash,
		Magnet:           magnet,
		Name:             magnet.Name,
		Arr:              arr,
		Size:             entry.Size,
		Files:            make(map[string]debridTypes.File),
		DownloadUncached: entry.DownloadUncached,
	}

	dbT, err := checkProviderStatus(ctx, client, debridTorrent)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if entry.DownloadUncached && providerConfirmedTerminalTransfer(dbT, err) {
		// The provider's explicit terminal sentinel is the portable contract.
		// A normalized error status alone can also represent an unknown future
		// provider state, so require the sentinel and a cache-bypassing repeat
		// observation before handing the release back to Arr.
		m.clearUncachedStallCandidate(stallKey)
		m.uncachedFreshChecks.Add(1)
		freshSource := dbT
		if freshSource == nil {
			freshSource = debridTorrent
		}
		fresh, freshErr := checkFreshProviderStatus(
			ctx,
			client,
			freshUncachedProviderStatusProbe(freshSource),
		)
		if providerConfirmedTerminalTransfer(fresh, freshErr) {
			entry.TerminalChecks++
			if entry.TerminalChecks >= 2 {
				entry.MarkAsStalled(fmt.Errorf("provider confirmed terminal uncached transfer"))
				entry.GetActiveProvider().Status = debridTypes.TorrentStatusError
				entry.HandoffReason = "terminal"
				m.uncachedTerminalConfirmed.Add(1)
				m.logger.Warn().Str("name", entry.Name).Str("provider", entry.ActiveProvider).Msg("Confirmed terminal uncached transfer; waiting for Arr replacement handoff")
			} else {
				m.logger.Warn().Str("name", entry.Name).Msg("Provider reported terminal transfer state; awaiting second fresh confirmation")
			}
			return m.queue.Update(entry)
		}
		if freshErr != nil {
			m.logger.Warn().Err(freshErr).Str("name", entry.Name).Msg("Could not confirm provider transfer failure; retaining queued job")
			entry.TerminalChecks = 0
			return m.queue.Update(entry)
		}
		entry.TerminalChecks = 0
		dbT = fresh
		err = nil
	} else if entry.DownloadUncached && providerReportedTransferError(dbT) {
		// Keep unclassified provider states visible and retryable. In particular,
		// TorBox maps unknown future download_state values to StatusError so they
		// fail closed, but does not mark them terminal. Deleting the placement or
		// handing it to Arr here could replace a transfer that is still active.
		m.clearUncachedStallCandidate(stallKey)
		m.logger.Warn().
			Err(err).
			Str("name", entry.Name).
			Str("provider", entry.ActiveProvider).
			Str("provider_state", dbT.ProviderState).
			Msg("Provider returned unclassified uncached error state; retaining queued job")
		if entry.TerminalChecks != 0 {
			entry.TerminalChecks = 0
			return m.queue.Update(entry)
		}
		return nil
	} else if err != nil {
		if dbT == nil || dbT.Status != debridTypes.TorrentStatusError {
			m.clearUncachedStallCandidate(stallKey)
			m.logger.Warn().Err(err).Str("name", entry.Name).Msg("Provider status unavailable; retaining queued job for retry")
			if entry.TerminalChecks != 0 {
				entry.TerminalChecks = 0
				return m.queue.Update(entry)
			}
			return nil
		}
	}
	if err != nil && dbT != nil && dbT.Status == debridTypes.TorrentStatusError {
		if dbT.Id != "" {
			err = errors.Join(err, m.deleteProviderTorrent(client, dbT.Id))
		}
		m.logger.Error().Err(err).Str("name", entry.Name).Msg("Error checking status")
		entry.MarkAsError(err)
		_ = m.queue.Update(entry)
		return nil
	}

	debridTorrent = dbT

	if debridTorrent == nil {
		m.logger.Error().Str("name", entry.Name).Msg("Provider entry not found")
		entry.MarkAsError(fmt.Errorf("debrid entry not found"))
		_ = m.queue.Update(entry)
		return nil
	}
	entry.TerminalChecks = 0

	if err := m.validateResolvedTorrentNames(debridTorrent, entry.Action); err != nil {
		err = errors.Join(err, m.deleteProviderTorrent(client, debridTorrent.Id))
		m.logger.Error().Err(err).Str("name", entry.Name).Msg("Provider returned unsafe torrent name")
		entry.MarkAsError(err)
		_ = m.queue.Update(entry)
		return nil
	}

	if debridTorrent.Status == debridTypes.TorrentStatusError {
		m.clearUncachedStallCandidate(stallKey)
		m.logger.Error().
			Str("debrid", debridTorrent.Debrid).
			Str("name", debridTorrent.Name).
			Str("status", string(debridTorrent.Status)).
			Msg("Entry in error state")
		entry.MarkAsError(fmt.Errorf("entry in error state on debrid: %s", debridTorrent.Debrid))
		_ = m.queue.Update(entry)
		return nil
	}

	// Record observation before replacing Progress so unchanged successful polls
	// cannot masquerade as forward transfer progress.
	observedAt := time.Now()
	entry.ObserveTransfer(debridTorrent.Progress/100.0, observedAt)
	// Update entry progress
	entry.Progress = debridTorrent.Progress / 100.0
	entry.Speed = debridTorrent.Speed
	entry.Size = debridTorrent.GetSize()
	entry.Seeders = debridTorrent.Seeders
	entry.UpdatedAt = observedAt

	// Update placement progress
	if placement := entry.GetActiveProvider(); placement != nil {
		placement.Progress = entry.Progress
	}

	stallKind, stallTimeout := uncachedStallPolicy(
		debridTorrent.ProviderState,
		debridTorrent.Status,
		debridTorrent.Seeders,
		m.uncachedStallTimeout,
	)
	if stallKind != "" && uncachedTransferStalled(
		entry,
		debridTorrent.Status,
		observedAt,
		stallTimeout,
	) {
		m.uncachedFreshChecks.Add(1)
		fresh, freshErr := checkFreshProviderStatus(ctx, client, debridTorrent)
		if freshErr != nil || fresh == nil {
			m.clearUncachedStallCandidate(stallKey)
			m.logger.Warn().Err(freshErr).Str("name", entry.Name).Msg("Could not confirm uncached stall; retaining queued job")
			return m.queue.Update(entry)
		}
		freshKind, _ := uncachedStallPolicy(
			fresh.ProviderState,
			fresh.Status,
			fresh.Seeders,
			m.uncachedStallTimeout,
		)
		if fresh.Status != debridTypes.TorrentStatusDownloading || freshKind != stallKind || fresh.Speed > 0 || fresh.Progress/100.0 > entry.Progress {
			m.clearUncachedStallCandidate(stallKey)
			entry.ObserveTransfer(fresh.Progress/100.0, time.Now())
			entry.Progress = fresh.Progress / 100.0
			entry.Speed = fresh.Speed
			entry.TerminalChecks = 0
			return m.queue.Update(entry)
		}
		if fresh.Progress/100.0 < entry.Progress {
			// Contradictory progress snapshots are not evidence for removal.
			m.clearUncachedStallCandidate(stallKey)
			return m.queue.Update(entry)
		}
		if !m.confirmUncachedStall(stallKey, stallKind, time.Now()) {
			return m.queue.Update(entry)
		}
		stallErr := fmt.Errorf(
			"uncached provider transfer remained in %s without progress for %s",
			stallKind, stallTimeout,
		)
		entry.MarkAsStalled(stallErr)
		entry.HandoffReason = stallKind
		m.uncachedStallConfirmed.Add(1)
		if placement := entry.GetActiveProvider(); placement != nil {
			placement.Status = debridTypes.TorrentStatusError
		}
		m.logger.Warn().
			Str("debrid", entry.ActiveProvider).
			Str("name", entry.Name).
			Dur("no_progress_for", observedAt.Sub(*entry.LastProgressAt)).
			Msg("Uncached transfer stalled; waiting for Arr blocklist and replacement handoff")
		return m.queue.Update(entry)
	}
	m.clearUncachedStallCandidate(stallKey)

	// Check if done or failed.
	if debridTorrent.Status == debridTypes.TorrentStatusDownloaded {
		ready, err := applyCompletedTorrentFiles(entry, debridTorrent)
		if err != nil {
			return fmt.Errorf("reconcile completed provider files: %w", err)
		}
		if !ready {
			if err := m.queue.Update(entry); err != nil {
				return err
			}
			m.logger.Debug().
				Str("debrid", debridTorrent.Debrid).
				Str("name", debridTorrent.Name).
				Msg("Provider completed transfer before file links were ready; keeping entry queued")
			return nil
		}
		return m.processAction(ctx, entry)
	}
	return m.queue.Update(entry)
}

func (m *Manager) processAction(ctx context.Context, entry *storage.Entry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entry.Status = debridTypes.TorrentStatusDownloaded
	entry.UpdatedAt = time.Now()
	if err := m.queue.Update(entry); err != nil {
		return err
	}
	m.logger.Info().
		Str("name", entry.Name).
		Str("action", string(entry.Action)).
		Msg("Download completed, processing action")

	// Merge with existing entry if same infohash already exists (e.g., same
	// torrent on a different provider). The queue entry only knows about the
	// provider it was queued for, so we need to preserve other placements.
	if existing, err := m.storage.Get(entry.InfoHash); err == nil && existing != nil {
		if err := storage.ReconcileCompletedTorrentEntry(existing, entry); err != nil {
			return err
		}
		entry = storage.HandleExistingEntryMerge(existing, entry)
	}

	// Explicit same-key reimports are authorized only by the exact durable
	// queue incarnation created after the prior main entry was retired. This
	// transient bind cannot be minted by provider refresh or an old worker.
	if err := m.storage.PrepareQueuedReplacement(entry); err != nil {
		m.logger.Error().Err(err).Str("name", entry.Name).Msg("Failed to authorize completed download")
		entry.MarkAsError(err)
		_ = m.queue.Update(entry)
		return nil
	}

	// Now add entry to the main storage
	if err := m.AddOrUpdate(entry, func(t *storage.Entry) {
		m.RefreshEntries(true)
	}); err != nil {
		m.logger.Error().Err(err).Str("name", entry.Name).Msg("Failed to persist completed download")
		entry.MarkAsError(err)
		_ = m.queue.Update(entry)
		return nil
	}
	err := m.downloader.download(ctx, entry)
	if err != nil {
		if errors.Is(err, errDeleteQueueEntryOnJobFinish) {
			return err
		}
		m.logger.Error().
			Err(err).
			Str("name", entry.Name).
			Msg("Error running post-download action")
		entry.MarkAsError(err)
		_ = m.queue.Update(entry)
		return nil
	}
	return nil
}

// processTorrent handles the complete torrent lifecycle
func (m *Manager) processNewTorrent(ctx context.Context, torrent *storage.Entry, debridTorrent *debridTypes.Torrent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Update status to submitting
	torrent.UpdatedAt = time.Now()

	if debridTorrent.Status != debridTypes.TorrentStatusDownloaded {
		if err := applyDebridTorrentToEntry(torrent, debridTorrent); err != nil {
			return fmt.Errorf("apply provider transfer state: %w", err)
		}
		if err := m.queue.Update(torrent); err != nil {
			return err
		}
		m.logger.Info().
			Str("debrid", debridTorrent.Debrid).
			Str("name", debridTorrent.Name).
			Msg("Started downloading torrent")
		return nil
	}
	ready, err := applyCompletedTorrentFiles(torrent, debridTorrent)
	if err != nil {
		return fmt.Errorf("reconcile completed provider files: %w", err)
	}
	if !ready {
		// Keep the entry in the existing cancellation-aware queue lifecycle. A
		// later provider status check will apply the completed file snapshot
		// above instead of permanently failing an otherwise healthy grab.
		torrent.Status = debridTypes.TorrentStatusDownloading
		if err := m.queue.Update(torrent); err != nil {
			return err
		}
		m.logger.Debug().
			Str("debrid", debridTorrent.Debrid).
			Str("name", debridTorrent.Name).
			Msg("Provider completed transfer before file links were ready; keeping entry queued")
		return nil
	}

	return m.processAction(ctx, torrent)
}

// applyCompletedTorrentFiles reconciles the authoritative provider snapshot
// into an entry and reports whether every provider file has a usable link. A
// completed transfer with an empty or partially populated file tree is an
// eventual-consistency state, not a completed Tessarr entry. Partial trees
// update provider state but never leak into the canonical file map.
func applyCompletedTorrentFiles(entry *storage.Entry, torrent *debridTypes.Torrent) (bool, error) {
	if entry == nil || torrent == nil || torrent.Status != debridTypes.TorrentStatusDownloaded {
		return false, nil
	}
	if !isComplete(torrent.Files) {
		if err := applyIncompleteDebridTorrentState(entry, torrent); err != nil {
			return false, err
		}
		entry.Status = debridTypes.TorrentStatusDownloading
		return false, nil
	}
	if err := applyDebridTorrentToEntry(entry, torrent); err != nil {
		return false, err
	}
	return true, nil
}

func applyDebridTorrentToEntry(torrent *storage.Entry, debridTorrent *debridTypes.Torrent) error {
	if err := applyDebridTorrentState(torrent, debridTorrent); err != nil {
		return err
	}
	if len(torrent.Files) != 0 {
		return nil
	}
	for _, file := range debridTorrent.GetFiles() {
		torrent.Files[file.Name] = &storage.File{
			Name:      file.Name,
			Path:      file.LocalPath(),
			Size:      file.Size,
			ByteRange: file.ByteRange,
			Deleted:   file.Deleted,
			InfoHash:  torrent.InfoHash,
			AddedOn:   torrent.AddedOn,
		}
	}
	return nil
}

func applyDebridTorrentState(torrent *storage.Entry, debridTorrent *debridTypes.Torrent) error {
	debridTorrent = preserveMaterializedTorrentTitle(torrent, debridTorrent)
	if _, err := torrent.AddTorrentProvider(debridTorrent); err != nil {
		return err
	}
	applyDebridTorrentMetadata(torrent, debridTorrent)

	if debridTorrent.Status != debridTypes.TorrentStatusDownloaded {
		return nil
	}
	if placement := torrent.GetActiveProvider(); placement != nil {
		now := time.Now()
		placement.DownloadedAt = &now
		placement.Progress = 1.0
	}
	return nil
}

func applyIncompleteDebridTorrentState(torrent *storage.Entry, debridTorrent *debridTypes.Torrent) error {
	debridTorrent = preserveMaterializedTorrentTitle(torrent, debridTorrent)
	if _, err := torrent.UpdateTorrentProviderState(debridTorrent); err != nil {
		return err
	}
	applyDebridTorrentMetadata(torrent, debridTorrent)
	return nil
}

func preserveMaterializedTorrentTitle(entry *storage.Entry, remote *debridTypes.Torrent) *debridTypes.Torrent {
	if entry.CanApplyTorrentTitle(remote) {
		return remote
	}
	// The provider's current title is not allowed to reinterpret paths already
	// exposed to an Arr. Keep the existing owned output identity stable.
	stable := remote.Copy()
	stable.Name = ""
	stable.OriginalFilename = ""
	return stable
}

func applyDebridTorrentMetadata(torrent *storage.Entry, debridTorrent *debridTypes.Torrent) {
	torrent.ActiveProvider = debridTorrent.Debrid
	if size := debridTorrent.GetSize(); size > 0 || torrent.Size <= 0 {
		torrent.Bytes = size
		torrent.Size = size
	}
	if debridTorrent.Name != "" {
		torrent.Name = debridTorrent.Name
	}
	if debridTorrent.OriginalFilename != "" {
		torrent.OriginalFilename = debridTorrent.OriginalFilename
	}
	torrent.UpdatedAt = time.Now()
}

// SendToDebrid submits a magnet to debrid service(s) - replaces debrid.Parse
func (m *Manager) SendToDebrid(ctx context.Context, importRequest *ImportRequest) (*debridTypes.Torrent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := m.validateTorrentImportRequest(importRequest); err != nil {
		return nil, err
	}
	clients := m.FilterDebrid(func(c common.Client) bool {
		if importRequest.SelectedDebrid != "" && c.Config().Name != importRequest.SelectedDebrid {
			return false
		}
		return true
	})

	if len(clients) == 0 {
		return nil, fmt.Errorf("no debrid clients available")
	}

	errs := make([]error, 0, len(clients)*2)
	cacheMisses := make(map[string]bool, len(clients))

	// Cached-first is a property of the complete provider set, not each
	// provider in isolation. Trying one provider with uncached downloads enabled
	// before asking the next provider for a cached placement can start needless
	// upstream work. Pass one therefore forces cached-only submission everywhere.
	// Only providers that report a definite cache miss and are explicitly allowed
	// to download uncached are eligible for pass two.
	for pass := 0; pass < 2; pass++ {
		for _, db := range clients {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			providerConfig := db.Config()
			providerName := providerConfig.Name
			if providerName == "" {
				providerName = providerConfig.Provider
			}
			if providerName == "" {
				providerName = "unnamed provider"
			}
			_logger := db.Logger()
			downloadUncachedAllowed := false
			if importRequest.DownloadUncached != nil {
				downloadUncachedAllowed = *importRequest.DownloadUncached
			} else {
				downloadUncachedAllowed = providerConfig.DownloadUncached
			}
			if pass == 1 && (!downloadUncachedAllowed || !cacheMisses[providerName]) {
				continue
			}
			if rejectionErr := m.cachedSubmissionRejection(providerName, importRequest.Magnet.InfoHash); rejectionErr != nil {
				_logger.Warn().
					Str("Provider", providerName).
					Str("Hash", importRequest.Magnet.InfoHash).
					Msg("Skipping provider during content-rejection cooldown")
				errs = append(errs, fmt.Errorf("%s submission: %w", providerName, rejectionErr))
				continue
			}
			overrideDownloadUncached := pass == 1
			// Providers are allowed to populate the torrent in place. Construct each
			// fallback candidate from source fields so an earlier failed provider
			// cannot leak state into the next attempt. Do not copy Torrent: it owns
			// synchronization state used by its file map.
			debridTorrent := &debridTypes.Torrent{
				InfoHash:         importRequest.Magnet.InfoHash,
				Magnet:           importRequest.Magnet,
				Name:             importRequest.Magnet.Name,
				Arr:              importRequest.Arr,
				Size:             importRequest.Magnet.Size,
				Files:            make(map[string]debridTypes.File),
				DownloadUncached: overrideDownloadUncached,
			}
			passLabel := "cached"
			if pass == 1 {
				passLabel = "uncached"
			}
			policySource := uncachedPolicySource(importRequest)
			_logger.Info().
				Str("Provider", providerName).
				Str("Arr", importRequest.Arr.Name).
				Str("Hash", debridTorrent.InfoHash).
				Str("Name", debridTorrent.Name).
				Str("Action", string(importRequest.Action)).
				Str("pass", passLabel).
				Str("policy_source", policySource).
				Bool("uncached_allowed", downloadUncachedAllowed).
				Msg("Processing torrent")

			dbt, err := submitProviderMagnet(ctx, db, debridTorrent)
			if ctxErr := ctx.Err(); ctxErr != nil {
				if dbt != nil && dbt.Id != "" {
					ctxErr = errors.Join(ctxErr, m.deleteProviderTorrent(db, dbt.Id))
				}
				return nil, ctxErr
			}
			if err != nil || dbt == nil || dbt.Id == "" {
				if err == nil {
					err = fmt.Errorf("provider returned an incomplete submission response")
				} else {
					if pass == 0 && isTorrentNotCachedError(err) {
						cacheMisses[providerName] = true
					}
					m.recordSubmissionRejection(
						providerName,
						importRequest.Magnet.InfoHash,
						importRequest.Magnet.Name,
						err,
					)
				}
				errs = append(errs, fmt.Errorf("%s submission: %w", providerName, err))
				continue
			}
			dbt.Arr = importRequest.Arr
			_logger.Info().Str("id", dbt.Id).Msgf("Entry: %s submitted to %s", dbt.Name, providerName)

			torrent, err := checkProviderStatus(ctx, db, dbt)
			if ctxErr := ctx.Err(); ctxErr != nil {
				rollbackID := dbt.Id
				if torrent != nil && torrent.Id != "" {
					rollbackID = torrent.Id
				}
				return nil, errors.Join(ctxErr, m.deleteProviderTorrent(db, rollbackID))
			}
			if err != nil {
				if pass == 0 && isTorrentNotCachedError(err) {
					cacheMisses[providerName] = true
				}
				m.recordSubmissionRejection(
					providerName,
					importRequest.Magnet.InfoHash,
					importRequest.Magnet.Name,
					err,
				)
				rollbackID := dbt.Id
				if torrent != nil && torrent.Id != "" {
					rollbackID = torrent.Id
				}
				rollbackErr := m.deleteProviderTorrent(db, rollbackID)
				errs = append(errs, errors.Join(
					fmt.Errorf("%s status check: %w", providerName, err),
					rollbackErr,
				))
				continue
			}
			if torrent == nil {
				statusErr := fmt.Errorf("%s returned nil after checking torrent %s status", providerName, dbt.Name)
				rollbackErr := m.deleteProviderTorrent(db, dbt.Id)
				errs = append(errs, errors.Join(statusErr, rollbackErr))
				continue
			}
			if err := m.validateResolvedTorrentNames(torrent, importRequest.Action); err != nil {
				rollbackErr := m.deleteProviderTorrent(db, torrent.Id)
				errs = append(errs, errors.Join(
					fmt.Errorf("%s returned an unsafe torrent name: %w", providerName, err),
					rollbackErr,
				))
				continue
			}
			return torrent, nil
		}
	}
	if len(errs) == 0 {
		return nil, fmt.Errorf("failed to process torrent: no clients available")
	}
	joinedErrors := errors.Join(errs...)
	return nil, fmt.Errorf("failed to process torrent: %w", joinedErrors)
}

// uncachedPolicySource returns the origin of the effective uncached policy for
// operational logs. API imports carry an explicit request-time override;
// qBittorrent imports carry the selected Arr's tri-state policy; nil inherits
// the provider configuration.
func uncachedPolicySource(importRequest *ImportRequest) string {
	if importRequest == nil || importRequest.DownloadUncached == nil {
		return "provider"
	}
	if importRequest.Type == ImportTypeAPI {
		return "request"
	}
	return "arr"
}

func isTorrentNotCachedError(err error) bool {
	if err == nil {
		return false
	}
	var providerErr *customerror.Error
	return errors.As(err, &providerErr) && providerErr.Code == "torrent_not_cached"
}
