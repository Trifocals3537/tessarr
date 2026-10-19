package manager

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/Trifocals3537/tessarr/pkg/usenet"
	"github.com/Trifocals3537/tessarr/pkg/usenet/parser"
)

var usenetSubmissionMu sync.Mutex

// ErrQueueAddAmbiguous means a queue write failed without proving that the
// record is absent. The NZB metadata and directory claim are deliberately
// preserved so a visible or crash-recoverable record never loses dependencies.
var ErrQueueAddAmbiguous = errors.New("NZB queue write outcome is ambiguous; artifacts preserved")

type queueEntryLookup func(string) (*storage.Entry, error)

// inspectFailedQueueAdd returns true when dependent state must be preserved.
// Only the typed queue-not-found error is authoritative enough to permit
// rollback. A record must also describe the expected NZB owner; a key collision
// or corrupt record is ambiguous and is preserved for operator recovery.
func inspectFailedQueueAdd(expected *storage.Entry, lookup queueEntryLookup) (bool, error) {
	if expected == nil || lookup == nil {
		return true, fmt.Errorf("%w: queue reconciliation input is nil", ErrQueueAddAmbiguous)
	}
	current, err := lookup(expected.InfoHash)
	if err != nil {
		if storage.IsQueuedEntryNotFound(err) {
			return false, nil
		}
		return true, fmt.Errorf("%w: verify queue record: %v", ErrQueueAddAmbiguous, err)
	}
	if current == nil {
		return true, fmt.Errorf("%w: queue lookup returned a nil record", ErrQueueAddAmbiguous)
	}
	expectedOwner := expected.GetActiveProvider()
	currentOwner := current.GetActiveProvider()
	if !strings.EqualFold(current.InfoHash, expected.InfoHash) ||
		!current.IsNZB() ||
		!strings.EqualFold(current.ActiveProvider, expected.ActiveProvider) ||
		expectedOwner == nil ||
		currentOwner == nil ||
		!strings.EqualFold(currentOwner.Provider, expectedOwner.Provider) ||
		currentOwner.ID != expectedOwner.ID ||
		currentOwner.ID != expected.InfoHash {
		return true, fmt.Errorf(
			"%w: queue key %q belongs to a different record",
			ErrQueueAddAmbiguous,
			expected.InfoHash,
		)
	}
	return true, fmt.Errorf("%w: matching queue record is visible", ErrQueueAddAmbiguous)
}

// AddNewNZB parses an NZB before entering the active-download queue.
func (m *Manager) AddNewNZB(ctx context.Context, req *ImportRequest) (string, error) {
	if m.usenet == nil {
		return "", fmt.Errorf("usenet not configured")
	}
	if req == nil || len(req.NZBContent) == 0 {
		return "", fmt.Errorf("NZB content is empty")
	}
	if req.Arr == nil {
		return "", fmt.Errorf("arr is required")
	}
	downloadRoot, err := requireConfiguredUsenetDownloadRoot(m.config.DownloadFolder, req.DownloadFolder)
	if err != nil {
		return "", err
	}
	if _, _, err := usenetEntryPaths(downloadRoot, req.Arr.Name, req.Name); err != nil {
		return "", err
	}
	reservation, err := m.reserveJob(ctx, req.Id)
	if err != nil {
		return "", err
	}
	defer reservation.release()

	m.logger.Info().
		Str("name", req.Name).
		Str("category", req.Arr.Name).
		Msg("Adding new NZB to usenet")

	// Persist the exact ID-bound source before parsing or exposing a queue row.
	// A restart can therefore resume the same deterministic watcher ID without
	// trusting the mutable importing filename alone.
	stagedPath, err := m.usenet.StageNZB(req.Id, req.NZBContent)
	if err != nil {
		return "", fmt.Errorf("stage NZB source: %w", err)
	}
	cleanupRejected := func(id string) error {
		return errors.Join(
			m.usenet.Delete(id),
			m.usenet.RemoveStagedNZB(id, stagedPath),
		)
	}

	meta, groups, err := m.usenet.ParseWithID(
		reservation.Context(),
		req.Id,
		req.Name,
		req.NZBContent,
		req.Arr.Name,
	)
	if err != nil {
		parseErr := fmt.Errorf("usenet parse failed: %w", err)
		if cleanupErr := cleanupRejected(req.Id); cleanupErr != nil {
			return "", errors.Join(parseErr, fmt.Errorf("cleanup rejected NZB state: %w", cleanupErr))
		}
		return "", parseErr
	}
	savePath, downloadPath, err := usenetEntryPaths(downloadRoot, req.Arr.Name, meta.Name)
	if err != nil {
		cleanupErr := cleanupRejected(meta.ID)
		if cleanupErr != nil {
			return "", errors.Join(err, fmt.Errorf("cleanup rejected NZB state: %w", cleanupErr))
		}
		return "", err
	}

	entry := &storage.Entry{
		InfoHash:         meta.ID,
		Name:             meta.Name,
		OriginalFilename: meta.Name,
		OutputName:       filepath.Base(downloadPath),
		Size:             meta.TotalSize,
		Protocol:         config.ProtocolNZB,
		Bytes:            meta.TotalSize,
		Magnet:           stagedPath,
		Category:         req.Arr.Name,
		SavePath:         savePath,
		Status:           debridTypes.TorrentStatusDownloading,
		State:            storage.EntryStateDownloading,
		Progress:         0,
		Action:           req.Action,
		CallbackURL:      req.CallBackUrl,
		SkipMultiSeason:  req.SkipMultiSeason,
		CreatedAt:        time.Now(),
		UpdatedAt:        time.Now(),
		AddedOn:          time.Now(),
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Tags:             []string{},
	}

	entry.ContentPath = downloadPath
	entry.ActiveProvider = "usenet"
	_ = entry.AddUsenetProvider(meta)

	// Serialize the claim-plus-queue transaction so a same-ID concurrent
	// submission cannot begin relying on a newly created marker while another
	// submission is rolling it back.
	usenetSubmissionMu.Lock()
	_, newlyClaimed, claimErr := claimUsenetEntryDirectory(downloadRoot, entry)
	var queueErr error
	if claimErr == nil {
		queueErr = m.queue.Add(entry)
	}
	var rollbackErr error
	var queueStateErr error
	preserveQueueState := false
	if queueErr != nil {
		preserveQueueState, queueStateErr = inspectFailedQueueAdd(entry, m.queue.GetTorrent)
		if !preserveQueueState && newlyClaimed {
			rollbackErr = rollbackUsenetEntryClaim(downloadRoot, entry)
		}
	}
	usenetSubmissionMu.Unlock()

	if claimErr != nil {
		cleanupErr := cleanupRejected(meta.ID)
		if cleanupErr != nil {
			return "", errors.Join(
				fmt.Errorf("failed to claim NZB release directory: %w", claimErr),
				fmt.Errorf("cleanup rejected NZB state: %w", cleanupErr),
			)
		}
		return "", fmt.Errorf("failed to claim NZB release directory: %w", claimErr)
	}
	if queueErr != nil {
		queueFailure := fmt.Errorf("failed to add nzb to queue: %w", queueErr)
		if preserveQueueState {
			return meta.ID, errors.Join(queueFailure, queueStateErr)
		}
		if rollbackErr != nil {
			return "", errors.Join(queueFailure, fmt.Errorf("rollback NZB directory claim: %w", rollbackErr))
		}
		if cleanupErr := cleanupRejected(meta.ID); cleanupErr != nil {
			return "", errors.Join(queueFailure, fmt.Errorf("cleanup unqueued NZB state: %w", cleanupErr))
		}
		return "", queueFailure
	}

	req.Status = "started"
	job := NewJob(JobTypeNZB, req)
	job.ID = entry.InfoHash
	job.Entry = entry
	job.NZBMeta = meta
	job.NZBGroups = groups
	if err := m.submitReservedJob(reservation, job); err != nil {
		entry.MarkAsError(err)
		_ = m.queue.Update(entry)
		return "", fmt.Errorf("failed to queue NZB: %w", err)
	}
	return meta.ID, nil
}

func (m *Manager) processNZBJob(ctx context.Context, job *Job) error {
	if job == nil || job.Entry == nil {
		return fmt.Errorf("invalid NZB job")
	}
	if _, err := m.queue.GetTorrent(job.Entry.InfoHash); err != nil {
		return nil
	}
	if job.NZBMeta == nil {
		if job.Request == nil {
			m.waitForDownloadCompletion(ctx, job.Entry)
			return nil
		}
		return fmt.Errorf("parsed NZB metadata missing")
	}
	if job.Request != nil {
		job.Request.Status = "started"
	}
	return m.processNewNzb(ctx, job.Entry, job.NZBMeta, job.NZBGroups)
}

func (m *Manager) processNZB(ctx context.Context, entry *storage.Entry, metadata *storage.NZB) error {
	if _, err := safeUsenetEntryDownloadPath(m.config.DownloadFolder, entry); err != nil {
		return fmt.Errorf("unsafe usenet download path: %w", err)
	}
	// Add files using logical streamable files
	for _, file := range metadata.Files {
		if _, err := safeUsenetFilePath(m.config.DownloadFolder, entry, file.Name); err != nil {
			return err
		}
		tFile := &storage.File{
			Name:     file.Name,
			Size:     file.Size,
			InfoHash: entry.InfoHash,
			AddedOn:  entry.AddedOn,
		}
		entry.Files[file.Name] = tFile
	}
	// Mark as complete
	if placement := entry.GetActiveProvider(); placement != nil {
		now := time.Now()
		placement.DownloadedAt = &now
		placement.Progress = 1.0
	}
	entry.Size = metadata.TotalSize
	entry.Progress = 1.0
	entry.UpdatedAt = time.Now()
	_ = m.queue.Update(entry)

	if len(entry.Files) == 0 {
		return fmt.Errorf("nzb has no files")
	}

	return m.processAction(ctx, entry)
}

// processNewNzb processes a new NZB entry after it has been added to the usenet client
func (m *Manager) processNewNzb(parentCtx context.Context, entry *storage.Entry, metadata *storage.NZB, groups map[string]*parser.FileGroup) error {
	// Create context with timeout for processing
	ctx, cancel := context.WithTimeout(parentCtx, m.usenetTimeout)
	defer cancel()

	updatedNZB, err := m.usenet.Process(ctx, metadata, groups)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return fmt.Errorf("usenet processing timed out after %s: %w", m.usenetTimeout, err)
		}
		return fmt.Errorf("failed to process nzb: %w", err)
	}

	metadata = updatedNZB
	return m.processNZB(ctx, entry, metadata)
}

// HasUsenet returns true if usenet is configured
func (m *Manager) HasUsenet() bool {
	return m.usenet != nil
}

// UsenetStats returns usenet client statistics
func (m *Manager) UsenetStats() map[string]any {
	if m.usenet == nil {
		return nil
	}
	return m.usenet.Stats()
}

// SpeedTestRequest represents a speed test request payload
type SpeedTestRequest struct {
	Protocol string `json:"protocol"` // "nntp" or "debrid"
	Provider string `json:"provider"` // provider host/identifier
}

// SpeedTestResponse represents a speed test result
type SpeedTestResponse struct {
	Provider  string  `json:"provider"`
	Protocol  string  `json:"protocol"`
	SpeedMBps float64 `json:"speed_mbps"`
	LatencyMs int64   `json:"latency_ms"`
	BytesRead int64   `json:"bytes_read"`
	TestedAt  string  `json:"tested_at"`
	Error     string  `json:"error,omitempty"`
}

// SpeedTest runs a speed test for a specific provider based on protocol
func (m *Manager) SpeedTest(ctx context.Context, req SpeedTestRequest) SpeedTestResponse {
	switch req.Protocol {
	case "nntp":
		if m.usenet == nil {
			return SpeedTestResponse{
				Provider: req.Provider,
				Protocol: req.Protocol,
				Error:    "usenet not configured",
			}
		}
		result := m.usenet.SpeedTest(ctx, req.Provider)
		return SpeedTestResponse{
			Provider:  result.Provider,
			Protocol:  req.Protocol,
			SpeedMBps: result.SpeedMBps,
			LatencyMs: result.LatencyMs,
			BytesRead: result.BytesRead,
			TestedAt:  result.TestedAt.Format("2006-01-02T15:04:05Z07:00"),
			Error:     result.Error,
		}
	case "debrid":
		// Look up debrid client by provider name
		client, exists := m.clients.Load(req.Provider)
		if !exists {
			return SpeedTestResponse{
				Provider: req.Provider,
				Protocol: req.Protocol,
				Error:    "debrid provider not found: " + req.Provider,
			}
		}
		result := client.SpeedTest(ctx)

		// Store the result for persistence (so it shows up in stats)
		if result.Error == "" {
			m.debridSpeedTestResults.Store(req.Provider, result)
		}

		return SpeedTestResponse{
			Provider:  result.Provider,
			Protocol:  req.Protocol,
			SpeedMBps: result.SpeedMBps,
			LatencyMs: result.LatencyMs,
			BytesRead: result.BytesRead,
			TestedAt:  result.TestedAt.Format("2006-01-02T15:04:05Z07:00"),
			Error:     result.Error,
		}
	default:
		return SpeedTestResponse{
			Provider: req.Provider,
			Protocol: req.Protocol,
			Error:    "unknown protocol: " + req.Protocol,
		}
	}
}

func (m *Manager) syncNZBs(ctx context.Context) error {
	if m.usenet == nil {
		return nil
	}

	m.nzbSyncMu.Lock()
	defer m.nzbSyncMu.Unlock()

	claimResult, claimErr := m.usenet.ClaimNewNZBsBounded(
		usenet.DefaultClaimNewNZBLimits(),
	)
	var errs []error
	if claimErr != nil {
		errs = append(errs, fmt.Errorf("claim watched NZBs: %w", claimErr))
	}
	metadataRoot := filepath.Join(config.GetMainPath(), "usenet", "nzbs")
	for _, pending := range claimResult.Pending {
		if ctx.Err() != nil {
			return errors.Join(errors.Join(errs...), ctx.Err())
		}
		if err := m.syncWatchedNZB(ctx, metadataRoot, pending); err != nil {
			m.logger.Error().Err(err).Str("name", pending.Name).Msg("Failed to queue watched NZB")
			errs = append(errs, fmt.Errorf("sync watched NZB %q: %w", pending.Name, err))
		}
	}

	// Terminal cleanup is deliberately separate and bounded. A failed removal
	// never makes an accepted file eligible for resubmission.
	cleanupResult, cleanupErr := m.usenet.CleanupAcceptedNZBs(
		usenet.DefaultAcceptedNZBCleanupLimits(),
	)
	if cleanupErr != nil {
		m.logger.Warn().
			Err(cleanupErr).
			Int("failed", cleanupResult.Failed).
			Msg("Accepted watched NZB cleanup left recoverable tombstones")
	}
	return errors.Join(errs...)
}

func (m *Manager) syncWatchedNZB(
	ctx context.Context,
	metadataRoot string,
	pending usenet.PendingNZB,
) error {
	identity, err := newWatchedNZBIdentity(pending.Name, pending.Content)
	if err != nil {
		return err
	}
	if pending.ContentDigest != identity.ContentDigest ||
		pending.Size != int64(len(pending.Content)) {
		return fmt.Errorf(
			"%w: claimed snapshot identity changed before reconciliation",
			errWatchedNZBStateAmbiguous,
		)
	}

	reconcile := func() (watchedNZBReconciliationState, error) {
		return reconcileWatchedNZBState(
			identity,
			metadataRoot,
			usenet.DefaultWatchedNZBMaxFileBytes,
			m.queue.GetTorrent,
			m.GetEntry,
			m.usenet.GetNZBHeader,
			storage.IsQueuedEntryNotFound,
			storage.IsEntryNotFound,
			usenet.IsNZBNotFound,
		)
	}
	submit := func() (string, error) {
		req := NewNZBRequest(
			pending.Name,
			m.config.DownloadFolder,
			pending.Content,
			m.arr.GetOrCreate(""),
			config.DownloadActionNone,
			"",
			ImportTypeWatch,
			false,
		)
		req.Id = identity.ID
		return m.AddNewNZB(ctx, req)
	}
	accept := func() (string, error) {
		return m.usenet.AcceptClaimedNZB(
			pending.Path,
			identity.ContentDigest,
			usenet.DefaultWatchedNZBMaxFileBytes,
		)
	}
	cleanup := func(acceptedPath string) {
		if err := m.usenet.RemoveAcceptedNZB(
			acceptedPath,
			usenet.DefaultWatchedNZBMaxFileBytes,
		); err != nil {
			m.logger.Warn().
				Err(err).
				Str("path", acceptedPath).
				Msg("Accepted watched NZB tombstone retained for bounded cleanup")
		}
	}
	return runWatchedNZBTransaction(
		identity,
		reconcile,
		submit,
		m.queue.Sync,
		accept,
		cleanup,
	)
}
