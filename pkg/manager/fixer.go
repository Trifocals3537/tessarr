package manager

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
)

// Fixer handles torrent repair with cascading re-insertion across debrids
type Fixer struct {
	manager *Manager

	// Track re-insertion attempts and failures
	failedToReinsert   *xsync.Map[string, struct{}]      // infohash:debrid -> failed completely
	inFlightRepairs    *xsync.Map[string, *FixerRequest] // infohash -> repair request
	providerOrder      []string                          // Order of providers to try (from config)
	maxReinsertRetries int
}

// FixerRequest tracks an ongoing repair operation
type FixerRequest struct {
	InfoHash         string
	CurrentDebrid    string
	AttemptedDebrids []string
	StartedAt        time.Time
	LastAttempt      time.Time
	done             chan struct{}
	result           *FixResult
	err              error
}

// FixResult is the result of a fix operation
type FixResult struct {
	Success       bool
	NewDebrid     string
	Error         error
	AttemptsCount int
}

// NewFixer creates a new Fixer instance
func NewFixer(manager *Manager) *Fixer {
	// GetReader debrid order from config
	cfg := config.Get()
	debridOrder := make([]string, 0, len(cfg.Debrids))
	for _, d := range cfg.Debrids {
		debridOrder = append(debridOrder, d.Name)
	}

	return &Fixer{
		manager:            manager,
		failedToReinsert:   xsync.NewMap[string, struct{}](),
		inFlightRepairs:    xsync.NewMap[string, *FixerRequest](),
		providerOrder:      debridOrder,
		maxReinsertRetries: 2, // retry each debrid up to 2 times
	}
}

// FixTorrent attempts to fix a broken torrent by re-inserting across debrids
// Strategy:
// 1. Try to re-insert on current active debrid, except if skipCurrent is true
// 2. If fails, cascade through other debrids in config order
// 3. Skip debrids where torrent already exists (unless they're also broken)
// 4. Mark as completely failed if all debrids fail
func (f *Fixer) FixTorrent(
	ctx context.Context,
	entry *storage.Entry,
	skipCurrent bool,
) (finalResult *FixResult, finalErr error) {
	if entry == nil {
		return nil, fmt.Errorf("entry is nil")
	}
	if !entry.CanBeFixed() {
		return &FixResult{
			Success:       false,
			Error:         fmt.Errorf("entry %s cannot be fixed", entry.Name),
			AttemptsCount: 0,
		}, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	// Claim the repair atomically. Load followed by Store permits two callers to
	// miss each other and both mutate the same provider placement. Waiters join
	// the winning request and observe its broadcast result without consuming it.
	req, joined := f.claimRepair(entry)
	if joined {
		return req.wait(ctx, entry.Name)
	}
	defer func() {
		req.complete(finalResult, finalErr)
		f.inFlightRepairs.Delete(entry.InfoHash)
	}()

	// Build debrid attempt order: current debrid first, then others in config order
	attemptOrder := f.buildAttemptOrder(entry, skipCurrent)

	var lastErr error
	totalAttempts := 0

	for _, debridName := range attemptOrder {
		// Check if entry has been marked as failed to re-insert
		if f.IsFailedToReinsert(entry.InfoHash, debridName) {
			continue
		}

		select {
		case <-ctx.Done():
			result := &FixResult{Success: false, Error: ctx.Err(), AttemptsCount: totalAttempts}
			return result, ctx.Err()
		default:
		}

		req.AttemptedDebrids = append(req.AttemptedDebrids, debridName)
		req.LastAttempt = time.Now()

		f.manager.logger.Trace().
			Str("debrid", debridName).
			Str("infohash", entry.InfoHash).
			Str("name", entry.Name).
			Int("attempt", totalAttempts+1).
			Msg("Attempting re-insertion")

		// Force a fresh submit only for the broken active provider; for any other
		// debrid, let MoveTorrent reuse an existing valid placement if present.
		reinsert := debridName == entry.ActiveProvider
		success, err := f.MoveTorrentContext(ctx, entry, debridName, reinsert)
		totalAttempts++

		if success {
			f.manager.logger.Info().
				Str("debrid", debridName).
				Str("name", entry.Name).
				Str("infohash", entry.InfoHash).
				Msg("Successfully re-inserted entry")

			// Mark as successful
			f.ResetFailureState(entry.InfoHash)

			result := &FixResult{
				Success:       true,
				NewDebrid:     debridName,
				Error:         nil,
				AttemptsCount: totalAttempts,
			}
			return result, nil
		}

		lastErr = err
		// Add failed state for this debrid
		f.failedToReinsert.Store(fmt.Sprintf("%s:%s", entry.InfoHash, debridName), struct{}{})
	}

	// All debrids failed - mark as completely failed
	f.manager.logger.Error().
		Err(lastErr).
		Str("infohash", entry.InfoHash).
		Int("attempts", totalAttempts).
		Msg("All re-insertion attempts failed")

	f.failedToReinsert.Store(entry.InfoHash, struct{}{})

	// Mark entry as bad
	entry.Bad = true
	entry.UpdatedAt = time.Now()
	_ = f.manager.AddOrUpdate(entry, func(t *storage.Entry) {
		f.manager.RefreshEntries(true)
	})

	result := &FixResult{
		Success:       false,
		Error:         fmt.Errorf("all re-insertion attempts failed: %w", lastErr),
		AttemptsCount: totalAttempts,
	}
	return result, result.Error
}

func (f *Fixer) claimRepair(entry *storage.Entry) (*FixerRequest, bool) {
	now := time.Now()
	candidate := &FixerRequest{
		InfoHash:         entry.InfoHash,
		CurrentDebrid:    entry.ActiveProvider,
		AttemptedDebrids: make([]string, 0),
		StartedAt:        now,
		LastAttempt:      now,
		done:             make(chan struct{}),
	}
	return f.inFlightRepairs.LoadOrStore(entry.InfoHash, candidate)
}

func (r *FixerRequest) wait(ctx context.Context, entryName string) (*FixResult, error) {
	if r == nil || r.done == nil {
		return nil, fmt.Errorf("repair request for %s is invalid", entryName)
	}
	timer := time.NewTimer(5 * time.Minute)
	defer timer.Stop()
	select {
	case <-r.done:
		return r.result, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, fmt.Errorf("repair timeout for %s", entryName)
	}
}

func (r *FixerRequest) complete(result *FixResult, err error) {
	if result == nil && err == nil {
		err = errors.New("repair completed without a result")
	}
	r.result = result
	r.err = err
	close(r.done)
}

// MoveTorrent attempts to re-insert a torrent on a specific debrid
func (f *Fixer) MoveTorrent(entry *storage.Entry, debridName string, reinsert bool) (bool, error) {
	return f.MoveTorrentContext(context.Background(), entry, debridName, reinsert)
}

// MoveTorrentContext attempts to re-insert a torrent on a specific debrid and
// stops provider work when its repair request is canceled.
func (f *Fixer) MoveTorrentContext(ctx context.Context, entry *storage.Entry, debridName string, reinsert bool) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	// Check if entry can be moved
	if entry == nil {
		return false, fmt.Errorf("entry is nil")
	}
	if !entry.CanBeMoved() {
		return false, fmt.Errorf("entry %s cannot be moved", entry.Name)
	}

	client := f.manager.ProviderClient(debridName)
	if client == nil {
		return false, fmt.Errorf("debrid client %s not found", debridName)
	}

	// Prefer activating an existing, completed placement on the target debrid
	// before re-submitting the magnet. Skipped when reinsert=true — e.g. the
	// current active provider just failed and its placement is presumed stale.
	if !reinsert {
		if target, ok := entry.Providers[debridName]; ok && target != nil && target.ID != "" && target.Status == types.TorrentStatusDownloaded {
			if err := entry.ActivatePlacement(debridName); err == nil {
				entry.Bad = false
				entry.UpdatedAt = time.Now()
				if err := f.manager.AddOrUpdate(entry, nil); err != nil {
					return false, fmt.Errorf("persist target placement %s: %w", debridName, err)
				}
				return true, nil
			}
			// Activation failed — fall through to a fresh submit.
		}
	}

	// Reuse the exact verified .torrent source when one was admitted. Missing
	// legacy sources may fall back to a magnet; corrupted sources fail closed.
	magnet, err := f.manager.torrentMagnetForEntry(entry)
	if err != nil {
		return false, err
	}

	if magnet == nil {
		return false, fmt.Errorf("failed to construct magnet for entry %s", entry.Name)
	}
	if magnet.Link == "" {
		return false, fmt.Errorf("failed to construct magnet for entry %s", entry.Name)
	}

	// Submit to debrid
	newDebridTorrent := &types.Torrent{
		Name:             entry.Name,
		Magnet:           magnet,
		InfoHash:         entry.InfoHash,
		Size:             entry.Size,
		Files:            make(map[string]types.File),
		DownloadUncached: false,
	}

	newDebridTorrent, err = submitProviderMagnet(ctx, client, newDebridTorrent)
	if err != nil {
		submitErr := fmt.Errorf("failed to submit magnet: %w", err)
		if newDebridTorrent != nil && newDebridTorrent.Id != "" {
			cleanupErr := f.manager.deleteProviderTorrent(client, newDebridTorrent.Id)
			if cleanupErr != nil {
				cleanupErr = fmt.Errorf(
					"roll back target placement %s/%s: %w",
					debridName,
					newDebridTorrent.Id,
					cleanupErr,
				)
			}
			return false, errors.Join(submitErr, cleanupErr)
		}
		return false, submitErr
	}

	if newDebridTorrent == nil || newDebridTorrent.Id == "" {
		return false, fmt.Errorf("failed to submit magnet: empty entry")
	}
	submittedID := newDebridTorrent.Id
	rollbackTarget := func(id string, cause error) error {
		if id == "" {
			id = submittedID
		}
		cleanupErr := f.manager.deleteProviderTorrent(client, id)
		if cleanupErr != nil {
			cleanupErr = fmt.Errorf("roll back target placement %s/%s: %w", debridName, id, cleanupErr)
		}
		return errors.Join(cause, cleanupErr)
	}

	// Check status
	newDebridTorrent.DownloadUncached = false
	newDebridTorrent, err = checkProviderStatus(ctx, client, newDebridTorrent)
	if err != nil {
		rollbackID := submittedID
		if newDebridTorrent != nil && newDebridTorrent.Id != "" {
			rollbackID = newDebridTorrent.Id
		}
		return false, rollbackTarget(rollbackID, fmt.Errorf("failed to check status: %w", err))
	}
	if newDebridTorrent == nil {
		return false, rollbackTarget(submittedID, fmt.Errorf("failed to check status: empty entry"))
	}
	if newDebridTorrent.Id == "" {
		return false, rollbackTarget(submittedID, fmt.Errorf("failed to check status: empty torrent ID"))
	}
	if newDebridTorrent.Debrid == "" {
		newDebridTorrent.Debrid = debridName
	}
	if newDebridTorrent.Debrid != debridName {
		return false, rollbackTarget(newDebridTorrent.Id, fmt.Errorf(
			"target provider mismatch: requested %s, received %s",
			debridName,
			newDebridTorrent.Debrid,
		))
	}
	if newDebridTorrent.Status != types.TorrentStatusDownloaded {
		return false, rollbackTarget(newDebridTorrent.Id, fmt.Errorf(
			"target placement %s is not downloaded: %s",
			debridName,
			newDebridTorrent.Status,
		))
	}

	// Verify files have links
	if len(newDebridTorrent.Files) == 0 {
		return false, rollbackTarget(newDebridTorrent.Id, fmt.Errorf("no files in entry after re-insertion"))
	}

	for _, f := range newDebridTorrent.GetFiles() {
		if f.Link == "" && f.Id == "" {
			return false, rollbackTarget(newDebridTorrent.Id, fmt.Errorf("empty link/id for file %s", f.Name))
		}
	}

	// Update entry with new placement
	if _, err := entry.AddTorrentProvider(newDebridTorrent); err != nil {
		return false, rollbackTarget(newDebridTorrent.Id, fmt.Errorf("reconcile target placement files: %w", err))
	}
	if len(entry.Files) == 0 {
		addedOn := newDebridTorrent.Added
		if addedOn.IsZero() {
			addedOn = time.Now()
		}
		for _, file := range newDebridTorrent.GetFiles() {
			entry.Files[file.Name] = &storage.File{
				Name:      file.Name,
				Path:      file.LocalPath(),
				Size:      file.Size,
				ByteRange: file.ByteRange,
				Deleted:   file.Deleted,
				InfoHash:  entry.InfoHash,
				AddedOn:   addedOn,
			}
		}
	}

	// Activate this debrid
	if err := entry.ActivatePlacement(debridName); err != nil {
		f.manager.logger.Warn().Err(err).Msg("failed to activate placement")
	}

	entry.Bad = false
	entry.UpdatedAt = time.Now()

	// The target must be durable before a migration caller is allowed to clean
	// up its source placement. MoveTorrent never owns source deletion: repair
	// callers intentionally retain their prior placement, while migration applies
	// keep-old policy using the source provider's own client.
	if err := f.manager.AddOrUpdate(entry, nil); err != nil {
		return false, fmt.Errorf("persist target placement %s: %w", debridName, err)
	}

	return true, nil
}

// buildAttemptOrder creates the order of debrids to attempt re-insertion
// Priority: current active debrid first, then others in config order
// If skipCurrent is true, current active debrid is skipped
func (f *Fixer) buildAttemptOrder(torrent *storage.Entry, skipCurrent bool) []string {
	order := make([]string, 0, len(f.providerOrder))

	// AddOrUpdate other debrids in config order
	for _, debridName := range f.providerOrder {
		if debridName == torrent.ActiveProvider && skipCurrent {
			continue
		}
		order = append(order, debridName)
	}

	return order
}

// IsFailedToReinsert checks if a torrent has been marked as failed to re-insert
func (f *Fixer) IsFailedToReinsert(infohash, debrid string) bool {
	_, failed := f.failedToReinsert.Load(fmt.Sprintf("%s:%s", infohash, debrid))
	return failed
}

// ResetFailureState manually resets the failure state for a torrent
func (f *Fixer) ResetFailureState(infohash string) {
	f.failedToReinsert.Delete(infohash)
}
