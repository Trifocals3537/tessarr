package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/pkg/arr"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

const recoveryRetryDelay = 5 * time.Minute

var errRecoveryNeedsProbe = errors.New("repair file needs fresh broken confirmation")

// Bind to the endpoint, not the API token: credential rotation is harmless,
// but reusing an Arr name for another server must not replay destructive work.
func recoveryEndpointBinding(a *arr.Arr) string {
	u, err := url.Parse(a.Host)
	if err != nil || u.Host == "" {
		return ""
	}
	value := string(a.Type) + "\x00" + strings.ToLower(u.Scheme+"://"+u.Host) + strings.TrimRight(u.EscapedPath(), "/")
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func recoveryID(bf storage.BrokenFile) string {
	// Do NOT include the current endpoint: a changed endpoint must find the old
	// job and stop on its binding check, not authorize a brand-new deletion.
	value := fmt.Sprintf("%s\x00%s\x00%d\x00%s\x00%s", bf.ArrName, bf.ArrKind, bf.ArrFileID, bf.SourcePath, bf.InfoHash)
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func (r *Repair) recoveryArr(job *storage.RepairRecovery) *arr.Arr {
	for _, a := range r.eligibleArrs(r.cfg().Arrs) {
		if a.Name == job.Original.ArrName && string(a.Type) == string(job.Original.ArrKind) {
			return a
		}
	}
	return nil
}

// healBrokenEntry records each file before touching Arr. Source entries are
// retained: accepting a search does not prove a replacement exists.
func (r *Repair) healBrokenEntry(ctx context.Context, run *storage.RepairRun, statsMu *sync.Mutex, name string, health *storage.EntryHealth) {
	if health == nil || health.Status != storage.HealthBroken {
		return
	}
	for _, bf := range health.BrokenFiles {
		if ctx.Err() != nil {
			return
		}
		if bf.ArrName == "" || bf.ArrFileID <= 0 {
			continue
		}
		bf.EntryName = name
		r.recoveryMu.Lock()
		job, err := r.manager.storage.GetRepairRecovery(recoveryID(bf))
		if err == nil && job == nil {
			job = &storage.RepairRecovery{ID: recoveryID(bf), Original: bf, State: storage.RecoveryPending}
			if a := r.recoveryArr(job); a != nil {
				job.EndpointBinding = recoveryEndpointBinding(a)
				err = r.manager.storage.SaveRepairRecovery(job)
			} else {
				job = nil
			}
		}
		if err != nil {
			r.recordRecoveryResult(run, statsMu, nil, err)
		} else if job != nil {
			r.processRecovery(ctx, run, statsMu, job, true)
		}
		r.recoveryMu.Unlock()
	}
}

func (r *Repair) resumeRecoveries(ctx context.Context, run *storage.RepairRun, names []string, scope string) error {
	ids, err := r.manager.storage.PendingRepairRecoveryIDs(names)
	if err != nil {
		return err
	}
	var statsMu sync.Mutex
	processed := 0
	for _, id := range ids {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if processed >= 100 {
			break
		}
		job, err := r.manager.storage.GetRepairRecovery(id)
		if err != nil {
			return err
		}
		if job == nil || !repairProtocolMatches(scope, job.Original.Protocol) {
			continue
		}
		r.recoveryMu.Lock()
		if r.processRecovery(ctx, run, &statsMu, job, false) {
			processed++
		}
		r.recoveryMu.Unlock()
	}
	return r.manager.storage.PruneRepairRecoveries(time.Now().Add(-30 * 24 * time.Hour))
}

func (r *Repair) processRecovery(ctx context.Context, run *storage.RepairRun, statsMu *sync.Mutex, job *storage.RepairRecovery, allowDelete bool) bool {
	// Replay may finish read-only preflight after a crash between saving the
	// initial intent and its binding. advanceRecovery still requires fresh
	// broken confirmation before deleting an existing Arr file.
	if job.State == storage.RecoveryComplete || job.LastRunID == run.ID || time.Now().Before(job.NextCheckAt) {
		return false
	}
	a := r.recoveryArr(job)
	if a == nil {
		return false
	} // Current opt-outs apply to restart recovery too.
	if ctx.Err() != nil {
		return false
	}
	job.LastRunID = run.ID
	job.NextCheckAt = time.Now().Add(recoveryRetryDelay)
	if err := r.manager.storage.SaveRepairRecovery(job); err != nil {
		r.recordRecoveryResult(run, statsMu, job, err)
		return true
	}
	jobCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	err := r.advanceRecovery(jobCtx, a, job, allowDelete)
	if errors.Is(err, errRecoveryNeedsProbe) {
		// Let this run's later probe authorize deletion if still broken. Merely
		// finding an old intent after restart does not establish current health.
		job.LastRunID = ""
		job.NextCheckAt = time.Time{}
		job.ErrorCode = "awaiting_broken_confirmation"
		err = r.manager.storage.SaveRepairRecovery(job)
	}
	if err != nil {
		// Never persist request/response text; the phase is enough to diagnose.
		if job.State != storage.RecoveryAttention {
			job.State = storage.RecoveryPending
		}
		if saveErr := r.manager.storage.SaveRepairRecovery(job); saveErr != nil {
			err = saveErr
		}
	}
	r.recordRecoveryResult(run, statsMu, job, err)
	return true
}

func (r *Repair) recordRecoveryResult(run *storage.RepairRun, mu *sync.Mutex, job *storage.RepairRecovery, err error) {
	mu.Lock()
	defer mu.Unlock()
	if err != nil {
		run.Stats.RepairFailed++
		code := "storage_unavailable"
		if job != nil && job.ErrorCode != "" {
			code = job.ErrorCode
		}
		event := r.logger.Warn().Str("phase", code)
		if job != nil {
			event = event.Str("recovery_id", job.ID).Str("arr", job.Original.ArrName)
		}
		event.Msg("Repair recovery incomplete; durable work retained")
	} else if job != nil && job.State == storage.RecoveryComplete {
		run.Stats.Repaired++
	} else {
		run.Stats.RepairPending++
	}
	r.saveRun(run)
}

func (r *Repair) advanceRecovery(ctx context.Context, a *arr.Arr, job *storage.RepairRecovery, allowDelete bool) error {
	save := func() error { return r.manager.storage.SaveRepairRecovery(job) }
	job.ErrorCode = "endpoint_changed"
	if job.EndpointBinding == "" || recoveryEndpointBinding(a) != job.EndpointBinding {
		job.State = storage.RecoveryAttention
		return errors.New("repair endpoint binding changed")
	}
	job.State = storage.RecoveryPending
	if !job.Prepared {
		job.ErrorCode = "prepare_binding"
		file, err := a.ReadRepairFile(ctx, job.Original.ArrFileID)
		if err != nil {
			return err
		}
		if file == nil {
			return errors.New("original Arr file disappeared before its binding was saved")
		}
		bf := job.Original
		if file.Path != bf.SourcePath || bf.Size > 0 && file.Size != bf.Size ||
			a.Type == arr.Radarr && file.MovieID != bf.MediaID ||
			a.Type == arr.Sonarr && (file.SeriesID != bf.MediaID || !slices.Contains(file.EpisodeIDs, bf.EpisodeID)) {
			job.State = storage.RecoveryAttention
			return errors.New("original Arr file identity changed")
		}
		job.File = *file
		job.ErrorCode = "prepare_history"
		grab, failed, err := a.ExactRepairHistory(ctx, bf.InfoHash, *file)
		if err != nil {
			return err
		}
		cfg, err := a.RepairConfig(ctx)
		if err != nil {
			return err
		}
		if grab != nil && failed == nil {
			job.HistoryID = grab.ID
			job.AutoSearch = cfg.AutoRedownloadFailed &&
				(!strings.EqualFold(grab.Data["releaseSource"], "InteractiveSearch") || cfg.AutoRedownloadFailedFromInteractiveSearch)
		}
		// A previously failed grab is already blocklisted. It does not promise a
		// NEW search after our deletion, so explicitly search this exact target.
		job.Prepared = true
		if err := save(); err != nil {
			return err
		}
	}

	job.ErrorCode = "verify_import"
	imported, err := a.RepairReplacementImported(ctx, job.File)
	if err != nil {
		return err
	}
	if imported {
		job.State = storage.RecoveryComplete
		job.ErrorCode = ""
		return save()
	}
	// A receipt is not completion; wait for the exact media to acquire a new
	// file ID. Never repeat a search just because an import is taking a while.
	if job.SearchReceiptID > 0 || job.FailureConfirmed && job.AutoSearch {
		setRecoveryWaiting(job)
		return save()
	}
	job.ErrorCode = "check_download_handling"
	if _, err := a.RepairConfig(ctx); err != nil {
		return err
	}
	if !job.Deleted {
		job.ErrorCode = "delete_file"
		if !allowDelete {
			current, err := a.ReadRepairFile(ctx, job.File.ID)
			if err != nil {
				return err
			}
			if current != nil {
				return errRecoveryNeedsProbe
			}
		}
		if err := a.DeleteRepairFile(ctx, job.File); err != nil {
			return err
		}
		job.Deleted = true
		if err := save(); err != nil {
			return err
		}
	}
	if job.HistoryID > 0 && !job.FailureConfirmed {
		job.ErrorCode = "blocklist_exact_grab"
		grab, failed, err := a.ExactRepairHistory(ctx, job.Original.InfoHash, job.File)
		if err != nil {
			return err
		}
		if failed != nil {
			if job.FailureIntentAt.IsZero() {
				job.AutoSearch = false
			}
			job.FailureConfirmed = true
		} else {
			if !job.FailureIntentAt.IsZero() {
				job.State = storage.RecoveryAttention
				return errors.New("failed-history outcome remains unknown; not resending")
			}
			if grab == nil || grab.ID != job.HistoryID {
				return errors.New("exact grab history changed")
			}
			cfg, err := a.RepairConfig(ctx)
			if err != nil {
				return err
			}
			job.AutoSearch = cfg.AutoRedownloadFailed &&
				(!strings.EqualFold(grab.Data["releaseSource"], "InteractiveSearch") || cfg.AutoRedownloadFailedFromInteractiveSearch)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			job.FailureIntentAt = time.Now()
			if err := save(); err != nil {
				return err
			}
			if err := a.FailRepairHistory(ctx, job.HistoryID); err != nil {
				if errors.Is(err, arr.ErrRepairRejected) {
					job.FailureIntentAt = time.Time{}
				} else {
					job.State = storage.RecoveryAttention
				}
				return err
			}
			job.FailureConfirmed = true
		}
		if err := save(); err != nil {
			return err
		}
	}
	if job.FailureConfirmed && job.AutoSearch {
		setRecoveryWaiting(job)
		return save()
	}
	job.ErrorCode = "request_search"
	if !job.SearchIntentAt.IsZero() {
		id, err := a.FindRepairSearch(ctx, job.File, job.SearchIntentAt)
		if err != nil {
			return err
		}
		if id == 0 {
			job.State = storage.RecoveryAttention
			return errors.New("search outcome remains unknown; not resending")
		}
		job.SearchReceiptID = id
	} else {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		job.SearchIntentAt = time.Now()
		if err := save(); err != nil {
			return err
		}
		id, err := a.SearchRepairFile(ctx, job.File)
		if err != nil {
			if errors.Is(err, arr.ErrRepairRejected) {
				job.SearchIntentAt = time.Time{}
			} else {
				job.State = storage.RecoveryAttention
			}
			return err
		}
		job.SearchReceiptID = id
	}
	setRecoveryWaiting(job)
	return save()
}

func setRecoveryWaiting(job *storage.RepairRecovery) {
	job.State = storage.RecoveryWaiting
	job.ErrorCode = ""
	since := job.SearchIntentAt
	if since.IsZero() {
		since = job.FailureIntentAt
	}
	if !since.IsZero() && time.Since(since) >= 24*time.Hour {
		job.State = storage.RecoveryAttention
		job.ErrorCode = "import_not_observed"
	}
}
