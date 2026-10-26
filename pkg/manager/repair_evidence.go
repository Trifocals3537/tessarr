package manager

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/nntp"
	"github.com/Trifocals3537/tessarr/pkg/manager/link"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"golang.org/x/sync/errgroup"
)

// Only the owner's configured derived folder can replace an absent runtime
// name. Case folding or extension stripping alone is never identity proof.
func (r *Repair) resolveHealthCandidate(c *candidate) error {
	return r.resolveHealthCandidateUsingNaming(c, config.Get().FolderNaming)
}

func (r *Repair) resolveHealthCandidateUsingNaming(c *candidate, naming config.WebDavFolderNaming) error {
	item, err := r.manager.storage.ResolveHealthEntry(c.name, naming)
	if err != nil {
		return err
	}
	c.name, c.item = item.Name, item
	return nil
}

func (r *Repair) fileProbeFingerprint(item *storage.EntryItem, name string) (string, error) {
	f := item.Files[name]
	if f == nil {
		return "", errors.New("missing file")
	}
	e, err := r.manager.GetEntry(f.InfoHash)
	if err != nil {
		return "", err
	}
	if e == nil {
		return "", errors.New("entry not found")
	}
	var target [sha256.Size]byte
	meta := ""
	if e.IsNZB() {
		if r.manager.usenet == nil {
			return "", errors.New("usenet unavailable")
		}
		meta, err = r.manager.usenet.FileRepairFingerprint(e.InfoHash, name)
		if err != nil {
			return "", err
		}
	} else {
		if r.manager.ProviderClient(e.ActiveProvider) == nil {
			return "", errors.New("active provider source unavailable")
		}
		target, err = importReadinessTargetFingerprint(e)
		if err != nil {
			return "", err
		}
		// A resumed range may have used any completed failover placement.
		// Bind its proof to all current accounts, including credential changes
		// that do not mutate the entry or increment its dirty revision.
		accounts := make(map[string]string, len(e.Providers))
		for provider := range e.Providers {
			accounts[provider] = "unavailable"
			if r.manager.clients != nil {
				if client := r.manager.ProviderClient(provider); client != nil {
					accounts[provider] = submissionAccount(client.Config())
				}
			}
		}
		accountData, err := json.Marshal(accounts)
		if err != nil {
			return "", err
		}
		meta = string(accountData)
	}
	h := sha256.Sum256([]byte(hex.EncodeToString(target[:]) + "\x00" + meta + "\x00" + storage.EntryItemRepairFingerprint(&storage.EntryItem{Files: map[string]*storage.File{name: f}})))
	return hex.EncodeToString(h[:]), nil
}

// Failed ranges are independent of availability. Legacy read-failure rows
// without ranges remain incomplete until explicit evidence is supplied.
func failedRangesFor(h *storage.EntryHealth, item *storage.EntryItem) (map[string][]storage.FailedReadRange, error) {
	out := make(map[string][]storage.FailedReadRange)
	if h.FailedReadsIncomplete || (h.Dirty && strings.HasPrefix(h.DirtyReason, "read_failure") && len(h.FailedReads) == 0) {
		return out, errors.New("failed_range_evidence_unavailable")
	}
	for _, failed := range h.FailedReads {
		f := item.Files[failed.FileName]
		if f == nil || f.Deleted || f.InfoHash != failed.InfoHash || failed.FileID != "" && failed.FileID != f.ID || failed.Offset < 0 || failed.Length <= 0 || failed.Offset > f.Size || failed.Length > f.Size-failed.Offset {
			return out, errors.New("failed_range_identity_changed")
		}
		out[failed.FileName] = append(out[failed.FileName], failed)
	}
	return out, nil
}

func (r *Repair) probeFilesWithEvidence(ctx context.Context, c *candidate, h *storage.EntryHealth, names []string, opts RepairRunOptions) []fileResult {
	ranges, evidenceErr := failedRangesFor(h, c.item)
	fingerprint := storage.EntryItemRepairFingerprint(c.item)
	mode := fmt.Sprintf("v2:content=true:unrestrict=%t:scope=%s", opts.UnrestrictLink, r.effectiveProtocolScope(opts))
	if h.ProbeProgress == nil || h.ProbeProgress.Revision != h.DirtyRevision || h.ProbeProgress.Fingerprint != fingerprint || h.ProbeProgress.Mode != mode || h.ProbeProgress.StartedAt.IsZero() || time.Since(h.ProbeProgress.StartedAt) > 24*time.Hour {
		h.ProbeProgress = &storage.HealthProbeProgress{Revision: h.DirtyRevision, Fingerprint: fingerprint, Mode: mode, StartedAt: time.Now(), Verified: make(map[string]string)}
	}
	if h.ProbeProgress.Verified == nil {
		h.ProbeProgress.Verified = make(map[string]string)
	}
	results := make([]fileResult, len(names))
	var mu sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(repairFilesPerEntry)
	for i, name := range names {
		g.Go(func() error {
			res := fileResult{name: name, reason: "context_cancelled"}
			if gctx.Err() != nil {
				results[i] = res
				return nil
			}
			before, err := r.fileProbeFingerprint(c.item, name)
			mu.Lock()
			cached := err == nil && before != "" && h.ProbeProgress.Verified[name] == before
			mu.Unlock()
			if cached {
				f := c.item.Files[name]
				e, loadErr := r.manager.GetEntry(f.InfoHash)
				if loadErr == nil && e != nil {
					res = fileResult{name: name, infoHash: f.InfoHash, protocol: e.Protocol, healthy: true}
				}
			} else {
				probeOpts := opts
				// Even a historically clean row can have a wrong head: old STAT
				// checks discarded its failure evidence. Rechecks always validate
				// supported NZB heads, with the existing global slot limit.
				probeOpts.VerifyContent = true
				res = r.probeFile(gctx, c.item, name, probeOpts)
				if res.healthy && len(ranges[name]) > 0 {
					if err := r.verifyFailedRanges(gctx, res, c.item.Files[name], ranges[name]); err != nil {
						res.healthy = false
						res.reason = "failed_range_check_incomplete"
						if isDefinitiveRangeFailure(err) {
							res.broken = true
							res.reason = "failed_range_unavailable"
						}
					}
				}
			}
			if evidenceErr != nil {
				res.healthy = false
				res.incomplete = true
				res.reason = evidenceErr.Error()
			}
			after, afterErr := r.fileProbeFingerprint(c.item, name)
			if err != nil || afterErr != nil || before != after || gctx.Err() != nil {
				res.healthy = false
				res.broken = false
				res.incomplete = true
				res.reason = "probe_target_changed_or_incomplete"
			}
			mu.Lock()
			defer mu.Unlock()
			if res.healthy {
				h.ProbeProgress.Verified[name] = after
				snapshot := *h
				progress := *h.ProbeProgress
				progress.Verified = maps.Clone(progress.Verified)
				snapshot.ProbeProgress = &progress
				if err := r.manager.storage.SaveEntryHealth(&snapshot); err != nil {
					res.healthy = false
					res.reason = "progress_persistence_failed"
				}
			}
			res.sourceFingerprint = after
			results[i] = res
			return nil
		})
	}
	_ = g.Wait()
	// A later file can change the source/account after an earlier file finished.
	// Revalidate every verdict before publishing or authorizing repair.
	for i := range results {
		res := &results[i]
		if !res.healthy && !res.broken {
			continue
		}
		current, err := r.fileProbeFingerprint(c.item, res.name)
		changed := err != nil || current != res.sourceFingerprint
		if changed || ctx.Err() != nil {
			res.healthy, res.broken, res.incomplete = false, false, true
			res.reason = "probe_target_changed_or_incomplete"
			if changed {
				delete(h.ProbeProgress.Verified, res.name)
			}
		}
	}
	return results
}

type boundedProbeWriter struct{ remaining int64 }

func isDefinitiveRangeFailure(err error) bool {
	return nntp.IsArticleNotFoundError(err) || errors.Is(err, customerror.UsenetSegmentMissingError) || errors.Is(err, customerror.UsenetCorruptContentError)
}
func (w *boundedProbeWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, io.ErrShortWrite
	}
	w.remaining -= int64(len(p))
	return len(p), nil
}

func (r *Repair) verifyFailedRanges(ctx context.Context, res fileResult, file *storage.File, ranges []storage.FailedReadRange) error {
	if res.protocol == config.ProtocolNZB {
		return r.manager.usenet.VerifyFileRanges(ctx, res.infoHash, res.name, ranges)
	}
	e, err := r.manager.GetEntry(res.infoHash)
	if err != nil {
		return err
	}
	for _, failed := range ranges {
		if failed.Offset < 0 || failed.Length <= 0 || failed.Offset > file.Size || failed.Length > file.Size-failed.Offset {
			return errors.New("invalid failed range")
		}
		w := &boundedProbeWriter{remaining: failed.Length}
		if err := r.manager.Stream(link.WithoutRepair(ctx), e, res.name, failed.Offset, failed.Offset+failed.Length-1, w, nil, "health-recheck"); err != nil {
			return err
		}
		if w.remaining != 0 {
			return io.ErrUnexpectedEOF
		}
	}
	return ctx.Err()
}
