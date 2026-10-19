package manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/utils"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

const (
	minimumUncachedStallTimeout = 10 * time.Minute
	noSeedsStallTimeout         = 10 * time.Minute
	metadataStallTimeout        = 20 * time.Minute
	minimumStallConfirmation    = 30 * time.Second
)

type stallCandidate struct {
	kind      string
	checkedAt time.Time
}

func uncachedStallCandidateKey(entry *storage.Entry) string {
	placement := entry.GetActiveProvider()
	if placement == nil {
		return ""
	}
	return entry.InfoHash + "\x00" + entry.QueueIncarnation + "\x00" + entry.ActiveProvider + "\x00" + placement.ID
}

// Confirmation is deliberately scoped to one queue-row incarnation and
// provider placement. A restart forgets pending evidence and therefore delays,
// rather than accelerates, any handoff.
func (m *Manager) confirmUncachedStall(key, kind string, now time.Time) bool {
	if key == "" || kind == "" {
		return false
	}
	m.stallCandidatesMu.Lock()
	defer m.stallCandidatesMu.Unlock()
	if m.stallCandidates == nil {
		m.stallCandidates = make(map[string]stallCandidate)
	}
	for candidateKey, candidate := range m.stallCandidates {
		if now.Sub(candidate.checkedAt) > 24*time.Hour {
			delete(m.stallCandidates, candidateKey)
		}
	}
	if previous, ok := m.stallCandidates[key]; ok && previous.kind == kind &&
		!now.Before(previous.checkedAt.Add(minimumStallConfirmation)) {
		delete(m.stallCandidates, key)
		return true
	}
	if previous, ok := m.stallCandidates[key]; ok && previous.kind == kind {
		return false
	}
	m.stallCandidates[key] = stallCandidate{kind: kind, checkedAt: now}
	return false
}

func (m *Manager) clearUncachedStallCandidate(key string) {
	if key == "" {
		return
	}
	m.stallCandidatesMu.Lock()
	delete(m.stallCandidates, key)
	m.stallCandidatesMu.Unlock()
}

// TorBox's explicit no-seeds and metadata states justify shorter waits than
// an ordinary slow transfer. A zero seeder count alone never does.
func uncachedStallPolicy(
	state string,
	status debridTypes.TorrentStatus,
	seeders int,
	generic time.Duration,
) (string, time.Duration) {
	if generic <= 0 || status != debridTypes.TorrentStatusDownloading {
		return "", 0
	}
	normalized := strings.ToLower(strings.TrimSpace(strings.SplitN(state, "(", 2)[0]))
	switch normalized {
	case "metadl":
		return "metadata", min(generic, metadataStallTimeout)
	case "stalled", "stalleddl":
		if seeders == 0 {
			return "no_seeds", min(generic, noSeedsStallTimeout)
		}
		return "stalled", generic
	case "paused", "pausedup", "pauseddl", "checkingresumedata", "checkingup", "checkingdl":
		return "", 0
	default:
		// Raw provider state is diagnostic data, not a complete cross-provider
		// policy vocabulary. Once an adapter has positively mapped the state to
		// downloading, use the generic no-progress timeout for provider-native
		// active states (for example AllDebrid 0-3, Premiumize running, and
		// Real-Debrid magnet_conversion) instead of silently disabling recovery.
		return "stalled", generic
	}
}

func parseUncachedStallTimeout(raw string) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, nil
	}
	timeout, err := utils.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("parse uncached stall timeout: %w", err)
	}
	if timeout < minimumUncachedStallTimeout {
		return 0, fmt.Errorf(
			"uncached stall timeout must be at least %s",
			minimumUncachedStallTimeout,
		)
	}
	return timeout, nil
}

// uncachedTransferStalled is deliberately conservative. A transfer is eligible
// only when uncached downloading was actually used, the provider is still
// reporting an incomplete download, a successful poll has established a
// progress baseline, and the provider currently reports no transfer speed.
func uncachedTransferStalled(
	entry *storage.Entry,
	providerStatus debridTypes.TorrentStatus,
	now time.Time,
	timeout time.Duration,
) bool {
	if entry == nil || timeout <= 0 || !entry.IsTorrent() || !entry.DownloadUncached {
		return false
	}
	if providerStatus != debridTypes.TorrentStatusDownloading ||
		entry.State != storage.EntryStateDownloading ||
		entry.Status != debridTypes.TorrentStatusDownloading {
		return false
	}
	if entry.Progress >= 1 || entry.Speed > 0 || entry.LastObservedAt == nil || entry.LastProgressAt == nil {
		return false
	}
	return !now.Before(entry.LastProgressAt.Add(timeout))
}

// handoffUncachedFailures asks the owning Arr to remove exactly one confirmed
// failed download, blocklist that release, and re-search. It runs outside the per-entry
// worker lease: the Arr synchronously calls Tessarr's qBittorrent delete API,
// which can then drain the old worker and durably clean the provider placement.
// A persisted stalledDL row makes the handoff restart-safe and naturally
// retryable until the Arr acknowledges it.
func (m *Manager) handoffUncachedFailures(ctx context.Context) error {
	if m == nil || m.queue == nil || m.arr == nil {
		return nil
	}
	entries := m.queue.ListFilter(
		"",
		config.ProtocolTorrent,
		storage.EntryStateStalledDL,
		nil,
		"added_on",
		false,
	)
	var handoffErr error
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return errors.Join(handoffErr, err)
		}
		if !storage.FailedUncachedHandoff(entry) {
			continue
		}
		owner := m.arr.Get(entry.Category)
		if owner == nil {
			m.logger.Debug().
				Str("entry", entry.InfoHash).
				Str("category", entry.Category).
				Msg("Failed uncached transfer has no configured Arr owner; leaving it for manual review")
			continue
		}
		var handedOff bool
		var err error
		if entry.ClientEndpoint == "" {
			m.logger.Warn().Str("entry", entry.InfoHash).Msg("Uncached transfer lacks submitting client evidence; manual Arr review required")
			continue
		}
		handedOff, err = owner.BlocklistAndResearchDownloadForEndpointBeforeDeleteCtx(ctx, entry.InfoHash, entry.ClientEndpoint, func() error { return m.queue.storage.AuthorizeUncachedHandoff(entry) })
		if err != nil {
			m.uncachedHandoffErrors.Add(1)
			handoffErr = errors.Join(
				handoffErr,
				fmt.Errorf("handoff failed transfer %s to %s: %w", entry.InfoHash, owner.Name, err),
			)
			continue
		}
		if !handedOff {
			m.logger.Debug().
				Str("entry", entry.InfoHash).
				Str("arr", owner.Name).
				Msg("Failed uncached transfer is not yet present in the Arr queue")
			continue
		}
		m.uncachedHandoffAccepted.Add(1)
		m.logger.Info().
			Str("entry", entry.InfoHash).
			Str("arr", owner.Name).
			Str("reason", entry.HandoffReason).
			Msg("Arr accepted failed uncached transfer for blocklist and replacement search")
	}
	return handoffErr
}
