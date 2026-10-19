package manager

import (
	"sort"
	"strings"
	"time"

	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
)

const (
	streamProviderPreferenceTTL  = time.Minute
	maxStreamProviderPreferences = 4096
)

type streamProviderPreference struct {
	provider  string
	expiresAt time.Time
}

type streamCandidate struct {
	provider  string
	placement *storage.ProviderEntry
	preferred bool
	recovery  bool
}

// StreamFailoverStats is a secret-free process-lifetime view of automatic
// provider failover. Attempts counts transitions to another candidate;
// Successes counts alternate providers that became ready before response
// commitment; Exhausted counts requests that tried every eligible placement.
type StreamFailoverStats struct {
	Attempts              uint64 `json:"attempts"`
	Successes             uint64 `json:"successes"`
	Exhausted             uint64 `json:"exhausted"`
	PreferredHits         uint64 `json:"preferred_hits"`
	ProviderDeferrals     uint64 `json:"provider_deferrals"`
	ProviderDegradations  uint64 `json:"provider_degradations"`
	ProviderRecoveries    uint64 `json:"provider_recoveries"`
	CommittedHandoffs     uint64 `json:"committed_handoffs"`
	HandoffSuccesses      uint64 `json:"handoff_successes"`
	FileCircuitOpens      uint64 `json:"file_circuit_opens"`
	FileCircuitDeferrals  uint64 `json:"file_circuit_deferrals"`
	FileCircuitRecoveries uint64 `json:"file_circuit_recoveries"`
	OpenFileCircuits      int    `json:"open_file_circuits"`
}

// StreamFailoverStats returns a secret-free snapshot of provider failover.
// Counters are lock-free; the bounded open-circuit gauge takes its small
// registry lock for a consistent current count.
func (m *Manager) StreamFailoverStats() StreamFailoverStats {
	if m == nil {
		return StreamFailoverStats{}
	}
	stats := StreamFailoverStats{
		Attempts:              m.streamFailoverAttempts.Load(),
		Successes:             m.streamFailoverSuccesses.Load(),
		Exhausted:             m.streamFailoverExhausted.Load(),
		PreferredHits:         m.streamPreferredHits.Load(),
		ProviderDeferrals:     m.streamProviderDeferrals.Load(),
		ProviderDegradations:  m.streamProviderDegraded.Load(),
		ProviderRecoveries:    m.streamProviderRecoveries.Load(),
		CommittedHandoffs:     m.streamHandoffAttempts.Load(),
		HandoffSuccesses:      m.streamHandoffSuccesses.Load(),
		FileCircuitOpens:      m.streamFileCircuitOpens.Load(),
		FileCircuitDeferrals:  m.streamFileCircuitDefers.Load(),
		FileCircuitRecoveries: m.streamFileCircuitRecovers.Load(),
	}
	if m.streamFileCircuits != nil {
		stats.OpenFileCircuits = m.streamFileCircuits.openCount()
	}
	return stats
}

func (m *Manager) streamCandidates(entry *storage.Entry, filename string) []streamCandidate {
	if entry == nil {
		return nil
	}

	providerNames := make([]string, 0, len(entry.Providers)+1)
	seen := make(map[string]struct{}, len(entry.Providers)+1)
	add := func(provider string, requireReady bool) {
		provider = strings.TrimSpace(provider)
		key := strings.ToLower(provider)
		if _, exists := seen[key]; exists {
			return
		}
		placement := findStreamPlacement(entry, provider)
		if requireReady && !streamPlacementReady(placement, filename) {
			return
		}
		seen[key] = struct{}{}
		providerNames = append(providerNames, provider)
	}

	// Preserve historical behavior even for incomplete/legacy rows: the
	// declared active provider gets the first attempt unless a recent proven
	// fallback is promoted below.
	add(entry.ActiveProvider, false)
	if m != nil && m.config != nil {
		for _, configured := range m.config.Debrids {
			add(configured.Name, true)
		}
	}

	extra := make([]string, 0, len(entry.Providers))
	for key, placement := range entry.Providers {
		provider := strings.TrimSpace(key)
		if provider == "" && placement != nil {
			provider = strings.TrimSpace(placement.Provider)
		}
		if provider != "" {
			extra = append(extra, provider)
		}
	}
	sort.Slice(extra, func(i, j int) bool {
		left, right := strings.ToLower(extra[i]), strings.ToLower(extra[j])
		if left == right {
			return extra[i] < extra[j]
		}
		return left < right
	})
	for _, provider := range extra {
		add(provider, true)
	}

	preferred := m.preferredStreamProvider(entry, filename)
	if preferred != "" && !strings.EqualFold(preferred, entry.ActiveProvider) {
		for index, provider := range providerNames {
			if !strings.EqualFold(provider, preferred) {
				continue
			}
			copy(providerNames[1:index+1], providerNames[0:index])
			providerNames[0] = provider
			break
		}
	}

	candidates := make([]streamCandidate, 0, len(providerNames))
	for _, provider := range providerNames {
		placement := findStreamPlacement(entry, provider)
		candidates = append(candidates, streamCandidate{
			provider:  provider,
			placement: placement,
			preferred: preferred != "" && strings.EqualFold(provider, preferred),
		})
	}
	candidates = m.orderStreamCandidatesByWeather(candidates)
	return m.orderStreamCandidatesByFileCircuit(entry, filename, candidates)
}

func (candidate streamCandidate) entryForAttempt(original *storage.Entry, filename string) *storage.Entry {
	if original == nil || strings.EqualFold(candidate.provider, original.ActiveProvider) {
		return original
	}
	return cloneStreamEntry(original, candidate.provider, candidate.placement, filename)
}

// streamAttemptCandidates appends one full-lifecycle active-provider recovery
// only when the active placement was probed before another candidate. This
// keeps the playback path responsive—cheap read-only probes first—without
// removing the repair/retry behavior that single-provider users already had.
func streamAttemptCandidates(candidates []streamCandidate, activeProvider string) []streamCandidate {
	if len(candidates) < 2 {
		return candidates
	}
	for index, candidate := range candidates {
		if !strings.EqualFold(candidate.provider, activeProvider) || index == len(candidates)-1 {
			continue
		}
		recovery := candidate
		recovery.preferred = false
		recovery.recovery = true
		return append(candidates, recovery)
	}
	return candidates
}

func findStreamPlacement(entry *storage.Entry, provider string) *storage.ProviderEntry {
	if entry == nil || entry.Providers == nil {
		return nil
	}
	if placement := entry.Providers[provider]; placement != nil {
		return placement
	}
	for key, placement := range entry.Providers {
		if placement == nil {
			continue
		}
		if strings.EqualFold(key, provider) || strings.EqualFold(placement.Provider, provider) {
			return placement
		}
	}
	return nil
}

func streamPlacementReady(placement *storage.ProviderEntry, filename string) bool {
	if placement == nil || placement.RemovedAt != nil ||
		placement.Status != debridTypes.TorrentStatusDownloaded {
		return false
	}
	file := placement.Files[filename]
	return file != nil && (file.Id != "" || file.Link != "")
}

// cloneStreamEntry creates the smallest entry snapshot the link service needs
// for an alternate placement. Copying only the requested file avoids cloning a
// complete multi-file torrent for every seek while still isolating temporary
// ActiveProvider selection and link bookkeeping from durable state.
func cloneStreamEntry(entry *storage.Entry, provider string, selected *storage.ProviderEntry, filename string) *storage.Entry {
	if entry == nil {
		return nil
	}
	cloned := *entry
	cloned.ActiveProvider = provider
	cloned.Tags = append([]string(nil), entry.Tags...)

	cloned.Files = make(map[string]*storage.File, 1)
	if file := entry.Files[filename]; file != nil {
		fileCopy := *file
		if file.ByteRange != nil {
			byteRange := *file.ByteRange
			fileCopy.ByteRange = &byteRange
		}
		cloned.Files[filename] = &fileCopy
	}

	cloned.Providers = make(map[string]*storage.ProviderEntry, 1)
	if provider != "" && selected != nil {
		cloned.Providers[provider] = cloneStreamPlacement(selected, filename)
	}
	return &cloned
}

func cloneStreamPlacement(placement *storage.ProviderEntry, filename string) *storage.ProviderEntry {
	if placement == nil {
		return nil
	}
	cloned := *placement
	if placement.RemovedAt != nil {
		removedAt := *placement.RemovedAt
		cloned.RemovedAt = &removedAt
	}
	if placement.DownloadedAt != nil {
		downloadedAt := *placement.DownloadedAt
		cloned.DownloadedAt = &downloadedAt
	}
	cloned.Files = make(map[string]*storage.ProviderFile, 1)
	if file := placement.Files[filename]; file != nil {
		fileCopy := *file
		cloned.Files[filename] = &fileCopy
	}
	return &cloned
}

func (m *Manager) preferredStreamProvider(entry *storage.Entry, filename string) string {
	if m == nil || m.streamProviderPreferences == nil || entry == nil {
		return ""
	}
	key := streamPreferenceKey(entry, filename)
	preference, ok := m.streamProviderPreferences.Load(key)
	if !ok {
		return ""
	}
	if !time.Now().Before(preference.expiresAt) {
		m.streamProviderPreferences.Delete(key)
		return ""
	}
	return preference.provider
}

func (m *Manager) rememberStreamProvider(entry *storage.Entry, filename, provider string) {
	if m == nil || m.streamProviderPreferences == nil || entry == nil {
		return
	}
	key := streamPreferenceKey(entry, filename)
	if provider == "" || strings.EqualFold(provider, entry.ActiveProvider) {
		m.streamProviderPreferences.Delete(key)
		return
	}
	if m.streamProviderPreferences.Size() >= maxStreamProviderPreferences {
		m.streamProviderPreferences.Clear()
	}
	m.streamProviderPreferences.Store(key, streamProviderPreference{
		provider:  provider,
		expiresAt: time.Now().Add(streamProviderPreferenceTTL),
	})
}

func streamPreferenceKey(entry *storage.Entry, filename string) string {
	return entry.InfoHash + "\x00" + filename
}

func (m *Manager) updateActiveStreamProvider(streamID, provider string) {
	if m == nil || m.activeStreams == nil || streamID == "" || provider == "" {
		return
	}
	m.activeStreams.Compute(streamID, func(current *ActiveStream, loaded bool) (*ActiveStream, xsync.ComputeOp) {
		if !loaded || current == nil || current.Debrid == provider {
			return current, xsync.CancelOp
		}
		updated := *current
		updated.Debrid = provider
		updated.LastActive = time.Now().Unix()
		return &updated, xsync.UpdateOp
	})
}
