package manager

import (
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/internal/customerror"
)

const (
	defaultSubmissionRejectionTTL      = 24 * time.Hour
	defaultSubmissionRejectionCapacity = 4096
)

type submissionRejection struct {
	name      string
	expiresAt time.Time
}

// submissionRejectionCache bounds repeated provider calls for content that a
// specific provider has already refused. It is deliberately provider-scoped,
// process-local, TTL-limited and size-limited: another provider may still
// accept the same hash, and policy changes are retried after the cooldown.
type submissionRejectionCache struct {
	mu       sync.Mutex
	entries  map[string]submissionRejection
	ttl      time.Duration
	capacity int
	now      func() time.Time
}

func newSubmissionRejectionCache(ttl time.Duration, capacity int) *submissionRejectionCache {
	if ttl <= 0 {
		ttl = defaultSubmissionRejectionTTL
	}
	if capacity <= 0 {
		capacity = defaultSubmissionRejectionCapacity
	}
	return &submissionRejectionCache{
		entries:  make(map[string]submissionRejection),
		ttl:      ttl,
		capacity: capacity,
		now:      time.Now,
	}
}

func submissionRejectionKey(provider, infoHash string) string {
	provider = strings.ToLower(strings.TrimSpace(provider))
	infoHash = strings.ToLower(strings.TrimSpace(infoHash))
	if provider == "" || infoHash == "" {
		return ""
	}
	return provider + "\x00" + infoHash
}

func (c *submissionRejectionCache) get(provider, infoHash string) (*customerror.Error, bool) {
	if c == nil {
		return nil, false
	}
	key := submissionRejectionKey(provider, infoHash)
	if key == "" {
		return nil, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	entry, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if !now.Before(entry.expiresAt) {
		delete(c.entries, key)
		return nil, false
	}
	return customerror.NewTorrentContentRejectedError(entry.name), true
}

func (c *submissionRejectionCache) put(provider, infoHash, name string) {
	if c == nil {
		return
	}
	key := submissionRejectionKey(provider, infoHash)
	if key == "" {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	c.purgeExpiredLocked(now)
	if _, exists := c.entries[key]; !exists && len(c.entries) >= c.capacity {
		oldestKey := ""
		var oldestExpiry time.Time
		for candidateKey, candidate := range c.entries {
			if oldestKey == "" || candidate.expiresAt.Before(oldestExpiry) {
				oldestKey = candidateKey
				oldestExpiry = candidate.expiresAt
			}
		}
		delete(c.entries, oldestKey)
	}
	c.entries[key] = submissionRejection{
		name:      name,
		expiresAt: now.Add(c.ttl),
	}
}

func (c *submissionRejectionCache) len() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.purgeExpiredLocked(c.now())
	return len(c.entries)
}

func (c *submissionRejectionCache) purgeExpiredLocked(now time.Time) {
	for key, entry := range c.entries {
		if !now.Before(entry.expiresAt) {
			delete(c.entries, key)
		}
	}
}

func isTorrentContentRejection(err error) bool {
	var customErr *customerror.Error
	return errors.As(err, &customErr) &&
		customErr.Code == "torrent_content_rejected" &&
		customErr.IsPermanent()
}

func (m *Manager) recordSubmissionRejection(provider, infoHash, name string, err error) {
	if m == nil || m.submissionRejections == nil || !isTorrentContentRejection(err) {
		return
	}
	m.submissionRejections.put(provider, infoHash, name)
	m.submissionContentRejections.Add(1)
}

func (m *Manager) cachedSubmissionRejection(provider, infoHash string) error {
	if m == nil || m.submissionRejections == nil {
		return nil
	}
	err, ok := m.submissionRejections.get(provider, infoHash)
	if !ok {
		return nil
	}
	m.submissionRejectionHits.Add(1)
	return err
}

// TorrentAdmissionStats is a secret-free summary of provider content-policy
// admission outcomes. It intentionally exposes no hashes or release names.
type TorrentAdmissionStats struct {
	ContentRejections     uint64 `json:"content_rejections"`
	SuppressedSubmissions uint64 `json:"suppressed_submissions"`
	ActiveCooldowns       int    `json:"active_cooldowns"`
	CooldownSeconds       int64  `json:"cooldown_seconds"`
}

func (m *Manager) TorrentAdmissionStats() TorrentAdmissionStats {
	if m == nil {
		return TorrentAdmissionStats{}
	}
	stats := TorrentAdmissionStats{
		ContentRejections:     m.submissionContentRejections.Load(),
		SuppressedSubmissions: m.submissionRejectionHits.Load(),
	}
	if m.submissionRejections != nil {
		stats.ActiveCooldowns = m.submissionRejections.len()
		stats.CooldownSeconds = int64(m.submissionRejections.ttl / time.Second)
	}
	return stats
}
