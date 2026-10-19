package link

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/internal/cdntraffic"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/utils"
	accountpkg "github.com/Trifocals3537/tessarr/pkg/debrid/account"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"golang.org/x/sync/singleflight"
)

const (
	MaxReinsertionAttempt = 3
	maxLinkRefreshes      = 1
	maxValidatedEntries   = 8192
	maxLinkRetryWait      = 30 * time.Second
	refreshBackoffBase    = 30 * time.Second
	refreshBackoffMax     = 5 * time.Minute
	maxRefreshBackoffs    = 4096
	sharedLinkTimeout     = 2 * time.Minute
)

var (
	emptyDownloadLink = types.DownloadLink{}
)

// EntryRefresher is a function that refreshes an entry by infohash
type EntryRefresher func(infohash string) (*storage.Entry, error)
type EntryRepairer func(ctx context.Context, entry *storage.Entry) error
type EntrySaver func(entry *storage.Entry) error
type ProviderTypeResolver func(provider string) string
type cachedLinkInvalidator interface {
	InvalidateCachedLink(types.DownloadLink) error
}

type refreshBackoffState struct {
	failures    int
	nextAttempt time.Time
}

type repairPolicyContextKey struct{}
type retryPolicyContextKey struct{}

// WithoutRepair marks a link attempt as read-only with respect to entry
// placement state. Automatic stream failover uses it for cheap provider probes:
// a candidate may resolve, refresh its cached URL, or disable a bad account,
// but it must never reinsert the torrent, mark the entry bad, or persist a
// temporary ActiveProvider selection. The final active-provider recovery keeps
// the normal lifecycle policy.
func WithoutRepair(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, repairPolicyContextKey{}, true)
}

func repairDisabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	disabled, _ := ctx.Value(repairPolicyContextKey{}).(bool)
	return disabled
}

// WithFailFast limits link validation to one network attempt. Stream failover
// probes use it while another completed placement remains, reserving the normal
// retry budget for the final active-provider recovery attempt.
func WithFailFast(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, retryPolicyContextKey{}, true)
}

func failFast(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	enabled, _ := ctx.Value(retryPolicyContextKey{}).(bool)
	return enabled
}

func lifecyclePolicyKey(ctx context.Context, key string) string {
	if repairDisabled(ctx) {
		key += "\x00read-only"
	}
	if failFast(ctx) {
		key += "\x00fail-fast"
	}
	return key
}

// Service handles download link fetching and validation.
// It uses the account-level cache for storing links and only tracks validation state.
type Service struct {
	validated       *xsync.Map[string, struct{}]
	singleflight    singleflight.Group
	refreshflight   singleflight.Group
	refreshMu       sync.Mutex
	refreshBackoffs map[string]refreshBackoffState
	clients         *xsync.Map[string, debrid.Client]
	entryRefresher  EntryRefresher
	repairer        EntryRepairer
	entrySaver      EntrySaver
	providerType    ProviderTypeResolver
	httpClient      *http.Client
	retries         int
	wait            func(context.Context, time.Duration) error
	now             func() time.Time
	logger          zerolog.Logger
}

// New creates a new LinkService
func New(
	clients *xsync.Map[string, debrid.Client],
	entryRefresher EntryRefresher,
	entryReinsert EntryRepairer,
	entrySaver EntrySaver,
	httpClient *http.Client,
	retries int,
	logger zerolog.Logger,
	providerTypes ...ProviderTypeResolver,
) *Service {
	var providerType ProviderTypeResolver
	if len(providerTypes) > 0 {
		providerType = providerTypes[0]
	}
	return &Service{
		validated:       xsync.NewMap[string, struct{}](),
		refreshBackoffs: make(map[string]refreshBackoffState),
		clients:         clients,
		entryRefresher:  entryRefresher,
		repairer:        entryReinsert,
		entrySaver:      entrySaver,
		providerType:    providerType,
		httpClient:      httpClient,
		retries:         retries,
		wait:            waitWithContext,
		now:             time.Now,
		logger:          logger,
	}
}

// GetLink fetches and validates a download link for a file in an entry.
// Links are cached at the account level; this service only tracks validation state.
func (s *Service) GetLink(ctx context.Context, entry *storage.Entry, filename string) (types.DownloadLink, error) {
	key := lifecyclePolicyKey(ctx, linkLifecycleKey(entry, filename))
	return doLinkFlight(ctx, &s.singleflight, key, func(sharedCtx context.Context) (types.DownloadLink, error) {
		return s.fetchAndValidate(sharedCtx, entry, filename, 0, 0)
	})
}

// Refresh evicts a rejected URL from local caches and returns a validated
// replacement. Concurrent refreshes for the same file are coalesced, and a
// refresh never calls a provider-side deletion endpoint.
// filename is the requested entry file, not bad.Filename: providers may return
// an archive name or rename the underlying object without changing its media files.
func (s *Service) Refresh(ctx context.Context, entry *storage.Entry, filename string, bad types.DownloadLink) (types.DownloadLink, error) {
	if err := ctx.Err(); err != nil {
		return emptyDownloadLink, err
	}
	if filename == "" {
		return emptyDownloadLink, NewPermanentError(ErrEmptyLink, "empty_link")
	}
	if _, err := entry.GetFile(filename); err != nil {
		return emptyDownloadLink, NewPermanentError(
			fmt.Errorf("file %s not found in entry %s: %w", filename, entry.Name, err),
			"file_not_found",
		)
	}

	return s.refreshRejectedLink(ctx, entry, filename, bad, 0, 0)
}

func (s *Service) getClient(provider string) (debrid.Client, error) {
	c, ok := s.clients.Load(provider)
	if !ok {
		return nil, fmt.Errorf("client for provider %s not found", provider)
	}
	return c, nil
}

func (s *Service) withCDNIdentity(ctx context.Context, link *types.DownloadLink) context.Context {
	identity := cdntraffic.Identity{}
	if link != nil {
		identity.Provider = link.Debrid
		identity.AccountToken = link.Token
		if link.Link != "" {
			identity.LinkKey = link.Link
		} else if link.Id != "" || link.Filename != "" {
			identity.LinkKey = link.Id + "\x00" + link.Filename
		} else {
			identity.LinkKey = link.DownloadLink
		}
	}
	if identity.Provider != "" && s.providerType != nil {
		identity.ProviderType = s.providerType(identity.Provider)
	}
	return cdntraffic.WithIdentity(ctx, identity)
}

// fetchAndValidate fetches a download link and validates it.
// repairAttempt tracks how many re-insertion cycles we've already paid for during
// this GetLink call so we can bail out instead of looping forever when the
// underlying file never resolves (see fetchLink/handleBadLink).
func (s *Service) fetchAndValidate(ctx context.Context, entry *storage.Entry, filename string, repairAttempt, linkRefreshes int) (types.DownloadLink, error) {
	if err := ctx.Err(); err != nil {
		return emptyDownloadLink, err
	}
	link, err := s.fetchLink(ctx, entry, filename, repairAttempt, linkRefreshes)
	if err != nil {
		return s.handleBadLink(ctx, err, entry, filename, link, repairAttempt, linkRefreshes)
	}

	// Only successful validations are memoized. Transient failures must be
	// allowed to recover on the next request.
	if _, exists := s.validated.Load(validationKey(link)); exists && s.accountValidationIsCurrent(link) {
		s.clearRefreshBackoff(lifecyclePolicyKey(ctx, linkLifecycleKey(entry, filename)))
		return link, nil
	}
	s.validated.Delete(validationKey(link))

	validationErr := s.validateWithRetry(ctx, &link)

	if validationErr != nil {
		// Handle link error categories
		if linkErr := GetLinkError(validationErr); linkErr != nil {
			if linkErr.ShouldSuspendAccount() {
				hasAlternate, status, err := s.suspendLinkAccount(link, linkErr)
				if err != nil {
					s.logger.Error().
						Err(err).
						Str("debrid", link.Debrid).
						Str("token", utils.Mask(link.Token)).
						Str("reason", linkErr.Code).
						Msg("Failed to suspend account after link error")
					return emptyDownloadLink, err
				}
				if !hasAlternate {
					if status.State == accountpkg.StatePermanentlyDisabled {
						return emptyDownloadLink, NewPermanentError(ErrNoActiveAccount, "no_active_account")
					}
					cooldownErr := NewLinkError(ErrNoActiveAccount, CategoryThrottled, "account_cooldown")
					cooldownErr.RetryAfter = status.RetryAfter
					return emptyDownloadLink, cooldownErr
				}
				// This will use the next available account and fetch a new link, so we need to refetch and revalidate.
				// Account swap doesn't consume a re-insertion attempt.
				return s.fetchAndValidate(ctx, entry, filename, repairAttempt, linkRefreshes)
			}
			s.failRecoveryProbe(link, validationErr)
			if linkErr.ShouldRefetch() {
				if linkRefreshes >= maxLinkRefreshes {
					// Preserve the refetchable error. The outer refresh governor records
					// the failed replacement and suppresses another provider regeneration
					// until its bounded cooldown expires.
					return emptyDownloadLink, validationErr
				}
				return s.refreshRejectedLink(ctx, entry, filename, link, repairAttempt, linkRefreshes)
			}
		} else {
			s.failRecoveryProbe(link, validationErr)
		}
		return emptyDownloadLink, validationErr
	}

	s.completeRecoveryProbe(link)
	if !s.accountValidationIsCurrent(link) {
		// Another request suspended or permanently disabled the account while
		// this validation was in flight. The successful URL may serve this
		// caller, but it must not let a later recovery probe skip its own range validation.
		return link, nil
	}
	if s.validated.Size() >= maxValidatedEntries {
		s.validated.Clear()
	}
	s.validated.Store(validationKey(link), struct{}{})
	s.clearRefreshBackoff(lifecyclePolicyKey(ctx, linkLifecycleKey(entry, filename)))
	return link, nil
}

// refreshRejectedLink coalesces provider refreshes across validation and stream
// recovery paths. A rejected replacement starts an adaptive per-file cooldown:
// callers continue probing the cached CDN URL for recovery, but do not repeatedly
// regenerate provider links while that URL remains rejected.
func (s *Service) refreshRejectedLink(ctx context.Context, entry *storage.Entry, filename string, rejected types.DownloadLink, repairAttempt, linkRefreshes int) (types.DownloadLink, error) {
	key := lifecyclePolicyKey(ctx, linkLifecycleKey(entry, filename))
	return doLinkFlight(ctx, &s.refreshflight, key, func(sharedCtx context.Context) (types.DownloadLink, error) {
		if err := sharedCtx.Err(); err != nil {
			return emptyDownloadLink, err
		}
		if delay, blocked := s.refreshDelay(key); blocked {
			linkErr := NewLinkError(
				fmt.Errorf("download link refresh deferred for %s", delay.Round(time.Millisecond)),
				CategoryThrottled,
				CodeLinkRefreshCooldown,
			)
			linkErr.RetryAfter = delay
			return emptyDownloadLink, linkErr
		}
		if err := s.invalidateCachedLink(rejected); err != nil {
			return emptyDownloadLink, err
		}

		replacement, err := s.fetchAndValidate(sharedCtx, entry, filename, repairAttempt, linkRefreshes+1)
		if err != nil {
			if linkErr := GetLinkError(err); linkErr != nil && linkErr.ShouldRefetch() &&
				!errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				delay, failures := s.recordRefreshFailure(key)
				s.logger.Warn().
					Str("debrid", entry.ActiveProvider).
					Str("infohash", entry.InfoHash).
					Str("filename", filename).
					Str("error_code", safeRefetchLogCode(linkErr.Code)).
					Str("error_category", linkErr.Category.String()).
					Str("failure_scope", "download_link").
					Int("failures", failures).
					Dur("retry_after", delay).
					Msg("Download link replacement was rejected; deferring provider refresh")
			}
			return emptyDownloadLink, err
		}

		s.clearRefreshBackoff(key)
		return replacement, nil
	})
}

// doLinkFlight keeps one bounded provider lifecycle operation running
// independently of any individual HTTP/FUSE caller. Each waiter may still
// leave immediately when its own request is canceled, but that cancellation
// cannot poison playback for callers sharing the same resolution or refresh.
func doLinkFlight(
	ctx context.Context,
	group *singleflight.Group,
	key string,
	work func(context.Context) (types.DownloadLink, error),
) (types.DownloadLink, error) {
	if err := ctx.Err(); err != nil {
		return emptyDownloadLink, err
	}

	result := group.DoChan(key, func() (any, error) {
		sharedCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedLinkTimeout)
		defer cancel()
		return work(sharedCtx)
	})
	select {
	case <-ctx.Done():
		return emptyDownloadLink, ctx.Err()
	case completed := <-result:
		if completed.Err != nil {
			return emptyDownloadLink, completed.Err
		}
		link, ok := completed.Val.(types.DownloadLink)
		if !ok {
			return emptyDownloadLink, fmt.Errorf("shared link operation returned %T", completed.Val)
		}
		return link, nil
	}
}

func linkLifecycleKey(entry *storage.Entry, filename string) string {
	placementID := ""
	if placement := entry.Providers[entry.ActiveProvider]; placement != nil {
		placementID = placement.ID
	}
	return entry.ActiveProvider + "\x00" + entry.InfoHash + "\x00" + placementID + "\x00" + filename
}

func (s *Service) refreshDelay(key string) (time.Duration, bool) {
	now := s.now()
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()

	state, exists := s.refreshBackoffs[key]
	if !exists || !now.Before(state.nextAttempt) {
		return 0, false
	}
	return state.nextAttempt.Sub(now), true
}

func (s *Service) recordRefreshFailure(key string) (time.Duration, int) {
	now := s.now()
	s.refreshMu.Lock()
	defer s.refreshMu.Unlock()

	state, exists := s.refreshBackoffs[key]
	if !exists && len(s.refreshBackoffs) >= maxRefreshBackoffs {
		s.evictOldestRefreshBackoffLocked()
	}
	state.failures++
	delay := refreshBackoffDelay(state.failures)
	state.nextAttempt = now.Add(delay)
	s.refreshBackoffs[key] = state
	return delay, state.failures
}

func (s *Service) clearRefreshBackoff(key string) {
	s.refreshMu.Lock()
	delete(s.refreshBackoffs, key)
	s.refreshMu.Unlock()
}

func (s *Service) evictOldestRefreshBackoffLocked() {
	var oldestKey string
	var oldestTime time.Time
	found := false
	for key, state := range s.refreshBackoffs {
		if !found || state.nextAttempt.Before(oldestTime) {
			oldestKey = key
			oldestTime = state.nextAttempt
			found = true
		}
	}
	if found {
		delete(s.refreshBackoffs, oldestKey)
	}
}

func refreshBackoffDelay(failures int) time.Duration {
	delay := refreshBackoffBase
	for attempt := 1; attempt < failures && delay < refreshBackoffMax; attempt++ {
		if delay >= refreshBackoffMax/2 {
			return refreshBackoffMax
		}
		delay *= 2
	}
	if delay > refreshBackoffMax {
		return refreshBackoffMax
	}
	return delay
}

func (s *Service) handleBadLink(ctx context.Context, err error, entry *storage.Entry, filename string, dl types.DownloadLink, repairAttempt, linkRefreshes int) (types.DownloadLink, error) {
	if errors.Is(err, customerror.HosterUnavailableError) {
		if repairDisabled(ctx) {
			return dl, err
		}
		if entry.Bad {
			return emptyDownloadLink, fmt.Errorf("can't repair %s since it's been marked as bad", entry.GetFolder())
		}
		if repairAttempt >= MaxReinsertionAttempt {
			s.markEntryBad(entry, filename, repairAttempt, "hoster_unavailable")
			return emptyDownloadLink, fmt.Errorf("entry %s file %s still unresolvable after %d re-insertion attempts", entry.GetFolder(), filename, repairAttempt)
		}
		if err := s.repairer(ctx, entry); err != nil {
			return emptyDownloadLink, err
		}

		if entry.Bad {
			// Entry is still bad
			return emptyDownloadLink, fmt.Errorf("entry %s still bad after repair, un-repairable", entry.GetFolder())
		}
		// Bypass singleflight re-entry to avoid deadlock
		return s.fetchAndValidate(ctx, entry, filename, repairAttempt+1, linkRefreshes)
	}
	// Just return the error
	return dl, err
}

// markEntryBad sets entry.Bad and persists it so subsequent GetLink calls
// for the same entry short-circuit instead of triggering another re-insertion
// cycle. Logged once per call.
func (s *Service) markEntryBad(entry *storage.Entry, filename string, attempt int, reason string) {
	entry.Bad = true
	if s.entrySaver != nil {
		if err := s.entrySaver(entry); err != nil {
			s.logger.Warn().
				Err(err).
				Str("infohash", entry.InfoHash).
				Msg("Failed to persist Bad flag after exhausting re-insertion attempts")
		}
	}
	s.logger.Warn().
		Str("infohash", entry.InfoHash).
		Str("name", entry.Name).
		Str("filename", filename).
		Int("attempts", attempt).
		Str("reason", reason).
		Msg("Giving up on entry after repeated failed re-insertions")
}

// fetchLink fetches a download link from the debrid provider (via account cache)
func (s *Service) fetchLink(ctx context.Context, entry *storage.Entry, filename string, attempt, linkRefreshes int) (types.DownloadLink, error) {
	file, err := entry.GetFile(filename)
	if err != nil {
		return emptyDownloadLink, NewPermanentError(
			fmt.Errorf("file %s not found in entry %s: %w", filename, entry.Name, err),
			"file_not_found",
		)
	}

	placementFile, err := s.getPlacementFile(ctx, entry, filename)
	if err != nil {
		return emptyDownloadLink, err
	}

	if placementFile.Link == "" && placementFile.Id == "" {
		return emptyDownloadLink, NewPermanentError(
			fmt.Errorf("file link is missing for %s in entry %s", filename, entry.Name),
			"link_missing",
		)
	}

	client, err := s.getClient(entry.ActiveProvider)
	if err != nil {
		return emptyDownloadLink, NewPermanentError(
			fmt.Errorf("debrid client not found: %s", entry.ActiveProvider),
			"client_not_found",
		)
	}

	placement := entry.Providers[entry.ActiveProvider]
	if placement == nil {
		return emptyDownloadLink, NewPermanentError(
			fmt.Errorf("no placement found for debrid %s with infohash %s", entry.ActiveProvider, entry.InfoHash),
			"placement_not_found",
		)
	}

	debridFile := &types.File{
		Id:        placementFile.Id,
		Link:      placementFile.Link,
		Path:      placementFile.Path,
		Name:      file.Name,
		Size:      file.Size,
		ByteRange: file.ByteRange,
		Deleted:   file.Deleted,
	}

	// This uses account-level caching internally
	downloadLink, err := debrid.ResolveDownloadLink(ctx, client, placement.ID, debridFile)
	if err != nil {
		var unavailable *accountpkg.UnavailableError
		if errors.As(err, &unavailable) {
			if unavailable.Temporary {
				cooldownErr := NewLinkError(err, CategoryThrottled, "account_cooldown")
				cooldownErr.RetryAfter = unavailable.RetryAfter
				return downloadLink, cooldownErr
			}
			return downloadLink, NewPermanentError(ErrNoActiveAccount, "no_active_account")
		}
		return downloadLink, err
	}

	if downloadLink.Empty() {
		if repairDisabled(ctx) {
			return emptyDownloadLink, NewPermanentError(
				fmt.Errorf("alternate placement returned an empty link for %s", filename),
				"empty_link",
			)
		}
		// Let's try to reinsert the entry
		if entry.Bad {
			return emptyDownloadLink, fmt.Errorf("can't repair %s since it's been marked as bad", entry.GetFolder())
		}
		if attempt >= MaxReinsertionAttempt {
			s.markEntryBad(entry, filename, attempt, "empty_link")
			return emptyDownloadLink, fmt.Errorf("entry %s file %s still resolves to an empty link after %d re-insertion attempts", entry.GetFolder(), filename, attempt)
		}
		if err := s.repairer(ctx, entry); err != nil {
			return emptyDownloadLink, err
		}

		if entry.Bad {
			// Entry is still bad
			return emptyDownloadLink, fmt.Errorf("entry %s still bad after repair, un-repairable", entry.GetFolder())
		}
		// Bypass singleflight re-entry to avoid deadlock
		return s.fetchAndValidate(ctx, entry, filename, attempt+1, linkRefreshes)
	}

	return downloadLink, nil
}

// getPlacementFile retrieves the placement file with refresh fallback
func (s *Service) getPlacementFile(ctx context.Context, entry *storage.Entry, filename string) (*storage.ProviderFile, error) {
	_, ok := entry.Files[filename]
	if !ok {
		return nil, NewPermanentError(
			fmt.Errorf("file %s not found in entry", filename),
			"file_not_found",
		)
	}

	placement := entry.Providers[entry.ActiveProvider]
	if placement == nil {
		return nil, NewPermanentError(
			fmt.Errorf("no placement found for debrid %s with infohash %s", entry.ActiveProvider, entry.InfoHash),
			"placement_not_found",
		)
	}

	placementFile := placement.Files[filename]
	if placementFile == nil || (placementFile.Link == "" && placementFile.Id == "") {
		if repairDisabled(ctx) {
			return nil, NewPermanentError(
				fmt.Errorf("file %s is unavailable in alternate placement %s", filename, entry.ActiveProvider),
				"file_not_available",
			)
		}
		if s.entryRefresher == nil {
			return nil, NewPermanentError(
				fmt.Errorf("file %s not available and no refresher configured", filename),
				"no_refresher",
			)
		}

		refreshed, err := s.entryRefresher(entry.InfoHash)
		if err != nil {
			return nil, NewRefetchableError(
				fmt.Errorf("failed to refresh entry: %w", err),
				"refresh_failed",
			)
		}

		file := refreshed.Files[filename]
		if file == nil {
			return nil, NewPermanentError(
				fmt.Errorf("file disappeared after refresh"),
				"file_disappeared",
			)
		}

		placement = refreshed.Providers[entry.ActiveProvider]
		if placement == nil {
			return nil, NewPermanentError(
				fmt.Errorf("placement disappeared after refresh for debrid %s", entry.ActiveProvider),
				"placement_disappeared",
			)
		}

		placementFile = placement.Files[filename]
		if placementFile == nil || (placementFile.Link == "" && placementFile.Id == "") {
			return nil, NewPermanentError(
				fmt.Errorf("file %s not available after refresh", filename),
				"file_not_available",
			)
		}

		*entry = *refreshed
	}

	return placementFile, nil
}

// validateLink validates both the generated URL and the exact byte-range
// behavior playback depends on. A successful HEAD alone is insufficient: some
// expired or intermediary URLs still answer HEAD while ignoring ranged GETs.
func (s *Service) validateLink(ctx context.Context, link *types.DownloadLink) error {
	if link == nil {
		return NewPermanentError(ErrEmptyLink, "empty_link")
	}
	if link.Empty() {
		return NewPermanentError(fmt.Errorf("download URL is empty for %s", link.Filename), "empty_link")
	}
	ctx = s.withCDNIdentity(ctx, link)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link.DownloadLink, nil)
	if err != nil {
		return NewPermanentError(
			fmt.Errorf("failed to create range probe: %w", err),
			"request_creation_failed",
		)
	}
	// Avoid a zero end for ordinary objects: some CDNs misinterpret 0-0
	// as an unbounded read. Still probe genuinely one-byte objects exactly.
	probeEnd := int64(1)
	if link.Size == 1 {
		probeEnd = 0
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", probeEnd))
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Cache-Control", "no-cache")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return NewRetryableError(
			fmt.Errorf("range probe failed: %w", err),
			"network_error",
		)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		errorCode := resp.Header.Get("X-Error")
		if errorCode != "" {
			linkErr := ErrorCodeToLinkError(errorCode)
			linkErr.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), s.now())
			return linkErr
		}
		return ClassifyHTTPStatus(resp.StatusCode, resp.Header)
	}
	if resp.StatusCode != http.StatusPartialContent {
		return NewRefetchableError(
			fmt.Errorf("range probe returned HTTP %d instead of 206", resp.StatusCode),
			"range_probe_status",
		)
	}
	if encoding := strings.TrimSpace(resp.Header.Get("Content-Encoding")); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return NewRefetchableError(
			fmt.Errorf("range probe returned unexpected content encoding %q", encoding),
			"range_probe_encoding",
		)
	}
	probeLength, err := validateLinkProbeContentRange(resp.Header.Get("Content-Range"), link.Size, probeEnd)
	if err != nil {
		return NewRefetchableError(err, "range_probe_content_range")
	}
	if resp.ContentLength >= 0 && resp.ContentLength != probeLength {
		return NewRefetchableError(
			fmt.Errorf("range probe Content-Length is %d, want %d", resp.ContentLength, probeLength),
			"range_probe_content_length",
		)
	}

	// Read the complete tiny response plus at most one sentinel byte. This
	// proves the body is neither empty nor overlong and lets compliant transports
	// reuse the connection without risking a full-file download when Range was
	// ignored.
	probe, err := io.ReadAll(io.LimitReader(resp.Body, probeLength+1))
	if err != nil {
		return NewRetryableError(fmt.Errorf("range probe body failed: %w", err), "range_probe_body")
	}
	if int64(len(probe)) != probeLength {
		return NewRefetchableError(
			fmt.Errorf("range probe returned %d body bytes, want %d", len(probe), probeLength),
			"range_probe_body_length",
		)
	}
	return nil
}

// validateLinkProbeContentRange returns the exact bounded body length. When
// size is unknown, a one-byte object may legitimately clamp the requested end
// at EOF; no other undersized range is accepted.
func validateLinkProbeContentRange(value string, expectedSize, requestedEnd int64) (int64, error) {
	value = strings.TrimSpace(value)
	fields := strings.Fields(value)
	if len(fields) != 2 || !strings.EqualFold(fields[0], "bytes") {
		return 0, fmt.Errorf("range probe returned invalid Content-Range %q", value)
	}
	bounds, totalText, found := strings.Cut(fields[1], "/")
	if !found || totalText == "" {
		return 0, fmt.Errorf("range probe returned invalid Content-Range %q", value)
	}
	total, err := strconv.ParseInt(totalText, 10, 64)
	if err != nil || total <= 0 {
		return 0, fmt.Errorf("range probe returned invalid Content-Range %q", value)
	}
	if expectedSize > 0 && total != expectedSize {
		return 0, fmt.Errorf("range probe total %d does not match expected size %d", total, expectedSize)
	}
	end := min(requestedEnd, total-1)
	if bounds != "0-"+strconv.FormatInt(end, 10) {
		return 0, fmt.Errorf("range probe returned invalid Content-Range %q", value)
	}
	return end + 1, nil
}

func (s *Service) validateWithRetry(ctx context.Context, link *types.DownloadLink) error {
	attempts := max(1, s.retries+1)
	if failFast(ctx) {
		attempts = 1
	}
	remainingWait := maxLinkRetryWait
	var lastErr error

	for attempt := 0; attempt < attempts; attempt++ {
		lastErr = s.validateLink(ctx, link)
		if lastErr == nil {
			return nil
		}
		linkErr := GetLinkError(lastErr)
		if linkErr == nil || (!linkErr.ShouldRetry() && !linkErr.ShouldBackoff()) || attempt+1 >= attempts {
			return lastErr
		}

		delay := linkRetryDelay(linkErr, attempt, remainingWait)
		if delay <= 0 {
			return lastErr
		}
		if err := s.wait(ctx, delay); err != nil {
			return err
		}
		remainingWait -= delay
	}
	return lastErr
}

func linkRetryDelay(linkErr *Error, attempt int, remaining time.Duration) time.Duration {
	if remaining <= 0 {
		return 0
	}
	delay := 500 * time.Millisecond
	if linkErr != nil && linkErr.ShouldBackoff() && linkErr.RetryAfter > 0 {
		delay = linkErr.RetryAfter
	} else {
		for range attempt {
			if delay >= maxLinkRetryWait/2 {
				delay = maxLinkRetryWait
				break
			}
			delay *= 2
		}
	}
	if delay > maxLinkRetryWait {
		delay = maxLinkRetryWait
	}
	if delay > remaining {
		delay = remaining
	}
	return delay
}

func waitWithContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func validationKey(link types.DownloadLink) string {
	return link.DownloadLink + "\x00" +
		link.Generated.UTC().Format(time.RFC3339Nano) + "\x00" +
		link.ExpiresAt.UTC().Format(time.RFC3339Nano)
}

// suspendLinkAccount applies a recoverable provider-pressure cooldown. Account
// expiration and authentication failures continue to use permanent disabling.
func (s *Service) suspendLinkAccount(link types.DownloadLink, linkErr *Error) (bool, accountpkg.RecoveryStatus, error) {
	client, err := s.getClient(link.Debrid)
	if err != nil {
		return false, accountpkg.RecoveryStatus{}, fmt.Errorf("failed to get client for debrid %s: %w", link.Debrid, err)
	}

	accountManager := client.AccountManager()
	if accountManager == nil {
		return false, accountpkg.RecoveryStatus{}, fmt.Errorf("account manager not available for debrid %s", link.Debrid)
	}
	account, err := accountManager.GetAccount(link.Token)
	if err != nil {
		return false, accountpkg.RecoveryStatus{}, fmt.Errorf("failed to get account for token %s: %w", utils.Mask(link.Token), err)
	}

	if account == nil {
		return false, accountpkg.RecoveryStatus{}, fmt.Errorf("account not found for token %s", utils.Mask(link.Token))
	}

	status, _ := accountManager.SuspendTemporary(account, link.RecoveryProbeID, linkErr.RetryAfter, linkErr.Code)

	// Remove all validations for all the links
	s.validated.Clear()
	return len(accountManager.Active()) > 0, status, nil
}

func (s *Service) completeRecoveryProbe(link types.DownloadLink) {
	if link.RecoveryProbeID == 0 {
		return
	}
	accountManager, account, err := s.linkAccount(link)
	if err != nil {
		s.logger.Error().Err(err).Str("debrid", link.Debrid).Msg("Failed to complete account recovery probe")
		return
	}
	accountManager.MarkHealthy(account, link.RecoveryProbeID)
}

func (s *Service) failRecoveryProbe(link types.DownloadLink, validationErr error) {
	if link.RecoveryProbeID == 0 {
		return
	}
	accountManager, account, err := s.linkAccount(link)
	if err != nil {
		s.logger.Error().Err(err).Str("debrid", link.Debrid).Msg("Failed to close account recovery probe")
		return
	}
	if errors.Is(validationErr, context.Canceled) || errors.Is(validationErr, context.DeadlineExceeded) {
		accountManager.ReleaseRecoveryProbe(account, link.RecoveryProbeID)
		return
	}
	reason := "link_validation_failed"
	var retryAfter time.Duration
	if linkErr := GetLinkError(validationErr); linkErr != nil {
		if linkErr.Code != "" {
			reason = linkErr.Code
		}
		retryAfter = linkErr.RetryAfter
	}
	accountManager.FailRecoveryProbe(account, link.RecoveryProbeID, retryAfter, reason)
}

func (s *Service) linkAccount(link types.DownloadLink) (*accountpkg.Manager, *accountpkg.Account, error) {
	client, err := s.getClient(link.Debrid)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get client for debrid %s: %w", link.Debrid, err)
	}
	accountManager := client.AccountManager()
	if accountManager == nil {
		return nil, nil, fmt.Errorf("account manager not available for debrid %s", link.Debrid)
	}
	account, err := accountManager.GetAccount(link.Token)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get account for token %s: %w", utils.Mask(link.Token), err)
	}
	return accountManager, account, nil
}

func (s *Service) accountValidationIsCurrent(link types.DownloadLink) bool {
	client, err := s.getClient(link.Debrid)
	if err != nil {
		return false
	}
	accountManager := client.AccountManager()
	if accountManager == nil {
		// Some non-account test and extension clients deliberately omit account
		// management. Preserve their existing validation-cache behavior.
		return true
	}
	account, err := accountManager.GetAccount(link.Token)
	if err != nil {
		return false
	}
	return accountManager.Status(account).State == accountpkg.StateActive
}

// invalidateCachedLink removes only local validation and account-cache state.
func (s *Service) invalidateCachedLink(link types.DownloadLink) error {
	s.validated.Delete(validationKey(link))

	if link.Debrid == "" {
		return fmt.Errorf("invalid link")
	}

	client, err := s.getClient(link.Debrid)
	if err != nil {
		return err
	}
	if invalidator, ok := client.(cachedLinkInvalidator); ok {
		return invalidator.InvalidateCachedLink(link)
	}
	accounts := client.AccountManager()
	if accounts == nil {
		return fmt.Errorf("account manager not available for debrid %s", link.Debrid)
	}
	return accounts.InvalidateDownloadLink(link)
}

// Clear removes all validation and refresh-backoff tracking entries.
func (s *Service) Clear() {
	s.validated.Clear()
	s.refreshMu.Lock()
	clear(s.refreshBackoffs)
	s.refreshMu.Unlock()
}
