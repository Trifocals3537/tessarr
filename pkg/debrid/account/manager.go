package account

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/providertraffic"
	"github.com/Trifocals3537/tessarr/internal/request"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"go.uber.org/ratelimit"
)

type LinkFetcher func(account *Account, id string, file *types.File) (types.DownloadLink, error)
type ContextLinkFetcher func(context.Context, *Account, string, *types.File) (types.DownloadLink, error)
type LinkDeleter func(account *Account, dl types.DownloadLink) error
type LinksFetcher func(account *Account) ([]types.DownloadLink, error)
type ContextLinksFetcher func(context.Context, *Account) ([]types.DownloadLink, error)
type SyncFunc func(account *Account) error
type ContextSyncFunc func(context.Context, *Account) error

type Manager struct {
	debrid   string
	current  atomic.Pointer[Account]
	accounts *xsync.Map[string, *Account]
	logger   zerolog.Logger
	now      func() time.Time
}

func NewManager(
	debridConf config.Debrid,
	downloadRL ratelimit.Limiter,
	logger zerolog.Logger,
	trafficControllers ...*providertraffic.Controller,
) *Manager {
	m := &Manager{
		debrid:   debridConf.Name,
		accounts: xsync.NewMap[string, *Account](),
		logger:   logger,
		now:      time.Now,
	}
	cfg := config.Get()
	var traffic *providertraffic.Controller
	if len(trafficControllers) > 0 {
		traffic = trafficControllers[0]
	}

	var firstAccount *Account
	for idx, token := range debridConf.DownloadAPIKeys {
		if token == "" {
			continue
		}
		headers := map[string]string{
			"Authorization": fmt.Sprintf("Bearer %s", token),
		}

		// Create request client with equivalent options
		opts := []request.ClientOption{
			request.WithRateLimiter(downloadRL),
			request.WithHeaders(headers),
			request.WithMaxRetries(cfg.Retries),
			request.WithRetryableStatus(http.StatusTooManyRequests, http.StatusBadGateway, 447),
		}
		if debridConf.Proxy != "" {
			opts = append(opts, request.WithProxy(debridConf.Proxy))
		}
		if traffic != nil {
			opts = append(opts, request.WithProviderTraffic(
				traffic,
				debridConf.Provider,
				token,
			))
		}

		account := &Account{
			Debrid:     debridConf.Name,
			Token:      token,
			Index:      idx,
			links:      xsync.NewMap[string, types.DownloadLink](),
			httpClient: request.New(opts...),
		}
		m.accounts.Store(token, account)
		if firstAccount == nil {
			firstAccount = account
		}
	}
	m.current.Store(firstAccount)
	return m
}

func (m *Manager) Active() []*Account {
	activeAccounts := make([]*Account, 0)
	now := m.nowTime()
	m.accounts.Range(func(key string, acc *Account) bool {
		status := acc.RecoveryStatus(now)
		if status.State == StateActive || status.State == StateRecoveryReady {
			activeAccounts = append(activeAccounts, acc)
		}
		return true
	})

	slices.SortFunc(activeAccounts, func(i, j *Account) int {
		return i.Index - j.Index
	})
	return activeAccounts
}

func (m *Manager) All() []*Account {
	allAccounts := make([]*Account, 0)
	m.accounts.Range(func(key string, acc *Account) bool {
		allAccounts = append(allAccounts, acc)
		return true
	})

	slices.SortFunc(allAccounts, func(i, j *Account) int {
		return i.Index - j.Index
	})
	return allAccounts
}

func (m *Manager) Current() *Account {
	now := m.nowTime()
	current := m.current.Load()
	if current != nil {
		state := current.RecoveryStatus(now).State
		if state == StateActive || state == StateRecoveryReady || state == StateRecoveryProbe {
			return current
		}
	}

	activeAccounts := m.Active()
	if len(activeAccounts) == 0 {
		m.current.Store(nil)
		return nil
	}

	newCurrent := activeAccounts[0]
	m.current.Store(newCurrent)
	return newCurrent
}

func (m *Manager) Disable(account *Account) {
	if account == nil {
		return
	}

	account.MarkDisabled()

	activeAccounts := m.Active()
	if len(activeAccounts) == 0 {
		m.current.Store(nil)
		return
	}
	m.current.Store(activeAccounts[0])
}

// SuspendTemporary removes an account from ordinary selection until its
// cooldown expires. It logs only a real state transition, not every lookup.
func (m *Manager) SuspendTemporary(account *Account, probeID uint64, retryAfter time.Duration, reason string) (RecoveryStatus, bool) {
	if account == nil {
		return RecoveryStatus{}, false
	}
	status, changed := account.SuspendTemporary(m.nowTime(), probeID, retryAfter, reason)
	if changed {
		m.logger.Warn().
			Str("debrid", m.debrid).
			Str("account_token", utils.Mask(account.Token)).
			Str("reason", reason).
			Int("failures", status.Failures).
			Dur("retry_after", status.RetryAfter).
			Msg("Debrid account temporarily suspended")
	}

	active := m.Active()
	if len(active) == 0 {
		m.current.Store(nil)
	} else if current := m.current.Load(); current == nil || current.Equals(account) {
		m.current.Store(active[0])
	}
	return status, changed
}

// MarkHealthy reactivates an account only when the matching half-open probe
// successfully validated a download URL.
func (m *Manager) MarkHealthy(account *Account, probeID uint64) bool {
	if account == nil || !account.MarkHealthy(probeID) {
		return false
	}
	m.current.Store(account)
	m.logger.Info().
		Str("debrid", m.debrid).
		Str("account_token", utils.Mask(account.Token)).
		Msg("Debrid account recovered after successful link validation")
	return true
}

// FailRecoveryProbe advances the account's cooldown only when probeID still
// owns the half-open lease.
func (m *Manager) FailRecoveryProbe(account *Account, probeID uint64, retryAfter time.Duration, reason string) (RecoveryStatus, bool) {
	return m.SuspendTemporary(account, probeID, retryAfter, reason)
}

func (m *Manager) ReleaseRecoveryProbe(account *Account, probeID uint64) bool {
	if account == nil {
		return false
	}
	return account.ReleaseProbe(probeID)
}

func (m *Manager) Status(account *Account) RecoveryStatus {
	if account == nil {
		return RecoveryStatus{State: StatePermanentlyDisabled}
	}
	return account.RecoveryStatus(m.nowTime())
}

func (m *Manager) Reset() {
	m.accounts.Range(func(key string, acc *Account) bool {
		acc.Reset()
		return true
	})

	// Set current to first active account
	activeAccounts := m.Active()
	if len(activeAccounts) > 0 {
		m.current.Store(activeAccounts[0])
	} else {
		m.current.Store(nil)
	}
}

func (m *Manager) GetAccount(token string) (*Account, error) {
	if token == "" {
		return nil, fmt.Errorf("token cannot be empty")
	}
	acc, ok := m.accounts.Load(token)
	if !ok {
		return nil, fmt.Errorf("account not found for token")
	}
	return acc, nil
}

func (m *Manager) GetDownloadLink(id string, file *types.File, fetcher LinkFetcher) (types.DownloadLink, error) {
	return m.GetDownloadLinkContext(context.Background(), id, file, func(_ context.Context, account *Account, id string, file *types.File) (types.DownloadLink, error) {
		return fetcher(account, id, file)
	})
}

func (m *Manager) GetDownloadLinkContext(ctx context.Context, id string, file *types.File, fetcher ContextLinkFetcher) (types.DownloadLink, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return types.DownloadLink{}, err
	}
	now := m.nowTime()
	accounts := m.All()
	if current := m.current.Load(); current != nil {
		for i, acc := range accounts {
			if acc.Equals(current) {
				accounts[0], accounts[i] = accounts[i], accounts[0]
				break
			}
		}
	}

	var lastLink types.DownloadLink
	var lastErr error
	var earliestRetry time.Duration
	temporaryUnavailable := false
	for _, acc := range accounts {
		if err := ctx.Err(); err != nil {
			return lastLink, err
		}
		acquired, probeID, retryAfter := acc.TryAcquire(now)
		if !acquired {
			status := acc.RecoveryStatus(now)
			if status.State != StatePermanentlyDisabled {
				temporaryUnavailable = true
				earliestRetry = earlierPositiveDuration(earliestRetry, retryAfter)
			}
			continue
		}

		dl, err := acc.GetDownloadLinkContext(ctx, id, file, fetcher)
		if probeID != 0 {
			dl.RecoveryProbeID = probeID
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			if probeID != 0 {
				m.ReleaseRecoveryProbe(acc, probeID)
			}
			return dl, ctxErr
		}
		if err == nil {
			m.current.Store(acc)
			return dl, nil
		}
		lastLink, lastErr = dl, err
		if probeID != 0 {
			m.FailRecoveryProbe(acc, probeID, 0, "link_fetch_failed")
		}
	}

	if lastErr != nil {
		return lastLink, lastErr
	}
	if temporaryUnavailable && earliestRetry <= 0 {
		earliestRetry = time.Second
	}
	return types.DownloadLink{}, &UnavailableError{
		Debrid:     m.debrid,
		RetryAfter: earliestRetry,
		Temporary:  temporaryUnavailable,
	}
}

func (m *Manager) StoreDownloadLink(downloadLink types.DownloadLink) {
	if downloadLink.Link == "" || downloadLink.Token == "" {
		return
	}
	account, err := m.GetAccount(downloadLink.Token)
	if err != nil || account == nil {
		return
	}
	account.storeLink(downloadLink)
}

func (m *Manager) DeleteDownloadLink(downloadLink types.DownloadLink, deleter LinkDeleter) error {
	if downloadLink.Link == "" || downloadLink.Token == "" {
		return fmt.Errorf("invalid download link")
	}
	account, err := m.GetAccount(downloadLink.Token)
	if err != nil || account == nil {
		return fmt.Errorf("account not found for download link")
	}
	return account.DeleteLink(downloadLink, deleter)
}

// InvalidateDownloadLink removes only the locally cached URL. Provider-side
// deletion is intentionally reserved for DeleteDownloadLink.
func (m *Manager) InvalidateDownloadLink(downloadLink types.DownloadLink) error {
	if downloadLink.Link == "" || downloadLink.Token == "" {
		return fmt.Errorf("invalid download link")
	}
	account, err := m.GetAccount(downloadLink.Token)
	if err != nil || account == nil {
		return fmt.Errorf("account not found for download link")
	}
	account.InvalidateLink(downloadLink)
	return nil
}

func (m *Manager) Stats() []map[string]any {
	stats := make([]map[string]any, 0)
	now := m.nowTime()
	current := m.Current()

	for _, acc := range m.All() {
		maskedToken := utils.Mask(acc.Token)
		recovery := acc.RecoveryStatus(now)
		accountDetail := map[string]any{
			"in_use":         acc.Equals(current),
			"order":          acc.Index,
			"disabled":       recovery.State == StatePermanentlyDisabled || recovery.State == StateTemporarilySuspended,
			"state":          recovery.State,
			"suspended":      recovery.State == StateTemporarilySuspended || recovery.State == StateRecoveryReady || recovery.State == StateRecoveryProbe,
			"retry_at":       recovery.RetryAt,
			"retry_after":    recovery.RetryAfter.Seconds(),
			"failure_count":  recovery.Failures,
			"failure_reason": recovery.Reason,
			"token_masked":   maskedToken,
			"username":       acc.Username,
			"traffic_used":   acc.TrafficUsed.Load(),
			"expiration":     acc.Expiration,
			"links_count":    acc.DownloadLinksCount(),
			"debrid":         acc.Debrid,
		}
		stats = append(stats, accountDetail)
	}
	return stats
}

func (m *Manager) nowTime() time.Time {
	if m.now == nil {
		return time.Now()
	}
	return m.now()
}

func earlierPositiveDuration(current, candidate time.Duration) time.Duration {
	if candidate <= 0 {
		return current
	}
	if current <= 0 || candidate < current {
		return candidate
	}
	return current
}

func (m *Manager) RefreshLinks(fetcher LinksFetcher) error {
	return m.RefreshLinksContext(context.Background(), func(_ context.Context, account *Account) ([]types.DownloadLink, error) {
		return fetcher(account)
	})
}

func (m *Manager) RefreshLinksContext(ctx context.Context, fetcher ContextLinksFetcher) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	accounts := m.All()
	var wg sync.WaitGroup
	errorsCh := make(chan error, len(accounts))
	for _, acc := range accounts {
		wg.Go(func() {
			links, err := fetcher(ctx, acc)
			if err != nil {
				if ctx.Err() == nil {
					m.logger.Error().Err(err).Str("debrid", m.debrid).Str("account_token", utils.Mask(acc.Token)).Msg("Failed to fetch download links for account")
				}
				errorsCh <- err
				return
			}
			for _, dl := range links {
				if ctx.Err() != nil {
					errorsCh <- ctx.Err()
					return
				}
				acc.storeLink(dl)
			}
		})
	}
	wg.Wait()
	close(errorsCh)
	var refreshErr error
	for err := range errorsCh {
		refreshErr = errors.Join(refreshErr, err)
	}
	return errors.Join(refreshErr, ctx.Err())
}

func (m *Manager) Sync(syncer SyncFunc) {
	_ = m.SyncContext(context.Background(), func(_ context.Context, account *Account) error {
		return syncer(account)
	})
}

func (m *Manager) SyncContext(ctx context.Context, syncer ContextSyncFunc) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	accounts := m.All()
	var wg sync.WaitGroup
	errorsCh := make(chan error, len(accounts))
	for _, acc := range accounts {
		wg.Go(func() {
			if err := syncer(ctx, acc); err != nil {
				if ctx.Err() == nil {
					m.logger.Error().Err(err).Str("debrid", m.debrid).Str("account_token", utils.Mask(acc.Token)).Msg("Failed to sync account")
				}
				errorsCh <- err
				return
			}
			if ctx.Err() != nil {
				errorsCh <- ctx.Err()
				return
			}
			// Check if account has expired
			if !acc.Expiration.IsZero() && utils.Now().After(acc.Expiration) {
				m.logger.Warn().Str("debrid", m.debrid).Str("account_token", utils.Mask(acc.Token)).Msg("Account has expired, disabling")
				m.Disable(acc)
			}
			m.UpdateAccount(acc)
		})
	}
	wg.Wait()
	close(errorsCh)
	var syncErr error
	for err := range errorsCh {
		syncErr = errors.Join(syncErr, err)
	}
	return errors.Join(syncErr, ctx.Err())
}

func (m *Manager) UpdateAccount(updatedAccount *Account) {
	if updatedAccount == nil {
		return
	}
	if updatedAccount.Token == "" {
		return
	}
	m.accounts.Store(updatedAccount.Token, updatedAccount)
}
