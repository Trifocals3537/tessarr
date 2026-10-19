package account

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trifocals3537/tessarr/internal/request"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/puzpuzpuz/xsync/v4"
)

type Account struct {
	Debrid      string                                 `json:"debrid"` // The debrid service name, e.g. "realdebrid"
	links       *xsync.Map[string, types.DownloadLink] // key is the sliced file link
	Index       int                                    `json:"index"` // The index of the account in the config
	Disabled    atomic.Bool                            `json:"disabled"`
	Token       string                                 `json:"token"`
	TrafficUsed atomic.Int64                           `json:"traffic_used"` // Traffic used in bytes
	Username    string                                 `json:"username"`     // Username for the account
	httpClient  *request.Client
	Expiration  time.Time `json:"expiration"`

	// Permanent-disable tracking. Temporary recovery state is protected below.
	DisableCount atomic.Int32 `json:"disable_count"`

	recoveryMu sync.Mutex
	recovery   recoveryState
}

func (a *Account) Equals(other *Account) bool {
	if other == nil {
		return false
	}
	return a.Token == other.Token && a.Debrid == other.Debrid
}

func (a *Account) Client() *request.Client {
	return a.httpClient
}

// slice download link
func (a *Account) sliceFileLink(fileLink string) string {
	if a.Debrid != "realdebrid" {
		return fileLink
	}
	if len(fileLink) < 39 {
		return fileLink
	}
	return fileLink[0:39]
}

func (a *Account) GetDownloadLink(id string, file *types.File, fetcher LinkFetcher) (types.DownloadLink, error) {
	return a.GetDownloadLinkContext(context.Background(), id, file, func(_ context.Context, account *Account, id string, file *types.File) (types.DownloadLink, error) {
		return fetcher(account, id, file)
	})
}

func (a *Account) GetDownloadLinkContext(ctx context.Context, id string, file *types.File, fetcher ContextLinkFetcher) (types.DownloadLink, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return types.DownloadLink{}, err
	}
	slicedLink := a.sliceFileLink(file.Link)
	dl, ok := a.links.Load(slicedLink)
	if ok && dl.NeedsRefresh(time.Now()) {
		a.links.Delete(slicedLink)
		ok = false
	}
	if !ok {
		var err error
		dl, err = fetcher(ctx, a, id, file)
		if err != nil {
			return dl, err
		}
		if err := ctx.Err(); err != nil {
			return dl, err
		}
		a.storeLink(dl)
	}
	if err := dl.Valid(); err != nil {
		return types.DownloadLink{}, err
	}
	return dl, nil
}

func (a *Account) storeLink(dl types.DownloadLink) {
	// Probe ownership is request-scoped and must never leak into the cache.
	dl.RecoveryProbeID = 0
	slicedLink := a.sliceFileLink(dl.Link)
	a.links.Store(slicedLink, dl)
}

// InvalidateLink evicts only Tessarr's cached URL. It deliberately does not
// invoke a provider deletion endpoint because link refresh must never remove
// the user's remote download or download-history record.
func (a *Account) InvalidateLink(link types.DownloadLink) {
	slicedLink := a.sliceFileLink(link.Link)
	a.links.Delete(slicedLink)
}

func (a *Account) DeleteLink(link types.DownloadLink, deleter LinkDeleter) error {
	a.InvalidateLink(link)
	if deleter != nil {
		return deleter(a, link)
	}
	return nil
}
func (a *Account) ClearDownloadLinks() {
	a.links.Clear()
}
func (a *Account) DownloadLinksCount() int {
	return a.links.Size()
}

// GetRandomLink returns any cached download link for speed testing
// Returns empty link if no links are cached
func (a *Account) GetRandomLink() (types.DownloadLink, bool) {
	var result types.DownloadLink
	found := false
	a.links.Range(func(_ string, link types.DownloadLink) bool {
		if !link.Empty() {
			result = link
			found = true
			return false // stop iteration
		}
		return true
	})
	return result, found
}

func (a *Account) StoreDownloadLinks(dls map[string]*types.DownloadLink) {
	for _, dl := range dls {
		a.storeLink(*dl)
	}
}

// MarkDisabled marks the account as disabled and increments the disable count
func (a *Account) MarkDisabled() {
	a.Disabled.Store(true)
	a.recoveryMu.Lock()
	a.recovery = recoveryState{}
	a.recoveryMu.Unlock()
	a.DisableCount.Add(1)
}

func (a *Account) Reset() {
	a.recoveryMu.Lock()
	a.recovery = recoveryState{}
	a.recoveryMu.Unlock()
	a.DisableCount.Store(0)
	a.Disabled.Store(false)
}
