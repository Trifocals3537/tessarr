package common

import (
	"context"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/debrid/account"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/rs/zerolog"
)

type Client interface {
	SubmitMagnet(tr *types.Torrent) (*types.Torrent, error)
	CheckStatus(tr *types.Torrent) (*types.Torrent, error)
	GetDownloadLink(torrentID string, file *types.File) (types.DownloadLink, error)
	DeleteTorrent(torrentId string) error
	IsAvailable(infohashes []string) map[string]bool
	UpdateTorrent(torrent *types.Torrent) error
	GetTorrent(torrentId string) (*types.Torrent, error)
	GetTorrents() ([]*types.Torrent, error)
	Config() config.Debrid
	Logger() zerolog.Logger
	RefreshDownloadLinks() error
	CheckFile(ctx context.Context, infohash, fileID string) error // fileID here can link, file id(in the case of torbox), etc.
	AccountManager() *account.Manager                             // Returns the active download account/token
	GetProfile() (*types.Profile, error)
	GetAvailableSlots() (int, error)
	SyncAccounts() // Updates each accounts details(like traffic, username, etc.)
	DeleteLink(dl types.DownloadLink) error
	SpeedTest(ctx context.Context) types.SpeedTestResult
	SupportsCheck() bool
}

// ContextTorrentLister is the optional cancellation-aware torrent listing
// capability used by manager reconciliation. Client implementations should
// provide it whenever their transport supports request contexts.
type ContextTorrentLister interface {
	GetTorrentsContext(context.Context) ([]*types.Torrent, error)
}

// ContextDownloadLinkRefresher is the optional cancellation-aware account
// link-cache refresh capability used by scheduled provider maintenance.
type ContextDownloadLinkRefresher interface {
	RefreshDownloadLinksContext(context.Context) error
}

// ContextAccountSyncer is the optional cancellation-aware account metadata
// synchronization capability used by scheduled provider maintenance.
type ContextAccountSyncer interface {
	SyncAccountsContext(context.Context) error
}

// ContextMagnetSubmitter is the optional cancellation-aware torrent submission
// capability used by imports and repairs.
type ContextMagnetSubmitter interface {
	SubmitMagnetContext(context.Context, *types.Torrent) (*types.Torrent, error)
}

// ContextStatusChecker is the optional cancellation-aware initial torrent
// status capability used immediately after submission and by queue workers.
type ContextStatusChecker interface {
	CheckStatusContext(context.Context, *types.Torrent) (*types.Torrent, error)
}

// ContextDownloadLinkResolver is the optional cancellation-aware playback link
// capability used by streams and repair probes.
type ContextDownloadLinkResolver interface {
	GetDownloadLinkContext(context.Context, string, *types.File) (types.DownloadLink, error)
}

// ResolveDownloadLink prefers cancellation-aware providers while preserving a
// synchronous fallback for third-party clients that implement only Client.
func ResolveDownloadLink(ctx context.Context, client Client, id string, file *types.File) (types.DownloadLink, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return types.DownloadLink{}, err
	}
	if contextual, ok := client.(ContextDownloadLinkResolver); ok {
		return contextual.GetDownloadLinkContext(ctx, id, file)
	}
	link, err := client.GetDownloadLink(id, file)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return link, ctxErr
	}
	return link, err
}
