package premiumize

import (
	"context"

	"github.com/Trifocals3537/tessarr/pkg/debrid/common"
)

func (pm *Premiumize) TorrentPresenceContext(ctx context.Context, id string) (common.TorrentPresence, error) {
	transfers, err := pm.listTransfersContext(ctx)
	if err != nil {
		return common.TorrentPresenceUnknown, err
	}
	for _, transfer := range transfers {
		if id != "" && transfer.ID == id {
			return common.TorrentPresent, nil
		}
	}
	// The provider exposes an account-wide list, not an exact-ID absence
	// response. Omitting a transfer never authorizes local cleanup.
	return common.TorrentPresenceUnknown, common.ErrPresenceUnverifiable
}
