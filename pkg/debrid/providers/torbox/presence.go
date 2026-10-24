package torbox

import (
	"context"
	"errors"

	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/pkg/debrid/common"
)

func (tb *Torbox) TorrentPresenceContext(ctx context.Context, id string) (common.TorrentPresence, error) {
	if id == "" {
		return common.TorrentPresenceUnknown, common.ErrPresenceUnverifiable
	}
	t, err := tb.GetTorrentContext(ctx, id)
	if errors.Is(err, customerror.TorrentNotFoundError) {
		return common.TorrentAbsent, nil // Exact-ID 404 with ITEM_NOT_FOUND envelope.
	}
	if err != nil {
		return common.TorrentPresenceUnknown, err
	}
	if t == nil || id == "" || t.Id != id {
		return common.TorrentPresenceUnknown, common.ErrPresenceUnverifiable
	}
	return common.TorrentPresent, nil
}
