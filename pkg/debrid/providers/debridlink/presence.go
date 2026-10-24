package debridlink

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/Trifocals3537/tessarr/pkg/debrid/common"
)

// TorrentPresenceContext uses the documented ids filter, never the full list.
// An empty, successful, complete response to this one-ID query proves absence.
func (dl *DebridLink) TorrentPresenceContext(ctx context.Context, id string) (common.TorrentPresence, error) {
	if strings.TrimSpace(id) == "" || strings.ContainsAny(id, ",[]\"\\ \t\r\n") {
		return common.TorrentPresenceUnknown, fmt.Errorf("invalid exact transfer ID")
	}
	var result torrentInfo
	resp, err := dl.doGetContext(ctx, "/seedbox/list", map[string]string{"ids": id, "page": "0", "perPage": "100"}, &result)
	if err != nil {
		return common.TorrentPresenceUnknown, err
	}
	if resp.StatusCode != http.StatusOK || !result.Success || result.Value == nil {
		return common.TorrentPresenceUnknown, common.ErrPresenceUnverifiable
	}
	if _, found := debridLinkTorrentByID(*result.Value, id); found {
		return common.TorrentPresent, nil
	}
	if len(*result.Value) != 0 {
		return common.TorrentPresenceUnknown, common.ErrPresenceUnverifiable
	}
	if p := result.Pagination; p != nil && (p.Page != 0 || p.Pages > 1 || p.Next != -1) {
		return common.TorrentPresenceUnknown, common.ErrPresenceUnverifiable
	}
	return common.TorrentAbsent, nil
}
