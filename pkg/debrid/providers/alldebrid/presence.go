package alldebrid

import (
	"context"
	"net/http"
	"net/url"
	"strconv"

	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/debrid/common"
)

func (ad *AllDebrid) TorrentPresenceContext(ctx context.Context, id string) (common.TorrentPresence, error) {
	n, err := strconv.ParseUint(id, 10, 64)
	if err != nil || n == 0 || strconv.FormatUint(n, 10) != id {
		return common.TorrentPresenceUnknown, common.ErrPresenceUnverifiable
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ad.Host+"/magnet/status?id="+url.QueryEscape(id), nil)
	if err != nil {
		return common.TorrentPresenceUnknown, err
	}
	resp, err := ad.client.Do(req)
	if err != nil {
		return common.TorrentPresenceUnknown, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusBadRequest && resp.StatusCode != http.StatusNotFound {
		return common.TorrentPresenceUnknown, common.ErrPresenceUnverifiable
	}
	var result TorrentInfoResponse
	if err := utils.DecodeJSONResponseBounded(resp.Body, &result, 64<<10); err != nil {
		return common.TorrentPresenceUnknown, common.ErrPresenceUnverifiable
	}
	if result.Status == "error" && result.Error != nil && result.Error.Code == "MAGNET_INVALID_ID" {
		return common.TorrentAbsent, nil
	}
	if resp.StatusCode == http.StatusOK && result.Status == "success" && result.Error == nil {
		if _, err := findMagnet(result.Data.Magnets, id); err == nil {
			return common.TorrentPresent, nil
		}
	}
	return common.TorrentPresenceUnknown, common.ErrPresenceUnverifiable
}
