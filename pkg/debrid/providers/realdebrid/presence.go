package realdebrid

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/debrid/common"
)

func (r *RealDebrid) TorrentPresenceContext(ctx context.Context, id string) (common.TorrentPresence, error) {
	if strings.TrimSpace(id) == "" {
		return common.TorrentPresenceUnknown, fmt.Errorf("transfer ID is empty")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.Host+"/torrents/info/"+url.PathEscape(id), nil)
	if err != nil {
		return common.TorrentPresenceUnknown, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return common.TorrentPresenceUnknown, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return common.TorrentPresenceUnknown, fmt.Errorf("realdebrid presence lookup: HTTP %d", resp.StatusCode)
	}
	var result struct {
		ID        string `json:"id"`
		Error     string `json:"error"`
		ErrorCode *int   `json:"error_code"`
	}
	if err := utils.DecodeJSONResponseBounded(resp.Body, &result, 64<<10); err != nil {
		return common.TorrentPresenceUnknown, common.ErrPresenceUnverifiable
	}
	if resp.StatusCode == http.StatusNotFound && result.ErrorCode != nil && *result.ErrorCode == 7 && result.Error != "" {
		return common.TorrentAbsent, nil // Documented resource-not-found code.
	}
	if resp.StatusCode == http.StatusOK && result.ID == id && result.Error == "" && result.ErrorCode == nil {
		return common.TorrentPresent, nil
	}
	return common.TorrentPresenceUnknown, common.ErrPresenceUnverifiable
}
