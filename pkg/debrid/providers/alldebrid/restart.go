package alldebrid

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
)

const (
	allDebridStatusNotDownloaded = 7

	allDebridRestartMaxAttempts = 2
	allDebridRestartCooldown    = 30 * time.Minute
	allDebridRestartStateTTL    = 24 * time.Hour
	allDebridRestartStateLimit  = 4096
)

type magnetRestartState struct {
	attempts    int
	nextAttempt time.Time
	expiresAt   time.Time
}

type magnetRestartDecision uint8

const (
	magnetRestartDeferred magnetRestartDecision = iota
	magnetRestartExecute
	magnetRestartExhausted
)

type magnetRestartResponse struct {
	Status string         `json:"status"`
	Error  *errorResponse `json:"error"`
}

var allDebridStatusDescriptions = map[int]string{
	5:  "upload failed",
	6:  "internal error while unpacking",
	7:  "not downloaded within 20 minutes",
	8:  "file too large",
	9:  "internal error",
	10: "download exceeded 72 hours",
	11: "deleted on the hoster website",
	12: "processing failed",
	13: "processing failed",
	14: "tracker contact failed",
	15: "file unavailable because no peers were found",
}

func newAllDebridStatusError(name string, statusCode int) error {
	description, terminal := allDebridStatusDescriptions[statusCode]
	if !terminal {
		description = "unknown provider status"
	}
	err := fmt.Errorf(
		"torrent %q has AllDebrid error status %d (%s)",
		name,
		statusCode,
		description,
	)
	if terminal {
		return fmt.Errorf("%w: %v", types.ErrTerminalProviderTorrent, err)
	}
	return err
}

func (ad *AllDebrid) recoverNotDownloadedTorrentContext(ctx context.Context, torrent *types.Torrent) (*types.Torrent, error) {
	attempt, decision := ad.planMagnetRestart(torrent.Id)
	switch decision {
	case magnetRestartExhausted:
		return torrent, newAllDebridStatusError(torrent.Name, allDebridStatusNotDownloaded)
	case magnetRestartDeferred:
		torrent.Status = types.TorrentStatusDownloading
		return torrent, nil
	case magnetRestartExecute:
		if err := ad.restartMagnetContext(ctx, torrent.Id); err != nil {
			ad.logger.Warn().
				Err(err).
				Str("torrent_id", torrent.Id).
				Int("attempt", attempt).
				Msg("AllDebrid magnet restart failed; deferring before the next bounded attempt")
		} else {
			ad.logger.Info().
				Str("torrent_id", torrent.Id).
				Int("attempt", attempt).
				Msg("Restarted transiently failed AllDebrid magnet")
		}
		torrent.Status = types.TorrentStatusDownloading
		return torrent, nil
	default:
		return torrent, newAllDebridStatusError(torrent.Name, allDebridStatusNotDownloaded)
	}
}

func (ad *AllDebrid) planMagnetRestart(torrentID string) (int, magnetRestartDecision) {
	now := time.Now()
	if ad.restartNow != nil {
		now = ad.restartNow()
	}

	ad.restartMu.Lock()
	defer ad.restartMu.Unlock()
	if ad.restartStates == nil {
		ad.restartStates = make(map[string]magnetRestartState)
	}
	for id, state := range ad.restartStates {
		if !now.Before(state.expiresAt) {
			delete(ad.restartStates, id)
		}
	}

	state := ad.restartStates[torrentID]
	if state.attempts >= allDebridRestartMaxAttempts {
		if now.Before(state.nextAttempt) {
			return state.attempts, magnetRestartDeferred
		}
		return state.attempts, magnetRestartExhausted
	}
	if now.Before(state.nextAttempt) {
		return state.attempts, magnetRestartDeferred
	}
	if state.attempts == 0 && len(ad.restartStates) >= allDebridRestartStateLimit {
		ad.evictOldestMagnetRestartLocked()
	}
	state.attempts++
	state.nextAttempt = now.Add(allDebridRestartCooldown)
	state.expiresAt = now.Add(allDebridRestartStateTTL)
	ad.restartStates[torrentID] = state
	return state.attempts, magnetRestartExecute
}

func (ad *AllDebrid) clearMagnetRestart(torrentID string) {
	if torrentID == "" {
		return
	}
	ad.restartMu.Lock()
	delete(ad.restartStates, torrentID)
	ad.restartMu.Unlock()
}

func (ad *AllDebrid) evictOldestMagnetRestartLocked() {
	oldestID := ""
	var oldestExpiry time.Time
	for id, state := range ad.restartStates {
		if oldestID == "" || state.expiresAt.Before(oldestExpiry) {
			oldestID = id
			oldestExpiry = state.expiresAt
		}
	}
	delete(ad.restartStates, oldestID)
}

func (ad *AllDebrid) restartMagnet(torrentID string) error {
	return ad.restartMagnetContext(context.Background(), torrentID)
}

func (ad *AllDebrid) restartMagnetContext(ctx context.Context, torrentID string) error {
	numericID, err := strconv.Atoi(torrentID)
	if err != nil || numericID <= 0 {
		return fmt.Errorf("invalid AllDebrid torrent ID")
	}

	form := url.Values{"id": {torrentID}}
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		allDebridRestartEndpoint(ad.Host),
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := ad.client.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("AllDebrid magnet restart request failed")
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10+1))
		_ = resp.Body.Close()
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("AllDebrid magnet restart returned HTTP %d", resp.StatusCode)
	}

	var result magnetRestartResponse
	if resp.ContentLength == 0 {
		return fmt.Errorf("AllDebrid magnet restart returned an empty response")
	}
	if err := utils.DecodeJSONResponseBounded(resp.Body, &result, 64<<10); err != nil {
		// Decoder errors can contain response excerpts. Provider responses may
		// include operational detail, so keep the surfaced error body-free.
		return fmt.Errorf("invalid AllDebrid magnet restart response")
	}
	if !strings.EqualFold(result.Status, "success") {
		code := "unknown"
		if result.Error != nil && result.Error.Code != "" {
			code = result.Error.Code
		}
		return fmt.Errorf("AllDebrid magnet restart failed (code %s)", code)
	}
	return nil
}

func allDebridRestartEndpoint(host string) string {
	endpoint := strings.TrimRight(host, "/")
	if strings.HasSuffix(endpoint, "/v4.1") {
		endpoint = strings.TrimSuffix(endpoint, "/v4.1") + "/v4"
	}
	return endpoint + "/magnet/restart"
}
