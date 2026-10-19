package torbox

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	json "github.com/bytedance/sonic"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/internal/providertraffic"
	"github.com/Trifocals3537/tessarr/internal/request"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/debrid/account"
	"github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/version"
	"github.com/rs/zerolog"
	"go.uber.org/ratelimit"
)

var planSlots = map[string]int{
	"essential": 3,
	"standard":  5,
	"pro":       10,
}

var torboxStatusDetails = regexp.MustCompile(`\s*\(.*?\)\s*`)

const (
	// A TorBox list is an authoritative provider snapshot. These ceilings keep
	// an ignored offset or a hostile response from turning refresh into an
	// unbounded loop/allocation while remaining generous for real accounts.
	torboxTorrentListPageSize = 1_000
	torboxTorrentListMaxPages = 1_000
	torboxTorrentListMaxItems = 100_000
)

type Torbox struct {
	Host                  string `json:"host"`
	APIKey                string
	accountsManager       *account.Manager
	autoExpiresLinksAfter time.Duration
	client                *request.Client
	logger                zerolog.Logger
	Profile               *types.Profile
	profileMu             sync.Mutex
	config                config.Debrid
	downloadPresentCache  sync.Map
	downloadPresentMu     sync.Mutex
	downloadPresentLoaded bool
}

var _ common.ContextTorrentLister = (*Torbox)(nil)
var _ common.ContextDownloadLinkRefresher = (*Torbox)(nil)
var _ common.ContextAccountSyncer = (*Torbox)(nil)
var _ common.ContextMagnetSubmitter = (*Torbox)(nil)
var _ common.ContextStatusChecker = (*Torbox)(nil)
var _ common.ContextDownloadLinkResolver = (*Torbox)(nil)

func New(
	dc config.Debrid,
	ratelimits map[string]ratelimit.Limiter,
	trafficControllers ...*providertraffic.Controller,
) (*Torbox, error) {
	cfg := config.Get()
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", dc.APIKey),
	}
	if dc.UserAgent != "" {
		headers["User-Agent"] = dc.UserAgent
	} else {
		headers["User-Agent"] = fmt.Sprintf("Tessarr/%s (%s; %s)", version.GetInfo(), runtime.GOOS, runtime.GOARCH)
	}
	_log := logger.New(dc.Name)

	// The provider traffic controller owns TorBox's documented per-endpoint
	// defaults. Keep an explicit user rate limit as an additional, potentially
	// tighter service-wide guard, but do not replace the endpoint model with an
	// implicit aggregate limiter.
	mainRL := ratelimits["main"]
	var traffic *providertraffic.Controller
	if len(trafficControllers) > 0 {
		traffic = trafficControllers[0]
	}
	if traffic == nil {
		traffic = providertraffic.New(providertraffic.Options{})
	}
	trafficProvider := strings.TrimSpace(dc.Provider)
	if trafficProvider == "" {
		trafficProvider = "torbox"
	}

	opts := []request.ClientOption{
		request.WithHeaders(headers),
		request.WithRateLimiter(mainRL),
		request.WithMaxRetries(cfg.Retries),
		request.WithRetryableStatus(http.StatusTooManyRequests, http.StatusBadGateway),
		request.WithLogger(_log),
		request.WithProviderTraffic(traffic, trafficProvider, dc.APIKey),
	}
	if dc.Proxy != "" {
		opts = append(opts, request.WithProxy(dc.Proxy))
	}

	autoExpiresLinksAfter, err := utils.ParseDuration(dc.AutoExpireLinksAfter)
	if autoExpiresLinksAfter == 0 || err != nil {
		autoExpiresLinksAfter = 48 * time.Hour
	}

	accountConfig := dc
	accountConfig.Provider = trafficProvider
	tb := &Torbox{
		Host:                  "https://api.torbox.app/v1",
		APIKey:                dc.APIKey,
		accountsManager:       account.NewManager(accountConfig, ratelimits["download"], _log, traffic),
		config:                dc,
		autoExpiresLinksAfter: autoExpiresLinksAfter,
		client:                request.New(opts...),
		logger:                _log,
	}
	return tb, nil
}

func (tb *Torbox) Config() config.Debrid {
	return tb.config
}

func (tb *Torbox) Logger() zerolog.Logger {
	return tb.logger
}

// doGet performs a GET request and unmarshals the response
func (tb *Torbox) doGet(endpoint string, queryParams map[string]string, result any) (*http.Response, error) {
	return tb.doGetContext(context.Background(), endpoint, queryParams, result)
}

// doGetContext performs a cancellable GET request and unmarshals the response.
func (tb *Torbox) doGetContext(
	ctx context.Context,
	endpoint string,
	queryParams map[string]string,
	result any,
) (*http.Response, error) {
	return tb.doGetContextBounded(
		ctx,
		endpoint,
		queryParams,
		result,
		utils.MaxJSONResponseBytes,
	)
}

func (tb *Torbox) doGetContextBounded(
	ctx context.Context,
	endpoint string,
	queryParams map[string]string,
	result any,
	maxResponseBytes int64,
) (*http.Response, error) {
	if ctx == nil {
		return nil, fmt.Errorf("torbox request context is required")
	}

	u, err := url.Parse(tb.Host + endpoint)
	if err != nil {
		return nil, err
	}

	if queryParams != nil {
		q := u.Query()
		for k, v := range queryParams {
			q.Set(k, v)
		}
		u.RawQuery = q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := tb.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && resp.ContentLength != 0 {
		if err := utils.DecodeJSONResponseBounded(resp.Body, result, maxResponseBytes); err != nil {
			return resp, err
		}
	}

	return resp, nil
}

// doPostForm performs a POST request with form data
func (tb *Torbox) doPostFormContext(
	ctx context.Context,
	endpoint string,
	formData map[string]string,
	result any,
	operations ...providertraffic.Operation,
) (*http.Response, error) {
	form := url.Values{}
	for k, v := range formData {
		form.Set(k, v)
	}

	if len(operations) > 0 && operations[0] != providertraffic.OperationNone {
		ctx = providertraffic.WithOperation(ctx, operations[0])
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tb.Host+endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := tb.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer closeTorboxResponse(resp.Body)

	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && resp.ContentLength != 0 {
		if err := utils.DecodeJSONResponseBounded(resp.Body, result, utils.MaxJSONResponseBytes); err != nil {
			return resp, err
		}
	}

	return resp, nil
}

func (tb *Torbox) doPostTorrentFileContext(
	ctx context.Context,
	endpoint string,
	fileData []byte,
	addOnlyIfCached bool,
	result any,
	operation providertraffic.Operation,
) (*http.Response, error) {
	if len(fileData) == 0 {
		return nil, fmt.Errorf("torrent file is empty")
	}
	if int64(len(fileData)) > utils.MaxMetadataFileBytes {
		return nil, fmt.Errorf("torrent file exceeds %d bytes", utils.MaxMetadataFileBytes)
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("file", "upload.torrent")
	if err != nil {
		return nil, fmt.Errorf("create torrent upload: %w", err)
	}
	if _, err := part.Write(fileData); err != nil {
		return nil, fmt.Errorf("write torrent upload: %w", err)
	}
	if addOnlyIfCached {
		if err := writer.WriteField("add_only_if_cached", "true"); err != nil {
			return nil, fmt.Errorf("write torrent upload policy: %w", err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("finish torrent upload: %w", err)
	}

	if operation != providertraffic.OperationNone {
		ctx = providertraffic.WithOperation(ctx, operation)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tb.Host+endpoint, &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := tb.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer closeTorboxResponse(resp.Body)
	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && resp.ContentLength != 0 {
		if err := utils.DecodeJSONResponseBounded(resp.Body, result, utils.MaxJSONResponseBytes); err != nil {
			return resp, err
		}
	}
	return resp, nil
}

func closeTorboxResponse(body io.ReadCloser) {
	_, _ = io.Copy(io.Discard, io.LimitReader(body, (64<<10)+1))
	_ = body.Close()
}

// doPostJSON performs a POST request with a JSON body.
func (tb *Torbox) doPostJSON(endpoint string, payload any) (*http.Response, error) {
	var body io.Reader
	if payload != nil {
		data, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(data)
	}

	req, err := http.NewRequest(http.MethodPost, tb.Host+endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := tb.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, (64<<10)+1))
		_ = resp.Body.Close()
	}()

	return resp, nil
}

func (tb *Torbox) IsAvailable(hashes []string) map[string]bool {
	result := make(map[string]bool)

	for i := 0; i < len(hashes); i += 100 {
		end := min(i+100, len(hashes))

		validHashes := make([]string, 0, end-i)
		for _, hash := range hashes[i:end] {
			if hash != "" {
				validHashes = append(validHashes, hash)
			}
		}

		if len(validHashes) == 0 {
			continue
		}

		hashStr := strings.Join(validHashes, ",")
		var res AvailableResponse

		resp, err := tb.doGet("/api/torrents/checkcached", map[string]string{"hash": hashStr}, &res)
		if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
			continue
		}
		if res.Data == nil {
			return result
		}

		for h, c := range *res.Data {
			if c.Size > 0 {
				result[strings.ToUpper(h)] = true
			}
		}
	}
	return result
}

// CheckCacheAvailability preserves TorBox's distinction between a definite
// miss and an unavailable or malformed cache probe. The single-hash endpoint
// path is intentionally reused here because its response validation is strict;
// a future bounded batch implementation can replace it without changing the
// evidence contract.
func (tb *Torbox) CheckCacheAvailability(ctx context.Context, hashes []string) map[string]common.CacheEvidence {
	if ctx == nil {
		ctx = context.Background()
	}
	result := make(map[string]common.CacheEvidence, len(hashes))
	provider := tb.config.Name
	if strings.TrimSpace(provider) == "" {
		provider = "torbox"
	}
	for _, hash := range hashes {
		normalized := strings.ToLower(strings.TrimSpace(hash))
		if normalized == "" {
			continue
		}
		state := common.CacheStateUnknown
		cached, known := tb.isCachedContext(ctx, normalized)
		if known && cached {
			state = common.CacheStateCached
		} else if known {
			state = common.CacheStateUncached
		}
		result[normalized] = common.NewCacheEvidence(
			provider,
			normalized,
			state,
			"torbox.checkcached",
			time.Now(),
		)
		if ctx.Err() != nil {
			break
		}
	}
	return result
}

// isCached checks one hash without conflating a failed probe with a definite
// cache miss. The public IsAvailable method intentionally returns only positive
// results, which is useful for bulk lookup but cannot tell callers whether an
// absent key means "not cached" or "the request failed".
func (tb *Torbox) isCached(hash string) (cached bool, known bool) {
	return tb.isCachedContext(context.Background(), hash)
}

func (tb *Torbox) isCachedContext(ctx context.Context, hash string) (cached bool, known bool) {
	if strings.TrimSpace(hash) == "" {
		return false, false
	}

	var res AvailableResponse
	resp, err := tb.doGetContext(ctx, "/api/torrents/checkcached", map[string]string{
		"hash":       hash,
		"format":     "object",
		"list_files": "false",
	}, &res)
	if err != nil || resp == nil || resp.StatusCode < 200 || resp.StatusCode >= 300 ||
		!res.Success {
		return false, false
	}
	if res.Data == nil {
		// TorBox v4.3 documented a successful null-data response for a cache
		// miss, while newer versions use an empty object. Only accept the legacy
		// form when its detail explicitly states the negative result.
		detail := strings.ToLower(strings.TrimSpace(res.Detail))
		return false, strings.Contains(detail, "no cached data")
	}
	if len(*res.Data) == 0 {
		return false, true
	}

	for responseHash, cachedTorrent := range *res.Data {
		if strings.EqualFold(responseHash, hash) {
			if cachedTorrent.Size > 0 {
				return true, true
			}
			// A matching but incomplete object is not the documented cached
			// representation. Preserve the existing create call instead of
			// converting a response-shape change into a false cache miss.
			return false, false
		}
	}
	// A non-empty response for some other hash is inconsistent with this
	// single-hash lookup, so its absence is not trustworthy evidence of a miss.
	return false, false
}

func (tb *Torbox) SubmitMagnet(torrent *types.Torrent) (*types.Torrent, error) {
	return tb.SubmitMagnetContext(context.Background(), torrent)
}

func (tb *Torbox) SubmitMagnetContext(ctx context.Context, torrent *types.Torrent) (*types.Torrent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if torrent == nil || torrent.Magnet == nil {
		return nil, fmt.Errorf("missing torrent magnet")
	}
	var data AddMagnetResponse

	if !torrent.DownloadUncached {
		// TorBox can leave createtorrent unanswered for a known cache miss,
		// causing the request client's timeout and retry policy to turn a cheap
		// refusal into minutes of latency. Probe first, but only act on a
		// trustworthy answer so a provider outage cannot become a false miss.
		if cached, known := tb.isCachedContext(ctx, torrent.InfoHash); known && !cached {
			return nil, customerror.NewTorrentNotCachedError(torrent.Name)
		}
	}

	operation := providertraffic.OperationAPI
	if torrent.DownloadUncached {
		operation = providertraffic.OperationCreateTorrentUncached
	}
	var (
		resp *http.Response
		err  error
	)
	if torrent.Magnet.IsTorrent() {
		resp, err = tb.doPostTorrentFileContext(
			ctx,
			"/api/torrents/createtorrent",
			torrent.Magnet.File,
			!torrent.DownloadUncached,
			&data,
			operation,
		)
	} else {
		formData := map[string]string{"magnet": torrent.Magnet.Link}
		if !torrent.DownloadUncached {
			formData["add_only_if_cached"] = "true"
		}
		resp, err = tb.doPostFormContext(ctx, "/api/torrents/createtorrent", formData, &data, operation)
	}
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}
	if !data.Success || data.Data == nil {
		return nil, fmt.Errorf("error adding torrent")
	}
	dt := *data.Data
	torrentId := strconv.Itoa(dt.Id)
	torrent.Id = torrentId
	torrent.Debrid = tb.config.Name
	torrent.Added = time.Now()

	return torrent, nil
}

func (tb *Torbox) getTorboxStatus(status string, finished bool) types.TorrentStatus {
	// download_finished is TorBox's authoritative completion flag. Its API
	// documentation explicitly warns consumers not to infer completion from
	// download_state alone.
	if finished {
		return types.TorrentStatusDownloaded
	}

	status = strings.ToLower(strings.TrimSpace(torboxStatusDetails.ReplaceAllString(status, "")))
	switch status {
	case "queued", "queueddl", "queuedup":
		return types.TorrentStatusQueued
	case "paused", "downloading", "checkingresumedata", "metadl",
		"pausedup", "checkingup", "forcedup", "allocating", "pauseddl",
		"checkingdl", "forceddl", "moving", "stalled", "stalledup",
		"stalleddl", "completed", "cached", "uploading", "downloaded":
		// Stalled downloads are still active according to TorBox. Likewise, a
		// ready-looking text state is not complete until download_finished is
		// true, so keep polling instead of importing incomplete file metadata.
		return types.TorrentStatusDownloading
	case "error", "failed", "expired", "incomplete", "missing",
		"missingfiles", "reported missing":
		return types.TorrentStatusError
	default:
		// Preserve the existing fail-closed behavior for truly unknown states.
		return types.TorrentStatusError
	}
}

func terminalTorboxState(state string) bool {
	state = strings.ToLower(strings.TrimSpace(torboxStatusDetails.ReplaceAllString(state, "")))
	switch state {
	case "error", "failed", "expired", "incomplete", "missing", "missingfiles", "reported missing":
		return true
	default:
		return false
	}
}

func (tb *Torbox) GetTorrent(torrentId string) (*types.Torrent, error) {
	var res InfoResponse

	resp, err := tb.doGet("/api/torrents/mylist", map[string]string{"id": torrentId}, &res)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}
	data := res.torrent(torrentId)
	if !res.Success || data == nil {
		return nil, fmt.Errorf("error getting torrent")
	}
	t := &types.Torrent{
		Id:               strconv.Itoa(data.Id),
		Name:             data.Name,
		Bytes:            data.Size,
		Progress:         data.Progress * 100,
		Status:           tb.getTorboxStatus(data.DownloadState, data.DownloadFinished),
		ProviderState:    data.DownloadState,
		Speed:            data.DownloadSpeed,
		Seeders:          data.Seeds,
		Filename:         data.Name,
		OriginalFilename: data.Name,
		Debrid:           tb.config.Name,
		Files:            make(map[string]types.File),
		Added:            data.CreatedAt,
	}
	cfg := config.Get()
	files := make([]types.File, 0, len(data.Files))

	for _, f := range data.Files {
		if err := cfg.IsFileAllowed(f.AbsolutePath, f.Size); err != nil {
			continue
		}

		file := types.File{
			TorrentId: t.Id,
			Id:        strconv.Itoa(f.Id),
			Size:      f.Size,
			Path:      f.Name,
		}

		if data.DownloadFinished {
			file.Link = fmt.Sprintf("torbox://%s/%d", t.Id, f.Id)
		}

		files = append(files, file)
	}
	t.Files, err = torboxFilesByLogicalName(files)
	if err != nil {
		return nil, fmt.Errorf("normalize TorBox torrent files: %w", err)
	}
	var cleanPath string
	if len(t.Files) > 0 {
		cleanPath = path.Clean(data.Files[0].Name)
	} else {
		cleanPath = path.Clean(data.Name)
	}

	t.OriginalFilename = strings.Split(cleanPath, "/")[0]
	t.Debrid = tb.config.Name

	return t, nil
}

func (tb *Torbox) loadDownloadPresentContext(ctx context.Context) error {
	return tb.loadDownloadPresentBoundedContext(
		ctx,
		torboxTorrentListMaxPages,
		torboxTorrentListMaxItems,
	)
}

func (tb *Torbox) loadDownloadPresentBoundedContext(ctx context.Context, maxPages, maxItems int) error {
	if maxPages <= 0 || maxItems <= 0 {
		return fmt.Errorf("torbox download-present list bounds must be positive")
	}

	offset := 0
	present := make(map[string]bool)
	for page := 0; page < maxPages; page++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var res TorrentsListResponse
		resp, err := tb.doGetContext(ctx, "/api/torrents/mylist", map[string]string{"offset": fmt.Sprintf("%d", offset)}, &res)
		if err != nil {
			return fmt.Errorf(
				"torbox download-present page %d at offset %d: %w",
				page+1,
				offset,
				err,
			)
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
		}
		if !res.Success || res.Data == nil {
			return fmt.Errorf("torbox API returned an unsuccessful download-present list response")
		}
		if len(*res.Data) == 0 {
			tb.downloadPresentCache = sync.Map{}
			for id, isPresent := range present {
				tb.downloadPresentCache.Store(id, isPresent)
			}
			tb.logger.Info().Int("count", len(present)).Msg("loaded download_present cache for repair")
			return nil
		}
		if len(*res.Data) > maxItems-len(present) {
			return fmt.Errorf(
				"torbox download-present list exceeds %d items",
				maxItems,
			)
		}
		for _, torrent := range *res.Data {
			id := strconv.Itoa(torrent.Id)
			if _, exists := present[id]; exists {
				return fmt.Errorf(
					"torbox download-present list repeated torrent ID %q at offset %d",
					id,
					offset,
				)
			}
			present[id] = torrent.DownloadPresent
		}
		nextOffset := offset + len(*res.Data)
		if nextOffset <= offset {
			return fmt.Errorf(
				"torbox download-present list made no offset progress from %d",
				offset,
			)
		}
		offset = nextOffset
	}
	return fmt.Errorf(
		"torbox download-present list exceeds %d non-empty pages",
		maxPages,
	)
}

func (tb *Torbox) UpdateTorrent(t *types.Torrent) error {
	return tb.updateTorrentContext(context.Background(), t)
}

func (tb *Torbox) updateTorrentContext(ctx context.Context, t *types.Torrent) error {
	return tb.updateTorrentWithCacheContext(ctx, t, false)
}

func (tb *Torbox) updateTorrentWithCacheContext(ctx context.Context, t *types.Torrent, bypassCache bool) error {
	if t == nil {
		return fmt.Errorf("torrent is nil")
	}
	var res InfoResponse

	query := map[string]string{"id": t.Id}
	if bypassCache {
		query["bypass_cache"] = "true"
	}
	resp, err := tb.doGetContext(ctx, "/api/torrents/mylist", query, &res)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}
	data := res.torrent(t.Id)
	if !res.Success || data == nil {
		return fmt.Errorf("error updating torrent")
	}
	name := data.Name

	t.Name = name
	t.Bytes = data.Size
	t.Progress = data.Progress * 100
	t.Status = tb.getTorboxStatus(data.DownloadState, data.DownloadFinished)
	t.ProviderState = data.DownloadState
	t.Speed = data.DownloadSpeed
	t.Seeders = data.Seeds
	t.Filename = name
	t.OriginalFilename = name
	if data.Hash != "" {
		t.InfoHash = data.Hash
	}
	t.Debrid = tb.config.Name

	cfg := config.Get()
	files := make([]types.File, 0, len(data.Files))

	for _, f := range data.Files {
		if err := cfg.IsFileAllowed(f.AbsolutePath, f.Size); err != nil {
			continue
		}

		file := types.File{
			TorrentId: t.Id,
			Id:        strconv.Itoa(f.Id),
			Size:      f.Size,
			Path:      f.Name,
		}

		if data.DownloadFinished {
			file.Link = fmt.Sprintf("torbox://%s/%s", t.Id, strconv.Itoa(f.Id))
		}

		files = append(files, file)
	}
	t.Files, err = torboxFilesByLogicalName(files)
	if err != nil {
		normalizationErr := fmt.Errorf("normalize TorBox torrent files: %w", err)
		if terminalTorboxState(data.DownloadState) {
			return fmt.Errorf(
				"%w: %s: %v",
				types.ErrTerminalProviderTorrent,
				data.DownloadState,
				normalizationErr,
			)
		}
		return normalizationErr
	}

	var cleanPath string
	if len(t.Files) > 0 {
		cleanPath = path.Clean(data.Files[0].Name)
	} else {
		cleanPath = path.Clean(data.Name)
	}

	t.OriginalFilename = strings.Split(cleanPath, "/")[0]
	t.Debrid = tb.config.Name
	return nil
}

func (tb *Torbox) CheckStatus(torrent *types.Torrent) (*types.Torrent, error) {
	return tb.CheckStatusContext(context.Background(), torrent)
}

func (tb *Torbox) CheckStatusContext(ctx context.Context, torrent *types.Torrent) (*types.Torrent, error) {
	return tb.checkStatusContext(ctx, torrent, false)
}

// CheckStatusFreshContext is used only for an imminent failure handoff.
func (tb *Torbox) CheckStatusFreshContext(ctx context.Context, torrent *types.Torrent) (*types.Torrent, error) {
	return tb.checkStatusContext(ctx, torrent, true)
}

func (tb *Torbox) checkStatusContext(ctx context.Context, torrent *types.Torrent, bypassCache bool) (*types.Torrent, error) {
	if err := ctx.Err(); err != nil {
		return torrent, err
	}
	if torrent == nil {
		return nil, fmt.Errorf("torrent is nil")
	}
	for {
		err := tb.updateTorrentWithCacheContext(ctx, torrent, bypassCache)

		if err != nil || torrent == nil {
			return torrent, err
		}

		switch torrent.Status {
		case types.TorrentStatusDownloaded:
			tb.logger.Info().Msgf("Torrent: %s downloaded", torrent.Name)
			return torrent, nil
		case types.TorrentStatusQueued:
			// TorBox will start this automatically when an account slot or
			// backend capacity becomes available. It is not a torrent failure.
			return torrent, nil
		case types.TorrentStatusDownloading:
			if !torrent.DownloadUncached {
				return torrent, customerror.NewTorrentNotCachedError(torrent.Name)
			}
			return torrent, nil
		default:
			if terminalTorboxState(torrent.ProviderState) {
				return torrent, fmt.Errorf("%w: %s", types.ErrTerminalProviderTorrent, torrent.ProviderState)
			}
			return torrent, fmt.Errorf("torrent: %s has error", torrent.Name)
		}
	}
}

func (tb *Torbox) DeleteTorrent(torrentId string) error {
	numericID, err := strconv.Atoi(torrentId)
	if err != nil {
		return fmt.Errorf("invalid TorBox torrent ID %q: %w", torrentId, err)
	}
	payload := map[string]any{"torrent_id": numericID, "operation": "delete"}

	resp, err := tb.doPostJSON("/api/torrents/controltorrent", payload)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return customerror.TorrentNotFoundError
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}

	tb.logger.Info().Msgf("Torrent %s deleted from Torbox", torrentId)
	return nil
}

func (tb *Torbox) GetDownloadLink(id string, file *types.File) (types.DownloadLink, error) {
	return tb.accountsManager.GetDownloadLink(id, file, tb.fetchDownloadLink)
}

func (tb *Torbox) GetDownloadLinkContext(ctx context.Context, id string, file *types.File) (types.DownloadLink, error) {
	return tb.accountsManager.GetDownloadLinkContext(ctx, id, file, tb.fetchDownloadLinkContext)
}

func (tb *Torbox) fetchDownloadLink(account *account.Account, id string, file *types.File) (types.DownloadLink, error) {
	return tb.fetchDownloadLinkContext(context.Background(), account, id, file)
}

func (tb *Torbox) fetchDownloadLinkContext(ctx context.Context, account *account.Account, id string, file *types.File) (types.DownloadLink, error) {
	if err := ctx.Err(); err != nil {
		return types.DownloadLink{}, err
	}
	query := url.Values{}
	query.Set("token", account.Token)
	query.Set("torrent_id", id)
	query.Set("file_id", file.Id)
	// TorBox explicitly recommends this revocable permalink instead of
	// pre-generating and retaining its short-lived signed CDN URLs. The shared
	// traffic layer treats this resolver hop separately from redirected media
	// bytes, so seeks do not occupy the signed link's four-connection budget.
	query.Set("redirect", "true")

	downloadURL := fmt.Sprintf("%s/api/torrents/requestdl?%s", tb.Host, query.Encode())

	now := time.Now()

	// Always expires
	dl := types.DownloadLink{
		Filename:     file.Name,
		Size:         file.Size,
		Token:        account.Token,
		Link:         file.Link,
		DownloadLink: downloadURL,
		Debrid:       tb.config.Name,
		Id:           file.Id,
		Generated:    now,
		ExpiresAt:    now.Add(tb.autoExpiresLinksAfter),
	}
	return dl, nil
}

func (tb *Torbox) GetTorrents() ([]*types.Torrent, error) {
	return tb.GetTorrentsContext(context.Background())
}

func (tb *Torbox) GetTorrentsContext(ctx context.Context) ([]*types.Torrent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return tb.getTorrentsBoundedContext(
		ctx,
		torboxTorrentListMaxPages,
		torboxTorrentListMaxItems,
		torboxTorrentListPageSize,
	)
}

func (tb *Torbox) getTorrentsBounded(maxPages, maxItems, pageSize int) ([]*types.Torrent, error) {
	return tb.getTorrentsBoundedContext(context.Background(), maxPages, maxItems, pageSize)
}

func (tb *Torbox) getTorrentsBoundedContext(ctx context.Context, maxPages, maxItems, pageSize int) ([]*types.Torrent, error) {
	if maxPages <= 0 || maxItems <= 0 || pageSize <= 0 {
		return nil, fmt.Errorf("torbox torrent list bounds must be positive")
	}

	offset := 0
	allTorrents := make([]*types.Torrent, 0)
	seenIDs := make(map[string]int)

	for page := 0; page < maxPages; page++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		torrents, err := tb.getTorrentsContext(ctx, offset, pageSize)
		if err != nil {
			// Never expose a partial list: manager reconciliation treats this
			// return value as the provider's complete authoritative snapshot.
			return nil, fmt.Errorf("torbox torrent list page %d at offset %d: %w", page+1, offset, err)
		}
		if len(torrents) == 0 {
			return allTorrents, nil
		}
		if len(torrents) > maxItems-len(allTorrents) {
			return nil, fmt.Errorf(
				"torbox torrent list exceeds %d items",
				maxItems,
			)
		}
		for _, torrent := range torrents {
			if torrent == nil || torrent.Id == "" {
				return nil, fmt.Errorf(
					"torbox torrent list page %d at offset %d contains an invalid item",
					page+1,
					offset,
				)
			}
			if previousOffset, exists := seenIDs[torrent.Id]; exists {
				return nil, fmt.Errorf(
					"torbox torrent list repeated torrent ID %q at offsets %d and %d",
					torrent.Id,
					previousOffset,
					offset,
				)
			}
			seenIDs[torrent.Id] = offset
		}
		allTorrents = append(allTorrents, torrents...)
		// TorBox documents limit as the maximum number of items returned by
		// /mylist. A short page is therefore terminal. Avoiding a speculative
		// empty-page request matters when bypass_cache is enabled: each request
		// may observe a newer list, so a shifted boundary can otherwise repeat
		// an ID and reject an otherwise complete snapshot.
		if len(torrents) < pageSize {
			return allTorrents, nil
		}
		nextOffset := offset + len(torrents)
		if nextOffset <= offset {
			return nil, fmt.Errorf(
				"torbox torrent list made no offset progress from %d",
				offset,
			)
		}
		offset = nextOffset
	}
	return nil, fmt.Errorf(
		"torbox torrent list exceeds %d non-empty pages",
		maxPages,
	)
}

func (tb *Torbox) getTorrentsContext(ctx context.Context, offset, limit int) ([]*types.Torrent, error) {
	var res TorrentsListResponse

	resp, err := tb.doGetContext(ctx, "/api/torrents/mylist", map[string]string{
		"bypass_cache": "true",
		"offset":       strconv.Itoa(offset),
		"limit":        strconv.Itoa(limit),
	}, &res)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}

	if !res.Success || res.Data == nil {
		// Error/detail values can echo submitted URLs. The status and operation
		// identify this failure without exposing provider response contents.
		return nil, fmt.Errorf("torbox API returned an unsuccessful torrent list response")
	}

	torrents := make([]*types.Torrent, 0, len(*res.Data))
	cfg := config.Get()

	for _, data := range *res.Data {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		t := &types.Torrent{
			Id:               strconv.Itoa(data.Id),
			Name:             data.Name,
			Bytes:            data.Size,
			Progress:         data.Progress * 100,
			Status:           tb.getTorboxStatus(data.DownloadState, data.DownloadFinished),
			Speed:            data.DownloadSpeed,
			Seeders:          data.Seeds,
			Filename:         data.Name,
			OriginalFilename: data.Name,
			Debrid:           tb.config.Name,
			Files:            make(map[string]types.File),
			Added:            data.CreatedAt,
			InfoHash:         data.Hash,
		}
		files := make([]types.File, 0, len(data.Files))

		for _, f := range data.Files {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := cfg.IsFileAllowed(f.AbsolutePath, f.Size); err != nil {
				continue
			}
			file := types.File{
				TorrentId: t.Id,
				Id:        strconv.Itoa(f.Id),
				Size:      f.Size,
				Path:      f.Name,
			}

			if data.DownloadFinished {
				file.Link = fmt.Sprintf("torbox://%s/%d", t.Id, f.Id)
			}

			files = append(files, file)
		}
		t.Files, err = torboxFilesByLogicalName(files)
		if err != nil {
			return nil, fmt.Errorf("normalize TorBox torrent %s files: %w", t.Id, err)
		}

		var cleanPath string
		if len(t.Files) > 0 {
			cleanPath = path.Clean(data.Files[0].Name)
		} else {
			cleanPath = path.Clean(data.Name)
		}
		t.OriginalFilename = strings.Split(cleanPath, "/")[0]

		torrents = append(torrents, t)
	}

	return torrents, nil
}

// torboxFilesByLogicalName keeps the basename key used by existing databases
// when it is unambiguous. When nested files share a basename, every member of
// that group is keyed and named by its provider path so no file is overwritten
// before manager-level portable-path validation can run.
func torboxFilesByLogicalName(files []types.File) (map[string]types.File, error) {
	return types.FilesByLogicalName(files)
}

func (tb *Torbox) fetchDownloadLinks(account *account.Account) ([]types.DownloadLink, error) {
	return []types.DownloadLink{}, nil
}

func (tb *Torbox) RefreshDownloadLinks() error {
	return tb.RefreshDownloadLinksContext(context.Background())
}

func (tb *Torbox) RefreshDownloadLinksContext(ctx context.Context) error {
	return tb.accountsManager.RefreshLinksContext(ctx, func(ctx context.Context, account *account.Account) ([]types.DownloadLink, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return tb.fetchDownloadLinks(account)
	})
}

func (tb *Torbox) CheckFile(ctx context.Context, infohash, link string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tb.downloadPresentMu.Lock()
	if err := ctx.Err(); err != nil {
		tb.downloadPresentMu.Unlock()
		return err
	}
	if !tb.downloadPresentLoaded {
		if err := tb.loadDownloadPresentContext(ctx); err != nil {
			tb.downloadPresentMu.Unlock()
			return err
		}
		tb.downloadPresentLoaded = true
	}
	tb.downloadPresentMu.Unlock()

	torrentID := link
	if after, ok := strings.CutPrefix(link, "torbox://"); ok {
		parts := strings.SplitN(after, "/", 2)
		if len(parts) > 0 {
			torrentID = parts[0]
		}
	}

	if present, ok := tb.downloadPresentCache.Load(torrentID); ok {
		if !present.(bool) {
			return customerror.HosterUnavailableError
		}
		return nil
	}
	return customerror.HosterUnavailableError
}

func (tb *Torbox) GetAvailableSlots() (int, error) {
	var accountSlots = 1
	profile, err := tb.GetProfile()
	if err != nil {
		return 0, err
	}

	if slots, ok := planSlots[profile.Type]; ok {
		accountSlots = slots
	}
	return accountSlots, nil
}

func (tb *Torbox) GetProfile() (*types.Profile, error) {
	return tb.GetProfileContext(context.Background())
}

func (tb *Torbox) GetProfileContext(ctx context.Context) (*types.Profile, error) {
	tb.profileMu.Lock()
	defer tb.profileMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tb.Profile != nil {
		cached := *tb.Profile
		return &cached, nil
	}
	var data ProfileResponse

	resp, err := tb.doGetContext(ctx, "/api/user/me", map[string]string{"settings": "true"}, &data)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("torbox API error: Status: %d", resp.StatusCode)
	}

	userData := data.Data
	if !data.Success || userData == nil {
		return nil, fmt.Errorf("error getting user profile")
	}

	expiration, err := time.Parse(time.RFC3339, userData.PremiumExpiresAt)
	if err != nil {
		expiration = time.Time{}
	}

	profile := &types.Profile{
		Name:       tb.config.Name,
		Id:         userData.Id,
		Username:   userData.Email,
		Email:      userData.Email,
		Expiration: expiration,
	}

	switch userData.Plan {
	case 1:
		profile.Type = "essential"
	case 2:
		profile.Type = "pro"
	case 3:
		profile.Type = "standard"
	default:
		profile.Type = "free"
	}

	stored := *profile
	tb.Profile = &stored

	return profile, nil
}

func (tb *Torbox) AccountManager() *account.Manager {
	return tb.accountsManager
}

func (tb *Torbox) syncAccount(account *account.Account) error {
	return nil
}

func (tb *Torbox) SyncAccounts() {
	_ = tb.SyncAccountsContext(context.Background())
}

func (tb *Torbox) SyncAccountsContext(ctx context.Context) error {
	return tb.accountsManager.SyncContext(ctx, func(ctx context.Context, account *account.Account) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return tb.syncAccount(account)
	})
}

func (tb *Torbox) deleteDownloadLink(account *account.Account, downloadLink types.DownloadLink) error {
	return nil
}

func (tb *Torbox) DeleteLink(downloadLink types.DownloadLink) error {
	return tb.accountsManager.DeleteDownloadLink(downloadLink, tb.deleteDownloadLink)
}

// SpeedTest measures API latency and download speed using cached links
func (tb *Torbox) SpeedTest(ctx context.Context) types.SpeedTestResult {
	result := types.SpeedTestResult{
		Provider: tb.config.Name,
		TestedAt: time.Now(),
	}

	start := time.Now()
	resp, err := tb.doGet("/api/user/me", nil, nil)
	latency := time.Since(start)

	if err != nil {
		result.Error = fmt.Sprintf("latency test failed: %v", err)
		return result
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		result.Error = fmt.Sprintf("latency test unexpected status: %d", resp.StatusCode)
		return result
	}
	result.LatencyMs = latency.Milliseconds()

	// Try to measure download speed using a cached link
	current := tb.accountsManager.Current()
	if current == nil {
		return result
	}

	link, found := current.GetRandomLink()
	if !found || link.DownloadLink == "" {
		return result
	}

	bytesRead, downloadDuration, err := common.ProbeDownload(ctx, current.Client(), link.DownloadLink)
	if err != nil {
		return result
	}

	result.BytesRead = bytesRead
	if downloadDuration.Seconds() > 0 {
		result.SpeedMBps = float64(result.BytesRead) / downloadDuration.Seconds() / (1024 * 1024)
	}

	return result
}

func (tb *Torbox) SupportsCheck() bool {
	return true
}
