package realdebrid

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/providertraffic"
	"github.com/Trifocals3537/tessarr/internal/request"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/debrid/account"
	"github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/debrid/common/rar"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"go.uber.org/ratelimit"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/rs/zerolog"
)

const (
	profileCacheDuration              = 1 * time.Hour
	realDebridDownloadLinkConcurrency = 8
	realDebridTorrentListMaxPages     = 1_000
	realDebridTorrentListMaxItems     = 100_000
	realDebridDownloadListPageSize    = 1_000
	realDebridDownloadListMaxItems    = 100_000
	realDebridDownloadListMaxPages    = realDebridDownloadListMaxItems/realDebridDownloadListPageSize + 1
)

var realDebridRARSafeNameReplacer = strings.NewReplacer(
	"|", "_",
	"\"", "_",
	"\\", "_",
	"?", "_",
	"*", "_",
	":", "_",
	"<", "_",
	">", "_",
)

type RealDebrid struct {
	Host string `json:"host"`

	APIKey                string
	accountsManager       *account.Manager
	client                *request.Client
	repairClient          *request.Client
	autoExpiresLinksAfter time.Duration
	logger                zerolog.Logger

	rarSemaphore       chan struct{}
	Profile            *types.Profile
	profileLastFetched time.Time
	profileMu          sync.Mutex
	config             config.Debrid
	retries            int
}

var _ common.ContextTorrentLister = (*RealDebrid)(nil)
var _ common.ContextDownloadLinkRefresher = (*RealDebrid)(nil)
var _ common.ContextAccountSyncer = (*RealDebrid)(nil)
var _ common.ContextMagnetSubmitter = (*RealDebrid)(nil)
var _ common.ContextStatusChecker = (*RealDebrid)(nil)
var _ common.ContextDownloadLinkResolver = (*RealDebrid)(nil)

func New(
	dc config.Debrid,
	ratelimits map[string]ratelimit.Limiter,
	trafficControllers ...*providertraffic.Controller,
) (*RealDebrid, error) {
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", dc.APIKey),
	}
	if dc.UserAgent != "" {
		headers["User-Agent"] = dc.UserAgent
	}
	_log := logger.New(dc.Name)

	autoExpiresLinksAfter, err := utils.ParseDuration(dc.AutoExpireLinksAfter)
	if autoExpiresLinksAfter == 0 || err != nil {
		autoExpiresLinksAfter = 48 * time.Hour
	}

	cfg := config.Get()
	var traffic *providertraffic.Controller
	if len(trafficControllers) > 0 {
		traffic = trafficControllers[0]
	}
	if traffic == nil {
		traffic = providertraffic.New(providertraffic.Options{})
	}
	trafficProvider := strings.TrimSpace(dc.Provider)
	if trafficProvider == "" {
		trafficProvider = "realdebrid"
	}

	opts := []request.ClientOption{
		request.WithHeaders(headers),
		request.WithMaxRetries(cfg.Retries),
		request.WithRateLimiter(ratelimits["main"]),
		request.WithRetryableStatus(http.StatusTooManyRequests),
		request.WithProxy(dc.Proxy),
		request.WithProviderTraffic(traffic, trafficProvider, dc.APIKey),
	}

	repairOpts := []request.ClientOption{
		request.WithHeaders(headers),
		request.WithLogger(_log),
		request.WithMaxRetries(4),
		request.WithRetryableStatus(429),
		request.WithRateLimiter(ratelimits["repair"]),
		request.WithProxy(dc.Proxy),
		request.WithProviderTraffic(traffic, trafficProvider, dc.APIKey),
	}
	accountConfig := dc
	accountConfig.Provider = trafficProvider

	r := &RealDebrid{
		Host:                  "https://api.real-debrid.com/rest/1.0",
		APIKey:                dc.APIKey,
		accountsManager:       account.NewManager(accountConfig, ratelimits["download"], _log, traffic),
		autoExpiresLinksAfter: autoExpiresLinksAfter,
		client:                request.New(opts...),
		repairClient:          request.New(repairOpts...),
		logger:                logger.New(dc.Name),
		rarSemaphore:          make(chan struct{}, 2),
		config:                dc,
		retries:               cfg.Retries,
	}

	return r, nil
}

func (r *RealDebrid) Logger() zerolog.Logger {
	return r.logger
}

// doGet performs a GET request using the main client
func (r *RealDebrid) doGet(endpoint string, result any) (*http.Response, error) {
	return r.doGetContext(context.Background(), endpoint, result)
}

func (r *RealDebrid) doGetContext(ctx context.Context, endpoint string, result any) (*http.Response, error) {
	u, err := url.Parse(r.Host + endpoint)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && resp.ContentLength != 0 {
		if err := utils.DecodeJSONResponse(resp.Body, result); err != nil {
			return resp, err
		}
	}

	return resp, nil
}

func (r *RealDebrid) doPostFormContext(ctx context.Context, endpoint string, formData map[string]string, result any) (*http.Response, error) {
	form := url.Values{}
	for k, v := range formData {
		form.Set(k, v)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.Host+endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && resp.ContentLength != 0 {
		if err := utils.DecodeJSONResponse(resp.Body, result); err != nil {
			return resp, err
		}
	}

	return resp, nil
}

func (r *RealDebrid) doPutContext(ctx context.Context, endpoint string, body []byte, contentType string, result any) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, r.Host+endpoint, bodyReader)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && resp.ContentLength != 0 {
		if err := utils.DecodeJSONResponse(resp.Body, result); err != nil {
			return resp, err
		}
	}

	return resp, nil
}

func (r *RealDebrid) doGetWithClientContext(ctx context.Context, client *request.Client, fullURL string, queryParams map[string]string, result any) (*http.Response, error) {
	u, err := url.Parse(fullURL)
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

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && resp.ContentLength != 0 {
		if err := utils.DecodeJSONResponse(resp.Body, result); err != nil {
			return resp, err
		}
	}

	return resp, nil
}

// doPostFormWithClient performs a POST with form data using a specific client
func (r *RealDebrid) doPostFormWithClientContext(ctx context.Context, client *request.Client, fullURL string, formData map[string]string, result any, errorResult any) (*http.Response, error) {
	form := url.Values{}
	for k, v := range formData {
		form.Set(k, v)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if result != nil && resp.ContentLength != 0 {
			if err := utils.DecodeJSONResponse(resp.Body, result); err != nil {
				return resp, err
			}
		}
	} else {
		if errorResult != nil && resp.ContentLength != 0 {
			if err := utils.DecodeJSONResponseBounded(resp.Body, errorResult, 64<<10); err != nil {
				return resp, err
			}
		}
	}

	return resp, nil
}

func (r *RealDebrid) getSelectedFiles(t *types.Torrent, data torrentInfo) (map[string]types.File, error) {
	files := make(map[string]types.File)
	selectedFiles := make([]types.File, 0)

	for _, f := range data.Files {
		if f.Selected == 1 {
			providerPath := strings.TrimLeft(
				strings.ReplaceAll(f.Path, `\`, "/"),
				"/",
			)
			selectedFiles = append(selectedFiles, types.File{
				TorrentId: t.Id,
				Name:      path.Base(providerPath),
				Path:      providerPath,
				Size:      f.Bytes,
				Id:        strconv.Itoa(f.ID),
			})
		}
	}

	if len(selectedFiles) == 0 {
		return files, nil
	}

	// Handle RARed torrents (single link, multiple files)
	if len(data.Links) == 1 && len(selectedFiles) > 1 {
		return r.handleRarArchive(t, data, selectedFiles)
	}

	// Standard case - map files to links
	if len(selectedFiles) != len(data.Links) {
		return nil, fmt.Errorf(
			"realdebrid returned %d selected files and %d download links",
			len(selectedFiles),
			len(data.Links),
		)
	}

	for i, f := range selectedFiles {
		f.Link = data.Links[i]
		selectedFiles[i] = f
	}
	return types.FilesByLogicalName(selectedFiles)
}

func (r *RealDebrid) handleRarFallback(t *types.Torrent, data torrentInfo) (map[string]types.File, error) {
	file := types.File{
		TorrentId: t.Id,
		Id:        "0",
		Name:      t.Name + ".rar",
		Size:      data.Bytes,
		IsRar:     true,
		ByteRange: nil,
		Path:      t.Name + ".rar",
		Link:      data.Links[0],
		Generated: time.Now(),
	}
	return types.FilesByLogicalName([]types.File{file})
}

type rarPathIndexNode struct {
	children   map[string]*rarPathIndexNode
	matchCount int
	matchIndex int
}

func normalizeRARMatchPath(providerPath, fallbackName string) ([]string, error) {
	value := strings.TrimSpace(providerPath)
	if value == "" {
		value = strings.TrimSpace(fallbackName)
	}
	if value == "" {
		return nil, fmt.Errorf("path is empty")
	}
	if strings.IndexByte(value, 0) >= 0 {
		return nil, fmt.Errorf("path contains a NUL byte")
	}

	value = strings.ReplaceAll(value, `\`, "/")
	value = strings.TrimLeft(value, "/")
	clean := path.Clean(value)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return nil, fmt.Errorf("path traverses outside the archive")
	}

	parts := strings.Split(clean, "/")
	for index := range parts {
		// Real-Debrid replaces filesystem-reserved characters when it exposes
		// archive members. Apply the same transform to both sides and use a
		// portable key so path matching agrees with the logical file map.
		parts[index] = strings.ToLower(strings.TrimRight(
			realDebridRARSafeNameReplacer.Replace(parts[index]),
			" .",
		))
		if parts[index] == "" {
			return nil, fmt.Errorf("path contains an empty portable component")
		}
	}
	return parts, nil
}

func addRARMatchPath(root *rarPathIndexNode, parts []string, index int) {
	node := root
	for partIndex := len(parts) - 1; partIndex >= 0; partIndex-- {
		if node.children == nil {
			node.children = make(map[string]*rarPathIndexNode)
		}
		child := node.children[parts[partIndex]]
		if child == nil {
			child = &rarPathIndexNode{matchIndex: index}
			node.children[parts[partIndex]] = child
		}
		child.matchCount++
		if child.matchCount > 1 {
			child.matchIndex = -1
		}
		node = child
	}
}

func findUniqueRARMatch(root *rarPathIndexNode, parts []string) (index int, matched bool, ambiguous bool) {
	node := root
	var deepest *rarPathIndexNode
	for partIndex := len(parts) - 1; partIndex >= 0; partIndex-- {
		node = node.children[parts[partIndex]]
		if node == nil {
			break
		}
		deepest = node
	}
	if deepest == nil {
		return 0, false, false
	}
	if deepest.matchCount != 1 || deepest.matchIndex < 0 {
		return 0, false, true
	}
	return deepest.matchIndex, true, false
}

func mapStoredRARFiles(
	selectedFiles []types.File,
	rarFiles []*rar.File,
	archiveLink string,
	generated time.Time,
) (map[string]types.File, error) {
	if len(selectedFiles) == 0 {
		return nil, fmt.Errorf("RAR mapping has no selected files")
	}

	archiveIndex := &rarPathIndexNode{}
	archiveFiles := make([]*rar.File, 0, len(rarFiles))
	for _, rarFile := range rarFiles {
		if rarFile == nil || rarFile.IsDirectory {
			continue
		}
		parts, err := normalizeRARMatchPath(rarFile.Path, rarFile.Name())
		if err != nil {
			return nil, fmt.Errorf("RAR entry %q has an invalid path: %w", rarFile.Path, err)
		}
		index := len(archiveFiles)
		archiveFiles = append(archiveFiles, rarFile)
		addRARMatchPath(archiveIndex, parts, index)
	}
	if len(archiveFiles) == 0 {
		return nil, fmt.Errorf("RAR contains no file entries")
	}

	mapped := make([]types.File, 0, len(selectedFiles))
	matchedArchiveFiles := make(map[int]int, len(selectedFiles))
	for selectedIndex := range selectedFiles {
		parts, err := normalizeRARMatchPath(selectedFiles[selectedIndex].Path, selectedFiles[selectedIndex].Name)
		if err != nil {
			return nil, fmt.Errorf("RAR selected file %d has an invalid path: %w", selectedIndex, err)
		}
		archiveFileIndex, matched, ambiguous := findUniqueRARMatch(archiveIndex, parts)
		if ambiguous {
			return nil, fmt.Errorf(
				"RAR selected file %q has an ambiguous archive path match",
				selectedFiles[selectedIndex].Path,
			)
		}
		if !matched {
			continue
		}
		if previousSelected, duplicate := matchedArchiveFiles[archiveFileIndex]; duplicate {
			return nil, fmt.Errorf(
				"RAR selected files %q and %q resolve to the same archive entry %q",
				selectedFiles[previousSelected].Path,
				selectedFiles[selectedIndex].Path,
				archiveFiles[archiveFileIndex].Path,
			)
		}

		rarFile := archiveFiles[archiveFileIndex]
		byteRange, err := rarFile.StreamByteRange()
		if err != nil {
			return nil, fmt.Errorf("RAR entry %q is not directly streamable: %w", rarFile.Path, err)
		}

		file := selectedFiles[selectedIndex]
		file.Size = rarFile.Size
		file.IsRar = true
		file.ByteRange = byteRange
		file.Link = archiveLink
		file.Generated = generated
		mapped = append(mapped, file)
		matchedArchiveFiles[archiveFileIndex] = selectedIndex
	}

	if len(mapped) == 0 {
		return nil, fmt.Errorf("RAR contains no directly streamable selected files")
	}
	if len(mapped) != len(selectedFiles) {
		return nil, fmt.Errorf("RAR matched %d of %d selected files", len(mapped), len(selectedFiles))
	}

	return types.FilesByLogicalName(mapped)
}

// handleRarArchive processes RAR archives with multiple files
func (r *RealDebrid) handleRarArchive(t *types.Torrent, data torrentInfo, selectedFiles []types.File) (map[string]types.File, error) {
	// This will block if 2 RAR operations are already in progress
	r.rarSemaphore <- struct{}{}
	defer func() {
		<-r.rarSemaphore
	}()

	if !r.config.UnpackRar {
		r.logger.Debug().Msgf("RAR file detected, but unpacking is disabled: %s. Falling back to single file representation.", t.Name)
		return r.handleRarFallback(t, data)
	}

	r.logger.Info().Msgf("RAR file detected, unpacking: %s", t.Name)
	linkFile := &types.File{TorrentId: t.Id, Link: data.Links[0]}
	downloadLinkObj, err := r.GetDownloadLink(t.Id, linkFile)

	if err != nil {
		r.logger.Debug().Err(err).Msgf("Error getting download link for RAR file: %s. Falling back to single file representation.", t.Name)
		return r.handleRarFallback(t, data)
	}

	dlLink := downloadLinkObj.DownloadLink
	reader, err := rar.NewReader(dlLink)

	if err != nil {
		r.logger.Debug().Err(err).Msgf("Error creating RAR reader for %s. Falling back to single file representation.", t.Name)
		return r.handleRarFallback(t, data)
	}

	rarFiles, err := reader.GetFiles()

	if err != nil {
		r.logger.Debug().Err(err).Msgf("Error reading RAR files for %s. Falling back to single file representation.", t.Name)
		return r.handleRarFallback(t, data)
	}

	files, err := mapStoredRARFiles(selectedFiles, rarFiles, data.Links[0], time.Now())
	if err != nil {
		r.logger.Warn().Err(err).
			Msgf("RAR archive is not directly streamable: %s. Falling back to single file representation.", t.Name)
		return r.handleRarFallback(t, data)
	}
	r.logger.Info().Msgf("Unpacked RAR archive for torrent: %s with %d files", t.Name, len(files))
	return files, nil
}

func (r *RealDebrid) getTorrentFiles(t *types.Torrent, data torrentInfo) (map[string]types.File, error) {
	files := make([]types.File, 0, len(data.Files))
	cfg := config.Get()

	for _, f := range data.Files {
		providerPath := strings.TrimLeft(
			strings.ReplaceAll(f.Path, `\`, "/"),
			"/",
		)
		name := path.Base(providerPath)
		// Filter the portable basename, without changing the native provider
		// identity used to locate the mounted file. Output names cannot retain
		// trailing spaces or dots; those must not hide an otherwise valid extension.
		if err := cfg.IsFileAllowed(strings.TrimRight(name, " ."), f.Bytes); err != nil {
			continue
		}

		file := types.File{
			TorrentId: t.Id,
			Name:      name,
			Path:      providerPath,
			Size:      f.Bytes,
			Id:        strconv.Itoa(f.ID),
		}
		files = append(files, file)
	}
	return types.FilesByLogicalName(files)
}

func (r *RealDebrid) IsAvailable(hashes []string) map[string]bool {
	result := make(map[string]bool)

	for i := 0; i < len(hashes); i += 200 {
		end := min(i+200, len(hashes))

		validHashes := make([]string, 0, end-i)
		for _, hash := range hashes[i:end] {
			if hash != "" {
				validHashes = append(validHashes, hash)
			}
		}

		if len(validHashes) == 0 {
			continue
		}

		hashStr := strings.Join(validHashes, "/")
		var data AvailabilityResponse

		resp, err := r.doGet(fmt.Sprintf("/torrents/instantAvailability/%s", hashStr), &data)
		if err != nil {
			r.logger.Error().Err(err).Msg("Error checking availability")
			continue
		}

		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			for _, h := range hashes[i:end] {
				hosters, exists := data[strings.ToLower(h)]
				if exists && len(hosters.Rd) > 0 {
					result[h] = true
				}
			}
		}
	}
	return result
}

func (r *RealDebrid) SubmitMagnet(t *types.Torrent) (*types.Torrent, error) {
	return r.SubmitMagnetContext(context.Background(), t)
}

func (r *RealDebrid) SubmitMagnetContext(ctx context.Context, t *types.Torrent) (*types.Torrent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if t == nil || t.Magnet == nil {
		return nil, fmt.Errorf("missing torrent magnet")
	}
	if t.Magnet.IsTorrent() {
		return r.addTorrentContext(ctx, t)
	}
	return r.addMagnetContext(ctx, t)
}

func (r *RealDebrid) addTorrentContext(ctx context.Context, t *types.Torrent) (*types.Torrent, error) {
	var data AddMagnetSchema

	resp, err := r.doPutContext(ctx, "/torrents/addTorrent", t.Magnet.File, "application/x-bittorrent", &data)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		switch resp.StatusCode {
		case http.StatusUnavailableForLegalReasons:
			return nil, customerror.NewTorrentContentRejectedError(t.Name)
		case 509:
			return nil, customerror.TooManyActiveDownloadsError
		}
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	t.Id = data.Id
	t.Debrid = r.config.Name
	t.Added = time.Now()

	return t, nil
}

func (r *RealDebrid) addMagnetContext(ctx context.Context, t *types.Torrent) (*types.Torrent, error) {
	var data AddMagnetSchema

	formData := map[string]string{"magnet": t.Magnet.Link}
	resp, err := r.doPostFormContext(ctx, "/torrents/addMagnet", formData, &data)
	if err != nil {
		return nil, err
	}

	switch resp.StatusCode {
	case http.StatusOK, http.StatusCreated:
		t.Id = data.Id
		t.Debrid = r.config.Name
		t.Added = time.Now()
		return t, nil

	case 509:
		return nil, customerror.TooManyActiveDownloadsError

	case http.StatusUnavailableForLegalReasons:
		return nil, customerror.NewTorrentContentRejectedError(t.Name)

	default:
		return nil, fmt.Errorf("realdebrid API error: Status: %d", resp.StatusCode)
	}
}

func (r *RealDebrid) GetTorrent(torrentId string) (*types.Torrent, error) {
	var data torrentInfo

	resp, err := r.doGet(fmt.Sprintf("/torrents/info/%s", torrentId), &data)
	if err != nil {
		return nil, err
	}

	switch resp.StatusCode {
	case http.StatusOK:
		addedOn := data.Added
		if addedOn.IsZero() {
			addedOn = time.Now()
		}
		t := &types.Torrent{
			Id:               data.ID,
			Name:             data.Filename,
			Bytes:            data.Bytes,
			Progress:         data.Progress,
			Speed:            data.Speed,
			Seeders:          data.Seeders,
			Added:            addedOn,
			Status:           types.TorrentStatus(data.Status),
			Filename:         data.Filename,
			OriginalFilename: data.OriginalFilename,
			Links:            data.Links,
			Debrid:           r.config.Name,
		}

		t.Files, err = r.getTorrentFiles(t, data)
		if err != nil {
			return nil, err
		}
		return t, nil
	case http.StatusNotFound:
		return nil, customerror.TorrentNotFoundError

	default:
		return nil, fmt.Errorf("realdebrid API error: Status: %d", resp.StatusCode)
	}
}

func (r *RealDebrid) GetDownloadingStatus() []string {
	return []string{"downloading", "magnet_conversion", "queued", "compressing", "uploading"}
}

func getStatus(status string) types.TorrentStatus {
	switch status {
	case "downloading", "magnet_conversion", "queued", "compressing", "uploading", "waiting_files_selection":
		return types.TorrentStatusDownloading
	case "downloaded":
		return types.TorrentStatusDownloaded
	default:
		return types.TorrentStatusError
	}
}

func realDebridStatusError(name, status string) error {
	err := fmt.Errorf("torrent: %s has error status: %s", name, status)
	switch status {
	case "magnet_error", "error", "virus", "dead":
		return fmt.Errorf("%w: %v", types.ErrTerminalProviderTorrent, err)
	default:
		return err
	}
}

func (r *RealDebrid) UpdateTorrent(t *types.Torrent) error {
	var data torrentInfo

	resp, err := r.doGet(fmt.Sprintf("/torrents/info/%s", t.Id), &data)
	if err != nil {
		return err
	}

	switch resp.StatusCode {
	case http.StatusOK:
		t.Name = data.Filename
		t.Bytes = data.Bytes
		t.Progress = data.Progress
		t.Status = types.TorrentStatus(data.Status)
		t.Speed = data.Speed
		t.Seeders = data.Seeders
		t.Status = getStatus(data.Status)
		t.ProviderState = data.Status
		t.Filename = data.Filename
		t.OriginalFilename = data.OriginalFilename
		t.Links = data.Links
		t.Debrid = r.config.Name
		t.Files, err = r.getSelectedFiles(t, data)
		return err

	case http.StatusNotFound:
		return customerror.TorrentNotFoundError

	default:
		return fmt.Errorf("realdebrid API error: Status: %d", resp.StatusCode)
	}
}

func (r *RealDebrid) CheckStatus(t *types.Torrent) (*types.Torrent, error) {
	return r.CheckStatusContext(context.Background(), t)
}

func (r *RealDebrid) CheckStatusContext(ctx context.Context, t *types.Torrent) (*types.Torrent, error) {
	if err := ctx.Err(); err != nil {
		return t, err
	}
	if t == nil {
		return nil, fmt.Errorf("torrent is nil")
	}
	for {
		timer := time.NewTimer(2 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return t, ctx.Err()
		case <-timer.C:
		}

		var data torrentInfo

		resp, err := r.doGetContext(ctx, fmt.Sprintf("/torrents/info/%s", t.Id), &data)
		if err != nil {
			r.logger.Info().Msgf("ERROR Checking file: %v", err)
			return t, err
		}

		if resp.StatusCode == http.StatusUnavailableForLegalReasons {
			return t, customerror.NewTorrentContentRejectedError(t.Name)
		}
		if resp.StatusCode != http.StatusOK {
			return t, fmt.Errorf("realdebrid API error: Status: %d", resp.StatusCode)
		}

		debridStatus := data.Status
		t.Name = data.Filename
		t.Filename = data.Filename
		t.OriginalFilename = data.OriginalFilename
		t.Bytes = data.Bytes
		t.Progress = data.Progress

		t.Speed = data.Speed
		t.Seeders = data.Seeders
		t.Links = data.Links
		t.Status = getStatus(debridStatus)
		t.ProviderState = debridStatus
		t.Debrid = r.config.Name
		t.Added = data.Added
		if data.Hash != "" {
			t.InfoHash = data.Hash
		}
		if debridStatus == "waiting_files_selection" {
			t.Status = types.TorrentStatusDownloading
			t.Files, err = r.getTorrentFiles(t, data)
			if err != nil {
				return t, err
			}
			if len(t.Files) == 0 {
				return t, fmt.Errorf("no valid files found")
			}
			filesId := make([]string, 0)
			for _, f := range t.Files {
				filesId = append(filesId, f.Id)
			}

			selectURL := fmt.Sprintf("/torrents/selectFiles/%s", t.Id)
			selectResp, err := r.doPostFormContext(ctx, selectURL, map[string]string{"files": strings.Join(filesId, ",")}, nil)
			if err != nil {
				return t, err
			}

			if selectResp.StatusCode != http.StatusNoContent {
				switch selectResp.StatusCode {
				case http.StatusUnavailableForLegalReasons:
					return t, customerror.NewTorrentContentRejectedError(t.Name)
				case 509:
					return nil, customerror.TooManyActiveDownloadsError
				}
				return t, fmt.Errorf("realdebrid API error: Status: %d", selectResp.StatusCode)
			}
			continue
		} else if debridStatus == "downloaded" {
			t.Status = types.TorrentStatusDownloaded
			t.Files, err = r.getSelectedFiles(t, data)
			if err != nil {
				return t, err
			}

			r.logger.Info().Msgf("Torrent: %s downloaded to RD", t.Name)
			return t, nil
		} else if t.Status == types.TorrentStatusDownloading {
			if !t.DownloadUncached {
				return t, customerror.NewTorrentNotCachedError(t.Name)
			}
			return t, nil
		} else {
			r.logger.Warn().
				Str("torrent_id", t.Id).
				Str("debrid_status", debridStatus).
				Str("mapped_status", string(t.Status)).
				Msg("Unexpected debrid status, treating as error")
			return t, realDebridStatusError(t.Name, debridStatus)
		}
	}
}

func (r *RealDebrid) DeleteTorrent(torrentId string) error {
	req, err := http.NewRequest(http.MethodDelete, r.Host+fmt.Sprintf("/torrents/delete/%s", torrentId), nil)
	if err != nil {
		return err
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, (64<<10)+1))
		_ = resp.Body.Close()
	}()
	if resp.StatusCode == http.StatusNotFound {
		return customerror.TorrentNotFoundError
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("realdebrid API error: Status: %d", resp.StatusCode)
	}
	r.logger.Info().Msgf("Torrent: %s deleted from RD", torrentId)
	return nil
}

func (r *RealDebrid) GetFileDownloadLinks(t *types.Torrent) (map[string]types.DownloadLink, error) {
	return resolveRealDebridFileDownloadLinks(
		t,
		realDebridDownloadLinkConcurrency,
		func(torrentID string, file *types.File) (types.DownloadLink, error) {
			return r.GetDownloadLink(torrentID, file)
		},
	)
}

type realDebridDownloadLinkResolver func(string, *types.File) (types.DownloadLink, error)

// resolveRealDebridFileDownloadLinks keeps large releases from creating one
// blocked goroutine per file while the request client's rate limiter drains.
// Every file is still attempted so a transient failure does not strand work
// that was already admitted to this bounded batch.
func resolveRealDebridFileDownloadLinks(
	t *types.Torrent,
	maxConcurrency int,
	resolve realDebridDownloadLinkResolver,
) (map[string]types.DownloadLink, error) {
	if t == nil {
		return nil, fmt.Errorf("realdebrid torrent is nil")
	}
	if resolve == nil {
		return nil, fmt.Errorf("realdebrid download-link resolver is nil")
	}
	if maxConcurrency <= 0 {
		return nil, fmt.Errorf("realdebrid download-link concurrency must be positive")
	}

	input := t.GetFiles()
	files := make(map[string]types.File, len(input))
	links := make(map[string]types.DownloadLink, len(input))
	if len(input) == 0 {
		t.Files = files
		return links, nil
	}

	workerCount := min(maxConcurrency, len(input))
	jobs := make(chan types.File)
	var workers sync.WaitGroup
	var resultMu sync.Mutex
	var firstErr error
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for file := range jobs {
				link, err := resolve(t.Id, &file)
				if err == nil && link.Empty() {
					err = fmt.Errorf(
						"realdebrid API error: download link not found for file %s",
						file.Name,
					)
				}

				resultMu.Lock()
				if err != nil {
					if firstErr == nil {
						firstErr = err
					}
				} else {
					file.DownloadLink = link
					files[file.Name] = file
					links[file.Name] = link
				}
				resultMu.Unlock()
			}
		}()
	}
	for _, file := range input {
		jobs <- file
	}
	close(jobs)
	workers.Wait()

	if firstErr != nil {
		return nil, firstErr
	}
	t.Files = files
	return links, nil
}

func (r *RealDebrid) CheckFile(ctx context.Context, infohash, link string) error {
	formData := map[string]string{"link": link}

	form := url.Values{}
	for k, v := range formData {
		form.Set(k, v)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.Host+"/unrestrict/check", strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := r.repairClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return customerror.HosterUnavailableError
	}

	return nil
}

func (r *RealDebrid) fetchDownloadLink(account *account.Account, id string, file *types.File) (types.DownloadLink, error) {
	return r.fetchDownloadLinkContext(context.Background(), account, id, file)
}

func (r *RealDebrid) fetchDownloadLinkContext(ctx context.Context, account *account.Account, id string, file *types.File) (types.DownloadLink, error) {
	emptyLink := types.DownloadLink{}
	link := file.Link
	if strings.HasPrefix(file.Link, "https://real-debrid.com/d/") && len(file.Link) > 39 {
		link = file.Link[0:39]
	}

	formData := map[string]string{"link": link}
	var errResp ErrorResponse
	var data UnrestrictResponse

	resp, err := r.doPostFormWithClientContext(ctx, account.Client(), fmt.Sprintf("%s/unrestrict/link/", r.Host), formData, &data, &errResp)
	if err != nil {
		return emptyLink, err
	}
	if resp.StatusCode != http.StatusOK {
		switch errResp.ErrorCode {
		case 19, 24, 35:
			return emptyLink, customerror.HosterUnavailableError
		case 23, 34, 36:
			return emptyLink, customerror.TrafficExceededError
		default:
			return emptyLink, fmt.Errorf("realdebrid API error: Status: %d || Code: %d", resp.StatusCode, errResp.ErrorCode)
		}
	}
	if data.Download == "" {
		return emptyLink, fmt.Errorf("realdebrid API error: download link not found")
	}
	now := time.Now()
	dl := types.DownloadLink{
		Debrid:       r.config.Name,
		Token:        account.Token,
		Filename:     data.Filename,
		Size:         data.Filesize,
		Link:         data.Link,
		DownloadLink: data.Download,
		Generated:    now,
		ExpiresAt:    now.Add(r.autoExpiresLinksAfter),
	}
	return dl, nil
}

func (r *RealDebrid) GetDownloadLink(id string, file *types.File) (types.DownloadLink, error) {
	return r.accountsManager.GetDownloadLink(id, file, r.fetchDownloadLink)
}

func (r *RealDebrid) GetDownloadLinkContext(ctx context.Context, id string, file *types.File) (types.DownloadLink, error) {
	return r.accountsManager.GetDownloadLinkContext(ctx, id, file, r.fetchDownloadLinkContext)
}

func (r *RealDebrid) getTorrentsContext(ctx context.Context, offset int, limit int) (int, int, []*types.Torrent, error) {
	torrents := make([]*types.Torrent, 0)
	if offset < 0 || limit <= 0 {
		return 0, 0, torrents, fmt.Errorf(
			"invalid realdebrid torrent page offset %d limit %d",
			offset,
			limit,
		)
	}

	queryParams := make(map[string]string)
	if offset > 0 {
		queryParams["offset"] = fmt.Sprintf("%d", offset)
	}
	if limit > 0 {
		queryParams["limit"] = fmt.Sprintf("%d", limit)
	}

	// Need to get headers, so we create request manually
	u, err := url.Parse(r.Host + "/torrents")
	if err != nil {
		return 0, 0, torrents, err
	}
	q := u.Query()
	for k, v := range queryParams {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, 0, torrents, err
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return 0, 0, torrents, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return 0, 0, torrents, nil
	}

	if resp.StatusCode != http.StatusOK {
		return 0, 0, torrents, fmt.Errorf("realdebrid API error: %d", resp.StatusCode)
	}

	var data []TorrentsResponse
	if err := utils.DecodeJSONResponse(resp.Body, &data); err != nil {
		return 0, 0, torrents, err
	}

	totalItems, _ := strconv.Atoi(resp.Header.Get("X-Total-Count"))
	for _, remote := range data {
		if err := ctx.Err(); err != nil {
			return 0, 0, nil, err
		}
		if remote.Status != "downloaded" {
			continue
		}
		torrent := &types.Torrent{
			Id:               remote.Id,
			Name:             remote.Filename,
			Bytes:            remote.Bytes,
			Progress:         remote.Progress,
			Status:           types.TorrentStatusDownloaded,
			Filename:         remote.Filename,
			OriginalFilename: remote.Filename,
			Links:            remote.Links,
			Files:            make(map[string]types.File),
			InfoHash:         remote.Hash,
			Debrid:           r.config.Name,
			Added:            remote.Added,
		}
		torrents = append(torrents, torrent)
	}
	return totalItems, len(data), torrents, nil
}

func (r *RealDebrid) GetTorrents() ([]*types.Torrent, error) {
	return r.GetTorrentsContext(context.Background())
}

func (r *RealDebrid) GetTorrentsContext(ctx context.Context) ([]*types.Torrent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	limit := 1000
	if r.config.Limit > 0 {
		limit = r.config.Limit
	}

	allTorrents := make([]*types.Torrent, 0)
	seenIDs := make(map[string]int)
	offset := 0
	for page := 0; page < realDebridTorrentListMaxPages; page++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		totalItems, returnedItems, torrents, err := r.getTorrentsContext(ctx, offset, limit)
		if err != nil {
			return nil, fmt.Errorf(
				"realdebrid torrent list page %d at offset %d: %w",
				page+1,
				offset,
				err,
			)
		}
		if totalItems > realDebridTorrentListMaxItems {
			return nil, fmt.Errorf(
				"realdebrid torrent list reports %d items, maximum is %d",
				totalItems,
				realDebridTorrentListMaxItems,
			)
		}
		if returnedItems == 0 {
			if totalItems > offset {
				return nil, fmt.Errorf(
					"realdebrid torrent list ended at offset %d before reported total %d",
					offset,
					totalItems,
				)
			}
			return allTorrents, nil
		}
		if returnedItems > realDebridTorrentListMaxItems-offset {
			return nil, fmt.Errorf(
				"realdebrid torrent list exceeds %d remote items",
				realDebridTorrentListMaxItems,
			)
		}
		for _, torrent := range torrents {
			if torrent == nil || torrent.Id == "" {
				return nil, fmt.Errorf(
					"realdebrid torrent list page %d at offset %d contains an invalid item",
					page+1,
					offset,
				)
			}
			if previousOffset, exists := seenIDs[torrent.Id]; exists {
				return nil, fmt.Errorf(
					"realdebrid torrent list repeated torrent ID %q at offsets %d and %d",
					torrent.Id,
					previousOffset,
					offset,
				)
			}
			seenIDs[torrent.Id] = offset
		}
		allTorrents = append(allTorrents, torrents...)
		nextOffset := offset + returnedItems
		if nextOffset <= offset {
			return nil, fmt.Errorf(
				"realdebrid torrent list made no offset progress from %d",
				offset,
			)
		}
		offset = nextOffset
		if totalItems > 0 && offset >= totalItems {
			return allTorrents, nil
		}
	}
	return nil, fmt.Errorf(
		"realdebrid torrent list exceeds %d non-empty pages",
		realDebridTorrentListMaxPages,
	)
}

func (r *RealDebrid) RefreshDownloadLinks() error {
	return r.RefreshDownloadLinksContext(context.Background())
}

func (r *RealDebrid) RefreshDownloadLinksContext(ctx context.Context) error {
	return r.accountsManager.RefreshLinksContext(ctx, r.fetchDownloadLinksContext)
}

func (r *RealDebrid) fetchDownloadLinksContext(ctx context.Context, acc *account.Account) ([]types.DownloadLink, error) {
	return collectRealDebridDownloadLinksContext(ctx, func(offset, limit int) ([]types.DownloadLink, error) {
		return r.getDownloadLinksContext(ctx, acc, offset, limit)
	})
}

func collectRealDebridDownloadLinks(
	fetch func(offset, limit int) ([]types.DownloadLink, error),
) ([]types.DownloadLink, error) {
	return collectRealDebridDownloadLinksContext(context.Background(), fetch)
}

func collectRealDebridDownloadLinksContext(
	ctx context.Context,
	fetch func(offset, limit int) ([]types.DownloadLink, error),
) ([]types.DownloadLink, error) {
	if fetch == nil {
		return nil, fmt.Errorf("realdebrid download list fetcher is nil")
	}
	links := make([]types.DownloadLink, 0)
	offset := 0
	seenIDs := make(map[string]int, realDebridDownloadListPageSize)
	for page := 0; page < realDebridDownloadListMaxPages; page++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		batchLinks, err := fetch(
			offset,
			realDebridDownloadListPageSize,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"realdebrid download list page %d at offset %d: %w",
				page+1,
				offset,
				err,
			)
		}
		if len(batchLinks) == 0 {
			return links, nil
		}
		if len(batchLinks) > realDebridDownloadListMaxItems-len(links) {
			return nil, fmt.Errorf(
				"realdebrid download list exceeds %d items",
				realDebridDownloadListMaxItems,
			)
		}
		for _, link := range batchLinks {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			id := strings.TrimSpace(link.Id)
			if id == "" {
				return nil, fmt.Errorf(
					"realdebrid download list page %d at offset %d contains an empty ID",
					page+1,
					offset,
				)
			}
			if previousOffset, exists := seenIDs[id]; exists {
				return nil, fmt.Errorf(
					"realdebrid download list repeated ID %q at offsets %d and %d",
					id,
					previousOffset,
					offset,
				)
			}
			seenIDs[id] = offset
		}
		links = append(links, batchLinks...)
		nextOffset := offset + len(batchLinks)
		if nextOffset <= offset {
			return nil, fmt.Errorf(
				"realdebrid download list made no offset progress from %d",
				offset,
			)
		}
		offset = nextOffset
		if len(batchLinks) < realDebridDownloadListPageSize {
			return links, nil
		}
	}
	return nil, fmt.Errorf(
		"realdebrid download list exceeds %d non-empty pages",
		realDebridDownloadListMaxPages,
	)
}

func (r *RealDebrid) getDownloadLinksContext(ctx context.Context, acc *account.Account, offset int, limit int) ([]types.DownloadLink, error) {
	var data []DownloadsResponse

	queryParams := map[string]string{
		"limit": fmt.Sprintf("%d", limit),
	}
	if offset > 0 {
		queryParams["offset"] = fmt.Sprintf("%d", offset)
	}

	resp, err := r.doGetWithClientContext(ctx, acc.Client(), fmt.Sprintf("%s/downloads", r.Host), queryParams, &data)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("realdebrid API error: Status: %d", resp.StatusCode)
	}
	links := make([]types.DownloadLink, 0)
	for _, d := range data {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		links = append(links, types.DownloadLink{
			Debrid:       r.config.Name,
			Token:        acc.Token,
			Filename:     d.Filename,
			Size:         d.Filesize,
			Link:         d.Link,
			DownloadLink: d.Download,
			Generated:    d.Generated,
			ExpiresAt:    d.Generated.Add(r.autoExpiresLinksAfter),
			Id:           d.Id,
		})
	}
	return links, nil
}

func (r *RealDebrid) Config() config.Debrid {
	return r.config
}

func (r *RealDebrid) getClientProfileContext(ctx context.Context, client *request.Client) (*types.Profile, error) {
	var data profileResponse

	resp, err := r.doGetWithClientContext(ctx, client, fmt.Sprintf("%s/user", r.Host), nil, &data)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("realdebrid API error: Status: %d", resp.StatusCode)
	}

	profile := &types.Profile{
		Name:       r.config.Name,
		Id:         data.Id,
		Username:   data.Username,
		Email:      data.Email,
		Points:     data.Points,
		Premium:    data.Premium,
		Expiration: data.Expiration,
		Type:       data.Type,
	}
	return profile, nil
}

func (r *RealDebrid) GetProfile() (*types.Profile, error) {
	return r.GetProfileContext(context.Background())
}

func (r *RealDebrid) GetProfileContext(ctx context.Context) (*types.Profile, error) {
	r.profileMu.Lock()
	defer r.profileMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.Profile != nil && time.Since(r.profileLastFetched) < profileCacheDuration {
		cached := *r.Profile
		return &cached, nil
	}
	profile, err := r.getClientProfileContext(ctx, r.client)
	if err != nil {
		return nil, err
	}
	stored := *profile
	r.Profile = &stored
	r.profileLastFetched = time.Now()
	return profile, nil
}

func (r *RealDebrid) GetAvailableSlots() (int, error) {
	var data AvailableSlotsResponse

	resp, err := r.doGet("/torrents/activeCount", &data)
	if err != nil {
		return 0, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("realdebrid API error: Status: %d", resp.StatusCode)
	}

	return data.TotalSlots - data.ActiveSlots - r.config.MinimumFreeSlot, nil
}

func (r *RealDebrid) AccountManager() *account.Manager {
	return r.accountsManager
}

func (r *RealDebrid) SyncAccounts() {
	_ = r.SyncAccountsContext(context.Background())
}

func (r *RealDebrid) SyncAccountsContext(ctx context.Context) error {
	return r.accountsManager.SyncContext(ctx, r.syncAccountContext)
}

func (r *RealDebrid) syncAccountContext(ctx context.Context, acc *account.Account) error {
	if acc.Token == "" {
		return fmt.Errorf("account %s has no token", acc.Username)
	}
	profile, err := r.getClientProfileContext(ctx, acc.Client())
	if err != nil {
		return fmt.Errorf("error syncing account %s: %w", acc.Username, err)
	}
	acc.Username = profile.Username
	acc.Expiration = profile.Expiration

	var trafficData TrafficResponse
	trafficResp, err := r.doGetWithClientContext(ctx, acc.Client(), fmt.Sprintf("%s/traffic/details", r.Host), nil, &trafficData)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return nil
	}
	if trafficResp.StatusCode != http.StatusOK {
		return nil
	}

	if len(trafficData) == 0 {
		acc.TrafficUsed.Store(0)
	} else {
		today := time.Now().Format(time.DateOnly)
		if todayData, exists := trafficData[today]; exists {
			acc.TrafficUsed.Store(todayData.Bytes)
		}
	}
	return nil
}

func (r *RealDebrid) deleteDownloadLink(account *account.Account, downloadLink types.DownloadLink) error {
	req, err := http.NewRequest(http.MethodDelete, fmt.Sprintf("%s/downloads/delete/%s", r.Host, downloadLink.Id), nil)
	if err != nil {
		return err
	}

	if _, err = account.Client().Do(req); err != nil {
		return err
	}
	return nil
}

func (r *RealDebrid) DeleteLink(downloadLink types.DownloadLink) error {
	return r.accountsManager.DeleteDownloadLink(downloadLink, r.deleteDownloadLink)
}

// SpeedTest measures API latency and download speed using cached links
func (r *RealDebrid) SpeedTest(ctx context.Context) types.SpeedTestResult {
	result := types.SpeedTestResult{
		Provider: r.config.Name,
		TestedAt: time.Now(),
	}

	// Measure latency by hitting the user endpoint
	start := time.Now()
	resp, err := r.doGet("/user", nil)
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
	current := r.accountsManager.Current()
	if current == nil {
		return result // Latency only, no cached links
	}

	link, found := current.GetRandomLink()
	if !found || link.DownloadLink == "" {
		return result // Latency only, no cached links
	}

	bytesRead, downloadDuration, err := common.ProbeDownload(ctx, current.Client(), link.DownloadLink)
	if err != nil {
		return result // Return latency, skip speed test
	}

	result.BytesRead = bytesRead
	if downloadDuration.Seconds() > 0 {
		result.SpeedMBps = float64(result.BytesRead) / downloadDuration.Seconds() / (1024 * 1024)
	}

	return result
}

func (r *RealDebrid) SupportsCheck() bool {
	return true
}
