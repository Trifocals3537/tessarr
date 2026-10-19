package premiumize

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/internal/request"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/debrid/account"
	"github.com/Trifocals3537/tessarr/pkg/debrid/common"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/version"
	"github.com/rs/zerolog"
	"go.uber.org/ratelimit"
)

const (
	defaultHost          = "https://www.premiumize.me"
	profileCacheDuration = time.Hour
	premiumizeTreeDepth  = 64
	premiumizeTreeItems  = 100_000
)

var _ common.Client = (*Premiumize)(nil)
var _ common.ContextTorrentLister = (*Premiumize)(nil)
var _ common.ContextDownloadLinkRefresher = (*Premiumize)(nil)
var _ common.ContextAccountSyncer = (*Premiumize)(nil)
var _ common.ContextMagnetSubmitter = (*Premiumize)(nil)
var _ common.ContextStatusChecker = (*Premiumize)(nil)
var _ common.ContextDownloadLinkResolver = (*Premiumize)(nil)

type Premiumize struct {
	Host                  string `json:"host"`
	APIKey                string
	client                *request.Client
	accountsManager       *account.Manager
	autoExpiresLinksAfter time.Duration
	logger                zerolog.Logger
	config                config.Debrid
	profile               *types.Profile
	profileLastFetched    time.Time
	profileMu             sync.Mutex
	isFileAllowed         func(string, int64) error
}

func New(dc config.Debrid, ratelimits map[string]ratelimit.Limiter) (*Premiumize, error) {
	cfg := config.Get()
	_log := logger.New(dc.Name)
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", dc.APIKey),
	}
	if dc.UserAgent != "" {
		headers["User-Agent"] = dc.UserAgent
	} else {
		headers["User-Agent"] = fmt.Sprintf("Tessarr/%s (%s; %s)", version.GetInfo(), runtime.GOOS, runtime.GOARCH)
	}

	autoExpiresLinksAfter, err := utils.ParseDuration(dc.AutoExpireLinksAfter)
	if autoExpiresLinksAfter == 0 || err != nil {
		autoExpiresLinksAfter = 48 * time.Hour
	}

	opts := []request.ClientOption{
		request.WithHeaders(headers),
		request.WithLogger(_log),
		request.WithMaxRetries(cfg.Retries),
		request.WithRateLimiter(ratelimits["main"]),
		request.WithRetryableStatus(http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout),
	}
	if dc.Proxy != "" {
		opts = append(opts, request.WithProxy(dc.Proxy))
	}

	return &Premiumize{
		Host:                  defaultHost,
		APIKey:                dc.APIKey,
		client:                request.New(opts...),
		accountsManager:       account.NewManager(dc, ratelimits["download"], _log),
		autoExpiresLinksAfter: autoExpiresLinksAfter,
		logger:                _log,
		config:                dc,
		isFileAllowed:         config.Get().IsFileAllowed,
	}, nil
}

func (pm *Premiumize) Logger() zerolog.Logger {
	return pm.logger
}

func (pm *Premiumize) Config() config.Debrid {
	return pm.config
}

func (pm *Premiumize) endpoint(apiPath string) string {
	if strings.HasPrefix(apiPath, "/") {
		return pm.Host + apiPath
	}
	return pm.Host + "/" + apiPath
}

func (pm *Premiumize) do(req *http.Request, out any) (*http.Response, error) {
	resp, err := pm.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Provider error bodies may echo submitted magnets or signed URLs. Read
		// only a bounded prefix for connection/resource safety and never expose
		// the raw body in the returned error.
		_, _ = utils.ReadAllLimited(resp.Body, 64<<10)
		return resp, fmt.Errorf("premiumize API error: Status: %d", resp.StatusCode)
	}

	if resp.ContentLength == 0 {
		return resp, nil
	}

	var body json.RawMessage
	if err := utils.DecodeJSONResponse(resp.Body, &body); err != nil {
		return resp, err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return resp, nil
	}
	var envelope apiEnvelope
	if err := json.Unmarshal(body, &envelope); err == nil && envelope.Status == "error" {
		return resp, &premiumizeAPIError{Code: envelope.Code}
	}
	if out != nil {
		if err := json.Unmarshal(body, out); err != nil {
			return resp, err
		}
	}

	return resp, nil
}

func (pm *Premiumize) doForm(ctx context.Context, method, apiPath string, values url.Values, out any) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, pm.endpoint(apiPath), strings.NewReader(values.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return pm.do(req, out)
}

func (pm *Premiumize) SubmitMagnet(t *types.Torrent) (*types.Torrent, error) {
	return pm.SubmitMagnetContext(context.Background(), t)
}

func (pm *Premiumize) SubmitMagnetContext(ctx context.Context, t *types.Torrent) (*types.Torrent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if t == nil || t.Magnet == nil {
		return nil, fmt.Errorf("missing magnet")
	}
	if t.Magnet.IsTorrent() {
		return pm.addTorrentContext(ctx, t)
	}
	return pm.addMagnetContext(ctx, t)
}

func (pm *Premiumize) addMagnetContext(ctx context.Context, t *types.Torrent) (*types.Torrent, error) {
	src := t.Magnet.Link
	if src == "" && t.InfoHash != "" {
		src = utils.ConstructMagnet(t.InfoHash, t.Name).Link
	}
	var data transferCreateResponse
	_, err := pm.doForm(ctx, http.MethodPost, "/api/transfer/create", url.Values{"src": {src}}, &data)
	if err != nil {
		return nil, err
	}
	pm.applySubmittedTorrent(t, data)
	return t, nil
}

func (pm *Premiumize) addTorrentContext(ctx context.Context, t *types.Torrent) (*types.Torrent, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("src", cmp.Or(t.Filename, t.Magnet.Name, "upload.torrent"))
	if err != nil {
		return nil, err
	}
	if _, err := part.Write(t.Magnet.File); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, pm.endpoint("/api/transfer/create"), &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	var data transferCreateResponse
	if _, err := pm.do(req, &data); err != nil {
		return nil, err
	}
	pm.applySubmittedTorrent(t, data)
	return t, nil
}

func (pm *Premiumize) applySubmittedTorrent(t *types.Torrent, data transferCreateResponse) {
	t.Id = data.ID
	t.Debrid = pm.config.Name
	t.Status = types.TorrentStatusQueued
	if data.Name != "" {
		t.Name = data.Name
		t.Filename = data.Name
	}
	if t.OriginalFilename == "" {
		t.OriginalFilename = cmp.Or(t.Magnet.Name, t.Name)
	}
}

func (pm *Premiumize) CheckStatus(t *types.Torrent) (*types.Torrent, error) {
	return pm.CheckStatusContext(context.Background(), t)
}

func (pm *Premiumize) CheckStatusContext(ctx context.Context, t *types.Torrent) (*types.Torrent, error) {
	if err := ctx.Err(); err != nil {
		return t, err
	}
	return pm.updateAndReturnTorrentContext(ctx, t)
}

func (pm *Premiumize) UpdateAndReturnTorrent(t *types.Torrent) (*types.Torrent, error) {
	return pm.updateAndReturnTorrentContext(context.Background(), t)
}

func (pm *Premiumize) updateAndReturnTorrentContext(ctx context.Context, t *types.Torrent) (*types.Torrent, error) {
	providerFinished, err := pm.updateTorrentContext(ctx, t)
	if err != nil {
		return t, err
	}
	if t.Status == types.TorrentStatusDownloaded {
		return t, nil
	}
	if providerFinished {
		// Premiumize can finish the transfer before item/folder links are
		// populated. This provider-side eventual consistency is not evidence
		// that a cached-only grab was uncached. Keep it in the manager's status
		// queue until transferToTorrentContext observes usable links.
		return t, nil
	}
	if t.Status == types.TorrentStatusDownloading || t.Status == types.TorrentStatusQueued {
		if !t.DownloadUncached {
			return t, customerror.NewTorrentNotCachedError(t.Name)
		}
		return t, nil
	}
	return t, premiumizeTransferStatusError(t.Name, t.ProviderState)
}

func premiumizeTransferStatusError(name, status string) error {
	err := fmt.Errorf("torrent: %s has error status: %s", name, status)
	if strings.EqualFold(strings.TrimSpace(status), "error") {
		return fmt.Errorf("%w: %v", types.ErrTerminalProviderTorrent, err)
	}
	return err
}

func (pm *Premiumize) GetTorrent(torrentID string) (*types.Torrent, error) {
	transfers, err := pm.listTransfers()
	if err != nil {
		return nil, err
	}
	for _, tr := range transfers {
		if tr.ID == torrentID {
			return pm.transferToTorrent(tr, "")
		}
	}
	return nil, customerror.TorrentNotFoundError
}

func (pm *Premiumize) UpdateTorrent(t *types.Torrent) error {
	_, err := pm.updateTorrentContext(context.Background(), t)
	return err
}

func (pm *Premiumize) updateTorrentContext(ctx context.Context, t *types.Torrent) (bool, error) {
	if t == nil {
		return false, fmt.Errorf("torrent is nil")
	}
	transfers, err := pm.listTransfersContext(ctx)
	if err != nil {
		return false, err
	}
	for _, tr := range transfers {
		if tr.ID == t.Id {
			updated, err := pm.transferToTorrentContext(ctx, tr, t.InfoHash)
			if err != nil {
				return false, err
			}
			t.Name = updated.Name
			t.Filename = updated.Filename
			t.OriginalFilename = updated.OriginalFilename
			t.Bytes = updated.Bytes
			t.Size = updated.Size
			t.Progress = updated.Progress
			t.Status = updated.Status
			t.ProviderState = updated.ProviderState
			t.Files = updated.Files
			t.Links = updated.Links
			t.Debrid = updated.Debrid
			t.InfoHash = cmp.Or(updated.InfoHash, t.InfoHash)
			return mapStatus(tr.Status) == types.TorrentStatusDownloaded, nil
		}
	}
	return false, customerror.TorrentNotFoundError
}

func (pm *Premiumize) DeleteTorrent(torrentID string) error {
	resp, err := pm.doForm(context.Background(), http.MethodPost, "/api/transfer/delete", url.Values{"id": {torrentID}}, nil)
	if resp != nil && resp.StatusCode == http.StatusNotFound {
		return customerror.TorrentNotFoundError
	}
	var apiErr *premiumizeAPIError
	if errors.As(err, &apiErr) && apiErr.Code == "not_found" {
		return customerror.TorrentNotFoundError
	}
	return err
}

func (pm *Premiumize) IsAvailable(infohashes []string) map[string]bool {
	result := make(map[string]bool, len(infohashes))
	const batchSize = 100
	for i := 0; i < len(infohashes); i += batchSize {
		end := min(i+batchSize, len(infohashes))
		values := url.Values{}
		hashByItem := make(map[string]string, end-i)
		for _, hash := range infohashes[i:end] {
			if hash == "" {
				continue
			}
			item := utils.ConstructMagnet(hash, "").Link
			values.Add("items[]", item)
			hashByItem[item] = hash
		}
		if len(values) == 0 {
			continue
		}
		var data cacheCheckResponse
		if _, err := pm.doForm(context.Background(), http.MethodPost, "/api/cache/check", values, &data); err != nil {
			pm.logger.Error().Err(err).Msg("Error checking Premiumize availability")
			continue
		}
		items := values["items[]"]
		for idx, available := range data.Response {
			if idx < len(items) && available {
				result[hashByItem[items[idx]]] = true
			}
		}
	}
	return result
}

func (pm *Premiumize) GetTorrents() ([]*types.Torrent, error) {
	return pm.GetTorrentsContext(context.Background())
}

func (pm *Premiumize) GetTorrentsContext(ctx context.Context) ([]*types.Torrent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	transfers, err := pm.listTransfersContext(ctx)
	if err != nil {
		return nil, err
	}
	torrents := make([]*types.Torrent, 0, len(transfers))
	for _, tr := range transfers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		torrent, err := pm.transferToTorrentContext(ctx, tr, "")
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			pm.logger.Warn().Err(err).Str("transfer_id", tr.ID).Msg("Skipping Premiumize transfer")
			continue
		}
		if torrent.Status == types.TorrentStatusDownloaded {
			torrents = append(torrents, torrent)
		}
	}
	return torrents, nil
}

func (pm *Premiumize) listTransfers() ([]premiumizeTransfer, error) {
	return pm.listTransfersContext(context.Background())
}

func (pm *Premiumize) listTransfersContext(ctx context.Context) ([]premiumizeTransfer, error) {
	var data transferListResponse
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pm.endpoint("/api/transfer/list"), nil)
	if err != nil {
		return nil, err
	}
	if _, err := pm.do(req, &data); err != nil {
		return nil, err
	}
	return data.Transfers, nil
}

func (pm *Premiumize) transferToTorrent(tr premiumizeTransfer, fallbackInfoHash string) (*types.Torrent, error) {
	return pm.transferToTorrentContext(context.Background(), tr, fallbackInfoHash)
}

func (pm *Premiumize) transferToTorrentContext(ctx context.Context, tr premiumizeTransfer, fallbackInfoHash string) (*types.Torrent, error) {
	providerState := strings.ToLower(strings.TrimSpace(tr.Status))
	status := mapStatus(providerState)
	files := make(map[string]types.File)
	var links []string
	if status == types.TorrentStatusDownloaded {
		var err error
		var ready bool
		files, links, ready, err = pm.filesForTransferContext(ctx, tr)
		if err != nil {
			return nil, err
		}
		// Premiumize can mark a transfer finished before its folder tree has
		// usable file links. Keep it active until linked files are visible.
		if !ready {
			status = types.TorrentStatusDownloading
		}
	}
	size := int64(0)
	for _, file := range files {
		size += file.Size
	}
	name := cmp.Or(tr.Name, tr.ID)
	added := time.Unix(tr.Created.Unix, 0)
	if tr.Created.Unix == 0 {
		added = time.Time{}
	}
	return &types.Torrent{
		Id:               tr.ID,
		InfoHash:         cmp.Or(utils.ExtractInfoHash(tr.Src), fallbackInfoHash),
		Name:             name,
		Filename:         name,
		OriginalFilename: name,
		Bytes:            size,
		Size:             size,
		Files:            files,
		Status:           status,
		ProviderState:    providerState,
		Progress:         normalizeProgress(tr.Progress),
		Links:            links,
		Debrid:           pm.config.Name,
		Added:            added,
	}, nil
}

func (pm *Premiumize) filesForTransfer(tr premiumizeTransfer) (map[string]types.File, []string, bool, error) {
	return pm.filesForTransferContext(context.Background(), tr)
}

func (pm *Premiumize) filesForTransferContext(ctx context.Context, tr premiumizeTransfer) (map[string]types.File, []string, bool, error) {
	fileRecords := make([]types.File, 0)
	links := make([]string, 0)
	if fileID := tr.FileID.String(); fileID != "" {
		item, err := pm.itemDetailsContext(ctx, fileID)
		if err != nil {
			return nil, nil, false, err
		}
		pm.addFile(&fileRecords, &links, tr.ID, item.Name, item.Name, item.Size, item.ID, item.Link)
		files, err := types.FilesByLogicalName(fileRecords)
		if err != nil {
			return nil, nil, false, err
		}
		return files, links, item.Link != "", nil
	}
	if folderID := tr.FolderID.String(); folderID != "" {
		linkedFiles, err := pm.addFolderFilesContext(ctx, &fileRecords, &links, tr.ID, folderID, "")
		if err != nil {
			return nil, nil, false, err
		}
		files, err := types.FilesByLogicalName(fileRecords)
		if err != nil {
			return nil, nil, false, err
		}
		return files, links, linkedFiles > 0, nil
	}
	return make(map[string]types.File), links, false, nil
}

func (pm *Premiumize) itemDetailsContext(ctx context.Context, id string) (*itemDetailsResponse, error) {
	var data itemDetailsResponse
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pm.endpoint("/api/item/details?id="+url.QueryEscape(id)), nil)
	if err != nil {
		return nil, err
	}
	if _, err := pm.do(req, &data); err != nil {
		return nil, err
	}
	return &data, nil
}

func (pm *Premiumize) addFolderFilesContext(ctx context.Context, files *[]types.File, links *[]string, transferID, folderID, prefix string) (int, error) {
	visitedItems := 0
	return pm.addFolderFilesBoundedContext(
		ctx,
		files,
		links,
		transferID,
		folderID,
		prefix,
		0,
		make(map[string]struct{}),
		&visitedItems,
	)
}

func (pm *Premiumize) addFolderFilesBoundedContext(
	ctx context.Context,
	files *[]types.File,
	links *[]string,
	transferID, folderID, prefix string,
	depth int,
	visitedFolders map[string]struct{},
	visitedItems *int,
) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if depth > premiumizeTreeDepth {
		return 0, fmt.Errorf(
			"premiumize folder tree exceeds depth %d",
			premiumizeTreeDepth,
		)
	}
	if folderID == "" {
		return 0, fmt.Errorf("premiumize folder tree contains an empty folder ID")
	}
	if _, exists := visitedFolders[folderID]; exists {
		return 0, fmt.Errorf(
			"premiumize folder tree repeats folder ID %q",
			folderID,
		)
	}
	visitedFolders[folderID] = struct{}{}

	var data folderListResponse
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pm.endpoint("/api/folder/list?id="+url.QueryEscape(folderID)), nil)
	if err != nil {
		return 0, err
	}
	if _, err := pm.do(req, &data); err != nil {
		return 0, err
	}
	// Count links from the full recursive tree, not just files that pass
	// Tessarr's extension/size filters, so readiness reflects Premiumize.
	linkedFiles := 0
	for _, item := range data.Content {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		(*visitedItems)++
		if *visitedItems > premiumizeTreeItems {
			return 0, fmt.Errorf(
				"premiumize folder tree exceeds %d items",
				premiumizeTreeItems,
			)
		}
		itemPath := path.Join(prefix, item.Name)
		if item.Type == "folder" {
			n, err := pm.addFolderFilesBoundedContext(
				ctx,
				files,
				links,
				transferID,
				item.ID,
				itemPath,
				depth+1,
				visitedFolders,
				visitedItems,
			)
			if err != nil {
				return 0, err
			}
			linkedFiles += n
			continue
		}
		if item.Link != "" {
			linkedFiles++
		}
		pm.addFile(files, links, transferID, item.Name, itemPath, item.Size, item.ID, item.Link)
	}
	return linkedFiles, nil
}

func (pm *Premiumize) addFile(files *[]types.File, links *[]string, transferID, name, itemPath string, size int64, id, link string) {
	if link == "" {
		return
	}
	if itemPath == "" {
		itemPath = name
	}
	if pm.isFileAllowed != nil {
		if err := pm.isFileAllowed(itemPath, size); err != nil {
			return
		}
	} else if filepath.Ext(itemPath) == "" {
		return
	}
	*files = append(*files, types.File{
		TorrentId: transferID,
		Id:        id,
		Name:      name,
		Path:      itemPath,
		Size:      size,
		Link:      link,
	})
	*links = append(*links, link)
}

func (pm *Premiumize) GetDownloadLink(id string, file *types.File) (types.DownloadLink, error) {
	return pm.accountsManager.GetDownloadLink(id, file, pm.fetchDownloadLink)
}

func (pm *Premiumize) fetchDownloadLink(acc *account.Account, id string, file *types.File) (types.DownloadLink, error) {
	return pm.fetchDownloadLinkContext(context.Background(), acc, id, file)
}

func (pm *Premiumize) GetDownloadLinkContext(ctx context.Context, id string, file *types.File) (types.DownloadLink, error) {
	return pm.accountsManager.GetDownloadLinkContext(ctx, id, file, pm.fetchDownloadLinkContext)
}

func (pm *Premiumize) fetchDownloadLinkContext(ctx context.Context, acc *account.Account, id string, file *types.File) (types.DownloadLink, error) {
	if err := ctx.Err(); err != nil {
		return types.DownloadLink{}, err
	}
	link := file.Link
	size := file.Size
	filename := file.Name
	// Premiumize's stored CDN URL expires. Treat it as the stable cache
	// identity and re-mint the usable URL through item/details whenever the
	// provider supplied an item ID. The account cache keeps this to one API
	// request per file until the link is expired or explicitly invalidated.
	if file.Id != "" {
		item, err := pm.itemDetailsContext(ctx, file.Id)
		if err != nil {
			return types.DownloadLink{}, err
		}
		link = item.Link
		size = item.Size
		filename = item.Name
	}
	if link == "" {
		return types.DownloadLink{}, customerror.HosterUnavailableError
	}
	// Account.GetDownloadLinkContext looks up by file.Link and storeLink writes
	// by DownloadLink.Link. Keep that cache identity stable while returning the
	// freshly minted provider URL in DownloadLink.
	cacheKey := file.Link
	if cacheKey == "" {
		cacheKey = link
	}
	now := time.Now()
	return types.DownloadLink{
		Debrid:       pm.config.Name,
		Token:        acc.Token,
		Filename:     filename,
		Size:         size,
		Link:         cacheKey,
		DownloadLink: link,
		Generated:    now,
		ExpiresAt:    now.Add(pm.autoExpiresLinksAfter),
		Id:           file.Id,
	}, nil
}

func (pm *Premiumize) RefreshDownloadLinks() error {
	return pm.RefreshDownloadLinksContext(context.Background())
}

func (pm *Premiumize) RefreshDownloadLinksContext(ctx context.Context) error {
	return pm.accountsManager.RefreshLinksContext(ctx, func(ctx context.Context, account *account.Account) ([]types.DownloadLink, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return []types.DownloadLink{}, nil
	})
}

func (pm *Premiumize) CheckFile(ctx context.Context, infohash, fileID string) error {
	if strings.HasPrefix(fileID, "http://") || strings.HasPrefix(fileID, "https://") {
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, fileID, nil)
		if err != nil {
			return err
		}
		resp, err := pm.client.Do(req)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			// net/http errors contain the complete signed provider URL.
			return fmt.Errorf("premiumize link check request failed")
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusGone {
			return customerror.HosterUnavailableError
		}
		if resp.StatusCode >= 400 {
			return fmt.Errorf("premiumize link check failed: Status: %d", resp.StatusCode)
		}
		return nil
	}
	if fileID == "" {
		return customerror.HosterUnavailableError
	}
	if _, err := pm.itemDetailsContext(ctx, fileID); err != nil {
		return err
	}
	return nil
}

func (pm *Premiumize) GetProfile() (*types.Profile, error) {
	return pm.GetProfileContext(context.Background())
}

func (pm *Premiumize) GetProfileContext(ctx context.Context) (*types.Profile, error) {
	pm.profileMu.Lock()
	defer pm.profileMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if pm.profile != nil && time.Since(pm.profileLastFetched) < profileCacheDuration {
		cached := *pm.profile
		return &cached, nil
	}
	profile, err := pm.getClientProfileContext(ctx, pm.client)
	if err != nil {
		return nil, err
	}
	stored := *profile
	pm.profile = &stored
	pm.profileLastFetched = time.Now()
	result := stored
	return &result, nil
}

func (pm *Premiumize) getClientProfileContext(ctx context.Context, client *request.Client) (*types.Profile, error) {
	var data accountInfoResponse
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pm.endpoint("/api/account/info"), nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = utils.ReadAllLimited(resp.Body, 64<<10)
		return nil, fmt.Errorf("premiumize API error: Status: %d", resp.StatusCode)
	}
	if err := utils.DecodeJSONResponse(resp.Body, &data); err != nil {
		return nil, err
	}
	if data.Status == "error" {
		return nil, &premiumizeAPIError{Code: data.Code}
	}
	expiration := time.Time{}
	premium := int64(0)
	accountType := "free"
	if data.PremiumUntil != nil {
		premium = *data.PremiumUntil
		expiration = time.Unix(*data.PremiumUntil, 0)
		if premium > time.Now().Unix() {
			accountType = "premium"
		}
	}
	customerID, err := data.customerIDInt64()
	if err != nil {
		return nil, err
	}
	return &types.Profile{
		Name:       pm.config.Name,
		Id:         customerID,
		Username:   strconv.FormatInt(customerID, 10),
		Points:     data.BoosterPoints,
		Premium:    premium,
		Expiration: expiration,
		Type:       accountType,
	}, nil
}

func (pm *Premiumize) GetAvailableSlots() (int, error) {
	// Premiumize does not provide active slot info without listing transfers.
	return config.DefaultAvailableSlots, nil
}

func (pm *Premiumize) AccountManager() *account.Manager {
	return pm.accountsManager
}

func (pm *Premiumize) SyncAccounts() {
	_ = pm.SyncAccountsContext(context.Background())
}

func (pm *Premiumize) SyncAccountsContext(ctx context.Context) error {
	return pm.accountsManager.SyncContext(ctx, pm.syncAccountContext)
}

func (pm *Premiumize) syncAccountContext(ctx context.Context, acc *account.Account) error {
	profile, err := pm.getClientProfileContext(ctx, acc.Client())
	if err != nil {
		return err
	}
	acc.Username = profile.Username
	acc.Expiration = profile.Expiration
	return nil
}

func (pm *Premiumize) DeleteLink(downloadLink types.DownloadLink) error {
	return pm.accountsManager.DeleteDownloadLink(downloadLink, func(account *account.Account, dl types.DownloadLink) error {
		// Premiumize exposes item and transfer deletion, but not a safe
		// generated-link deletion endpoint. Deleting dl.Id here would delete
		// the user's cloud file, not just invalidate this cached CDN link.
		return nil
	})
}

func (pm *Premiumize) SpeedTest(ctx context.Context) types.SpeedTestResult {
	result := types.SpeedTestResult{
		Provider: pm.config.Name,
		TestedAt: time.Now(),
	}
	start := time.Now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pm.endpoint("/api/account/info"), nil)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	resp, err := pm.client.Do(req)
	latency := time.Since(start)
	if err != nil {
		result.Error = fmt.Sprintf("latency test failed: %v", err)
		return result
	}
	resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		result.Error = fmt.Sprintf("latency test unexpected status: %d", resp.StatusCode)
		return result
	}
	result.LatencyMs = latency.Milliseconds()

	current := pm.accountsManager.Current()
	if current == nil {
		return result
	}
	link, found := current.GetRandomLink()
	if !found || link.DownloadLink == "" {
		return result
	}
	bytesRead, duration, err := common.ProbeDownload(ctx, current.Client(), link.DownloadLink)
	if err != nil {
		return result
	}
	result.BytesRead = bytesRead
	if duration.Seconds() > 0 {
		result.SpeedMBps = float64(result.BytesRead) / duration.Seconds() / (1024 * 1024)
	}
	return result
}

func (pm *Premiumize) SupportsCheck() bool {
	return true
}

func mapStatus(status string) types.TorrentStatus {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "finished", "seeding":
		return types.TorrentStatusDownloaded
	case "queued":
		return types.TorrentStatusQueued
	case "running":
		return types.TorrentStatusDownloading
	default:
		return types.TorrentStatusError
	}
}

func normalizeProgress(progress float64) float64 {
	if math.IsNaN(progress) || progress <= 0 {
		return 0
	}
	if progress <= 1 {
		return progress * 100
	}
	return min(progress, 100)
}
