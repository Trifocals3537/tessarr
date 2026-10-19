package alldebrid

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path"
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
	"github.com/rs/zerolog"
	"go.uber.org/ratelimit"
)

type AllDebrid struct {
	Host                  string `json:"host"`
	APIKey                string
	accountsManager       *account.Manager
	autoExpiresLinksAfter time.Duration
	client                *request.Client
	repairClient          *request.Client
	Profile               *types.Profile `json:"profile"`
	profileMu             sync.Mutex
	logger                zerolog.Logger
	config                config.Debrid
	restartMu             sync.Mutex
	restartStates         map[string]magnetRestartState
	restartNow            func() time.Time
}

var _ common.ContextTorrentLister = (*AllDebrid)(nil)
var _ common.ContextDownloadLinkRefresher = (*AllDebrid)(nil)
var _ common.ContextAccountSyncer = (*AllDebrid)(nil)
var _ common.ContextMagnetSubmitter = (*AllDebrid)(nil)
var _ common.ContextStatusChecker = (*AllDebrid)(nil)
var _ common.ContextDownloadLinkResolver = (*AllDebrid)(nil)

const (
	allDebridFileTreeMaxDepth = 64
	allDebridFileTreeMaxItems = 100_000
)

func New(dc config.Debrid, ratelimits map[string]ratelimit.Limiter) (*AllDebrid, error) {
	cfg := config.Get()
	headers := map[string]string{
		"Authorization": fmt.Sprintf("Bearer %s", dc.APIKey),
	}
	if dc.UserAgent != "" {
		headers["User-Agent"] = dc.UserAgent
	}
	_log := logger.New(dc.Name)

	opts := []request.ClientOption{
		request.WithHeaders(headers),
		request.WithRateLimiter(ratelimits["main"]),
		request.WithMaxRetries(cfg.Retries),
		request.WithRetryableStatus(http.StatusTooManyRequests, http.StatusBadGateway),
	}
	if dc.Proxy != "" {
		opts = append(opts, request.WithProxy(dc.Proxy))
	}
	repairOpts := []request.ClientOption{
		request.WithHeaders(headers),
		request.WithRateLimiter(ratelimits["repair"]),
		request.WithMaxRetries(4),
		request.WithRetryableStatus(http.StatusTooManyRequests),
	}
	if dc.Proxy != "" {
		repairOpts = append(repairOpts, request.WithProxy(dc.Proxy))
	}

	autoExpiresLinksAfter, err := utils.ParseDuration(dc.AutoExpireLinksAfter)
	if autoExpiresLinksAfter == 0 || err != nil {
		autoExpiresLinksAfter = 48 * time.Hour
	}
	ad := &AllDebrid{
		Host:                  "https://api.alldebrid.com/v4.1",
		APIKey:                dc.APIKey,
		accountsManager:       account.NewManager(dc, ratelimits["download"], _log),
		autoExpiresLinksAfter: autoExpiresLinksAfter,
		client:                request.New(opts...),
		repairClient:          request.New(repairOpts...),
		logger:                _log,
		config:                dc,
		restartStates:         make(map[string]magnetRestartState),
		restartNow:            time.Now,
	}
	return ad, nil
}

func (ad *AllDebrid) Config() config.Debrid {
	return ad.config
}

func (ad *AllDebrid) Logger() zerolog.Logger {
	return ad.logger
}

func (ad *AllDebrid) doAccountRequest(account *account.Account, endpoint string, queryParams map[string]string, result any) (*http.Response, error) {
	return ad.doAccountRequestContext(context.Background(), account, endpoint, queryParams, result)
}

func (ad *AllDebrid) doAccountRequestContext(ctx context.Context, account *account.Account, endpoint string, queryParams map[string]string, result any) (*http.Response, error) {
	u, err := url.Parse(ad.Host + endpoint)
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

	resp, err := account.Client().Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		// The query can contain a signed provider URL. net/http errors include
		// the complete request URL, so do not wrap or stringify them here.
		return nil, fmt.Errorf("alldebrid request to %s failed", endpoint)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10+1))
		_ = resp.Body.Close()
	}()

	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && resp.ContentLength != 0 {
		if err := utils.DecodeJSONResponse(resp.Body, result); err != nil {
			return resp, err
		}
	}

	return resp, nil
}

// doRequest performs a GET request and unmarshals the response
func (ad *AllDebrid) doRequest(endpoint string, queryParams map[string]string, result any) (*http.Response, error) {
	return ad.doRequestContext(context.Background(), endpoint, queryParams, result)
}

func (ad *AllDebrid) doRequestContext(ctx context.Context, endpoint string, queryParams map[string]string, result any) (*http.Response, error) {
	u, err := url.Parse(ad.Host + endpoint)
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

	resp, err := ad.client.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		// Magnet and unlock parameters can contain credentials or signed URLs.
		return nil, fmt.Errorf("alldebrid request to %s failed", endpoint)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10+1))
		_ = resp.Body.Close()
	}()

	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && resp.ContentLength != 0 {
		if err := utils.DecodeJSONResponse(resp.Body, result); err != nil {
			return resp, err
		}
	}

	return resp, nil
}

func (ad *AllDebrid) IsAvailable(hashes []string) map[string]bool {
	result := make(map[string]bool)
	// AllDebrid does not support checking cached infohashes
	return result
}

func (ad *AllDebrid) doPostFileContext(ctx context.Context, endpoint string, fileData []byte, result any) (*http.Response, error) {
	u, err := url.Parse(ad.Host + endpoint)
	if err != nil {
		return nil, err
	}

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("files[]", "torrent.torrent")
	if err != nil {
		return nil, err
	}
	if _, err = part.Write(fileData); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), &body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := ad.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if result != nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if err := utils.DecodeJSONResponse(resp.Body, result); err != nil {
			return resp, err
		}
	}
	return resp, nil
}

func (ad *AllDebrid) SubmitMagnet(torrent *types.Torrent) (*types.Torrent, error) {
	return ad.SubmitMagnetContext(context.Background(), torrent)
}

func (ad *AllDebrid) SubmitMagnetContext(ctx context.Context, torrent *types.Torrent) (*types.Torrent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if torrent == nil || torrent.Magnet == nil {
		return nil, fmt.Errorf("missing torrent magnet")
	}
	if torrent.Magnet.IsTorrent() {
		return ad.addTorrentFileContext(ctx, torrent)
	}
	return ad.addMagnetLinkContext(ctx, torrent)
}

func (ad *AllDebrid) addTorrentFileContext(ctx context.Context, torrent *types.Torrent) (*types.Torrent, error) {
	var data UploadFileResponse

	resp, err := ad.doPostFileContext(ctx, "/magnet/upload/file", torrent.Magnet.File, &data)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("alldebrid API error: Status: %d", resp.StatusCode)
	}

	files := data.Data.Files
	if len(files) == 0 {
		return nil, fmt.Errorf("error adding torrent file: no files returned")
	}
	f := files[0]
	if f.Error != nil {
		return nil, fmt.Errorf("alldebrid file upload error (code %s)", f.Error.Code)
	}
	torrent.Id = strconv.Itoa(f.ID)
	torrent.Added = time.Now()
	return torrent, nil
}

func (ad *AllDebrid) addMagnetLinkContext(ctx context.Context, torrent *types.Torrent) (*types.Torrent, error) {
	var data UploadMagnetResponse

	resp, err := ad.doRequestContext(ctx, "/magnet/upload", map[string]string{"magnets[]": torrent.Magnet.Link}, &data)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("alldebrid API error: Status: %d", resp.StatusCode)
	}

	magnets := data.Data.Magnets
	if len(magnets) == 0 {
		return nil, fmt.Errorf("error adding torrent. No magnets returned")
	}
	magnet := magnets[0]
	torrent.Id = strconv.Itoa(magnet.ID)
	torrent.Added = time.Now()
	return torrent, nil
}

func getAlldebridStatus(statusCode int) types.TorrentStatus {
	switch {
	case statusCode == 4:
		return types.TorrentStatusDownloaded
	case statusCode >= 0 && statusCode <= 3:
		return types.TorrentStatusDownloading
	default:
		return types.TorrentStatusError
	}
}

func (ad *AllDebrid) flattenFiles(torrentId string, files []MagnetFile) (map[string]types.File, error) {
	collected := make([]types.File, 0)
	visited := 0
	if err := ad.collectFiles(
		torrentId,
		files,
		"",
		0,
		&visited,
		&collected,
	); err != nil {
		return nil, err
	}
	return types.FilesByLogicalName(collected)
}

func (ad *AllDebrid) collectFiles(
	torrentId string,
	files []MagnetFile,
	parentPath string,
	depth int,
	visited *int,
	collected *[]types.File,
) error {
	if depth > allDebridFileTreeMaxDepth {
		return fmt.Errorf(
			"alldebrid file tree exceeds depth %d",
			allDebridFileTreeMaxDepth,
		)
	}
	cfg := config.Get()

	for _, f := range files {
		(*visited)++
		if *visited > allDebridFileTreeMaxItems {
			return fmt.Errorf(
				"alldebrid file tree exceeds %d items",
				allDebridFileTreeMaxItems,
			)
		}
		currentPath := strings.ReplaceAll(f.Name, `\`, "/")
		if parentPath != "" {
			currentPath = strings.TrimRight(parentPath, "/") + "/" + strings.TrimLeft(currentPath, "/")
		}

		if f.Elements != nil {
			if err := ad.collectFiles(
				torrentId,
				f.Elements,
				currentPath,
				depth+1,
				visited,
				collected,
			); err != nil {
				return err
			}
		} else {
			if err := cfg.IsFileAllowed(currentPath, f.Size); err != nil {
				continue
			}

			file := types.File{
				TorrentId: torrentId,
				Id:        allDebridFileID(torrentId, currentPath),
				Name:      f.Name,
				Size:      f.Size,
				Path:      currentPath,
				Link:      f.Link,
			}
			*collected = append(*collected, file)
		}
	}
	return nil
}

func allDebridFileID(torrentID, providerPath string) string {
	normalized := path.Clean(strings.ReplaceAll(providerPath, `\`, "/"))
	digest := sha256.Sum256([]byte(torrentID + "\x00" + normalized))
	return "path-" + hex.EncodeToString(digest[:])
}

func (ad *AllDebrid) GetTorrent(torrentId string) (*types.Torrent, error) {
	var res TorrentInfoResponse

	resp, err := ad.doRequest("/magnet/status", map[string]string{"id": torrentId}, &res)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("alldebrid API error: Status: %d", resp.StatusCode)
	}

	data, err := findMagnet(res.Data.Magnets, torrentId)
	if err != nil {
		return nil, err
	}
	status := getAlldebridStatus(data.StatusCode)
	name := data.Filename
	t := &types.Torrent{
		Id:               strconv.Itoa(data.Id),
		Name:             name,
		Status:           status,
		ProviderState:    strconv.Itoa(data.StatusCode),
		Filename:         name,
		OriginalFilename: name,
		Files:            make(map[string]types.File),
		InfoHash:         data.Hash,
		Debrid:           ad.config.Name,
		Added:            time.Unix(data.CompletionDate, 0),
	}
	t.Bytes = data.Size
	t.Seeders = data.Seeders
	if status == "downloaded" {
		t.Progress = 100
		files, err := ad.flattenFiles(t.Id, data.Files)
		if err != nil {
			return nil, err
		}
		t.Files = files
	} else {
		if data.Size > 0 {
			t.Progress = float64(data.Downloaded) / float64(data.Size) * 100
		}
		t.Speed = data.DownloadSpeed
	}
	return t, nil
}

func (ad *AllDebrid) UpdateTorrent(t *types.Torrent) error {
	_, err := ad.updateTorrent(t)
	return err
}

func (ad *AllDebrid) updateTorrent(t *types.Torrent) (int, error) {
	return ad.updateTorrentContext(context.Background(), t)
}

func (ad *AllDebrid) updateTorrentContext(ctx context.Context, t *types.Torrent) (int, error) {
	var res TorrentInfoResponse

	resp, err := ad.doRequestContext(ctx, "/magnet/status", map[string]string{"id": t.Id}, &res)
	if err != nil {
		return 0, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, fmt.Errorf("alldebrid API error: Status: %d", resp.StatusCode)
	}

	data, err := findMagnet(res.Data.Magnets, t.Id)
	if err != nil {
		return 0, err
	}
	status := getAlldebridStatus(data.StatusCode)
	name := data.Filename
	t.Name = name
	t.Status = status
	t.ProviderState = strconv.Itoa(data.StatusCode)
	t.Filename = name
	t.OriginalFilename = name
	t.Debrid = ad.config.Name
	t.Bytes = data.Size
	t.Seeders = data.Seeders
	if data.Hash != "" {
		t.InfoHash = data.Hash
	}
	t.Added = time.Unix(data.CompletionDate, 0)
	if status == "downloaded" {
		t.Progress = 100
		files, err := ad.flattenFiles(t.Id, data.Files)
		if err != nil {
			return data.StatusCode, err
		}
		t.Files = files
	} else {
		if data.Size > 0 {
			t.Progress = float64(data.Downloaded) / float64(data.Size) * 100
		}
		t.Speed = data.DownloadSpeed
	}
	return data.StatusCode, nil
}

func findMagnet(magnets Magnets, torrentId string) (magnetInfo, error) {
	for _, magnet := range magnets {
		if strconv.Itoa(magnet.Id) == torrentId {
			return magnet, nil
		}
	}

	return magnetInfo{}, customerror.TorrentNotFoundError
}

func (ad *AllDebrid) CheckStatus(torrent *types.Torrent) (*types.Torrent, error) {
	return ad.CheckStatusContext(context.Background(), torrent)
}

func (ad *AllDebrid) CheckStatusContext(ctx context.Context, torrent *types.Torrent) (*types.Torrent, error) {
	if err := ctx.Err(); err != nil {
		return torrent, err
	}
	if torrent == nil {
		return nil, fmt.Errorf("torrent is nil")
	}
	for {
		statusCode, err := ad.updateTorrentContext(ctx, torrent)

		if err != nil || torrent == nil {
			return torrent, err
		}
		switch torrent.Status {
		case types.TorrentStatusDownloaded:
			ad.clearMagnetRestart(torrent.Id)
			ad.logger.Info().Msgf("Torrent: %s downloaded", torrent.Name)
			return torrent, nil
		case types.TorrentStatusDownloading:
			if !torrent.DownloadUncached {
				return torrent, customerror.NewTorrentNotCachedError(torrent.Name)
			}
			return torrent, nil
		case types.TorrentStatusError:
			if statusCode == allDebridStatusNotDownloaded && !torrent.DownloadUncached {
				return torrent, customerror.NewTorrentNotCachedError(torrent.Name)
			}
			if statusCode == allDebridStatusNotDownloaded {
				return ad.recoverNotDownloadedTorrentContext(ctx, torrent)
			}
			ad.clearMagnetRestart(torrent.Id)
			return torrent, newAllDebridStatusError(torrent.Name, statusCode)
		default:
			return torrent, newAllDebridStatusError(torrent.Name, statusCode)
		}
	}
}

func (ad *AllDebrid) DeleteTorrent(torrentId string) error {
	resp, err := ad.doRequest("/magnet/delete", map[string]string{"id": torrentId}, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode == http.StatusNotFound {
		return customerror.TorrentNotFoundError
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("alldebrid API error: Status: %d", resp.StatusCode)
	}

	ad.logger.Info().Msgf("Torrent %s deleted from AD", torrentId)
	return nil
}

func (ad *AllDebrid) fetchDownloadLink(account *account.Account, id string, file *types.File) (types.DownloadLink, error) {
	return ad.fetchDownloadLinkContext(context.Background(), account, id, file)
}

func (ad *AllDebrid) fetchDownloadLinkContext(ctx context.Context, account *account.Account, id string, file *types.File) (types.DownloadLink, error) {
	var data DownloadLink

	resp, err := ad.doAccountRequestContext(ctx, account, "/link/unlock", map[string]string{"link": file.Link}, &data)
	if err != nil {
		return types.DownloadLink{}, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return types.DownloadLink{}, fmt.Errorf("alldebrid API error: Status: %d", resp.StatusCode)
	}

	if data.Error != nil {
		return types.DownloadLink{}, fmt.Errorf("error getting download link (code %s)", data.Error.Code)
	}
	link := data.Data.Link
	if link == "" {
		return types.DownloadLink{}, fmt.Errorf("download link is empty")
	}
	now := time.Now()
	dl := types.DownloadLink{
		Debrid:       ad.config.Name,
		Token:        account.Token,
		Link:         file.Link,
		DownloadLink: link,
		Id:           data.Data.Id,
		Size:         file.Size,
		Filename:     file.Name,
		Generated:    now,
		ExpiresAt:    now.Add(ad.autoExpiresLinksAfter),
	}
	return dl, nil
}

func (ad *AllDebrid) GetDownloadLink(id string, file *types.File) (types.DownloadLink, error) {
	return ad.accountsManager.GetDownloadLink(id, file, ad.fetchDownloadLink)
}

func (ad *AllDebrid) GetDownloadLinkContext(ctx context.Context, id string, file *types.File) (types.DownloadLink, error) {
	return ad.accountsManager.GetDownloadLinkContext(ctx, id, file, ad.fetchDownloadLinkContext)
}

func (ad *AllDebrid) GetTorrents() ([]*types.Torrent, error) {
	return ad.GetTorrentsContext(context.Background())
}

func (ad *AllDebrid) GetTorrentsContext(ctx context.Context) ([]*types.Torrent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	torrents := make([]*types.Torrent, 0)
	var res TorrentsListResponse

	resp, err := ad.doRequestContext(ctx, "/magnet/status", map[string]string{"status": "ready"}, &res)
	if err != nil {
		return torrents, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return torrents, fmt.Errorf("alldebrid API error: Status: %d", resp.StatusCode)
	}

	for _, magnet := range res.Data.Magnets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		t := &types.Torrent{
			Id:               strconv.Itoa(magnet.Id),
			Name:             magnet.Filename,
			Bytes:            magnet.Size,
			Status:           getAlldebridStatus(magnet.StatusCode),
			Filename:         magnet.Filename,
			OriginalFilename: magnet.Filename,
			Files:            make(map[string]types.File),
			InfoHash:         magnet.Hash,
			Debrid:           ad.config.Name,
			Added:            time.Unix(magnet.CompletionDate, 0),
		}
		files, err := ad.flattenFiles(t.Id, magnet.Files)
		if err != nil {
			return nil, err
		}
		t.Files = files
		torrents = append(torrents, t)
	}

	return torrents, nil
}

func (ad *AllDebrid) fetchDownloadLinks(account *account.Account) ([]types.DownloadLink, error) {
	// AllDebrid does not support fetching all download links
	downloadLinks := make([]types.DownloadLink, 0)
	return downloadLinks, nil
}

func (ad *AllDebrid) RefreshDownloadLinks() error {
	return ad.RefreshDownloadLinksContext(context.Background())
}

func (ad *AllDebrid) RefreshDownloadLinksContext(ctx context.Context) error {
	return ad.accountsManager.RefreshLinksContext(ctx, func(ctx context.Context, account *account.Account) ([]types.DownloadLink, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return ad.fetchDownloadLinks(account)
	})
}

func (ad *AllDebrid) CheckFile(ctx context.Context, _, link string) error {
	form := url.Values{}
	form.Add("link[]", link)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ad.Host+"/link/infos", strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := ad.repairClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("alldebrid API error: Status: %d", resp.StatusCode)
	}

	var data LinkInfosResponse
	if err := utils.DecodeJSONResponse(resp.Body, &data); err != nil {
		return err
	}
	if data.Status != "success" {
		code := "unknown"
		if data.Error != nil {
			code = data.Error.Code
		}
		return fmt.Errorf("alldebrid API error (code %s)", code)
	}
	if len(data.Data.Infos) != 1 {
		return fmt.Errorf("alldebrid API error: expected one link info, got %d", len(data.Data.Infos))
	}
	if linkErr := data.Data.Infos[0].Error; linkErr != nil {
		return fmt.Errorf("%w (code %s)", customerror.HosterUnavailableError, linkErr.Code)
	}
	return nil
}

func (ad *AllDebrid) GetAvailableSlots() (int, error) {
	// AllDebrid does not provide available slots info
	return config.DefaultAvailableSlots, nil
}

func (ad *AllDebrid) GetProfile() (*types.Profile, error) {
	return ad.GetProfileContext(context.Background())
}

func (ad *AllDebrid) GetProfileContext(ctx context.Context) (*types.Profile, error) {
	ad.profileMu.Lock()
	defer ad.profileMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ad.Profile != nil {
		cached := *ad.Profile
		return &cached, nil
	}
	var res UserProfileResponse

	resp, err := ad.doRequestContext(ctx, "/user", nil, &res)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("alldebrid API error: Status: %d", resp.StatusCode)
	}

	if res.Status != "success" {
		code := "unknown"
		if res.Error != nil {
			code = res.Error.Code
		}
		return nil, fmt.Errorf("error getting user profile (code %s)", code)
	}
	userData := res.Data.User
	expiration := time.Unix(userData.PremiumUntil, 0)
	profile := &types.Profile{
		Id:         1,
		Name:       ad.config.Name,
		Username:   userData.Username,
		Email:      userData.Email,
		Points:     userData.FidelityPoints,
		Premium:    userData.PremiumUntil,
		Expiration: expiration,
	}
	if userData.IsPremium {
		profile.Type = "premium"
	} else if userData.IsTrial {
		profile.Type = "trial"
	} else {
		profile.Type = "free"
	}
	stored := *profile
	ad.Profile = &stored
	return profile, nil
}

func (ad *AllDebrid) AccountManager() *account.Manager {
	return ad.accountsManager
}

func (ad *AllDebrid) syncAccount(account *account.Account) error {
	return nil
}

func (ad *AllDebrid) SyncAccounts() {
	_ = ad.SyncAccountsContext(context.Background())
}

func (ad *AllDebrid) SyncAccountsContext(ctx context.Context) error {
	return ad.accountsManager.SyncContext(ctx, func(ctx context.Context, account *account.Account) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return ad.syncAccount(account)
	})
}

func (ad *AllDebrid) deleteLink(account *account.Account, downloadLink types.DownloadLink) error {
	resp, err := ad.doAccountRequest(account, "/user/links/delete", map[string]string{"links": downloadLink.Link}, nil)
	if err != nil {
		return err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("alldebrid API error: Status: %d", resp.StatusCode)
	}
	return nil
}

func (ad *AllDebrid) DeleteLink(downloadLink types.DownloadLink) error {
	return ad.accountsManager.DeleteDownloadLink(downloadLink, ad.deleteLink)
}

// SpeedTest measures API latency and download speed using cached links
func (ad *AllDebrid) SpeedTest(ctx context.Context) types.SpeedTestResult {
	result := types.SpeedTestResult{
		Provider: ad.config.Name,
		TestedAt: time.Now(),
	}

	start := time.Now()
	resp, err := ad.doRequest("/user", nil, nil)
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
	current := ad.accountsManager.Current()
	if current == nil {
		return result // Latency only
	}

	link, found := current.GetRandomLink()
	if !found || link.DownloadLink == "" {
		return result // Latency only
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

func (ad *AllDebrid) SupportsCheck() bool {
	return true
}
