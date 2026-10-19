package arr

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/internal/request"
)

// RepairFile is an immutable Arr-side binding, captured before deletion.
// Like upstream's reacquire bindings, it includes every episode in a file.
type RepairFile struct {
	ID         int    `json:"id"`
	Path       string `json:"path"`
	Size       int64  `json:"size"`
	SeriesID   int    `json:"seriesId,omitempty"`
	MovieID    int    `json:"movieId,omitempty"`
	EpisodeIDs []int  `json:"episodeIds,omitempty"`
}

type RepairDownloadConfig struct {
	EnableCompletedDownloadHandling           bool `json:"enableCompletedDownloadHandling"`
	AutoRedownloadFailed                      bool `json:"autoRedownloadFailed"`
	AutoRedownloadFailedFromInteractiveSearch bool `json:"autoRedownloadFailedFromInteractiveSearch"`
}

// Keep the extra history data on this recovery-only response type. Existing
// queue/history readers do not need to allocate these maps for every record.
type RepairHistoryRecord struct {
	HistoryRecord
	Data map[string]string `json:"data,omitempty"`
}

type RepairCommand struct {
	ID     int       `json:"id"`
	Name   string    `json:"name"`
	Status string    `json:"status"`
	Queued time.Time `json:"queued"`
	Body   struct {
		EpisodeIDs []int `json:"episodeIds"`
		MovieIDs   []int `json:"movieIds"`
	} `json:"body"`
}

var repairClient = sync.OnceValue(func() *request.Client {
	return request.New(request.WithSingleAttempt(), request.WithTimeout(arrRequestTimeout), request.WithNoRedirects())
})

// ErrRepairRejected means Arr definitively rejected a request. Transport errors,
// redirects, malformed success bodies and 5xx responses have an unknown outcome.
var ErrRepairRejected = errors.New("arr rejected repair request")

func (a *Arr) repairRequest(ctx context.Context, method, endpoint string, payload, result any) (int, error) {
	resp, err := a.requestCtx(ctx, repairClient(), method, endpoint, payload, result)
	if result == nil {
		defer closeArrResponse(resp)
	}
	if err != nil {
		// Do not propagate URLs, API credentials, or response bodies into durable jobs.
		return 0, errors.New("arr repair request failed; outcome may be unknown")
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp.StatusCode, nil
	}
	if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusRequestTimeout {
		return resp.StatusCode, fmt.Errorf("%w (HTTP %d)", ErrRepairRejected, resp.StatusCode)
	}
	return resp.StatusCode, fmt.Errorf("arr repair request returned HTTP %d; outcome may be unknown", resp.StatusCode)
}

func (a *Arr) repairFileEndpoint(id int) (string, error) {
	if id <= 0 {
		return "", errors.New("repair file ID is missing")
	}
	switch a.Type {
	case Sonarr:
		return fmt.Sprintf("api/v3/episodefile/%d", id), nil
	case Radarr:
		return fmt.Sprintf("api/v3/moviefile/%d", id), nil
	default:
		return "", errors.New("repair supports Sonarr and Radarr only")
	}
}

func (a *Arr) ReadRepairFile(ctx context.Context, id int) (*RepairFile, error) {
	endpoint, err := a.repairFileEndpoint(id)
	if err != nil {
		return nil, err
	}
	var file RepairFile
	status, err := a.repairRequest(ctx, http.MethodGet, endpoint, nil, &file)
	if status == http.StatusNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if file.ID != id || file.Path == "" || file.Size <= 0 {
		return nil, errors.New("arr returned an incomplete repair file binding")
	}
	if a.Type == Radarr {
		if file.MovieID <= 0 {
			return nil, errors.New("arr repair file has no movie identity")
		}
		return &file, nil
	}
	if file.SeriesID <= 0 {
		return nil, errors.New("arr repair file has no series identity")
	}
	var episodes []episode
	_, err = a.repairRequest(ctx, http.MethodGet, fmt.Sprintf("api/v3/episode?seriesId=%d", file.SeriesID), nil, &episodes)
	if err != nil {
		return nil, err
	}
	file.EpisodeIDs = nil
	for _, ep := range episodes {
		if ep.EpisodeFileID == file.ID && ep.Id > 0 {
			file.EpisodeIDs = append(file.EpisodeIDs, ep.Id)
		}
	}
	slices.Sort(file.EpisodeIDs)
	file.EpisodeIDs = slices.Compact(file.EpisodeIDs)
	if len(file.EpisodeIDs) == 0 {
		return nil, errors.New("arr repair file has no episode identities")
	}
	return &file, nil
}

func SameRepairFile(a, b RepairFile) bool {
	return a.ID == b.ID && a.Path == b.Path && a.Size == b.Size &&
		a.SeriesID == b.SeriesID && a.MovieID == b.MovieID && slices.Equal(a.EpisodeIDs, b.EpisodeIDs)
}

// DeleteRepairFile never removes a local path. Arr owns its library file; a
// second local os.Remove could race with a newly imported replacement.
func (a *Arr) DeleteRepairFile(ctx context.Context, file RepairFile) error {
	current, err := a.ReadRepairFile(ctx, file.ID)
	if err != nil || current == nil {
		return err
	}
	if !SameRepairFile(file, *current) {
		return errors.New("arr repair file identity changed; deletion stopped")
	}
	endpoint, _ := a.repairFileEndpoint(file.ID)
	status, err := a.repairRequest(ctx, http.MethodDelete, endpoint, nil, nil)
	if status == http.StatusNotFound {
		return nil
	}
	return err
}

func (a *Arr) RepairConfig(ctx context.Context) (RepairDownloadConfig, error) {
	var cfg *RepairDownloadConfig
	_, err := a.repairRequest(ctx, http.MethodGet, "api/v3/config/downloadclient", nil, &cfg)
	if err != nil {
		return RepairDownloadConfig{}, err
	}
	if cfg == nil || !cfg.EnableCompletedDownloadHandling {
		return RepairDownloadConfig{}, errors.New("arr completed download handling is not enabled")
	}
	return *cfg, nil
}

// ExactRepairHistory must not substitute a newer grab for the same movie or
// episode. Bounded pagination fails closed if the server ignores its filter.
func (a *Arr) ExactRepairHistory(ctx context.Context, downloadID string, file RepairFile) (grab, failed *RepairHistoryRecord, err error) {
	if strings.TrimSpace(downloadID) == "" {
		return nil, nil, nil
	}
	query := url.Values{"downloadId": {downloadID}, "pageSize": {"100"}, "sortKey": {"date"}, "sortDirection": {"descending"}}
	for page := 1; page <= 20; page++ {
		query.Set("page", strconv.Itoa(page))
		var data *struct {
			TotalRecords *int                   `json:"totalRecords"`
			Records      *[]RepairHistoryRecord `json:"records"`
		}
		_, err = a.repairRequest(ctx, http.MethodGet, "api/v3/history?"+query.Encode(), nil, &data)
		if err != nil {
			return nil, nil, err
		}
		if data == nil || data.TotalRecords == nil || data.Records == nil ||
			*data.TotalRecords < 0 || len(*data.Records) > 100 ||
			(len(*data.Records) == 0 && (page-1)*100 < *data.TotalRecords) {
			return nil, nil, errors.New("arr returned incomplete repair history")
		}
		for _, record := range *data.Records {
			if record.ID <= 0 || !strings.EqualFold(record.DownloadID, downloadID) {
				continue
			}
			matches := a.Type == Radarr && record.MovieID == file.MovieID ||
				a.Type == Sonarr && record.SeriesID == file.SeriesID && slices.Contains(file.EpisodeIDs, record.EpisodeID)
			if !matches {
				continue
			}
			switch record.EventType {
			case "grabbed":
				if grab == nil {
					grab = &record
				}
			case "downloadFailed":
				if failed == nil {
					failed = &record
				}
			}
		}
		if page*100 >= *data.TotalRecords {
			return grab, failed, nil
		}
	}
	return nil, nil, errors.New("arr repair history exceeded safe lookup limit")
}

func (a *Arr) FailRepairHistory(ctx context.Context, id int) error {
	if id <= 0 {
		return errors.New("repair history ID is missing")
	}
	_, err := a.repairRequest(ctx, http.MethodPost, fmt.Sprintf("api/v3/history/failed/%d", id), nil, nil)
	return err
}

func (a *Arr) SearchRepairFile(ctx context.Context, file RepairFile) (int, error) {
	var payload any
	switch a.Type {
	case Sonarr:
		if len(file.EpisodeIDs) == 0 {
			return 0, errors.New("repair episode targets are missing")
		}
		payload = map[string]any{"name": "EpisodeSearch", "episodeIds": file.EpisodeIDs}
	case Radarr:
		if file.MovieID <= 0 {
			return 0, errors.New("repair movie target is missing")
		}
		payload = map[string]any{"name": "MoviesSearch", "movieIds": []int{file.MovieID}}
	default:
		return 0, errors.New("unsupported repair search")
	}
	var command RepairCommand
	_, err := a.repairRequest(ctx, http.MethodPost, "api/v3/command", payload, &command)
	if err != nil {
		return 0, err
	}
	if command.ID <= 0 {
		return 0, errors.New("arr search receipt is missing; outcome may be unknown")
	}
	return command.ID, nil
}

func (a *Arr) FindRepairSearch(ctx context.Context, file RepairFile, since time.Time) (int, error) {
	var commands *[]RepairCommand
	_, err := a.repairRequest(ctx, http.MethodGet, "api/v3/command", nil, &commands)
	if err != nil {
		return 0, err
	}
	if commands == nil {
		return 0, errors.New("arr command reconciliation response is incomplete")
	}
	for _, cmd := range *commands {
		if cmd.ID <= 0 || cmd.Queued.Before(since.Add(-2*time.Second)) {
			continue
		}
		matches := a.Type == Radarr && cmd.Name == "MoviesSearch" && slices.Equal(cmd.Body.MovieIDs, []int{file.MovieID}) ||
			a.Type == Sonarr && cmd.Name == "EpisodeSearch" && slices.Equal(cmd.Body.EpisodeIDs, file.EpisodeIDs)
		if matches {
			return cmd.ID, nil
		}
	}
	return 0, nil
}

// RepairReplacementImported confirms new Arr file identities, not playback or
// provider health. Retaining the original source is a separate cleanup policy.
func (a *Arr) RepairReplacementImported(ctx context.Context, old RepairFile) (bool, error) {
	if a.Type == Radarr {
		var movie struct {
			ID        int        `json:"id"`
			MovieFile RepairFile `json:"movieFile"`
		}
		_, err := a.repairRequest(ctx, http.MethodGet, fmt.Sprintf("api/v3/movie/%d", old.MovieID), nil, &movie)
		if err != nil {
			return false, err
		}
		if movie.ID != old.MovieID {
			return false, errors.New("repair movie identity changed")
		}
		if movie.MovieFile.ID > 0 && movie.MovieFile.MovieID != old.MovieID {
			return false, errors.New("imported repair file belongs to a different movie")
		}
		return movie.MovieFile.ID > 0 && movie.MovieFile.ID != old.ID && movie.MovieFile.Path != "" && movie.MovieFile.Size > 0, nil
	}
	if a.Type != Sonarr || len(old.EpisodeIDs) == 0 {
		return false, errors.New("repair episode identities are missing")
	}
	for _, id := range old.EpisodeIDs {
		var ep struct {
			ID       int `json:"id"`
			SeriesID int `json:"seriesId"`
			FileID   int `json:"episodeFileId"`
		}
		_, err := a.repairRequest(ctx, http.MethodGet, fmt.Sprintf("api/v3/episode/%d", id), nil, &ep)
		if err != nil {
			return false, err
		}
		if ep.ID != id || ep.SeriesID != old.SeriesID {
			return false, errors.New("repair episode identity changed")
		}
		if ep.FileID <= 0 || ep.FileID == old.ID {
			return false, nil
		}
		file, err := a.ReadRepairFile(ctx, ep.FileID)
		if err != nil {
			return false, err
		}
		if file == nil || file.SeriesID != old.SeriesID || !slices.Contains(file.EpisodeIDs, id) {
			return false, nil
		}
	}
	return true, nil
}
