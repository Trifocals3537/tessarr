package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"

	json "github.com/bytedance/sonic"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/Trifocals3537/tessarr/pkg/version"
	"github.com/go-chi/chi/v5"
	"github.com/sourcegraph/conc/iter"
)

type mountCacheCleaner interface {
	CleanupCache() (map[string]any, error)
}

type mountCachePurger interface {
	PurgeCache() (map[string]any, error)
}

func (s *Server) handleGetArrs(w http.ResponseWriter, r *http.Request) {
	utils.JSONResponse(w, s.manager.Arr().GetAll(), http.StatusOK)
}

func (s *Server) handleAddContent(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if err := utils.ParseMultipartFormBounded(
		w,
		r,
		utils.MaxImportRequestBytes,
		utils.MaxMultipartMemoryBytes,
	); err != nil {
		if utils.IsRequestTooLarge(err) {
			http.Error(w, "Import request is too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Invalid multipart import request", http.StatusBadRequest)
		return
	}
	if r.MultipartForm != nil {
		defer r.MultipartForm.RemoveAll()
	}
	if utils.MultipartFormPartCount(r.MultipartForm) > utils.MaxMultipartFormParts {
		http.Error(
			w,
			fmt.Sprintf("Import request exceeds the %d-part limit", utils.MaxMultipartFormParts),
			http.StatusRequestEntityTooLarge,
		)
		return
	}

	itemCount := countImportLines(r.FormValue("urls")) +
		countImportLines(r.FormValue("nzbURLs"))
	if r.MultipartForm != nil {
		itemCount += len(r.MultipartForm.File["files"])
		itemCount += len(r.MultipartForm.File["nzbFiles"])
	}
	if itemCount > utils.MaxImportItems {
		http.Error(
			w,
			fmt.Sprintf("Import request exceeds the %d-item limit", utils.MaxImportItems),
			http.StatusRequestEntityTooLarge,
		)
		return
	}

	arrName := r.FormValue("arr")
	action := r.FormValue("action")
	debridName := r.FormValue("debrid")
	callbackUrl := r.FormValue("callbackUrl")
	downloadFolder := r.FormValue("downloadFolder")
	if downloadFolder == "" {
		downloadFolder = config.Get().DownloadFolder
	}
	skipMultiSeason := r.FormValue("skipMultiSeason") == "true"

	dlUncached := r.FormValue("downloadUncached") == "true"
	var downloadUncached *bool
	if dlUncached {
		downloadUncached = &dlUncached
	}
	rmTrackerUrls := r.FormValue("rmTrackerUrls") == "true"

	// Check config setting - if always remove tracker URLs is enabled, force it to true
	cfg := config.Get()
	if cfg.AlwaysRmTrackerUrls {
		rmTrackerUrls = true
	}

	_arr := s.manager.Arr().Get(arrName)
	if _arr == nil {
		// These are not found in the config. They are throwaway arrs.
		_arr = arr.New(arrName, "", "", false, downloadUncached, "", "")
	}

	// Unified task type for all content types
	type addTask struct {
		taskType   string // "torrent", "nzbURL", "nzbFile"
		magnet     *utils.Magnet
		nzbContent []byte
		name       string
		source     string // redacted source for logs
		errMessage string
	}

	tasks := make([]addTask, 0, itemCount)
	var retainedBytes int64
	remainingBytes := func() int64 {
		return utils.MaxImportRequestBytes - retainedBytes
	}
	reserveBytes := func(size int64) error {
		if size < 0 || size > remainingBytes() {
			retainedBytes = utils.MaxImportRequestBytes
			return fmt.Errorf(
				"%w: aggregate maximum is %d bytes",
				utils.ErrContentTooLarge,
				utils.MaxImportRequestBytes,
			)
		}
		retainedBytes += size
		return nil
	}
	addErrorTask := func(message string) {
		tasks = append(tasks, addTask{taskType: "error", errMessage: message})
	}

	// Collect torrent URLs
	if urls := r.FormValue("urls"); urls != "" {
		for u := range strings.SplitSeq(urls, "\n") {
			if trimmed := strings.TrimSpace(u); trimmed != "" {
				safeURL := utils.RedactedURL(trimmed)
				maxBytes := min(utils.MaxMetadataFileBytes, remainingBytes())
				if maxBytes <= 0 {
					addErrorTask("Import metadata exceeds the aggregate byte limit")
					continue
				}

				var magnet *utils.Magnet
				var err error
				lowerURL := strings.ToLower(trimmed)
				if strings.HasPrefix(lowerURL, "http://") ||
					strings.HasPrefix(lowerURL, "https://") {
					_, torrentData, downloadErr := utils.DownloadFileBounded(
						ctx,
						trimmed,
						maxBytes,
					)
					downloaded := int64(len(torrentData))
					if downloaded == 0 && errors.Is(downloadErr, utils.ErrContentTooLarge) {
						// A declared Content-Length can be rejected before reading
						// the body. Pessimistically charge this URL its full
						// allowance so repeated oversized responses cannot bypass
						// the aggregate download budget.
						downloaded = maxBytes
					}
					if reserveErr := reserveBytes(downloaded); reserveErr != nil {
						addErrorTask("Import metadata exceeds the aggregate byte limit")
						continue
					}
					if downloadErr != nil {
						addErrorTask(fmt.Sprintf(
							"Failed to fetch torrent URL %s: %v",
							safeURL,
							downloadErr,
						))
						continue
					}
					magnet, err = utils.GetMagnetFromBytes(torrentData, rmTrackerUrls)
				} else {
					magnet, err = utils.GetMagnetFromURLContext(
						ctx,
						trimmed,
						rmTrackerUrls,
						maxBytes,
					)
				}
				if err != nil {
					addErrorTask(fmt.Sprintf("Failed to parse torrent URL %s: %v", safeURL, err))
					continue
				}
				retained := magnetRetainedBytes(magnet)
				if magnet.File != nil {
					// HTTP torrent bytes were already charged above.
					retained -= int64(len(magnet.File))
				}
				if err := reserveBytes(retained); err != nil {
					addErrorTask("Import metadata exceeds the aggregate byte limit")
					continue
				}
				tasks = append(tasks, addTask{
					taskType: "torrent",
					magnet:   magnet,
					source:   "torrent URL " + safeURL,
				})
			}
		}
	}

	// Collect torrent files
	if files := r.MultipartForm.File["files"]; len(files) > 0 {
		for _, fileHeader := range files {
			maxBytes := min(utils.MaxMetadataFileBytes, remainingBytes())
			if fileHeader.Size > maxBytes || maxBytes <= 0 {
				addErrorTask(fmt.Sprintf(
					"Torrent file %s exceeds the import byte limit",
					fileHeader.Filename,
				))
				continue
			}

			file, err := fileHeader.Open()
			if err != nil {
				addErrorTask(fmt.Sprintf("Failed to open torrent file %s", fileHeader.Filename))
				continue
			}

			magnet, parseErr := utils.GetMagnetFromFileBounded(
				file,
				fileHeader.Filename,
				rmTrackerUrls,
				maxBytes,
			)
			closeErr := file.Close()
			if parseErr == nil && closeErr != nil {
				parseErr = closeErr
			}
			if parseErr != nil {
				addErrorTask(fmt.Sprintf(
					"Failed to parse torrent file %s: %v",
					fileHeader.Filename,
					parseErr,
				))
				continue
			}
			if err := reserveBytes(magnetRetainedBytes(magnet)); err != nil {
				addErrorTask("Import metadata exceeds the aggregate byte limit")
				continue
			}
			tasks = append(tasks, addTask{
				taskType: "torrent",
				magnet:   magnet,
				source:   "torrent file " + fileHeader.Filename,
				name:     fileHeader.Filename,
			})
		}
	}

	// Collect NZB URLs
	if nzbURLs := r.FormValue("nzbURLs"); nzbURLs != "" {
		for u := range strings.SplitSeq(nzbURLs, "\n") {
			if trimmed := strings.TrimSpace(u); trimmed != "" {
				safeURL := utils.RedactedURL(trimmed)
				maxBytes := min(utils.MaxMetadataFileBytes, remainingBytes())
				if maxBytes <= 0 {
					addErrorTask("Import metadata exceeds the aggregate byte limit")
					continue
				}
				filename, content, err := utils.DownloadFileBounded(
					ctx,
					trimmed,
					maxBytes,
					utils.WithHeader("User-Agent", s.nzbUserAgent),
				)
				downloaded := int64(len(content))
				if downloaded == 0 && errors.Is(err, utils.ErrContentTooLarge) {
					downloaded = maxBytes
				}
				if reserveErr := reserveBytes(downloaded); reserveErr != nil {
					addErrorTask("Import metadata exceeds the aggregate byte limit")
					continue
				}
				if err != nil {
					addErrorTask(fmt.Sprintf("Failed to fetch NZB from URL %s: %v", safeURL, err))
					continue
				}
				tasks = append(tasks, addTask{
					taskType:   "nzb",
					nzbContent: content,
					name:       filename,
					source:     "NZB URL " + safeURL,
				})
			}
		}
	}

	// Collect NZB files
	if nzbFiles := r.MultipartForm.File["nzbFiles"]; len(nzbFiles) > 0 {
		for _, fileHeader := range nzbFiles {
			maxBytes := min(utils.MaxMetadataFileBytes, remainingBytes())
			if fileHeader.Size > maxBytes || maxBytes <= 0 {
				addErrorTask(fmt.Sprintf(
					"NZB file %s exceeds the import byte limit",
					fileHeader.Filename,
				))
				continue
			}
			content, err := getNZBContentFromFile(fileHeader, maxBytes)
			if err != nil {
				addErrorTask(fmt.Sprintf(
					"Failed to read NZB file %s: %v",
					fileHeader.Filename,
					err,
				))
				continue
			}
			if err := reserveBytes(int64(len(content))); err != nil {
				addErrorTask("Import metadata exceeds the aggregate byte limit")
				continue
			}
			tasks = append(tasks, addTask{
				taskType:   "nzb",
				nzbContent: content,
				source:     "NZB file " + fileHeader.Filename,
				name:       fileHeader.Filename,
			})
		}
	}

	if len(tasks) == 0 {
		utils.JSONResponse(w, []*manager.ImportRequest{}, http.StatusOK)
		return
	}

	// Parse all tasks in parallel using iter.Map
	mapper := iter.Mapper[addTask, *manager.ImportRequest]{
		MaxGoroutines: min(len(tasks), 10),
	}

	results := mapper.Map(tasks, func(task *addTask) *manager.ImportRequest {
		switch task.taskType {
		case "error":
			// Task already failed during collection phase
			return &manager.ImportRequest{
				Status: "error",
				Error:  task.errMessage,
			}

		case "torrent":
			importReq := manager.NewTorrentRequest(debridName, downloadFolder, task.magnet, _arr, config.DownloadAction(action), downloadUncached, callbackUrl, manager.ImportTypeAPI, skipMultiSeason)
			if err := s.manager.AddNewTorrent(ctx, importReq); err != nil {
				s.logger.Error().Err(err).Str("source", task.source).Msg("Failed to add torrent")
				importReq.Error = err.Error()
				importReq.Status = "error"
			}
			return importReq

		case "nzb":
			importReq := manager.NewNZBRequest(task.name, downloadFolder, task.nzbContent, _arr, config.DownloadAction(action), callbackUrl, manager.ImportTypeAPI, skipMultiSeason)
			nzoID, err := s.manager.AddNewNZB(ctx, importReq)
			if err != nil {
				s.logger.Error().Err(err).Str("source", task.source).Msg("Failed to add NZB")
				importReq.Error = err.Error()
				importReq.Status = "error"
			}
			importReq.Id = nzoID
			return importReq

		default:
			return nil
		}
	})

	// Filter out nil results
	filtered := make([]*manager.ImportRequest, 0, len(results))
	for _, r := range results {
		if r != nil {
			filtered = append(filtered, r)
		}
	}

	utils.JSONResponse(w, filtered, http.StatusOK)
}

func getNZBContentFromFile(fileHeader *multipart.FileHeader, maxBytes int64) ([]byte, error) {
	file, err := fileHeader.Open()
	if err != nil {
		return nil, err
	}
	defer file.Close()

	nzbContent, err := utils.ReadAllLimited(file, maxBytes)
	if err != nil {
		return nil, err
	}
	return nzbContent, nil
}

func countImportLines(values string) int {
	count := 0
	for value := range strings.SplitSeq(values, "\n") {
		if strings.TrimSpace(value) != "" {
			count++
		}
	}
	return count
}

func magnetRetainedBytes(magnet *utils.Magnet) int64 {
	if magnet == nil {
		return 0
	}
	return int64(len(magnet.File)) + int64(len(magnet.Link)) + int64(len(magnet.Name))
}

func (s *Server) handleGetVersion(w http.ResponseWriter, r *http.Request) {
	v := version.GetInfo()
	utils.JSONResponse(w, v, http.StatusOK)
}

func (s *Server) handleLiveness(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	utils.JSONResponse(w, map[string]string{"status": "live"}, http.StatusOK)
}

func (s *Server) handleReadiness(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	status := s.manager.ReadinessStatus()
	code := http.StatusOK
	if !status.Ready {
		code = http.StatusServiceUnavailable
		w.Header().Set("Retry-After", "5")
	}
	utils.JSONResponse(w, status, code)
}

func (s *Server) handleRunMountCacheCleanup(w http.ResponseWriter, r *http.Request) {
	mountMgr := s.manager.MountManager()
	if mountMgr == nil || !mountMgr.IsReady() {
		http.Error(w, "Mount is not ready", http.StatusServiceUnavailable)
		return
	}

	cleaner, ok := mountMgr.(mountCacheCleaner)
	if !ok {
		http.Error(w, "Manual cache cleanup is only available for DFS mounts", http.StatusBadRequest)
		return
	}

	cleanupStats, err := cleaner.CleanupCache()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to run mount cache cleanup")
		http.Error(w, "Failed to run mount cache cleanup", http.StatusInternalServerError)
		return
	}

	if s.stats != nil {
		s.stats.RequestRefresh()
	}

	utils.JSONResponse(w, map[string]any{
		"status": "success",
		"cache":  cleanupStats,
	}, http.StatusOK)
}

func (s *Server) handlePurgeMountCache(w http.ResponseWriter, r *http.Request) {
	mountMgr := s.manager.MountManager()
	if mountMgr == nil || !mountMgr.IsReady() {
		http.Error(w, "Mount is not ready", http.StatusServiceUnavailable)
		return
	}

	purger, ok := mountMgr.(mountCachePurger)
	if !ok {
		http.Error(w, "Cache purge is only available for DFS mounts", http.StatusBadRequest)
		return
	}

	purgeStats, err := purger.PurgeCache()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to purge mount cache")
		http.Error(w, "Failed to purge mount cache", http.StatusInternalServerError)
		return
	}

	if s.stats != nil {
		s.stats.RequestRefresh()
	}

	utils.JSONResponse(w, map[string]any{
		"status": "success",
		"cache":  purgeStats,
	}, http.StatusOK)
}

func (s *Server) handleGetTorrents(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters for server-side filtering, sorting, and pagination
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}

	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit < 1 || limit > 100 {
		limit = 20
	}

	search := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("search")))
	category := strings.TrimSpace(r.URL.Query().Get("category"))
	state := strings.TrimSpace(r.URL.Query().Get("state"))
	sortBy := strings.TrimSpace(r.URL.Query().Get("sort_by"))
	sortOrder := strings.TrimSpace(r.URL.Query().Get("sort_order"))

	if sortBy == "" {
		sortBy = "added_on"
	}
	if sortOrder == "" {
		sortOrder = "desc"
	}

	// GetReader all torrents
	allTorrents := s.manager.Queue().ListFilter("", config.ProtocolAll, "", nil, "added_on", false)
	for _, t := range allTorrents {
		t.Sanitize()
	}

	// Apply filters
	filteredTorrents := make([]*storage.Entry, 0)
	for _, t := range allTorrents {
		// Search filter - search in name and hash
		if search != "" {
			searchIn := strings.ToLower(t.Name + " " + t.InfoHash)
			if !strings.Contains(searchIn, search) {
				continue
			}
		}

		// Category filter
		if category != "" && t.Category != category {
			continue
		}

		// State filter
		if state != "" && t.State != storage.TorrentState(state) {
			continue
		}

		filteredTorrents = append(filteredTorrents, t)
	}

	// Apply sorting
	sortQueuedTorrents(filteredTorrents, sortBy, sortOrder)

	// Calculate pagination
	total := len(filteredTorrents)
	totalPages := (total + limit - 1) / limit
	offset := (page - 1) * limit

	// Apply pagination
	var paginatedTorrents []*storage.Entry
	if offset < total {
		end := min(offset+limit, total)
		paginatedTorrents = filteredTorrents[offset:end]
	} else {
		paginatedTorrents = []*storage.Entry{}
	}

	// GetReader unique categories
	categorySet := make(map[string]bool)
	for _, t := range allTorrents {
		if t.Category != "" {
			categorySet[t.Category] = true
		}
	}

	categories := make([]string, 0, len(categorySet))
	for c := range categorySet {
		categories = append(categories, c)
	}

	utils.JSONResponse(w, map[string]any{
		"torrents":    paginatedTorrents,
		"total":       total,
		"page":        page,
		"limit":       limit,
		"total_pages": totalPages,
		"has_prev":    page > 1,
		"has_next":    page < totalPages,
		"categories":  categories,
	}, http.StatusOK)
}

// sortQueuedTorrents sorts torrents based on the given field and order
func sortQueuedTorrents(torrents []*storage.Entry, sortBy, sortOrder string) {
	if len(torrents) == 0 {
		return
	}

	less := func(i, j int) bool {
		var result bool
		switch sortBy {
		case "name":
			result = strings.ToLower(torrents[i].Name) < strings.ToLower(torrents[j].Name)
		case "size":
			result = torrents[i].Size < torrents[j].Size
		case "added_on":
			result = torrents[i].AddedOn.Before(torrents[j].AddedOn)
		case "progress":
			result = torrents[i].Progress < torrents[j].Progress
		case "category":
			result = strings.ToLower(torrents[i].Category) < strings.ToLower(torrents[j].Category)
		case "state":
			result = torrents[i].State < torrents[j].State
		default:
			result = torrents[i].AddedOn.Before(torrents[j].AddedOn)
		}

		if sortOrder == "desc" {
			return !result
		}
		return result
	}

	sort.Slice(torrents, less)
}

func (s *Server) handleDeleteTorrent(w http.ResponseWriter, r *http.Request) {
	hash := chi.URLParam(r, "hash")
	removeFromDebrid := r.URL.Query().Get("removeFromDebrid") == "true"
	if hash == "" {
		http.Error(w, "No hash provided", http.StatusBadRequest)
		return
	}
	if err := s.manager.DeleteQueueEntry(hash, removeFromDebrid); err != nil {
		s.logger.Error().Err(err).Str("hash", hash).Msg("Failed to delete entry from queue")
		http.Error(w, "Failed to delete entry from queue", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDeleteTorrents(w http.ResponseWriter, r *http.Request) {
	hashesStr := r.URL.Query().Get("hashes")
	removeFromDebrid := r.URL.Query().Get("removeFromDebrid") == "true"
	if hashesStr == "" {
		http.Error(w, "No hashes provided", http.StatusBadRequest)
		return
	}
	hashes := strings.Split(hashesStr, ",")
	if err := s.manager.DeleteQueueEntries(hashes, removeFromDebrid); err != nil {
		s.logger.Error().Err(err).Msg("Failed to delete torrents")
		http.Error(w, "Failed to delete torrents", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleGetConfig(w http.ResponseWriter, r *http.Request) {
	arrStorage := s.manager.Arr()
	cfg := config.Get()
	response, err := newConfigResponse(cfg, arrStorage.SyncToConfig())
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to prepare configuration response")
		http.Error(w, "Failed to prepare configuration response", http.StatusInternalServerError)
		return
	}

	utils.JSONResponse(w, response, http.StatusOK)
}

func (s *Server) handleUpdateConfig(w http.ResponseWriter, r *http.Request) {
	s.configMu.Lock()
	defer s.configMu.Unlock()

	if s.restartPending {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "A configuration restart is already in progress", http.StatusConflict)
		return
	}

	body, err := readConfigRequest(r.Body)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to decode config update request")
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if r.Method == http.MethodPost {
		w.Header().Set("Deprecation", "true")
		w.Header().Set("Warning", `299 - "POST /api/config is deprecated; use PUT for replacement or PATCH for partial updates"`)
	}
	var clientErr error
	result, err := config.Update(func(draft *config.Config) error {
		newConfig, decodeErr := decodeConfigUpdate(r.Method, bytes.NewReader(body), draft)
		if decodeErr != nil {
			clientErr = decodeErr
			return decodeErr
		}
		if restoreErr := restoreConfigSecrets(newConfig, draft); restoreErr != nil {
			clientErr = restoreErr
			return restoreErr
		}

		if newConfig.BindAddress == "" {
			newConfig.BindAddress = config.DefaultBindAddress
		}
		if newConfig.Port == "" {
			newConfig.Port = config.DefaultPort
		}

		// These fields are managed outside the settings form. Preserve the
		// latest snapshot values so a concurrent auth change cannot be lost.
		newConfig.Auth = draft.Auth
		newConfig.UseAuth = draft.UseAuth
		newConfig.EnableWebdavAuth = draft.EnableWebdavAuth

		validArrs := make([]config.Arr, 0, len(newConfig.Arrs))
		for _, a := range newConfig.Arrs {
			if a.Name != "" && a.Host != "" && a.Token != "" {
				validArrs = append(validArrs, a)
			}
		}
		newConfig.Arrs = validArrs

		if validateErr := validateConfigUpdate(newConfig); validateErr != nil {
			clientErr = validateErr
			return validateErr
		}
		*draft = *newConfig
		return nil
	})
	if err != nil {
		if clientErr != nil {
			s.logger.Warn().Err(clientErr).Msg("Rejected configuration update")
			http.Error(w, "Invalid configuration: "+clientErr.Error(), http.StatusBadRequest)
			return
		}
		s.logger.Error().Err(err).Msg("Failed to save config")
		http.Error(w, "Error saving config: "+err.Error(), http.StatusInternalServerError)
		return
	}

	strmChanged := result.Previous.AppURL != result.Desired.AppURL ||
		!reflect.DeepEqual(result.Previous.Strm, result.Desired.Strm)

	if result.RestartRequired {
		// Reject any follow-up update until the new server instance is running.
		// Otherwise a request in the restart window could merge against the old
		// in-memory config and accidentally overwrite the just-saved document.
		s.restartPending = true
		go s.Restart()
	} else {
		// Only update dependent live state after the snapshot is durable and
		// published. Cold updates are applied by the replacement process.
		if err := logger.SetLevel(result.Active.LogLevel); err != nil {
			s.logger.Error().Err(err).Msg("Failed to apply log level after live update")
		}
		s.manager.Arr().SyncFromConfig(result.Active.Arrs)
		if strmChanged && s.manager.Strm() != nil {
			s.manager.Strm().SweepAsync("config_change")
		}
		// Reschedule/reapply the repair sweep if its settings changed.
		if svc := s.manager.Repair(); svc != nil {
			if err := svc.ApplyConfig(); err != nil {
				s.logger.Warn().Err(err).Msg("Failed to apply repair config after live update")
			}
		}
	}

	utils.JSONResponse(w, map[string]any{"status": "success", "restarted": result.RestartRequired}, http.StatusOK)
}

func (s *Server) handleStrmRegenerate(w http.ResponseWriter, _ *http.Request) {
	if !config.Get().Strm.Active() {
		http.Error(w, "STRM is disabled or has no export path", http.StatusBadRequest)
		return
	}
	if s.manager.Strm() == nil || !s.manager.Strm().SweepAsync("manual_regenerate") {
		http.Error(w, "STRM regeneration could not be scheduled", http.StatusServiceUnavailable)
		return
	}
	utils.JSONResponse(w, map[string]string{"status": "started"}, http.StatusAccepted)
}

func validateConfigUpdate(candidate *config.Config) error {
	if err := candidate.Validate(); err != nil {
		return err
	}
	if err := candidate.ValidateDeployment(); err != nil {
		return fmt.Errorf("deployment safety check: %w", err)
	}
	return nil
}

func (s *Server) handleGetRepairConfig(w http.ResponseWriter, r *http.Request) {
	utils.JSONResponse(w, config.Get().Repair, http.StatusOK)
}

func (s *Server) handleUpdateRepairConfig(w http.ResponseWriter, r *http.Request) {
	var req config.RepairConfig
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Enabled {
		if strings.TrimSpace(req.Schedule) == "" {
			http.Error(w, "Schedule is required when repair is enabled", http.StatusBadRequest)
			return
		}
		if _, err := utils.ConvertToJobDef(req.Schedule); err != nil {
			http.Error(w, fmt.Sprintf("Invalid schedule: %v", err), http.StatusBadRequest)
			return
		}
		if req.RecheckInterval != "" {
			if _, err := utils.ParseDuration(req.RecheckInterval); err != nil {
				http.Error(w, fmt.Sprintf("Invalid recheck_interval: %v", err), http.StatusBadRequest)
				return
			}
		}
		if req.Source != "" && req.Source != config.RepairSourceArr && req.Source != config.RepairSourceManaged {
			http.Error(w, "Invalid source (must be 'arr' or 'managed')", http.StatusBadRequest)
			return
		}
	}
	if req.NNTPConnectionPercent < 0 || req.NNTPConnectionPercent > 100 {
		http.Error(w, "Invalid nntp_connection_percent (must be between 0 and 100)", http.StatusBadRequest)
		return
	}

	result, err := config.Update(func(draft *config.Config) error {
		draft.Repair = req
		return nil
	})
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to save repair config")
		http.Error(w, "Failed to save config: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if result.RestartRequired {
		s.logger.Error().Msg("Repair-only update unexpectedly requires restart")
		http.Error(w, "Repair settings were saved but require a restart", http.StatusConflict)
		return
	}

	if svc := s.manager.Repair(); svc != nil {
		if err := svc.ApplyConfig(); err != nil {
			s.logger.Warn().Err(err).Msg("Failed to apply repair config")
			http.Error(w, "Saved, but failed to apply: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	utils.JSONResponse(w, result.Active.Repair, http.StatusOK)
}

func (s *Server) handleRepairStatus(w http.ResponseWriter, r *http.Request) {
	svc := s.manager.Repair()
	if svc == nil {
		utils.JSONResponse(w, manager.RepairStatus{}, http.StatusOK)
		return
	}
	utils.JSONResponse(w, svc.Status(), http.StatusOK)
}

// normalizeContentVerificationOptions makes a deep verification run safe by
// default. It is detect-only unless the caller explicitly asks for repair, and
// an omitted protocol narrows to NZB instead of spending time probing torrents
// that cannot use the content verifier.
func normalizeContentVerificationOptions(verifyContent bool, autoRepair *bool, protocolScope string) (*bool, string, error) {
	if !verifyContent {
		return autoRepair, protocolScope, nil
	}
	if protocolScope == "torrent" {
		return autoRepair, protocolScope, errors.New("verify_content requires protocol nzb or all")
	}
	if protocolScope == "" {
		protocolScope = "nzb"
	}
	if autoRepair == nil {
		detectOnly := false
		autoRepair = &detectOnly
	}
	return autoRepair, protocolScope, nil
}

func (s *Server) handleRunRepair(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IgnoreLastChecked bool   `json:"ignore_last_checked,omitempty"`
		Force             bool   `json:"force,omitempty"`
		AutoRepair        *bool  `json:"auto_repair,omitempty"`
		UnrestrictLink    bool   `json:"unrestrict_link,omitempty"`
		VerifyContent     bool   `json:"verify_content,omitempty"`
		Protocol          string `json:"protocol,omitempty"`
	}
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	ignoreLastChecked := req.IgnoreLastChecked || req.Force
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("ignore_last_checked"))) {
	case "1", "true", "yes", "on":
		ignoreLastChecked = true
	}
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("force"))) {
	case "1", "true", "yes", "on":
		ignoreLastChecked = true
	}
	autoRepair := req.AutoRepair
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("auto_repair"))) {
	case "1", "true", "yes", "on":
		v := true
		autoRepair = &v
	case "0", "false", "no", "off":
		v := false
		autoRepair = &v
	}
	unrestrictLink := req.UnrestrictLink
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("unrestrict_link"))) {
	case "1", "true", "yes", "on":
		unrestrictLink = true
	case "0", "false", "no", "off":
		unrestrictLink = false
	}
	verifyContent := req.VerifyContent
	switch strings.ToLower(strings.TrimSpace(r.URL.Query().Get("verify_content"))) {
	case "1", "true", "yes", "on":
		verifyContent = true
	case "0", "false", "no", "off":
		verifyContent = false
	}
	protocolScope := strings.ToLower(strings.TrimSpace(req.Protocol))
	if queryProtocol := strings.TrimSpace(r.URL.Query().Get("protocol")); queryProtocol != "" {
		protocolScope = strings.ToLower(queryProtocol)
	}
	switch protocolScope {
	case "", "all", "both", "torrent", "nzb":
		if protocolScope == "both" {
			protocolScope = "all"
		}
	default:
		http.Error(w, "Invalid protocol; expected all, torrent, or nzb", http.StatusBadRequest)
		return
	}
	autoRepair, protocolScope, err := normalizeContentVerificationOptions(verifyContent, autoRepair, protocolScope)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	id, err := svc.RunNow(manager.RepairRunOptions{
		IgnoreLastChecked: ignoreLastChecked,
		AutoRepair:        autoRepair,
		UnrestrictLink:    unrestrictLink,
		VerifyContent:     verifyContent,
		ProtocolScope:     protocolScope,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusConflict)
		return
	}
	utils.JSONResponse(w, map[string]string{"run_id": id}, http.StatusOK)
}

func (s *Server) handleStopRepair(w http.ResponseWriter, r *http.Request) {
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	if err := svc.StopRun(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleListRepairRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := s.manager.Storage().ListRepairRuns()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, runs, http.StatusOK)
}

func (s *Server) handleGetRepairRun(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		http.Error(w, "No run ID provided", http.StatusBadRequest)
		return
	}
	run, err := s.manager.Storage().GetRepairRun(id)
	if err != nil {
		http.Error(w, "Run not found", http.StatusNotFound)
		return
	}
	utils.JSONResponse(w, run, http.StatusOK)
}

func (s *Server) handleClearRepairRuns(w http.ResponseWriter, r *http.Request) {
	if err := s.manager.Storage().ClearRepairRuns(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleListEntryHealth(w http.ResponseWriter, r *http.Request) {
	statusFilter := strings.TrimSpace(r.URL.Query().Get("status"))
	out := make([]*storage.EntryHealth, 0)
	_ = s.manager.Storage().ForEachEntryHealth(func(state *storage.EntryHealth) error {
		if statusFilter != "" && string(state.Status) != statusFilter {
			return nil
		}
		out = append(out, state)
		return nil
	})
	sort.Slice(out, func(i, j int) bool {
		return out[i].EntryName < out[j].EntryName
	})
	utils.JSONResponse(w, out, http.StatusOK)
}

func (s *Server) handleGetEntryHealth(w http.ResponseWriter, r *http.Request) {
	name := utils.PathUnescape(chi.URLParam(r, "name"))
	if name == "" {
		http.Error(w, "No entry name provided", http.StatusBadRequest)
		return
	}
	state, err := s.manager.Storage().GetEntryHealth(name)
	if err != nil {
		http.Error(w, "Entry health not found", http.StatusNotFound)
		return
	}
	utils.JSONResponse(w, state, http.StatusOK)
}

func (s *Server) handleRecheckMedia(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Arr     string `json:"arr"`
		MediaID string `json:"media_id"`
		Fix     bool   `json:"fix"`
	}
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.MediaID) == "" {
		http.Error(w, "media_id is required", http.StatusBadRequest)
		return
	}
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	run, err := svc.RecheckMedia(s.manager.Context(), strings.TrimSpace(req.Arr), strings.TrimSpace(req.MediaID), req.Fix)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already running") {
			status = http.StatusConflict
		}
		// Returning the run record (when present) gives the caller the
		// failure detail captured in storage as well as the message.
		if run != nil {
			utils.JSONResponse(w, map[string]any{
				"error": err.Error(),
				"run":   run,
			}, status)
			return
		}
		http.Error(w, err.Error(), status)
		return
	}
	utils.JSONResponse(w, run, http.StatusOK)
}

func (s *Server) handleRecheckEntry(w http.ResponseWriter, r *http.Request) {
	name := utils.PathUnescape(chi.URLParam(r, "name"))
	if name == "" {
		http.Error(w, "No entry name provided", http.StatusBadRequest)
		return
	}
	fix := r.URL.Query().Get("fix") == "true"
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	state, err := svc.RecheckEntry(s.manager.Context(), name, fix)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	utils.JSONResponse(w, state, http.StatusOK)
}

// handleFixBroken kicks off the Arr delete + re-search pass on currently
// broken entries. Body: {"names": ["...", ...]}. Empty/missing names ⇒ fix
// every broken entry in storage.
func (s *Server) handleFixBroken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Names []string `json:"names,omitempty"`
	}
	// Body is optional; ignore decode errors for empty / missing bodies.
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	run, err := svc.FixBroken(s.manager.Context(), req.Names)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already running") {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	utils.JSONResponse(w, run, http.StatusOK)
}

// handleClearBroken clears currently broken files without asking the Arr to
// re-search for replacements. Body: {"names": ["...", ...]}. Empty/missing
// names ⇒ clear every broken entry in storage.
func (s *Server) handleClearBroken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Names []string `json:"names,omitempty"`
	}
	if r.Body != nil && r.ContentLength != 0 {
		if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
			return
		}
	}
	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	run, err := svc.ClearBroken(s.manager.Context(), req.Names)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already running") {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	utils.JSONResponse(w, run, http.StatusOK)
}

func (s *Server) handleClearRepairState(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Statuses []string `json:"statuses"`
	}
	if err := json.ConfigDefault.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	statuses := make([]storage.HealthStatus, 0, len(req.Statuses))
	for _, raw := range req.Statuses {
		status, ok := parseRepairHealthStatus(raw)
		if !ok {
			http.Error(w, "Invalid repair health status: "+raw, http.StatusBadRequest)
			return
		}
		statuses = append(statuses, status)
	}
	if len(statuses) == 0 {
		http.Error(w, "At least one status is required", http.StatusBadRequest)
		return
	}

	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}
	result, err := svc.ClearStates(statuses)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already running") {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}
	utils.JSONResponse(w, result, http.StatusOK)
}

func parseRepairHealthStatus(raw string) (storage.HealthStatus, bool) {
	switch storage.HealthStatus(strings.ToLower(strings.TrimSpace(raw))) {
	case storage.HealthHealthy:
		return storage.HealthHealthy, true
	case storage.HealthBroken:
		return storage.HealthBroken, true
	case storage.HealthRepairing:
		return storage.HealthRepairing, true
	case storage.HealthStale:
		return storage.HealthStale, true
	case storage.HealthUnknown:
		return storage.HealthUnknown, true
	case storage.HealthUnsupported:
		return storage.HealthUnsupported, true
	default:
		return "", false
	}
}

func (s *Server) handleRefreshAPIToken(w http.ResponseWriter, _ *http.Request) {
	token, err := s.refreshAPIToken()
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to refresh API token")
		http.Error(w, "Failed to refresh token: "+err.Error(), http.StatusInternalServerError)
		return
	}

	utils.JSONResponse(w, map[string]any{
		"token":   token,
		"message": "API token refreshed successfully",
	}, http.StatusOK)
}

func (s *Server) handleUpdateAuth(w http.ResponseWriter, r *http.Request) {
	s.configMu.Lock()
	defer s.configMu.Unlock()
	if s.restartPending {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "A configuration restart is already in progress", http.StatusConflict)
		return
	}

	var req struct {
		Username        string `json:"username"`
		Password        string `json:"password"`
		ConfirmPassword string `json:"confirm_password"`
	}
	if err := utils.DecodeJSONRequestBounded(
		w,
		r,
		&req,
		utils.MaxControlRequestBytes,
	); err != nil {
		if utils.IsRequestTooLarge(err) {
			http.Error(w, "Request is too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Check if trying to disable authentication (both empty)
	if req.Username == "" && req.Password == "" {
		authAlreadyDisabledErr := errors.New("authentication is already disabled")
		authRemoteDisableErr := errors.New("authentication cannot be disabled on a non-loopback listener")
		_, err := config.Update(func(draft *config.Config) error {
			if !draft.UseAuth {
				return authAlreadyDisabledErr
			}
			if !isLoopbackBindAddress(draft.BindAddress) {
				return authRemoteDisableErr
			}
			if draft.Auth == nil {
				return errors.New("authentication is not configured")
			}
			draft.UseAuth = false
			draft.Auth.Username = ""
			draft.Auth.Password = ""
			draft.Auth.SessionVersion++
			if draft.Auth.SessionVersion == 0 {
				draft.Auth.SessionVersion = 1
			}
			return nil
		})
		if err != nil {
			if errors.Is(err, authAlreadyDisabledErr) {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if errors.Is(err, authRemoteDisableErr) {
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
			s.logger.Error().Err(err).Msg("Failed to save config")
			http.Error(w, "Failed to save authentication settings", http.StatusInternalServerError)
			return
		}

		utils.JSONResponse(w, map[string]string{
			"message": "Authentication disabled successfully",
		}, http.StatusOK)
		return
	}

	if req.Password != req.ConfirmPassword {
		http.Error(w, "Passwords do not match", http.StatusBadRequest)
		return
	}

	if err := config.ValidateAuthCredentials(req.Username, req.Password); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	remoteAuthBootstrapErr := errors.New(
		"authentication must be enabled from the host with --set-auth before remote access",
	)
	_, err := config.Update(func(draft *config.Config) error {
		if !draft.UseAuth && !isLoopbackBindAddress(draft.BindAddress) {
			return remoteAuthBootstrapErr
		}
		return draft.ApplyAuthCredentials(req.Username, req.Password)
	})
	if err != nil {
		if errors.Is(err, remoteAuthBootstrapErr) {
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}
		s.logger.Error().Err(err).Msg("Failed to save authentication settings")
		http.Error(w, "Failed to save authentication settings", http.StatusInternalServerError)
		return
	}

	utils.JSONResponse(w, map[string]string{
		"message": "Authentication settings updated successfully",
	}, http.StatusOK)
}
