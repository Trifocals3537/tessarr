package qbit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/customerror"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/arr"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/storage"
)

func (q *QBit) handleLogin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	cfg := config.Get()
	username := r.FormValue("username")
	password := r.FormValue("password")
	_, err := q.authenticate(getCategory(ctx), username, password, false)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	if cfg.UseAuth {
		token, err := q.sessions.create(username, password)
		if err != nil {
			http.Error(w, "failed to create session", http.StatusInternalServerError)
			return
		}
		http.SetCookie(w, newSIDCookie(token, qbitCookieSecure(r)))
	}
	_, _ = w.Write([]byte("Ok."))
}

func newSIDCookie(value string, secure bool) *http.Cookie {
	return &http.Cookie{
		Name:     "SID",
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   secure,
		MaxAge:   int(qbitSessionTTL / time.Second),
		Expires:  time.Now().Add(qbitSessionTTL),
	}
}

func qbitCookieSecure(r *http.Request) bool {
	// Compatibility clients commonly use the private HTTP listener even when
	// the browser UI is exposed through an HTTPS reverse proxy. AppURL does not
	// describe this request's transport and must not make that private-client
	// cookie unusable.
	return r != nil && r.TLS != nil
}

func (q *QBit) handleVersion(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write([]byte("v4.3.2"))
}

func (q *QBit) handleWebAPIVersion(w http.ResponseWriter, r *http.Request) {
	_, _ = w.Write([]byte("2.7"))
}

func (q *QBit) handlePreferences(w http.ResponseWriter, r *http.Request) {
	preferences := getAppPreferences()

	preferences.SavePath = q.downloadFolder
	preferences.TempPath = filepath.Join(q.downloadFolder, "temp")

	utils.JSONResponse(w, preferences, http.StatusOK)
}

func (q *QBit) handleBuildInfo(w http.ResponseWriter, r *http.Request) {
	res := BuildInfo{
		Bitness:    64,
		Boost:      "1.75.0",
		Libtorrent: "1.2.11.0",
		Openssl:    "1.1.1i",
		Qt:         "5.15.2",
		Zlib:       "1.2.11",
	}
	utils.JSONResponse(w, res, http.StatusOK)
}

func (q *QBit) handleShutdown(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func normalizeStateFilter(raw string) string {
	state := strings.TrimSpace(raw)
	if strings.EqualFold(state, "all") {
		return ""
	}
	return state
}

func (q *QBit) handleTorrentsInfo(w http.ResponseWriter, r *http.Request) {
	//log all url params
	ctx := r.Context()
	category := getCategory(ctx)
	state := normalizeStateFilter(r.URL.Query().Get("filter"))
	hashes := getHashes(ctx)

	// Convert hashes to filter function
	torrents := q.manager.Queue().ListFilter(category, config.ProtocolTorrent, storage.TorrentState(state), hashes, "added_on", false)
	qbitTorrents := make([]Torrent, len(torrents))
	for i, t := range torrents {
		qbitTorrents[i] = convertToQBitTorrentTorrent(t)
	}
	utils.JSONResponse(w, qbitTorrents, http.StatusOK)
}

func (q *QBit) handleTorrentsAdd(w http.ResponseWriter, r *http.Request) {
	ctx := context.WithValue(r.Context(), qbitEndpointKey{}, r.Host)

	// Parse form based on content type
	contentType := strings.ToLower(r.Header.Get("Content-Type"))
	if strings.Contains(contentType, "multipart/form-data") {
		if err := utils.ParseMultipartFormBounded(
			w,
			r,
			utils.MaxImportRequestBytes,
			utils.MaxMultipartMemoryBytes,
		); err != nil {
			if utils.IsRequestTooLarge(err) {
				http.Error(w, "Request is too large", http.StatusRequestEntityTooLarge)
				return
			}
			q.logger.Error().Err(err).Msg("Error parsing multipart form")
			http.Error(w, "Invalid multipart form", http.StatusBadRequest)
			return
		}
		if r.MultipartForm != nil {
			defer r.MultipartForm.RemoveAll()
		}
		if utils.MultipartFormPartCount(r.MultipartForm) > utils.MaxMultipartFormParts {
			http.Error(w, "Request has too many multipart fields", http.StatusRequestEntityTooLarge)
			return
		}
	} else if strings.Contains(contentType, "application/x-www-form-urlencoded") {
		if err := utils.ParseFormBounded(w, r, utils.MaxImportRequestBytes); err != nil {
			if utils.IsRequestTooLarge(err) {
				http.Error(w, "Request is too large", http.StatusRequestEntityTooLarge)
				return
			}
			q.logger.Error().Err(err).Msg("Error parsing form")
			http.Error(w, "Invalid form", http.StatusBadRequest)
			return
		}
	} else {
		http.Error(w, "Invalid content type", http.StatusBadRequest)
		return
	}

	itemCount := 0
	if urls := r.FormValue("urls"); urls != "" {
		for rawURL := range strings.SplitSeq(urls, "\n") {
			if strings.TrimSpace(rawURL) != "" {
				itemCount++
			}
		}
	}
	if r.MultipartForm != nil {
		itemCount += len(r.MultipartForm.File["torrents"])
	}
	if itemCount > utils.MaxImportItems {
		http.Error(
			w,
			fmt.Sprintf("Request exceeds the %d-item limit", utils.MaxImportItems),
			http.StatusRequestEntityTooLarge,
		)
		return
	}

	cfg := config.Get()
	action := cfg.DefaultDownloadAction
	if strings.ToLower(r.FormValue("sequentialDownload")) == "true" {
		action = config.DownloadActionDownload
	}

	rmTrackerUrls := strings.ToLower(r.FormValue("firstLastPiecePrio")) == "true"

	// Check config setting - if always remove tracker URLs is enabled, force it to true
	if q.alwaysRemoveTrackerURLS {
		rmTrackerUrls = true
	}

	debridName := r.FormValue("debrid")
	category := r.FormValue("category")
	_arr := getArrFromContext(ctx)
	if _arr == nil {
		// Arr is not in context
		_arr = arr.New(category, "", "", false, nil, "", "")
	}
	atleastOne := false
	var retainedBytes int64

	// Handle magnet URLs
	if urls := r.FormValue("urls"); urls != "" {
		for u := range strings.SplitSeq(urls, "\n") {
			rawURL := strings.TrimSpace(u)
			if rawURL == "" {
				continue
			}
			remainingBytes := utils.MaxImportRequestBytes - retainedBytes
			maxBytes := min(utils.MaxMetadataFileBytes, remainingBytes)
			if maxBytes <= 0 {
				http.Error(w, "Request metadata exceeds the byte limit", http.StatusRequestEntityTooLarge)
				return
			}
			itemBytes, err := q.addMagnet(
				ctx,
				rawURL,
				_arr,
				debridName,
				action,
				cfg.Notifications.CallbackURL,
				rmTrackerUrls,
				cfg.SkipMultiSeason,
				maxBytes,
			)
			retainedBytes += itemBytes
			if err != nil {
				q.logger.Debug().Err(err).Msg("Error adding magnet")
				status, idempotent := torrentAddErrorStatus(err)
				if idempotent {
					atleastOne = true
					continue
				}
				if errors.Is(err, utils.ErrContentTooLarge) {
					http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
					return
				}
				writeTorrentAddError(w, err, status)
				return
			}
			atleastOne = true
		}
	}

	// Handle torrent files
	if r.MultipartForm != nil && r.MultipartForm.File != nil {
		if files := r.MultipartForm.File["torrents"]; len(files) > 0 {
			for _, fileHeader := range files {
				remainingBytes := utils.MaxImportRequestBytes - retainedBytes
				maxBytes := min(utils.MaxMetadataFileBytes, remainingBytes)
				if maxBytes <= 0 || fileHeader.Size > maxBytes {
					http.Error(w, "Request metadata exceeds the byte limit", http.StatusRequestEntityTooLarge)
					return
				}
				itemBytes, err := q.addTorrent(
					ctx,
					fileHeader,
					_arr,
					debridName,
					action,
					cfg.Notifications.CallbackURL,
					rmTrackerUrls,
					cfg.SkipMultiSeason,
					maxBytes,
				)
				retainedBytes += itemBytes
				if err != nil {
					q.logger.Debug().Err(err).Str("torrent", fileHeader.Filename).Msgf("Error adding torrent")
					status, idempotent := torrentAddErrorStatus(err)
					if idempotent {
						atleastOne = true
						continue
					}
					if errors.Is(err, utils.ErrContentTooLarge) {
						http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
						return
					}
					writeTorrentAddError(w, err, status)
					return
				}
				atleastOne = true
			}
		}
	}

	if !atleastOne {
		http.Error(w, "No valid URLs or torrents provided", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func torrentAddErrorStatus(err error) (status int, idempotent bool) {
	switch {
	case errors.Is(err, manager.ErrJobQueueDuplicate):
		// qBittorrent treats re-adding an already admitted hash as a successful
		// idempotent operation.
		return http.StatusOK, true
	case errors.Is(err, manager.ErrJobQueueFull):
		return http.StatusTooManyRequests, false
	case errors.Is(err, manager.ErrJobQueueClosed):
		return http.StatusServiceUnavailable, false
	case errors.Is(err, manager.ErrQueueEntryDeleting),
		errors.Is(err, storage.ErrQueuedEntryDeleting):
		return http.StatusConflict, false
	case onlyCustomErrorCode(err, "torrent_not_cached"):
		return http.StatusConflict, false
	case onlyCustomErrorCode(err, "torrent_content_rejected"):
		return http.StatusUnprocessableEntity, false
	default:
		return http.StatusBadRequest, false
	}
}

func writeTorrentAddError(w http.ResponseWriter, err error, status int) {
	for _, code := range []string{"torrent_not_cached", "torrent_content_rejected"} {
		if onlyCustomErrorCode(err, code) {
			w.Header().Set("X-Tessarr-Error-Code", code)
			break
		}
	}
	if status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "5")
	}
	http.Error(w, err.Error(), status)
}

// onlyCustomErrorCode follows ordinary wrappers and joined provider failures.
// It returns true only when every terminal failure has the same typed outcome;
// a cache miss from one provider plus an outage from another must not be
// mislabeled as an all-provider cache miss.
func onlyCustomErrorCode(err error, code string) bool {
	found := false
	var visit func(error) bool
	visit = func(current error) bool {
		if current == nil {
			return true
		}
		if customErr, ok := current.(*customerror.Error); ok {
			found = true
			return customErr.Code == code
		}
		if joined, ok := current.(interface{ Unwrap() []error }); ok {
			children := joined.Unwrap()
			if len(children) == 0 {
				return false
			}
			for _, child := range children {
				if !visit(child) {
					return false
				}
			}
			return true
		}
		if wrapped, ok := current.(interface{ Unwrap() error }); ok {
			child := wrapped.Unwrap()
			return child != nil && visit(child)
		}
		return false
	}
	return visit(err) && found
}

func (q *QBit) handleTorrentsDelete(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	hashes := getHashes(ctx)

	if len(hashes) == 0 {
		http.Error(w, "No hashes provided", http.StatusBadRequest)
		return
	}
	if containsAllHashes(hashes) {
		entries := q.manager.Queue().ListFilter(
			"",
			config.ProtocolTorrent,
			"",
			nil,
			"",
			false,
		)
		hashes = make([]string, 0, len(entries))
		for _, entry := range entries {
			if entry != nil {
				hashes = append(hashes, entry.InfoHash)
			}
		}
	}
	deleteFiles := shouldDeleteTorrentFiles(r)
	for _, hash := range hashes {
		err := q.manager.DeleteQBitQueueEntry(hash, deleteFiles)
		if err != nil && !storage.IsQueuedEntryNotFound(err) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
}

func containsAllHashes(hashes []string) bool {
	for _, hash := range hashes {
		if strings.EqualFold(strings.TrimSpace(hash), "all") {
			return true
		}
	}
	return false
}

func torrentHashPredicate(hashes []string) func(*storage.Entry) bool {
	if containsAllHashes(hashes) {
		return func(entry *storage.Entry) bool {
			return entry != nil
		}
	}

	hashSet := make(map[string]struct{}, len(hashes))
	for _, hash := range hashes {
		hash = strings.ToLower(strings.TrimSpace(hash))
		if hash != "" {
			hashSet[hash] = struct{}{}
		}
	}
	return func(entry *storage.Entry) bool {
		if entry == nil {
			return false
		}
		_, selected := hashSet[strings.ToLower(strings.TrimSpace(entry.InfoHash))]
		return selected
	}
}

func shouldDeleteTorrentFiles(r *http.Request) bool {
	return r != nil && strings.EqualFold(
		strings.TrimSpace(r.FormValue("deleteFiles")),
		"true",
	)
}

func (q *QBit) handleTorrentsPause(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	hashes := getHashes(ctx)
	for _, hash := range hashes {
		torrent, err := q.manager.Queue().GetTorrent(hash)
		if err != nil {
			continue
		}
		go q.PauseTorrent(torrent)
	}

	w.WriteHeader(http.StatusOK)
}

func (q *QBit) handleTorrentsResume(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	hashes := getHashes(ctx)
	for _, hash := range hashes {
		torrent, err := q.manager.Queue().GetTorrent(hash)
		if err != nil {
			continue
		}
		go q.ResumeTorrent(torrent)
	}

	w.WriteHeader(http.StatusOK)
}

func (q *QBit) handleTorrentRecheck(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	hashes := getHashes(ctx)
	for _, hash := range hashes {
		torrent, err := q.manager.Queue().GetTorrent(hash)
		if err != nil {
			continue
		}
		go q.RefreshTorrent(torrent)
	}

	w.WriteHeader(http.StatusOK)
}

func (q *QBit) handleCategories(w http.ResponseWriter, r *http.Request) {
	var categories = map[string]TorrentCategory{}
	for _, cat := range q.categories {
		path := filepath.Join(q.downloadFolder, cat)
		categories[cat] = TorrentCategory{
			Name:     cat,
			SavePath: path,
		}
	}
	utils.JSONResponse(w, categories, http.StatusOK)
}

func (q *QBit) handleCreateCategory(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Failed to parse form data", http.StatusBadRequest)
		return
	}

	name := r.Form.Get("category")
	if name == "" {
		http.Error(w, "No name provided", http.StatusBadRequest)
		return
	}

	q.categories = append(q.categories, name)

	utils.JSONResponse(w, nil, http.StatusOK)
}

func (q *QBit) handleTorrentProperties(w http.ResponseWriter, r *http.Request) {
	hash := r.URL.Query().Get("hash")
	torrent, err := q.manager.Queue().GetTorrent(hash)
	if err != nil {
		http.Error(w, "Entry not found", http.StatusNotFound)
		return
	}

	properties := q.GetTorrentProperties(torrent)
	utils.JSONResponse(w, properties, http.StatusOK)
}

func (q *QBit) handleTorrentFiles(w http.ResponseWriter, r *http.Request) {
	hash := r.URL.Query().Get("hash")
	torrent, err := q.manager.Queue().GetTorrent(hash)
	if err != nil {
		http.Error(w, "Entry not found", http.StatusNotFound)
		return
	}
	utils.JSONResponse(w, getTorrentFiles(torrent), http.StatusOK)
}

func (q *QBit) handleSetCategory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	category := getCategory(ctx)
	hashes := getHashes(ctx)
	if len(hashes) == 0 {
		http.Error(w, "No hashes provided", http.StatusBadRequest)
		return
	}
	filterFunc := torrentHashPredicate(hashes)

	updateFunc := func(t *storage.Entry) bool {
		if t.Category != category {
			t.Category = category
			return true
		}
		return false
	}

	if err := q.manager.Queue().UpdateWhere(filterFunc, updateFunc); err != nil {
		q.logger.Warn().Err(err).Msgf("Error adding torrent")
		http.Error(w, "Failed to update torrents", http.StatusInternalServerError)
		return
	}
	utils.JSONResponse(w, nil, http.StatusOK)
}

func (q *QBit) handleAddTorrentTags(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Failed to parse form data", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	hashes := getHashes(ctx)
	tags := strings.Split(r.FormValue("tags"), ",")
	for i, tag := range tags {
		tags[i] = strings.TrimSpace(tag)
	}
	torrents := q.manager.Queue().ListFilter("", config.ProtocolTorrent, "", hashes, "", false)
	for _, t := range torrents {
		q.setTorrentTags(t, tags)
	}
	utils.JSONResponse(w, nil, http.StatusOK)
}

func (q *QBit) handleRemoveTorrentTags(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Failed to parse form data", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	hashes := getHashes(ctx)
	tags := strings.Split(r.FormValue("tags"), ",")
	for i, tag := range tags {
		tags[i] = strings.TrimSpace(tag)
	}
	torrents := q.manager.Queue().ListFilter("", config.ProtocolTorrent, "", hashes, "", false)
	for _, torrent := range torrents {
		q.removeTorrentTags(torrent, tags)

	}
	utils.JSONResponse(w, nil, http.StatusOK)
}

func (q *QBit) handleGetTags(w http.ResponseWriter, r *http.Request) {
	utils.JSONResponse(w, q.Tags, http.StatusOK)
}

func (q *QBit) handleCreateTags(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		http.Error(w, "Failed to parse form data", http.StatusBadRequest)
		return
	}
	tags := strings.Split(r.FormValue("tags"), ",")
	for i, tag := range tags {
		tags[i] = strings.TrimSpace(tag)
	}
	q.addTags(tags)
	utils.JSONResponse(w, nil, http.StatusOK)
}
