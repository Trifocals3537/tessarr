package server

import (
	"cmp"
	"net/http"
	"strings"

	"github.com/Trifocals3537/tessarr/internal/utils"
)

// handleTautulli handles webhooks from Tautulli. When the payload includes a
// tvdb/tmdb id (or a generic media_id), the repair system runs a targeted
// recheck against that specific media — the v2 equivalent of v1's
// "media-id-scoped repair job". Untargeted payloads are rejected: a playback
// notification must never be interpreted as a request to sweep the library.
func (s *Server) handleTautulli(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload struct {
		Topic   string `json:"topic"`
		Arr     string `json:"arr,omitempty"`
		MediaID string `json:"media_id,omitempty"`
		TvdbID  string `json:"tvdb_id,omitempty"`
		TmdbID  string `json:"tmdb_id,omitempty"`
		Fix     bool   `json:"fix,omitempty"`
	}
	if err := utils.DecodeJSONRequestBounded(
		w,
		r,
		&payload,
		utils.MaxControlRequestBytes,
	); err != nil {
		if utils.IsRequestTooLarge(err) {
			http.Error(w, "Request is too large", http.StatusRequestEntityTooLarge)
			return
		}
		s.logger.Error().Err(err).Msg("Failed to parse webhook body")
		http.Error(w, "Failed to parse webhook body", http.StatusBadRequest)
		return
	}
	if payload.Topic != "tautulli" {
		http.Error(w, "Invalid topic", http.StatusBadRequest)
		return
	}

	mediaID := strings.TrimSpace(cmp.Or(payload.MediaID, payload.TmdbID, payload.TvdbID))
	if mediaID == "" {
		http.Error(w, "A media ID is required", http.StatusBadRequest)
		return
	}

	svc := s.manager.Repair()
	if svc == nil {
		http.Error(w, "Repair service not available", http.StatusServiceUnavailable)
		return
	}

	run, err := svc.RecheckMedia(s.manager.Context(), strings.TrimSpace(payload.Arr), mediaID, payload.Fix)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "already running") {
			status = http.StatusConflict
		}
		http.Error(w, err.Error(), status)
		return
	}

	if run != nil {
		s.logger.Info().
			Str("run_id", run.ID).
			Str("arr", payload.Arr).
			Str("media_id", mediaID).
			Bool("fix", payload.Fix).
			Msg("Tautulli webhook: media recheck triggered")
	}
	w.WriteHeader(http.StatusOK)
}
