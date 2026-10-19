package server

import (
	"net/http"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/utils"
)

func (s *Server) LoginHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	if cfg.NeedsAuth() {
		redirectLocal(w, cfg.URLBase, "register", http.StatusSeeOther)
		return
	}
	if r.Method == "GET" {
		data := map[string]any{
			"URLBase": cfg.URLBase,
			"Page":    "login",
			"Title":   "Login",
		}
		err := s.templates.ExecuteTemplate(w, "layout", data)
		if err != nil {
			s.logger.Warn().Err(err).Msg("error rendering /login template")
		}
		return
	}
	if !s.requireBrowserMutation(w, r) {
		return
	}

	var credentials struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}

	if err := utils.DecodeJSONRequestBounded(
		w,
		r,
		&credentials,
		utils.MaxControlRequestBytes,
	); err != nil {
		if utils.IsRequestTooLarge(err) {
			http.Error(w, "Request is too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	clientKey := loginClientKey(r)
	if retry := s.loginLimiter.retryAfter(clientKey); retry > 0 {
		setRetryAfter(w, int((retry+time.Second-1)/time.Second))
		http.Error(w, "Too many failed login attempts", http.StatusTooManyRequests)
		return
	}

	if s.verifyAuth(credentials.Username, credentials.Password) {
		s.loginLimiter.reset(clientKey)
		session, _ := s.cookie.Get(r, "auth-session")
		setAuthenticatedSession(session.Values, credentials.Username)
		if err := session.Save(r, w); err != nil {
			http.Error(w, "Error saving session", http.StatusInternalServerError)
			return
		}
		redirectLocal(w, cfg.URLBase, "", http.StatusSeeOther)
		return
	}

	s.loginLimiter.recordFailure(clientKey)
	http.Error(w, "Invalid credentials", http.StatusUnauthorized)
}

func (s *Server) LogoutHandler(w http.ResponseWriter, r *http.Request) {
	session, _ := s.cookie.Get(r, "auth-session")
	session.Values["authenticated"] = false
	session.Options.MaxAge = -1
	err := session.Save(r, w)
	if err != nil {
		return
	}
	redirectLocal(w, config.Get().URLBase, "login", http.StatusSeeOther)
}

func (s *Server) RegisterHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	if !registrationAllowed(cfg) {
		if cfg.NeedsAuth() && !isLoopbackBindAddress(cfg.BindAddress) {
			http.Error(
				w,
				"Remote registration is disabled; run tessarr --config PATH --set-auth USERNAME from the host",
				http.StatusForbidden,
			)
			return
		}
		http.Error(w, "Registration is not available", http.StatusForbidden)
		return
	}

	if r.Method == "GET" {
		data := map[string]any{
			"URLBase": cfg.URLBase,
			"Page":    "register",
			"Title":   "registerVolume",
		}
		err := s.templates.ExecuteTemplate(w, "layout", data)
		if err != nil {
			s.logger.Warn().Err(err).Msg("error rendering /register template")
		}
		return
	}
	if !s.requireBrowserMutation(w, r) {
		return
	}

	if err := utils.ParseAnyFormBounded(
		w,
		r,
		utils.MaxControlRequestBytes,
	); err != nil {
		if utils.IsRequestTooLarge(err) {
			http.Error(w, "Request is too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	username := r.FormValue("username")
	password := r.FormValue("password")
	confirmPassword := r.FormValue("confirmPassword")

	if password != confirmPassword {
		http.Error(w, "Passwords do not match", http.StatusBadRequest)
		return
	}

	if err := config.ValidateAuthCredentials(username, password); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result, err := config.Update(func(draft *config.Config) error {
		return draft.ApplyAuthCredentials(username, password)
	})
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to save registration credentials")
		http.Error(w, "Error saving credentials", http.StatusInternalServerError)
		return
	}

	// Create a session
	session, _ := s.cookie.Get(r, "auth-session")
	setAuthenticatedSession(session.Values, username)
	if err := session.Save(r, w); err != nil {
		http.Error(w, "Error saving session", http.StatusInternalServerError)
		return
	}

	redirectLocal(w, result.Active.URLBase, "", http.StatusSeeOther)
}

func registrationAllowed(cfg *config.Config) bool {
	return cfg != nil &&
		cfg.NeedsAuth() &&
		isLoopbackBindAddress(cfg.BindAddress)
}

func (s *Server) IndexHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	data := map[string]any{
		"URLBase":    cfg.URLBase,
		"Page":       "index",
		"Title":      "Queues",
		"SetupError": cfg.SetupError(),
	}
	err := s.templates.ExecuteTemplate(w, "layout", data)
	if err != nil {
		s.logger.Warn().Err(err).Msg("error rendering /index template")
	}
}

func (s *Server) DownloadHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	debrids := make([]string, 0)
	for _, d := range cfg.Debrids {
		debrids = append(debrids, d.Name)
	}
	data := map[string]any{
		"URLBase":                 cfg.URLBase,
		"Page":                    "download",
		"Title":                   "Download",
		"Debrids":                 debrids,
		"HasMultiDebrid":          len(debrids) > 1,
		"downloadFolder":          cfg.DownloadFolder,
		"alwaysRemoveTrackerURLS": cfg.AlwaysRmTrackerUrls,
		"SetupError":              cfg.SetupError(),
	}
	err := s.templates.ExecuteTemplate(w, "layout", data)
	if err != nil {
		s.logger.Warn().Err(err).Msg("error rendering /download template")
	}
}

func (s *Server) RepairHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	data := map[string]any{
		"URLBase":    cfg.URLBase,
		"Page":       "repair",
		"Title":      "Repair",
		"SetupError": cfg.SetupError(),
	}
	err := s.templates.ExecuteTemplate(w, "layout", data)
	if err != nil {
		s.logger.Warn().Err(err).Msg("error rendering /repair template")
	}
}

func (s *Server) ConfigHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	data := map[string]any{
		"URLBase":    cfg.URLBase,
		"Page":       "config",
		"Title":      "Config",
		"SetupError": cfg.SetupError(),
	}
	err := s.templates.ExecuteTemplate(w, "layout", data)
	if err != nil {
		s.logger.Warn().Err(err).Msg("error rendering /config template")
	}
}

func (s *Server) StatsHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	data := map[string]any{
		"URLBase": cfg.URLBase,
		"Page":    "stats",
		"Title":   "Statistics",
	}
	err := s.templates.ExecuteTemplate(w, "layout", data)
	if err != nil {
		s.logger.Warn().Err(err).Msg("error rendering /stats template")
	}
}

func (s *Server) BrowseHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	data := map[string]any{
		"URLBase":    cfg.URLBase,
		"Page":       "browse",
		"Title":      "Browse Torrents",
		"SetupError": cfg.SetupError(),
	}
	err := s.templates.ExecuteTemplate(w, "layout", data)
	if err != nil {
		s.logger.Warn().Err(err).Msg("error rendering /browse template")
	}
}
