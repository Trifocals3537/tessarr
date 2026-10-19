package server

import (
	"fmt"
	"net/http"

	json "github.com/bytedance/sonic"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/Trifocals3537/tessarr/internal/utils"
)

// SetupState tracks the current setup wizard state
type SetupState struct {
	Completed      bool   `json:"completed"`
	CurrentStep    int    `json:"current_step"`
	Username       string `json:"username,omitempty"`
	DebridProvider string `json:"debrid_provider,omitempty"`
	DebridAPIKey   string `json:"debrid_api_key,omitempty"`
	MountFolder    string `json:"mount_folder,omitempty"`
	DownloadFolder string `json:"download_folder,omitempty"`
	MountSystem    string `json:"mount_system,omitempty"` // "dfs" or "rclone"
	MountPath      string `json:"mount_path,omitempty"`
	CacheDir       string `json:"cache_dir,omitempty"`
}

// SetupWizardRequest represents a request from the setup wizard
type SetupWizardRequest struct {
	Step int            `json:"step"`
	Data map[string]any `json:"data"`
}

// SetupWizardResponse represents the response from setup wizard
type SetupWizardResponse struct {
	Success      bool        `json:"success"`
	Message      string      `json:"message,omitempty"`
	Error        string      `json:"error,omitempty"`
	NextStep     int         `json:"next_step,omitempty"`
	State        *SetupState `json:"state,omitempty"`
	Validation   any         `json:"validation,omitempty"`
	SetupNeeded  bool        `json:"setup_needed,omitempty"`
	RedirectTo   string      `json:"redirect_to,omitempty"`
	ConfigLoaded bool        `json:"config_loaded,omitempty"`
}

// SetupHandler renders the setup wizard page
func (s *Server) SetupHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()

	if err := cfg.SetupComplete(); err == nil {
		redirectLocal(w, cfg.URLBase, "", http.StatusSeeOther)
		return
	}
	if !s.requireSetupAccess(w, r) {
		return
	}
	data := map[string]any{
		"URLBase": cfg.URLBase,
		"Page":    "setup",
		"Title":   "Setup Wizard",
	}
	err := s.templates.ExecuteTemplate(w, "setup_layout", data)
	if err != nil {
		s.logger.Error().Err(err).Msg("template error")
	}
}

// sendSetupError sends an error response
func (s *Server) sendSetupError(w http.ResponseWriter, message string, err error) {
	response := SetupWizardResponse{
		Success: false,
		Error:   message,
	}
	if err != nil {
		response.Error = fmt.Sprintf("%s: %v", message, err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_ = json.ConfigDefault.NewEncoder(w).Encode(response)
}

// SetupCompleteRequest represents the complete setup data from frontend
type SetupCompleteRequest struct {
	Auth struct {
		Username string `json:"username,omitempty"`
		Password string `json:"password,omitempty"`
		SkipAuth bool   `json:"skip_auth,omitempty"`
	} `json:"auth"`
	Debrid struct {
		Provider string `json:"provider,omitempty"`
		APIKey   string `json:"api_key,omitempty"`
		Skip     bool   `json:"skip_debrid,omitempty"`
	} `json:"debrid"`
	Usenet struct {
		Host              string `json:"host,omitempty"`
		Port              int    `json:"port,omitempty"`
		Username          string `json:"username,omitempty"`
		Password          string `json:"password,omitempty"`
		MaxConnections    int    `json:"max_connections,omitempty"`
		ReaderConnections int    `json:"reader_connections,omitempty"`
		SSL               bool   `json:"ssl,omitempty"`
		Skip              bool   `json:"skip_usenet,omitempty"`
	} `json:"usenet"`
	Download struct {
		DownloadFolder string `json:"download_folder"`
	} `json:"download"`
	Mount struct {
		MountType        string `json:"mount_type"`
		MountPath        string `json:"mount_path"`
		CacheDir         string `json:"cache_dir"`
		RcloneBufferSize string `json:"rclone_buffer_size,omitempty"`
	} `json:"mount"`
}

// setupCompleteHandler handles the complete setup in a single request
func (s *Server) setupCompleteHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	// Prevent re-running setup once it has already been completed
	if err := cfg.SetupComplete(); err == nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if !s.requireSetupAccess(w, r) {
		return
	}
	if !s.requireBrowserMutation(w, r) {
		return
	}

	var req SetupCompleteRequest
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
		s.sendSetupError(w, "Invalid request format", err)
		return
	}

	hasDebrid := !req.Debrid.Skip && req.Debrid.Provider != "" && req.Debrid.APIKey != ""
	hasUsenet := !req.Usenet.Skip && req.Usenet.Host != "" && req.Usenet.Port > 0 && req.Usenet.Username != "" && req.Usenet.Password != ""

	if !hasDebrid && !hasUsenet {
		s.sendSetupError(w, "Please configure at least one Debrid or Usenet provider", nil)
		return
	}

	// Validate every step before changing either configuration file. This keeps
	// setup atomic: a late folder/provider error cannot leave auth half-applied.
	if req.Auth.SkipAuth {
		if !isLoopbackBindAddress(cfg.BindAddress) {
			s.sendSetupError(
				w,
				"Authentication cannot be disabled on a non-loopback listener",
				nil,
			)
			return
		}
	} else if req.Auth.Username != "" && req.Auth.Password != "" {
		if err := config.ValidateAuthCredentials(req.Auth.Username, req.Auth.Password); err != nil {
			s.sendSetupError(w, "Invalid authentication settings", err)
			return
		}
	} else {
		s.sendSetupError(
			w,
			"Username and password are required unless authentication is explicitly skipped",
			nil,
		)
		return
	}

	if hasDebrid && !config.IsSupportedDebridProvider(req.Debrid.Provider) {
		s.sendSetupError(w, "Invalid debrid provider", nil)
		return
	}

	if hasUsenet && (req.Usenet.Port < 1 || req.Usenet.Port > 65535) {
		s.sendSetupError(w, "Usenet port must be between 1 and 65535", nil)
		return
	}

	if req.Download.DownloadFolder == "" {
		s.sendSetupError(w, "Download folder is required", nil)
		return
	}

	downloadRoot, err := safepath.EnsureRoot(req.Download.DownloadFolder, 0o755)
	if err != nil {
		s.sendSetupError(w, "Failed to create download folder", err)
		return
	}
	req.Download.DownloadFolder = downloadRoot

	if req.Mount.MountType == "dfs" {
		cacheRoot, err := safepath.EnsureRoot(req.Mount.CacheDir, 0o755)
		if err != nil {
			s.sendSetupError(w, "Failed to create cache directory", err)
			return
		}
		req.Mount.CacheDir = cacheRoot
	}

	result, err := config.Update(func(draft *config.Config) error {
		if req.Auth.SkipAuth {
			draft.UseAuth = false
		} else if err := draft.ApplyAuthCredentials(req.Auth.Username, req.Auth.Password); err != nil {
			return err
		}

		if hasDebrid {
			debrid := config.Debrid{
				Provider:         req.Debrid.Provider,
				Name:             req.Debrid.Provider,
				APIKey:           req.Debrid.APIKey,
				DownloadAPIKeys:  []string{req.Debrid.APIKey},
				DownloadUncached: false,
				RateLimit:        config.DefaultRateLimit,
			}
			if len(draft.Debrids) == 0 {
				draft.Debrids = []config.Debrid{debrid}
			} else {
				draft.Debrids[0] = debrid
			}
		} else {
			draft.Debrids = nil
		}

		if hasUsenet {
			providerMax := req.Usenet.MaxConnections
			if providerMax <= 0 {
				providerMax = 30
			}
			readerConnections := req.Usenet.ReaderConnections
			if readerConnections <= 0 {
				readerConnections = 15
			}
			draft.Usenet.Providers = []config.UsenetProvider{{
				Host: req.Usenet.Host, Port: req.Usenet.Port,
				Username: req.Usenet.Username, Password: req.Usenet.Password,
				MaxConnections: providerMax, SSL: req.Usenet.SSL, Priority: 1,
			}}
			draft.Usenet.MaxConnections = readerConnections
			draft.Usenet.ProcessingMaxConnections = readerConnections
		} else {
			draft.Usenet.Providers = nil
		}

		draft.DownloadFolder = req.Download.DownloadFolder
		if len(draft.Categories) == 0 {
			draft.Categories = []string{"sonarr", "radarr"}
		}
		if draft.MaxActiveDownloads == 0 {
			draft.MaxActiveDownloads = 5
		}
		draft.Mount.Type = config.MountType(req.Mount.MountType)
		switch req.Mount.MountType {
		case "dfs":
			draft.Mount.MountPath = req.Mount.MountPath
			draft.Mount.DFS.CacheDir = req.Mount.CacheDir
			if draft.Mount.DFS.ChunkSize == "" {
				draft.Mount.DFS.ChunkSize = "8MB"
			}
			if draft.Mount.DFS.ReadAheadSize == "" {
				draft.Mount.DFS.ReadAheadSize = "32MB"
			}
			if draft.Mount.DFS.CacheExpiry == "" {
				draft.Mount.DFS.CacheExpiry = "24h"
			}
		case "rclone":
			draft.Mount.MountPath = req.Mount.MountPath
			if req.Mount.CacheDir != "" {
				draft.Mount.Rclone.CacheDir = req.Mount.CacheDir
			}
			if draft.Mount.Rclone.VfsCacheMode == "" {
				draft.Mount.Rclone.VfsCacheMode = "full"
			}
			if draft.Mount.Rclone.DirCacheTime == "" {
				draft.Mount.Rclone.DirCacheTime = "5m"
			}
		}
		return draft.ValidateNormalized()
	})
	if err != nil {
		s.sendSetupError(w, "Failed to save configuration", err)
		return
	}

	if err := result.Desired.SetupComplete(); err != nil {
		s.sendSetupError(w, "Setup completion validation failed", err)
		return
	}

	// Trigger manager restart to apply new config
	go s.Restart()

	response := SetupWizardResponse{
		Success:    true,
		Message:    "Setup completed successfully! Restarting services...",
		RedirectTo: "/",
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.ConfigDefault.NewEncoder(w).Encode(response)
}
