package server

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/server/qbit"
	"github.com/Trifocals3537/tessarr/pkg/server/sabnzbd"
	"github.com/Trifocals3537/tessarr/pkg/server/webdav"
	"github.com/Trifocals3537/tessarr/pkg/stats"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/gorilla/sessions"
	"github.com/rs/zerolog"
)

//go:embed templates/*
var content embed.FS

//go:embed assets/build/*
var assetsEmbed embed.FS

//go:embed assets/images/*
var imagesEmbed embed.FS

const (
	httpReadHeaderTimeout = 10 * time.Second
	httpIdleTimeout       = 2 * time.Minute
	httpShutdownTimeout   = 15 * time.Second
)

type AddRequest struct {
	Url        string   `json:"url"`
	Arr        string   `json:"arr"`
	File       string   `json:"file"`
	NotSymlink bool     `json:"notSymlink"`
	Content    string   `json:"content"`
	Seasons    []string `json:"seasons"`
	Episodes   []string `json:"episodes"`
}

type ArrResponse struct {
	Name string `json:"name"`
	Url  string `json:"url"`
}

type ContentResponse struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Type  string `json:"type"`
	ArrID string `json:"arr"`
}

type Server struct {
	router          *chi.Mux
	logger          zerolog.Logger
	manager         *manager.Manager
	stats           *stats.Collector
	cookie          *sessions.CookieStore
	templates       *template.Template
	nzbUserAgent    string
	urlBase         string
	restartFunc     func()
	configMu        sync.Mutex
	restartPending  bool
	loginLimiter    *loginAttemptLimiter
	allowedClients  []netip.Prefix
	accessPolicyErr error
	ready           chan struct{}
}

func New(mgr *manager.Manager) *Server {
	l := logger.New("http")
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)

	cfg := config.Get()

	templates := template.Must(template.ParseFS(
		content,
		"templates/layout.html",
		"templates/setup_layout.html",
		"templates/index.html",
		"templates/download.html",
		"templates/repair.html",
		"templates/stats.html",
		"templates/config.html",
		"templates/browse.html",
		"templates/login.html",
		"templates/register.html",
		"templates/setup.html",
	))
	cookieStore := newSessionCookieStore(
		cfg.SecretKey(),
		cfg.SecureSessionCookie,
	)

	statsCollector := stats.New(mgr)

	s := &Server{
		logger:       l,
		manager:      mgr,
		stats:        statsCollector,
		cookie:       cookieStore,
		templates:    templates,
		urlBase:      cfg.URLBase,
		loginLimiter: newLoginAttemptLimiter(),
		ready:        make(chan struct{}),
	}
	s.allowedClients, s.accessPolicyErr =
		config.ParseAllowedClientCIDRs(cfg.AllowedClientCIDRs)
	r.Use(s.clientNetworkMiddleware)
	r.Use(middleware.StripSlashes)
	r.Use(middleware.RedirectSlashes)

	qb := qbit.New(mgr)
	sb := sabnzbd.New(mgr)
	wd := webdav.NewHandler(mgr)

	routes := make(map[string]http.Handler)
	routes["/api/v2"] = qb.Routes()

	if !wd.IsDisabled() {
		routes["/webdav"] = wd.Routes()
	}
	// Signed STRM URLs are independent of the WebDAV mount surface.
	routes["/stream"] = wd.StreamRoutes()
	routes["/sabnzbd"] = sb.Routes()

	// Trim trailing slash so chi registers the URLBase root path itself
	routePath := cfg.URLBase
	if routePath != "/" {
		routePath = strings.TrimSuffix(routePath, "/")
	}
	r.Route(routePath, func(r chi.Router) {
		// Mount web routes
		r.Mount("/", s.WebRoutes())

		for path, handler := range routes {
			r.Mount(path, handler)
		}

		r.Group(func(r chi.Router) {
			r.Use(s.authMiddleware)

			//logs
			r.Get("/logs", s.getLogs) // deprecated, use /debug/logs

			r.Route("/debug", func(r chi.Router) {
				r.Get("/stats", s.stats.Handler())
				r.Post("/speedtest", s.handleSpeedTest)
				r.Get("/logs", s.getLogs)
				r.Get("/logs/rclone", s.getRcloneLogs)
				r.Get("/ingests", s.handleIngests)
				r.Get("/ingests/{debrid}", s.handleIngestsByDebrid)
			})
		})

		// Webhooks can trigger repair work and require the same bearer token or
		// authenticated session as the rest of the control-plane API.
		r.With(s.authMiddleware).Post("/webhooks/tautulli", s.handleTautulli)
	})
	s.router = r
	return s
}

func newSessionCookieStore(secret string, secure bool) *sessions.CookieStore {
	store := sessions.NewCookieStore([]byte(secret))
	store.Options = &sessions.Options{
		Path:     "/",
		MaxAge:   86400 * 7,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   secure,
	}
	return store
}

func (s *Server) SetRestartFunc(restartFunc func()) {
	s.restartFunc = restartFunc
}

// Ready closes only after the HTTP listener has bound successfully.
func (s *Server) Ready() <-chan struct{} { return s.ready }

func (s *Server) Restart() {
	if s.restartFunc != nil {
		time.Sleep(200 * time.Millisecond)
		s.restartFunc()
	} else {
		s.logger.Warn().Msg("Restart function not set")
	}
}

func (s *Server) Start(ctx context.Context) error {
	cfg := config.Get()
	if err := cfg.ValidateDeployment(); err != nil {
		return fmt.Errorf("deployment safety check: %w", err)
	}
	if s.accessPolicyErr != nil {
		return fmt.Errorf("client network policy: %w", s.accessPolicyErr)
	}
	if len(s.allowedClients) > 0 {
		s.logger.Info().
			Int("network_count", len(s.allowedClients)).
			Msg("Client network access policy enabled")
	}

	if !isLoopbackBindAddress(cfg.BindAddress) && len(s.allowedClients) == 0 {
		s.logger.Warn().
			Str("bind_address", cfg.BindAddress).
			Msg("The non-loopback HTTP listener has no client network boundary. Use a trusted network or TLS proxy and do not send credentials to this listener over the public Internet")
	}

	addr := fmt.Sprintf("%s:%s", cfg.BindAddress, cfg.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", addr, err)
	}
	close(s.ready)

	// Start background stats only after the HTTP listener is established.
	s.stats.Start(ctx)

	srv := &http.Server{
		Addr:              addr,
		Handler:           s.router,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		IdleTimeout:       httpIdleTimeout,
	}

	s.logger.Info().Msgf("Starting server on %s%s", addr, cfg.URLBase)
	serveErr := serveHTTP(ctx, srv, listener, httpShutdownTimeout)

	statsCtx, cancelStats := context.WithTimeout(context.Background(), httpShutdownTimeout)
	statsErr := s.stats.Stop(statsCtx)
	cancelStats()

	if serveErr != nil {
		serveErr = fmt.Errorf("HTTP server: %w", serveErr)
	}
	return errors.Join(serveErr, statsErr)
}

func isLoopbackBindAddress(bindAddress string) bool {
	return config.IsLoopbackBindAddress(bindAddress)
}

func serveHTTP(ctx context.Context, srv *http.Server, listener net.Listener, shutdownTimeout time.Duration) error {
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(listener)
	}()

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	shutdownErr := srv.Shutdown(shutdownCtx)
	cancel()

	if shutdownErr != nil {
		closeErr := srv.Close()
		err := <-serveErr
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		return errors.Join(
			fmt.Errorf("graceful shutdown: %w", shutdownErr),
			closeErr,
			err,
		)
	}

	err := <-serveErr
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (s *Server) getLogs(w http.ResponseWriter, r *http.Request) {
	s.streamLogFile(w, "tessarr.log")
}

func (s *Server) getRcloneLogs(w http.ResponseWriter, r *http.Request) {
	s.streamLogFile(w, "rclone.log")
}

func (s *Server) streamLogFile(w http.ResponseWriter, name string) {
	rooted, _, err := safepath.OpenRoot(logger.GetLogPath())
	if err != nil {
		http.Error(w, "Error reading log file", http.StatusInternalServerError)
		return
	}
	defer rooted.Close()

	info, err := rooted.Lstat(name)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		http.Error(w, "Error reading log file", http.StatusInternalServerError)
		return
	}
	file, err := rooted.Open(name)
	if err != nil {
		http.Error(w, "Error reading log file", http.StatusInternalServerError)
		return
	}
	defer func() {
		if err := file.Close(); err != nil {
			s.logger.Error().Err(err).Msg("Error closing log file")
		}
	}()
	openedInfo, err := file.Stat()
	if err != nil || !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) {
		http.Error(w, "Error reading log file", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Content-Disposition", "inline; filename=application.log")
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "0")

	if _, err := io.Copy(w, file); err != nil {
		s.logger.Warn().Err(err).Str("log_file", name).Msg("Log stream ended early")
	}
}
