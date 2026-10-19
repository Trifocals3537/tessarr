package rclone

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/internal/rclone"
	"github.com/Trifocals3537/tessarr/internal/safepath"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"
)

const (
	FSName     = "tessarr:"
	ConfigName = "tessarr"
)

// Manager handles the rclone RC server and provides mount operations
type Manager struct {
	cmd           *exec.Cmd
	configDir     string
	logger        zerolog.Logger
	ctx           context.Context
	cancel        context.CancelFunc
	serverReady   atomic.Bool
	serverStarted atomic.Bool
	info          atomic.Pointer[MountInfo]
	mountMu       sync.Mutex
	processMu     sync.Mutex
	processDone   chan struct{}
	processCancel context.CancelFunc
	command       func(string, ...string) *exec.Cmd
	manager       *manager.Manager
	webdavURL     string

	client *rclone.Client
}

type MountInfo struct {
	LocalPath  string `json:"local_path"`
	WebDAVURL  string `json:"webdav_url"`
	Mounted    bool   `json:"mounted"`
	MountedAt  string `json:"mounted_at,omitempty"`
	ConfigName string `json:"config_name"`
	Error      string `json:"error,omitempty"`
}

type RCRequest struct {
	Command string         `json:"command"`
	Args    map[string]any `json:"args,omitempty"`
}

type RCResponse struct {
	Result any    `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

// NewManager creates a new rclone RC manager
func NewManager(manager *manager.Manager) (*Manager, error) {

	mainCfg := config.Get()
	cfg := mainCfg.Mount
	configDir := filepath.Join(config.GetMainPath(), "rclone")
	_logger := logger.New("rclone")

	if mainCfg.DisableWebDav {
		return nil, fmt.Errorf("rclone mount requires WebDAV support")
	}

	// Ensure config directory exists
	var err error
	configDir, err = safepath.EnsureRoot(configDir, 0o755)
	if err != nil {
		return nil, fmt.Errorf("create rclone config directory: %w", err)
	}

	bindAddress := mainCfg.BindAddress
	if bindAddress == "" {
		bindAddress = "localhost"
	}

	baseUrl := fmt.Sprintf("http://%s:%s", bindAddress, mainCfg.Port)
	webdavUrl, err := url.JoinPath(baseUrl, mainCfg.URLBase, "webdav")
	if err != nil {
		return nil, fmt.Errorf("build rclone WebDAV URL: %w", err)
	}

	if !strings.HasSuffix(webdavUrl, "/") {
		webdavUrl += "/"
	}

	ctx, cancel := context.WithCancel(context.Background())
	rcServer := fmt.Sprintf("http://localhost:%s", cfg.Rclone.Port)
	rcloneClient := rclone.NewClient(rcServer, "", "", _logger)

	m := &Manager{
		configDir: configDir,
		logger:    _logger,
		ctx:       ctx,
		cancel:    cancel,
		client:    rcloneClient,
		command:   exec.Command,
		webdavURL: webdavUrl,
		manager:   manager,
	}
	return m, nil
}

// Start starts the rclone RC server
func (m *Manager) Start(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if err := m.ctx.Err(); err != nil {
		return err
	}

	m.processMu.Lock()
	defer m.processMu.Unlock()

	cfg := config.Get().Mount
	if m.serverStarted.Load() {
		return nil
	}
	// Use lumberjack for log rotation instead of rclone's --log-file
	rotatingLog := &lumberjack.Logger{
		Filename:   filepath.Join(logger.GetLogPath(), "rclone.log"),
		MaxSize:    10, // 10 MB
		MaxAge:     15, // 15 days
		MaxBackups: 5,  // Keep max 5 backup files
		Compress:   true,
	}

	args := []string{
		"rcd",
		"--rc-addr", ":" + cfg.Rclone.Port,
		"--rc-no-auth", // We'll handle auth at the application level
		"--config", filepath.Join(config.GetMainPath(), "rclone", "rclone.conf"),
		// No --log-file, we capture output directly
	}

	logLevel := cfg.Rclone.LogLevel
	if logLevel != "" {
		if !slices.Contains([]string{"DEBUG", "INFO", "NOTICE", "ERROR"}, logLevel) {
			logLevel = "INFO"
		}
		args = append(args, "--log-level", logLevel)
	}

	if cfg.Rclone.CacheDir != "" {
		cacheDir, err := safepath.EnsureRoot(cfg.Rclone.CacheDir, 0o755)
		if err != nil {
			return fmt.Errorf("create rclone cache directory: %w", err)
		}
		args = append(args, "--cache-dir", cacheDir)
	}
	command := m.command
	if command == nil {
		command = exec.Command
	}
	cmd := command("rclone", args...)

	// Route rclone output through lumberjack for rotation
	cmd.Stdout = rotatingLog
	cmd.Stderr = rotatingLog

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start rclone: %w", err)
	}
	processCtx, processCancel := context.WithCancel(m.ctx)
	processDone := make(chan struct{})
	m.cmd = cmd
	m.processCancel = processCancel
	m.processDone = processDone
	m.serverReady.Store(false)
	m.serverStarted.Store(true)

	// Exactly one goroutine owns Wait for this process. Initialization runs
	// separately so Stop can await the same completion signal without calling
	// Wait a second time.
	go m.monitorProcess(cmd, processDone, processCancel)
	go m.initializeProcess(processCtx, processDone)
	return nil
}

func (m *Manager) initializeProcess(ctx context.Context, processDone <-chan struct{}) {
	defer func() {
		if r := recover(); r != nil {
			m.logger.Error().Interface("panic", r).Msg("Panic while initializing rclone RC server")
		}
	}()

	if err := m.waitForServer(ctx, processDone); err != nil {
		if ctx.Err() == nil {
			m.logger.Error().Err(err).Msg("Client RC server did not become ready")
		}
		return
	}

	m.processMu.Lock()
	if m.processDone != processDone || !m.serverStarted.Load() {
		m.processMu.Unlock()
		return
	}
	m.serverReady.Store(true)
	m.processMu.Unlock()

	if err := m.startMount(ctx); err != nil {
		if ctx.Err() == nil {
			m.logger.Error().Err(err).Msg("Failed to mount rclone filesystem")
		}
	} else {
		m.logger.Info().Msg("Successfully mounted rclone filesystem")
	}
}

func (m *Manager) monitorProcess(cmd *exec.Cmd, processDone chan struct{}, processCancel context.CancelFunc) {
	err := cmd.Wait()
	processCancel()

	expected := false
	m.processMu.Lock()
	current := m.cmd == cmd && m.processDone == processDone
	if current {
		m.cmd = nil
		m.processDone = nil
		m.processCancel = nil
		expected = m.ctx.Err() != nil
		// Serialize the unavailable snapshot with mount and recovery work. If an
		// RC mount request was already in flight when rclone exited, its result is
		// published first and this terminal state wins.
		m.mountMu.Lock()
		m.updateMountInfo(func(info *MountInfo) {
			info.Mounted = false
			info.MountedAt = ""
			if expected {
				info.Error = ""
			} else if err != nil {
				info.Error = fmt.Sprintf("rclone RC server exited: %v", err)
			} else {
				info.Error = "rclone RC server exited unexpectedly"
			}
		})
		// Publish the stopped state only after the mount snapshot is unavailable.
		// Callers that observe serverStarted=false can then rely on MountInfo
		// already reflecting the same process transition.
		m.serverReady.Store(false)
		m.serverStarted.Store(false)
		m.mountMu.Unlock()
	}
	m.processMu.Unlock()

	if current {
		switch {
		case expected:
			m.logger.Info().Msg("Client RC server stopped")
		case err == nil:
			m.logger.Error().Msg("Client RC server exited unexpectedly")
		case errors.Is(err, context.Canceled):
			m.logger.Warn().Err(err).Msg("Client RC server context was canceled unexpectedly")
		case WasHardTerminated(err):
			m.logger.Error().Err(err).Msg("Client RC server was hard-terminated unexpectedly")
		default:
			m.logger.Error().Err(err).Msg("Client RC server exited unexpectedly")
		}
	}
	close(processDone)
}

// Stop stops the rclone RC server and unmounts all mounts
func (m *Manager) Stop() error {
	m.logger.Info().Msg("Stopping rclone RC server")
	// Stop readiness probing and mount monitoring before taking the mount lock.
	m.cancel()
	m.processMu.Lock()
	cmd := m.cmd
	processDone := m.processDone
	if m.processCancel != nil {
		m.processCancel()
	}
	m.processMu.Unlock()
	m.serverReady.Store(false)

	// The manager context is canceled above so the health monitor exits, but
	// unmount still needs a short independent window to reach the RC server.
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	m.stopMount(stopCtx)
	stopCancel()

	if cmd != nil && cmd.Process != nil && processDone != nil {
		// Try graceful shutdown first
		if err := cmd.Process.Signal(os.Interrupt); err != nil && !errors.Is(err, os.ErrProcessDone) {
			if killErr := cmd.Process.Kill(); killErr != nil && !errors.Is(killErr, os.ErrProcessDone) {
				return killErr
			}
		}

		graceTimer := time.NewTimer(2 * time.Second)
		select {
		case <-processDone:
			if !graceTimer.Stop() {
				select {
				case <-graceTimer.C:
				default:
				}
			}
		case <-graceTimer.C:
			if err := cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
				return err
			}
			select {
			case <-processDone:
			case <-time.After(5 * time.Second):
				return fmt.Errorf("timed out waiting for rclone process cleanup")
			}
		}
	}

	m.serverReady.Store(false)
	m.serverStarted.Store(false)
	m.logger.Info().Msg("Client RC server stopped")
	return nil
}

func (m *Manager) getMountInfo() *MountInfo {
	return m.info.Load()
}

// updateMountInfo publishes an immutable snapshot. Readers can safely retain
// the pointer returned by getMountInfo while health and recovery update state.
func (m *Manager) updateMountInfo(update func(*MountInfo)) bool {
	for {
		current := m.info.Load()
		if current == nil {
			return false
		}
		next := *current
		update(&next)
		if m.info.CompareAndSwap(current, &next) {
			return true
		}
	}
}

func (m *Manager) IsMounted() bool {
	info := m.getMountInfo()
	return info != nil && info.Mounted
}

// Start creates the mount using rclone RC
func (m *Manager) startMount(ctx context.Context) error {
	m.mountMu.Lock()
	defer m.mountMu.Unlock()

	// Check if already mounted
	if m.IsMounted() {
		m.logger.Info().Msg("Mount is already mounted")
		return nil
	}

	// Try to ping rcd
	if err := m.client.Ping(ctx); err != nil {
		return fmt.Errorf("rclone RC server is not reachable: %w", err)
	}

	if err := m.mountWithRetry(ctx, 3); err != nil {
		m.logger.Error().Err(err).Msg("Mount operation failed")
		return err
	}
	go m.MonitorMounts(ctx)
	return nil
}

func (m *Manager) stopMount(ctx context.Context) {
	m.mountMu.Lock()
	defer m.mountMu.Unlock()

	if !m.IsMounted() {
		m.logger.Info().Msgf("Mount is not mounted, skipping unmount")
		return
	}

	m.logger.Info().Msg("Unmounting via RC")

	if err := m.unmount(ctx); err != nil {
		m.logger.Error().Err(err).Msg("Failed to unmount rclone filesystem")
		return
	}
	if info := m.getMountInfo(); info != nil {
		m.logger.Info().Str("mount_path", info.LocalPath).Msg("Successfully unmounted rclone filesystem")
	}
}

// IsReady returns true only when both the RC control plane and the filesystem
// mount are ready. An available RC server without a mounted data path must not
// satisfy application readiness.
func (m *Manager) IsReady() bool {
	return m.serverReady.Load() && m.IsMounted()
}

// Refresh refreshes directories in the VFS cache
func (m *Manager) Refresh(dirs []string) error {
	mountInfo := m.getMountInfo()
	if mountInfo == nil || !mountInfo.Mounted {
		return fmt.Errorf("mount is not mounted")
	}

	if err := m.client.Refresh(context.Background(), dirs, FSName); err != nil {
		m.logger.Error().Err(err).
			Msg("Failed to refresh directory")
		return fmt.Errorf("failed to refresh directory %s : %w", dirs, err)
	}
	return nil
}

func (m *Manager) GetLogger() zerolog.Logger {
	return m.logger
}

func (m *Manager) Type() string {
	return "rclone"
}

// waitForServer waits for the RC server to become available. A failed probe
// sequence is an error; callers must never publish readiness after a timeout.
func (m *Manager) waitForServer(ctx context.Context, processDone <-chan struct{}) error {
	const maxAttempts = 30
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := m.client.Ping(ctx); err == nil {
			return nil
		}
		if attempt == maxAttempts {
			break
		}

		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-processDone:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return fmt.Errorf("rclone RC server exited before becoming ready")
		case <-timer.C:
		}
	}
	return fmt.Errorf("rclone RC server did not respond after %d attempts", maxAttempts)
}
