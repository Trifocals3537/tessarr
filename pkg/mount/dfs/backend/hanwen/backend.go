//go:build linux || (darwin && amd64)

package hanwen

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/mount/dfs/backend"
	"github.com/Trifocals3537/tessarr/pkg/mount/dfs/config"
	"github.com/Trifocals3537/tessarr/pkg/mount/dfs/vfs"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/rs/zerolog"
)

const (
	// ReadTimeout is the maximum time a single read operation can take
	// Increased to 120s to handle slow debrid/CDN connections
	ReadTimeout  = 120 * time.Second
	AttrTimeout  = 30 * time.Second
	EntryTimeout = 1 * time.Second
)

func init() {
	backend.Register(backend.Hanwen, NewBackend)
}

// Backend implements the hanwen/go-fuse backend
type Backend struct {
	config      *config.FuseConfig
	logger      zerolog.Logger
	server      *fuse.Server
	ready       atomic.Bool
	unmountFunc func(ctx context.Context)
	unmountOnce sync.Once
	root        *Dir
	vfs         *vfs.Manager
}

// NewBackend creates a new hanwen backend
func NewBackend(vfs *vfs.Manager, config *config.FuseConfig) (backend.Backend, error) {
	if vfs == nil {
		return nil, fmt.Errorf("VFS manager is required")
	}
	if config == nil {
		return nil, fmt.Errorf("FUSE config is required")
	}
	now := utils.Now()
	log := logger.New("hanwen-backend")
	// One shared rate-limited logger for the whole mount. Files/Dirs reference
	// it instead of allocating their own xsync map per inode — dedup keys are
	// already unique per inode so a shared map gives identical behaviour.
	rl := logger.NewRateLimitedLogger(logger.WithLogger(log))
	root := NewDir(vfs, "", LevelRoot, uint64(now.Unix()), config, log, rl)
	return &Backend{
		config: config,
		logger: log,
		root:   root,
		vfs:    vfs,
	}, nil
}

func (b *Backend) mountOptions() fuse.MountOptions {
	return fuse.MountOptions{
		FsName:               "tessarr",
		Debug:                false,
		Name:                 "tessarr",
		DisableXAttrs:        true,
		IgnoreSecurityLabels: true,
		MaxWrite:             1024 * 1024,
		AllowOther:           true,
		// Keep one bad handler request from taking down the mount, while routing
		// the panic and stack through Tessarr's normal structured logs.
		PanicHandler: func(p any) fuse.Status {
			b.logger.Error().Any("panic", p).Bytes("stack", debug.Stack()).Msg("FUSE handler panic")
			return fuse.EIO
		},
	}
}

func normalizeMountPath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("mount path is empty")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve mount path: %w", err)
	}
	abs = filepath.Clean(abs)
	if filepath.Dir(abs) == abs {
		return "", fmt.Errorf("refusing to use filesystem root as mount path: %s", abs)
	}
	return abs, nil
}

// Mount mounts the filesystem using hanwen/go-fuse
func (b *Backend) Mount(ctx context.Context) error {
	if b.config == nil {
		return fmt.Errorf("FUSE config is not initialized")
	}
	if b.root == nil {
		return fmt.Errorf("root node is not initialized")
	}
	if b.vfs == nil {
		return fmt.Errorf("VFS manager is not initialized")
	}
	mountPath, err := normalizeMountPath(b.config.MountPath)
	if err != nil {
		return err
	}
	b.config.MountPath = mountPath
	b.unmountOnce = sync.Once{}

	if err := os.MkdirAll(mountPath, 0755); err != nil {
		return fmt.Errorf("create mount point: %w", err)
	}
	// Recover a stale mount from a previous process without spawning unmount
	// helpers during every normal startup.
	mounted, mountCheckErr := mountPointActive(mountPath)
	if mountCheckErr != nil {
		b.logger.Warn().Err(mountCheckErr).Msg("Could not inspect mountpoint; attempting stale-mount cleanup")
		b.forceUnmount(ctx)
	} else if mounted {
		b.logger.Warn().Msg("Existing FUSE mount detected; attempting stale-mount cleanup")
		b.forceUnmount(ctx)
	}

	mountOpt := b.mountOptions()

	var opt []string

	opt = append(opt, "default_permissions")

	if runtime.GOOS == "darwin" {
		opt = append(opt, "volname=tessarr")
		opt = append(opt, "noapplexattr")
		opt = append(opt, "noappledouble")
	}

	mountOpt.Options = opt

	// Configure FUSE options
	// Use short entry timeout (1s) to ensure new files appear quickly
	entryTimeout := EntryTimeout
	attrTimeout := AttrTimeout
	opts := &fs.Options{
		AttrTimeout:  &attrTimeout,
		EntryTimeout: &entryTimeout,
		MountOptions: mountOpt,
		UID:          b.config.UID,
		GID:          b.config.GID,
	}

	// Start timer before creating NodeFS - adjust timeout duration as needed
	mountCtx, cancel := context.WithTimeout(ctx, b.config.DaemonTimeout)
	defer cancel()

	// Channel to receive the result of fs.Mount
	type fsResult struct {
		server *fuse.Server
		err    error
	}
	fsResultChan := make(chan fsResult, 1)

	// Run fs.Mount in a goroutine
	go func() {
		server, err := fs.Mount(b.config.MountPath, b.root, opts)
		fsResultChan <- fsResult{server: server, err: err}
	}()

	var server *fuse.Server
	select {
	case result := <-fsResultChan:
		server = result.server
		if result.err != nil {
			return fmt.Errorf("failed to create mount: %w", result.err)
		}
	case <-mountCtx.Done():
		b.ready.Store(false)
		// If fs.Mount later succeeds, nobody is left to read the result —
		// unmount the orphaned server instead of leaking a live mount.
		go func() {
			if result := <-fsResultChan; result.server != nil {
				_ = result.server.Unmount()
			}
		}()
		return fmt.Errorf("timeout creating mount: %w", mountCtx.Err())
	}

	b.server = server

	// Now wait for the mount to be ready with the same timeout context
	b.logger.Info().
		Str("mount_path", b.config.MountPath).
		Msg("Waiting for mount to be ready")

	waitChan := make(chan error, 1)
	go func() {
		waitChan <- server.WaitMount()
	}()

	select {
	case err := <-waitChan:
		if err != nil {
			_ = server.Unmount() // cleanup on error
			return fmt.Errorf("failed to wait for mount: %w", err)
		}
	case <-mountCtx.Done():
		_ = server.Unmount() // cleanup on timeout
		return fmt.Errorf("timeout waiting for mount to be ready: %w", mountCtx.Err())
	}

	umount := func(ctx context.Context) {
		b.ready.Store(false)
		b.logger.Info().Msg("Unmounting filesystem")

		// Server.Unmount is synchronous and only returns after its FUSE event
		// loops exit. Keep it behind a channel so the caller's shutdown deadline
		// is still honored.
		done := make(chan error, 1)

		go func() {
			// Close VFS manager
			if b.vfs != nil {
				if err := b.vfs.Close(); err != nil {
					b.logger.Warn().Err(err).Msg("Failed to close VFS")
				}
			}

			done <- server.Unmount()
		}()

		// Wait for unmount to complete or context timeout
		select {
		case err := <-done:
			if err != nil {
				b.logger.Warn().Err(err).Msg("Graceful FUSE unmount failed, attempting force unmount")
				b.forceUnmount(ctx)
				return
			}
			b.logger.Info().Msg("Filesystem unmounted successfully")
		case <-ctx.Done():
			b.logger.Warn().Err(ctx.Err()).Msg("Unmount timed out, forcing unmount")
			// The caller's context is already done, so give the bounded fallback
			// its own window instead of passing an immediately-cancelled context.
			forceCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			b.forceUnmount(forceCtx)
			cancel()
		}
	}

	b.unmountFunc = umount
	b.ready.Store(true)
	return nil
}

// Unmount unmounts the filesystem
func (b *Backend) Unmount(ctx context.Context) error {
	b.logger.Info().Msg("Unmounting hanwen backend")
	b.ready.Store(false)
	b.unmountOnce.Do(func() {
		if b.unmountFunc != nil {
			// unmountFunc already closes the VFS manager as part of its teardown;
			// don't close it a second time here.
			b.unmountFunc(ctx)
			return
		}
		// Mount never completed: force-unmount and close the VFS ourselves.
		b.forceUnmount(ctx)
		if b.vfs != nil {
			if err := b.vfs.Close(); err != nil {
				b.logger.Warn().Err(err).Msg("Failed to close VFS")
			}
		}
	})
	return nil
}

// WaitReady waits for the mount to be ready
func (b *Backend) WaitReady(ctx context.Context) error {
	if b.server == nil {
		return fmt.Errorf("server not initialized")
	}
	return b.server.WaitMount()
}

// IsReady returns true if the mount is ready
func (b *Backend) IsReady() bool {
	return b.ready.Load()
}

// Type returns the backend type
func (b *Backend) Type() backend.Type {
	return backend.Hanwen
}

func (b *Backend) Refresh(dir string) {
	// Refresh the root dir first
	if b.root != nil {
		b.root.Refresh()
		if dir != "" {
			b.root.RefreshChild(dir)
		}
	}
}

// forceUnmount attempts to force unmount a path using system commands
func (b *Backend) forceUnmount(ctx context.Context) {
	if b.config == nil {
		b.logger.Warn().Msg("Skipping force unmount without FUSE config")
		return
	}
	mountPath, err := normalizeMountPath(b.config.MountPath)
	if err != nil {
		b.logger.Warn().Err(err).Msg("Skipping force unmount for unsafe mount path")
		return
	}
	methods := [][]string{
		{"fusermount3", "-uz", mountPath},
		{"fusermount", "-uz", mountPath},
		{"umount", mountPath},
		{"umount", "-l", mountPath}, // lazy unmount
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	for _, method := range methods {
		if err := b.tryUnmountCommand(ctx, method...); err == nil {
			return
		}
		if ctx.Err() != nil {
			b.logger.Warn().Err(ctx.Err()).Msg("Unmount command timed out")
			return
		}
	}
}

// tryUnmountCommand tries to run an unmount command
func (b *Backend) tryUnmountCommand(ctx context.Context, args ...string) error {
	if len(args) == 0 {
		return fmt.Errorf("no command provided")
	}

	cmd := exec.CommandContext(ctx, args[0], args[1:]...)
	return cmd.Run()
}
