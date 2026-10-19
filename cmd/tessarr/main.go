package tessarr

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/internal/logger"
	"github.com/Trifocals3537/tessarr/internal/logsafe"
	"github.com/Trifocals3537/tessarr/internal/utils"
	"github.com/Trifocals3537/tessarr/pkg/manager"
	"github.com/Trifocals3537/tessarr/pkg/mount/dfs"
	"github.com/Trifocals3537/tessarr/pkg/mount/external"
	"github.com/Trifocals3537/tessarr/pkg/mount/rclone"
	"github.com/Trifocals3537/tessarr/pkg/server"
	"github.com/Trifocals3537/tessarr/pkg/version"
	"golang.org/x/sync/errgroup"
)

var errServicesStopped = errors.New("services stopped unexpectedly")

type serviceStarter interface {
	Start(context.Context) error
}

func Start(ctx context.Context) error {
	// Start the global cached time updater to reduce time.Now() syscall overhead
	utils.StartGlobalCachedTime()
	defer utils.StopGlobalCachedTime()

	recoveredRestart, err := config.PrepareRestartStartup()
	if err != nil {
		return fmt.Errorf("prepare configuration restart: %w", err)
	}

	if umaskStr := os.Getenv("UMASK"); umaskStr != "" {
		umask, err := strconv.ParseInt(umaskStr, 8, 32)
		if err != nil {
			return fmt.Errorf("invalid UMASK value: %s", umaskStr)
		}
		SetUmask(int(umask))
	}

	restartCh := make(chan struct{}, 1)
	restartFunc := func() {
		select {
		case restartCh <- struct{}{}:
		default:
		}
	}

	var mgr *manager.Manager

	// Create the logger path if it doesn't exist
	for {
		cfg := config.Get()
		if err := validateDeploymentConfig(cfg); err != nil {
			return err
		}
		if err := logger.SetLevel(cfg.LogLevel); err != nil {
			return fmt.Errorf("configure log level: %w", err)
		}
		if mgr == nil {
			mgr = manager.New()
		}
		_log := logger.Default()
		if recoveredRestart {
			_log.Warn().Msg("Restored the last-known-good configuration after an interrupted restart")
			recoveredRestart = false
		}

		fmt.Printf(`
+-------------------------------------------------------+
|                       TESSARR                         |
|               Many sources. One library.              |
+-------------------------------------------------------+
|  Version: %-43s |
+-------------------------------------------------------+
|  Log Level: %-41s |
+-------------------------------------------------------+
`, logsafe.Text(fmt.Sprint(version.GetInfo())), logsafe.Text(cfg.LogLevel))

		// Initialize services
		mountMgr, err := createMountManager(mgr, cfg)
		if err != nil {
			return fmt.Errorf("initialize mount manager: %w", err)
		}
		mgr.SetMountManager(mountMgr)
		srv := server.New(mgr)

		srv.SetRestartFunc(restartFunc)
		svcCtx, cancelSvc := context.WithCancel(ctx)

		resetFunc := func() error {
			if err := config.MarkRestartApplying(); err != nil {
				return fmt.Errorf("prepare restart rollback: %w", err)
			}
			config.Reset()
			if err := logger.SetLevel(config.Get().LogLevel); err != nil {
				return fmt.Errorf("apply restarted log level: %w", err)
			}
			// Stop manager to reset ready channel and cleanup resources
			if err := mgr.Reset(); err != nil {
				return err
			}
			// refresh GC
			runtime.GC()
			return nil
		}

		shutdownFunc := func() {
			// Stop manager to cleanup all resources including mounts
			if err := mgr.Stop(); err != nil {
				_log.Warn().Err(err).Msg("Failed to stop manager during shutdown")
			}
			config.Reset()
			// refresh GC
			runtime.GC()
		}

		serviceDone := make(chan error, 1)
		serviceStopped := make(chan struct{})
		go func(ctx context.Context) {
			serviceDone <- startServices(ctx, mgr, srv)
			close(serviceStopped)
		}(svcCtx)
		restartCommitErr := make(chan error, 1)
		go func() {
			if waitForServiceReadiness(svcCtx, mgr.IsReady(), srv.Ready(), serviceStopped) {
				if err := config.CommitRestart(); err != nil {
					restartCommitErr <- err
				}
			}
		}()

		select {
		case <-ctx.Done():
			// graceful shutdown
			cancelSvc() // propagate to services
			if err := <-serviceDone; err != nil {
				// A forced HTTP close can return before a stuck handler exits.
				// Do not close manager storage underneath any such handler;
				// return an error and let the process terminate as one unit.
				return fmt.Errorf("services did not stop cleanly: %w", err)
			}
			shutdownFunc() // cleanup all resources including mounts
			_log.Info().Msg("Tessarr has been stopped gracefully.")
			return nil

		case <-restartCh:
			cancelSvc() // tell existing services to shut down
			_log.Info().Msg("Restarting Tessarr...")
			restarted, err := finishRestart(ctx, serviceDone, resetFunc, shutdownFunc)
			if err != nil {
				_, rollbackErr := config.RollbackApplyingRestart()
				return errors.Join(
					fmt.Errorf("restart aborted: %w", err),
					wrapRestartRollbackError(rollbackErr),
				)
			}
			if !restarted {
				return nil
			}

			_log.Info().Msg("Tessarr has been restarted.")

		case err := <-serviceDone:
			cancelSvc()
			if err != nil {
				// An error may represent a forced HTTP close with handlers
				// still unwinding. Let the process supervisor restart from a
				// clean address space instead of closing shared resources
				// underneath those handlers.
				_, rollbackErr := config.RollbackApplyingRestart()
				return errors.Join(err, wrapRestartRollbackError(rollbackErr))
			}
			shutdownFunc()
			if ctx.Err() != nil {
				return nil
			}
			return errServicesStopped

		case err := <-restartCommitErr:
			cancelSvc()
			serviceErr := <-serviceDone
			_, rollbackErr := config.RollbackApplyingRestart()
			return errors.Join(
				fmt.Errorf("commit configuration restart: %w", err),
				serviceErr,
				wrapRestartRollbackError(rollbackErr),
			)
		}
	}
}

func waitForServiceReadiness(ctx context.Context, managerReady, httpReady, serviceStopped <-chan struct{}) bool {
	for managerReady != nil || httpReady != nil {
		select {
		case <-ctx.Done():
			return false
		case <-serviceStopped:
			return false
		case <-managerReady:
			managerReady = nil
		case <-httpReady:
			httpReady = nil
		}
	}
	// A concurrently failed service wins over readiness, even when both
	// channels were closed before this goroutine was scheduled.
	select {
	case <-ctx.Done():
		return false
	case <-serviceStopped:
		return false
	default:
		return true
	}
}

func wrapRestartRollbackError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("restore last-known-good configuration: %w", err)
}

func validateDeploymentConfig(cfg *config.Config) error {
	if err := cfg.ValidateDeployment(); err != nil {
		return fmt.Errorf("deployment safety check: %w", err)
	}
	return nil
}

// finishRestart only resets shared storage after every service reports a clean
// stop. An HTTP shutdown timeout may leave handler goroutines running even
// after their connections are force-closed, so an error must abort the process
// instead of reopening storage underneath those handlers.
func finishRestart(
	ctx context.Context,
	serviceDone <-chan error,
	resetFunc func() error,
	shutdownFunc func(),
) (bool, error) {
	if err := <-serviceDone; err != nil {
		return false, err
	}
	if ctx.Err() != nil {
		shutdownFunc()
		return false, nil
	}
	if err := resetFunc(); err != nil {
		return false, fmt.Errorf("reset services: %w", err)
	}
	return true, nil
}

func createMountManager(mgr *manager.Manager, cfg *config.Config) (manager.MountManager, error) {
	switch cfg.Mount.Type {
	case config.MountTypeRclone:
		return rclone.NewManager(mgr)
	case config.MountTypeDFS:
		return dfs.NewManager(mgr), nil
	case config.MountTypeExternalRclone:
		return external.NewManager(mgr), nil
	default:
		return manager.NewStubMountManager(), nil
	}
}

func startServices(ctx context.Context, managerService, httpService serviceStarter) error {
	group, serviceCtx := errgroup.WithContext(ctx)

	start := func(name string, service serviceStarter) {
		group.Go(func() error {
			return startService(serviceCtx, name, service)
		})
	}

	// The HTTP service remains active until cancellation. Manager.Start performs
	// initialization and then returns while its workers continue on serviceCtx.
	start("HTTP service", httpService)
	start("manager", managerService)

	err := group.Wait()
	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return nil
	}
	return errServicesStopped
}

func startService(ctx context.Context, name string, service serviceStarter) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%s panicked: %v\n%s", name, recovered, debug.Stack())
		}
	}()

	if err := service.Start(ctx); err != nil {
		return fmt.Errorf("%s failed: %w", name, err)
	}
	return nil
}
