package rclone

import (
	"context"
	"fmt"
	"time"
)

const mountRecoveryDelay = time.Second

// RecoverMount attempts to recover a failed mount
func (m *Manager) RecoverMount(ctx context.Context) error {
	return m.recoverMountAfter(ctx, mountRecoveryDelay)
}

func (m *Manager) recoverMountAfter(ctx context.Context, delay time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}

	m.mountMu.Lock()
	defer m.mountMu.Unlock()

	mountInfo := m.getMountInfo()

	if mountInfo == nil {
		return fmt.Errorf("no mount info available for recovery")
	}

	m.logger.Warn().Msg("Attempting to recover mount")

	// First try to unmount cleanly
	if err := m.unmount(ctx); err != nil {
		return fmt.Errorf("failed to unmount unhealthy mount: %w", err)
	}

	// Give the OS time to release the mount point without making shutdown wait
	// for an unconditional sleep.
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// The rclone RC process is already running. Remount through that process;
	// Start would return early when serverStarted is true and falsely report a
	// successful recovery without creating the mount.
	if err := m.mountWithRetry(ctx, 3); err != nil {
		m.updateMountInfo(func(info *MountInfo) {
			info.Error = err.Error()
		})
		return fmt.Errorf("failed to recover mount: %w", err)
	}

	m.logger.Info().Msg("Successfully recovered mount")
	return nil
}

// MonitorMounts continuously monitors mount health and attempts recovery
func (m *Manager) MonitorMounts(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second) // Check every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Debug().Msg("Mount monitoring stopped")
			return
		case <-ticker.C:
			m.performMountHealthCheck(ctx)
		}
	}
}

// performMountHealthCheck checks and attempts to recover unhealthy mounts
func (m *Manager) performMountHealthCheck(ctx context.Context) {
	m.performMountHealthCheckAfter(ctx, mountRecoveryDelay)
}

func (m *Manager) performMountHealthCheckAfter(ctx context.Context, recoveryDelay time.Duration) {
	if err := m.client.CheckMountHealth(ctx, FSName); err != nil {
		if ctx.Err() != nil {
			return
		}
		m.logger.Warn().Err(err).Msg("Mount health check failed, attempting recovery")

		// Recover synchronously so the next health tick cannot stack another
		// unmount/remount transaction on top of the current one.
		if err := m.recoverMountAfter(ctx, recoveryDelay); err != nil && ctx.Err() == nil {
			m.logger.Error().Err(err).Msg("Failed to recover mount")
		}
		return
	}

	// Clear a prior transient recovery error after a healthy probe.
	m.updateMountInfo(func(info *MountInfo) {
		info.Error = ""
	})
}
