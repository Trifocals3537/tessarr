package manager

import (
	"context"
	"fmt"
	"time"
)

type LifecyclePhase string

const (
	LifecyclePhaseInitializing LifecyclePhase = "initializing"
	LifecyclePhaseStarting     LifecyclePhase = "starting"
	LifecyclePhaseMounting     LifecyclePhase = "mounting"
	LifecyclePhaseReady        LifecyclePhase = "ready"
	LifecyclePhaseDegraded     LifecyclePhase = "degraded"
	LifecyclePhaseFailed       LifecyclePhase = "failed"
	LifecyclePhaseStopping     LifecyclePhase = "stopping"
	LifecyclePhaseStopped      LifecyclePhase = "stopped"

	defaultMountReadyTimeout      = 90 * time.Second
	defaultMountReadyPollInterval = 100 * time.Millisecond
)

type lifecycleSnapshot struct {
	phase   LifecyclePhase
	message string
}

// RuntimeStatus is a credential-free snapshot suitable for public liveness
// and readiness endpoints. Detailed errors remain in structured logs.
type RuntimeStatus struct {
	Phase           LifecyclePhase `json:"phase"`
	Ready           bool           `json:"ready"`
	MountConfigured bool           `json:"mount_configured"`
	MountReady      bool           `json:"mount_ready"`
	MountType       string         `json:"mount_type,omitempty"`
	Message         string         `json:"message"`
}

func (m *Manager) setLifecyclePhase(phase LifecyclePhase, message string) {
	if m == nil {
		return
	}
	m.lifecycle.Store(&lifecycleSnapshot{phase: phase, message: message})
}

// ReadinessStatus reports effective data-plane readiness. Mount readiness is
// evaluated live so a mount that disappears after startup immediately makes
// WebDAV and the health checker unready even though the one-shot compatibility
// channel has already closed.
func (m *Manager) ReadinessStatus() RuntimeStatus {
	status := RuntimeStatus{
		Phase:   LifecyclePhaseInitializing,
		Message: "manager is initializing",
	}
	if m == nil {
		return status
	}
	if snapshot := m.lifecycle.Load(); snapshot != nil {
		status.Phase = snapshot.phase
		status.Message = snapshot.message
	}

	mount := m.mountManager
	if mount != nil {
		status.MountType = mount.Type()
		status.MountConfigured = status.MountType != "" && status.MountType != "none"
	}
	status.MountReady = !status.MountConfigured || mount.IsReady()
	status.Ready = status.Phase == LifecyclePhaseReady && status.MountReady
	if status.Phase == LifecyclePhaseReady && !status.MountReady {
		status.Phase = LifecyclePhaseDegraded
		status.Message = "media mount is not ready"
	}
	return status
}

func (m *Manager) DataReady() bool {
	return m.ReadinessStatus().Ready
}

func waitForMountReady(ctx context.Context, mount MountManager, timeout, pollInterval time.Duration) error {
	if mount == nil || mount.Type() == "none" || mount.IsReady() {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout <= 0 {
		return fmt.Errorf("mount %q is not ready", mount.Type())
	}
	if pollInterval <= 0 {
		pollInterval = defaultMountReadyPollInterval
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("mount %q did not become ready within %s", mount.Type(), timeout)
		case <-ticker.C:
			if mount.IsReady() {
				return nil
			}
		}
	}
}
