package manager

import (
	"context"

	"github.com/Trifocals3537/tessarr/pkg/arr"
	debrid "github.com/Trifocals3537/tessarr/pkg/debrid/common"
	debridTypes "github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/Trifocals3537/tessarr/pkg/usenet"
	"github.com/go-co-op/gocron/v2"
	"github.com/puzpuzpuz/xsync/v4"
)

func (m *Manager) SetMountManager(mountMgr MountManager) {
	m.mountManager = mountMgr
}

// Repair returns the repair service. It is created during init() so callers
// can rely on a non-nil value once the manager has been constructed.
func (m *Manager) Repair() *Repair {
	return m.repair
}

// Strm returns the mountless-library reconciler.
func (m *Manager) Strm() *Strm {
	return m.strm
}

func (m *Manager) Scheduler() gocron.Scheduler {
	return m.scheduler
}

// Migrator returns the migrator instance
func (m *Manager) Migrator() *Migrator {
	return m.migrator
}

// Arr returns the Arr storage instance
func (m *Manager) Arr() *arr.Storage {
	return m.arr
}

func (m *Manager) Queue() *Queue {
	return m.queue
}

func (m *Manager) JobQueue() *JobQueue {
	return m.jobQueue
}

func (m *Manager) Clients() *xsync.Map[string, debrid.Client] {
	return m.clients
}

func (m *Manager) MountManager() MountManager {
	return m.mountManager
}

func (m *Manager) Storage() *storage.Storage {
	return m.storage
}

func (m *Manager) Context() context.Context {
	return m.ctx
}

func (m *Manager) Usenet() *usenet.Usenet {
	return m.usenet
}

// GetDebridSpeedTestResult returns stored speed test result for a specific debrid provider
func (m *Manager) GetDebridSpeedTestResult(provider string) (debridTypes.SpeedTestResult, bool) {
	return m.debridSpeedTestResults.Load(provider)
}
