package manager

import (
	"context"
	"strings"
	"time"

	"github.com/Trifocals3537/tessarr/internal/config"
	"github.com/Trifocals3537/tessarr/pkg/storage"
	"github.com/google/uuid"
)

type repairEventOutcome string

const (
	repairEventHealthy    repairEventOutcome = "healthy"
	repairEventBroken     repairEventOutcome = "broken"
	repairEventIncomplete repairEventOutcome = "incomplete"
)

// RepairEventQueueStatus is a secret-free process-lifetime snapshot of the
// event-driven health reconciler. Pending work remains bounded; durable dirty
// health records are retained when admission or a probe cannot complete.
type RepairEventQueueStatus struct {
	Running    bool   `json:"running"`
	Pending    int    `json:"pending"`
	Current    string `json:"current,omitempty"`
	Processed  uint64 `json:"processed"`
	Healthy    uint64 `json:"healthy"`
	Broken     uint64 `json:"broken"`
	Incomplete uint64 `json:"incomplete"`
	Coalesced  uint64 `json:"coalesced"`
	Dropped    uint64 `json:"dropped"`
}

type repairEventWork struct {
	entryName  string
	generation uint64
}

// QueueDirtyEntry requests an immediate, non-destructive health recheck. The
// caller persists the dirty record first, so refusing admission never loses
// the repair signal.
func (r *Repair) QueueDirtyEntry(entryName string) bool {
	if r == nil {
		return false
	}
	entryName = strings.TrimSpace(entryName)
	if entryName == "" {
		return false
	}

	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	if !r.eventStarted {
		return false
	}
	if _, exists := r.eventQueued[entryName]; exists {
		r.eventGeneration[entryName]++
		r.eventCoalesced.Add(1)
		return false
	}
	now := time.Now()
	if r.eventNow != nil {
		now = r.eventNow()
	}
	if last, exists := r.eventLast[entryName]; exists && r.eventCooldown > 0 && now.Sub(last) < r.eventCooldown {
		r.eventCoalesced.Add(1)
		return false
	}
	capacity := r.eventCapacity
	if capacity <= 0 {
		capacity = repairEventQueueCapacity
	}
	if len(r.eventQueue) >= capacity {
		r.eventDropped.Add(1)
		return false
	}

	r.eventGeneration[entryName]++
	r.eventQueued[entryName] = struct{}{}
	r.eventQueue = append(r.eventQueue, entryName)
	select {
	case r.eventSignal <- struct{}{}:
	default:
	}
	return true
}

func (r *Repair) startEventWorker(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.reserveRun(); err != nil {
		return err
	}
	eventCtx, cancel := context.WithCancel(ctx)

	r.eventMu.Lock()
	if r.eventStarted {
		r.eventMu.Unlock()
		cancel()
		r.releaseRun()
		return nil
	}
	r.eventStarted = true
	r.eventCancel = cancel
	r.eventMu.Unlock()

	r.runReserved(func() {
		r.eventLoop(eventCtx)
	})
	r.enqueuePersistedDirty()
	return nil
}

func (r *Repair) stopEventWorker() {
	if r == nil {
		return
	}
	r.eventMu.Lock()
	cancel := r.eventCancel
	r.eventCancel = nil
	r.eventStarted = false
	r.eventCurrent = ""
	r.eventQueue = nil
	clear(r.eventQueued)
	clear(r.eventGeneration)
	r.eventMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (r *Repair) enqueuePersistedDirty() {
	if r == nil || r.manager == nil || r.manager.storage == nil {
		return
	}
	_ = r.manager.storage.ForEachEntryHealth(func(health *storage.EntryHealth) error {
		if health != nil && health.Dirty {
			r.QueueDirtyEntry(health.EntryName)
		}
		return nil
	})
}

func (r *Repair) eventLoop(ctx context.Context) {
	defer func() {
		r.eventMu.Lock()
		r.eventStarted = false
		r.eventCancel = nil
		r.eventCurrent = ""
		r.eventQueue = nil
		clear(r.eventQueued)
		clear(r.eventGeneration)
		r.eventMu.Unlock()
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.eventSignal:
		}

		for {
			work, ok := r.nextDirtyEvent()
			if !ok {
				break
			}
			startedAt := time.Now()
			outcome := r.processDirtyEvent(ctx, work.entryName)
			outcome = r.finishDirtyEvent(work, outcome)
			status := r.eventQueueStatus()
			logEvent := r.logger.Debug()
			if outcome != repairEventHealthy {
				logEvent = r.logger.Warn()
			}
			logEvent.
				Str("entry", work.entryName).
				Str("outcome", string(outcome)).
				Dur("latency", time.Since(startedAt)).
				Int("pending", status.Pending).
				Msg("Event-driven health recheck completed")
			if ctx.Err() != nil {
				return
			}
		}
	}
}

func (r *Repair) nextDirtyEvent() (repairEventWork, bool) {
	r.eventMu.Lock()
	defer r.eventMu.Unlock()
	if len(r.eventQueue) == 0 {
		return repairEventWork{}, false
	}
	entryName := r.eventQueue[0]
	copy(r.eventQueue, r.eventQueue[1:])
	r.eventQueue = r.eventQueue[:len(r.eventQueue)-1]
	r.eventCurrent = entryName
	return repairEventWork{entryName: entryName, generation: r.eventGeneration[entryName]}, true
}

func (r *Repair) processDirtyEvent(ctx context.Context, entryName string) repairEventOutcome {
	r.eventProbeMu.Lock()
	defer r.eventProbeMu.Unlock()
	if ctx.Err() != nil {
		return repairEventIncomplete
	}

	timeout := r.eventProbeTimeout
	if timeout <= 0 {
		timeout = repairEventProbeTimeout
	}
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if r.eventProcess != nil {
		return r.eventProcess(probeCtx, entryName)
	}
	return r.reconcileDirtyEntry(probeCtx, entryName)
}

func (r *Repair) reconcileDirtyEntry(ctx context.Context, entryName string) repairEventOutcome {
	if r.manager == nil || r.manager.storage == nil {
		return repairEventIncomplete
	}
	previous, _ := r.manager.storage.GetEntryHealth(entryName)
	dirtyReason := "event_recheck_incomplete"
	var protocol config.Protocol
	if previous != nil {
		if previous.DirtyReason != "" {
			dirtyReason = previous.DirtyReason
		}
		protocol = previous.Protocol
	}

	health := r.probeEntry(
		ctx,
		"event-"+uuid.NewString(),
		&candidate{name: entryName},
		newHealCache(),
		RepairRunOptions{},
		false,
	)
	if ctx.Err() != nil || health == nil || health.Status == storage.HealthUnknown ||
		health.Status == storage.HealthRepairing {
		_ = r.manager.storage.MarkEntryDirtyChecked(entryName, protocol, dirtyReason)
		return repairEventIncomplete
	}
	if health.Status == storage.HealthBroken {
		return repairEventBroken
	}
	return repairEventHealthy
}

func (r *Repair) finishDirtyEvent(work repairEventWork, outcome repairEventOutcome) repairEventOutcome {
	now := time.Now()
	if r.eventNow != nil {
		now = r.eventNow()
	}
	r.eventMu.Lock()
	r.eventCurrent = ""
	redirtied := r.eventGeneration[work.entryName] != work.generation
	delete(r.eventQueued, work.entryName)
	delete(r.eventGeneration, work.entryName)
	if len(r.eventLast) >= repairEventHistoryCapacity {
		for key, last := range r.eventLast {
			if r.eventCooldown <= 0 || now.Sub(last) >= r.eventCooldown {
				delete(r.eventLast, key)
			}
			if len(r.eventLast) < repairEventHistoryCapacity {
				break
			}
		}
	}
	if len(r.eventLast) < repairEventHistoryCapacity {
		r.eventLast[work.entryName] = now
	}
	r.eventMu.Unlock()
	if redirtied {
		outcome = repairEventIncomplete
		r.preserveConcurrentDirtySignal(work.entryName)
	}

	r.eventProcessed.Add(1)
	switch outcome {
	case repairEventHealthy:
		r.eventHealthy.Add(1)
	case repairEventBroken:
		r.eventBroken.Add(1)
	default:
		r.eventIncomplete.Add(1)
	}
	return outcome
}

func (r *Repair) preserveConcurrentDirtySignal(entryName string) {
	if r == nil || r.manager == nil || r.manager.storage == nil {
		return
	}
	health, _ := r.manager.storage.GetEntryHealth(entryName)
	var protocol config.Protocol
	if health != nil {
		protocol = health.Protocol
	}
	_ = r.manager.storage.MarkEntryDirtyChecked(entryName, protocol, "read_failure_during_recheck")
}

func (r *Repair) eventQueueStatus() RepairEventQueueStatus {
	if r == nil {
		return RepairEventQueueStatus{}
	}
	r.eventMu.Lock()
	status := RepairEventQueueStatus{
		Running: r.eventStarted,
		Pending: len(r.eventQueue),
		Current: r.eventCurrent,
	}
	r.eventMu.Unlock()
	status.Processed = r.eventProcessed.Load()
	status.Healthy = r.eventHealthy.Load()
	status.Broken = r.eventBroken.Load()
	status.Incomplete = r.eventIncomplete.Load()
	status.Coalesced = r.eventCoalesced.Load()
	status.Dropped = r.eventDropped.Load()
	return status
}
