package stats

import (
	"github.com/Trifocals3537/tessarr/internal/cdntraffic"
	"github.com/Trifocals3537/tessarr/pkg/debrid/types"
	"github.com/Trifocals3537/tessarr/pkg/manager"
)

// Snapshot holds a point-in-time stats snapshot.
// Using typed structs avoids map[string]any allocations on every JSON encode.
type Snapshot struct {
	System           SystemStats                   `json:"system"`
	Debrids          []types.Stats                 `json:"debrids"`
	Mount            MountStats                    `json:"mount"`
	Usenet           map[string]any                `json:"usenet,omitempty"`
	ActiveStreams    ActiveStreamStats             `json:"active_streams"`
	CDNTraffic       cdntraffic.Stats              `json:"cdn_traffic"`
	StreamFailover   manager.StreamFailoverStats   `json:"stream_failover"`
	StreamSessions   manager.StreamSessionStats    `json:"stream_sessions"`
	TorrentAdmission manager.TorrentAdmissionStats `json:"torrent_admission"`
	Storage          StorageStats                  `json:"storage"`
	Queue            QueueStats                    `json:"queue"`
	Arrs             ArrStats                      `json:"arrs"`
	Repair           RepairStats                   `json:"repair"`
}

type SystemStats struct {
	// MemoryUsed is Go runtime memory: Sys minus HeapReleased. It excludes
	// direct mmap allocations and native library allocations.
	MemoryUsed string `json:"memory_used"`
	// ProcessRSSMB is the OS estimate of resident process memory. It includes
	// resident mmap pages and native library memory.
	ProcessRSSMB string `json:"process_rss_mb,omitempty"`
	// HeapAllocMB includes allocated objects that GC has not yet collected.
	HeapAllocMB string `json:"heap_alloc_mb"`
	// HeapInuseMB is the bytes in heap spans currently in use (HeapInuse).
	HeapInuseMB string `json:"heap_inuse_mb"`
	// HeapReleasedMB is heap memory already returned to the OS (HeapReleased).
	HeapReleasedMB string `json:"heap_released_mb"`
	// SysMB is the total address space reserved from the OS (Sys). Includes
	// released memory, so it is NOT a measure of real usage.
	SysMB         string `json:"sys_mb"`
	GCCycles      uint32 `json:"gc_cycles"`
	Goroutines    int    `json:"goroutines"`
	NumCPU        int    `json:"num_cpu"`
	OS            string `json:"os"`
	Arch          string `json:"arch"`
	GoVersion     string `json:"go_version"`
	UptimeSeconds int64  `json:"uptime_seconds"`
	Uptime        string `json:"uptime"`
	StartTime     string `json:"start_time"`
}

type MountStats struct {
	Ready   bool   `json:"ready"`
	Enabled bool   `json:"enabled"`
	Type    string `json:"type,omitempty"`
	Error   string `json:"error,omitempty"`
	// Detail holds the subsystem-specific stats (e.g. VFS counters).
	// nil when mount is not ready.
	Detail map[string]any `json:"detail,omitempty"`
}

type ActiveStreamStats struct {
	Count   int                     `json:"count"`
	Streams []*manager.ActiveStream `json:"streams"`
}

type StorageStats struct {
	DBSize       int64 `json:"db_size"`
	TotalEntries int   `json:"total_entries"`
}

type QueueStats struct {
	Pending int `json:"pending"`
	Active  int `json:"active"`
}

type ArrStats struct {
	Count int      `json:"count"`
	Names []string `json:"names"`
}

// RepairStats is the dashboard view of the repair system's state.
type RepairStats struct {
	Enabled bool           `json:"enabled"`
	Active  bool           `json:"active"`
	Health  map[string]int `json:"health,omitempty"`
}
