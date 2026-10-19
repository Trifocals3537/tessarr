package rclone

import (
	"context"
	"sync"

	json "github.com/bytedance/sonic"

	"github.com/Trifocals3537/tessarr/internal/rclone"
)

// Stats represents rclone statistics
type Stats struct {
	Type      string                   `json:"type"`
	Enabled   bool                     `json:"enabled"`
	Ready     bool                     `json:"ready"`
	Core      rclone.CoreStatsResponse `json:"core"`
	Memory    rclone.MemoryStats       `json:"memory"`
	Mount     *MountInfo               `json:"mounts"`
	Bandwidth rclone.BandwidthStats    `json:"bandwidth"`
	Version   rclone.VersionResponse   `json:"version"`
}

// Stats retrieves statistics from the rclone RC server
func (m *Manager) Stats() map[string]any {
	return m.StatsContext(context.Background())
}

// StatsContext retrieves statistics from the rclone RC server while honoring
// the caller's lifecycle and timeout.
func (m *Manager) StatsContext(ctx context.Context) map[string]any {
	empty := make(map[string]any)
	stats := &Stats{}
	stats.Ready = m.IsReady()
	stats.Enabled = true
	stats.Type = m.Type()

	var requests sync.WaitGroup
	requests.Add(4)
	go func() {
		defer requests.Done()
		if value, err := m.client.GetCoreStats(ctx); err == nil && value != nil {
			stats.Core = *value
		}
	}()
	go func() {
		defer requests.Done()
		if value, err := m.client.GetMemoryUsage(ctx); err == nil && value != nil {
			stats.Memory = *value
		}
	}()
	go func() {
		defer requests.Done()
		if value, err := m.client.GetBandwidthStats(ctx); err == nil && value != nil {
			stats.Bandwidth = *value
		}
	}()
	go func() {
		defer requests.Done()
		if value, err := m.client.GetVersion(ctx); err == nil && value != nil {
			stats.Version = *value
		}
	}()

	// Add mount infos
	mountInfo := m.getMountInfo()
	stats.Mount = mountInfo

	requests.Wait()

	// Convert to map[string]interface{}
	statsMap := make(map[string]any)
	data, err := json.Marshal(stats)
	if err != nil {
		return empty
	}
	if err := json.Unmarshal(data, &statsMap); err != nil {
		return empty
	}

	return statsMap
}
