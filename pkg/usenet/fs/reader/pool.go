package reader

import (
	"errors"
	"sync"

	"github.com/Trifocals3537/tessarr/internal/buffer"
	"github.com/Trifocals3537/tessarr/internal/config"
)

// Pools owns the shared stream-buffer budget for one Usenet service run.
// Scoping it to the service makes config restarts apply the new budget and
// ensures old mmap allocations are released before the replacement starts.
type Pools struct {
	buffers *buffer.Pool
	mu      sync.Mutex
	closed  bool
	caches  map[*SegmentCache]struct{}
}

func NewPools(memoryBudget int64) *Pools {
	return &Pools{
		buffers: buffer.NewPool(buffer.PoolConfig{
			Name:         "usenet",
			MemoryBudget: memoryBudget,
		}),
		caches: make(map[*SegmentCache]struct{}),
	}
}

func defaultPools() *Pools {
	return NewPools(config.Get().Usenet.BufferMemoryBytes())
}

// Stats reports allocated buffer bytes. These are not process RSS values.
func (p *Pools) Stats() map[string]any {
	if p == nil {
		return map[string]any{}
	}
	blocks := p.buffers.Stats()
	return map[string]any{
		"memory_in_use":    blocks.MemoryInUse,
		"memory_allocated": blocks.MemoryAllocated,
		"memory_budget":    blocks.MemoryBudget,
		"buffers":          blocks.Buffers,
	}
}

func (p *Pools) unregister(cache *SegmentCache) {
	p.mu.Lock()
	delete(p.caches, cache)
	p.mu.Unlock()
}

// Close releases this service run's buffers. Call after its readers stop.
func (p *Pools) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	caches := make([]*SegmentCache, 0, len(p.caches))
	for cache := range p.caches {
		caches = append(caches, cache)
	}
	p.mu.Unlock()
	var closeErr error
	for _, cache := range caches {
		closeErr = errors.Join(closeErr, cache.Close())
	}
	if err := p.buffers.Close(); err != nil && !errors.Is(err, buffer.ErrClosed) {
		closeErr = errors.Join(closeErr, err)
	}
	return closeErr
}
