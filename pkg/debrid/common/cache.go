package common

import (
	"context"
	"strings"
	"time"
)

// CacheState is deliberately tri-state. Provider/API failures are unknown,
// never cache misses, so callers cannot start uncached work on ambiguous data.
type CacheState string

const (
	CacheStateUnknown  CacheState = "unknown"
	CacheStateCached   CacheState = "cached"
	CacheStateUncached CacheState = "uncached"
)

type CacheEvidence struct {
	Provider   string
	InfoHash   string
	State      CacheState
	Source     string
	ObservedAt time.Time
}

// CacheAvailabilityChecker is an optional capability. Providers without a
// trustworthy preflight endpoint must omit it rather than convert failures or
// unsupported checks into false cache misses.
type CacheAvailabilityChecker interface {
	CheckCacheAvailability(context.Context, []string) map[string]CacheEvidence
}

func NewCacheEvidence(provider, infoHash string, state CacheState, source string, observedAt time.Time) CacheEvidence {
	return CacheEvidence{
		Provider:   strings.TrimSpace(provider),
		InfoHash:   strings.ToLower(strings.TrimSpace(infoHash)),
		State:      state,
		Source:     strings.TrimSpace(source),
		ObservedAt: observedAt.UTC(),
	}
}
