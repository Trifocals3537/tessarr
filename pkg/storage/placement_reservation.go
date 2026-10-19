package storage

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/Trifocals3537/tessarr/pkg/storage/hybrid"
)

var ErrPlacementCleanupReserved = errors.New("provider placement reserved for transfer cleanup; manual reconciliation required")

// A fence is durable before the first destructive call and remains afterward:
// even a successful DELETE may be followed by stale provider listings, and a
// failed request may already have deleted its target. Releasing on either result
// would allow publishing a library row pointing at an absent target. Provider
// IDs are scoped by account/provider; new IDs are unaffected. No automatic
// clearing is safe without authoritative absence/replacement evidence.
func placementReservationKey(p *ProviderEntry) string {
	return "transfer_placement_fence/" + hex.EncodeToString([]byte(strings.ToLower(p.Provider))) + "/" + hex.EncodeToString([]byte(p.ID))
}

// ReserveTransferCleanup closes the final scan/delete interval with all main
// publication paths. It takes no lifecycle lock (callers may already own one),
// and releases the shared mutex before any callback or provider call.
func (s *Storage) ReserveTransferCleanup(e *Entry) error {
	s.placementPublicationMu.Lock()
	defer s.placementPublicationMu.Unlock()
	if err := s.ValidateTransferCleanup(e); err != nil {
		return err
	}
	key := placementReservationKey(e.GetActiveProvider())
	if err := s.storageState.Put(key, []byte("reserved-v1"), nil); err != nil {
		return err
	}
	return s.storageState.Sync()
}

// Called only under placementPublicationMu, including rollback publication.
func (s *Storage) validateMainPlacementPublication(e *Entry) error {
	for _, p := range e.Providers {
		if p == nil || p.Provider == "" || p.ID == "" {
			continue
		}
		_, err := s.storageState.Get(placementReservationKey(p))
		if hybrid.IsNotFound(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect placement reservation: %w", err)
		}
		return fmt.Errorf("%w: %s/%s", ErrPlacementCleanupReserved, p.Provider, p.ID)
	}
	return nil
}
