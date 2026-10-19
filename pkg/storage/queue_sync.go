package storage

import "fmt"

// SyncQueue flushes every queue append completed before this call. A caller
// must not consume the queue row's recoverable input until this succeeds:
// hybrid-store reads can observe an indexed append whose earlier fsync failed.
func (s *Storage) SyncQueue() error {
	if s == nil || s.queue == nil {
		return fmt.Errorf("queue storage is unavailable")
	}
	if err := s.queue.Sync(); err != nil {
		return fmt.Errorf("sync queue storage: %w", err)
	}
	return nil
}
