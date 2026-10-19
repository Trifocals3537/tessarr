package manager

import "fmt"

// Sync flushes queue mutations completed before this call. Watched-file
// acceptance uses it as a durability barrier after reconciling a visible row.
func (q *Queue) Sync() error {
	if q == nil || q.storage == nil {
		return fmt.Errorf("queue storage is unavailable")
	}
	return q.storage.SyncQueue()
}
