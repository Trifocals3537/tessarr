package vfs

import (
	"context"
	"sync"
)

// cleanupMutex is a zero-value-ready maintenance lock. Downloader admission
// must be cancellable: cleanup may hold this lock while Close cancels and
// joins that very downloader. Cancellation breaks the join/admission cycle;
// an active (uncanceled) writer waits for reclamation rather than denying
// growth solely because another cleanup owns the lock.
type cleanupMutex struct {
	once  sync.Once
	token chan struct{}
}

func (m *cleanupMutex) LockContext(ctx context.Context) error {
	return m.LockContextOrSpace(ctx, nil)
}

func (m *cleanupMutex) LockContextOrSpace(ctx context.Context, space <-chan struct{}) error {
	m.once.Do(func() { m.token = make(chan struct{}, 1) })
	select {
	case m.token <- struct{}{}:
		// Cancellation may race acquisition; never begin another cleanup for a
		// downloader whose closer is waiting for it to exit.
		if err := ctx.Err(); err != nil {
			m.Unlock()
			return err
		}
		return nil
	case <-space:
		return errQuotaSpaceChanged
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (m *cleanupMutex) Lock()   { _ = m.LockContext(context.Background()) }
func (m *cleanupMutex) Unlock() { <-m.token }
