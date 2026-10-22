// Package stateowner provides exclusive process ownership of mutable state.
package stateowner

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/Trifocals3537/tessarr/internal/filelock"
	"github.com/Trifocals3537/tessarr/internal/safepath"
)

var ErrInUse = filelock.ErrLocked

// Owner pins the directory and holds an OS lock until all state users drain.
// The lock file is retained across shutdown and crashes; only its open handle
// carries ownership, so a stale file never prevents a subsequent startup.
type Owner struct {
	root *os.Root
	lock *filelock.Lock
	once sync.Once
	err  error
}

// Acquire fails immediately when another owner holds name beneath rootPath.
// No state recovery or cleanup may run before this succeeds.
func Acquire(rootPath, name string) (*Owner, error) {
	root, absolute, err := safepath.EnsureOpenRoot(rootPath, 0o700)
	if err != nil {
		return nil, err
	}
	lock, err := filelock.TryAcquire(root, name, 0o600)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("acquire state ownership: %w", err), root.Close())
	}
	owner := &Owner{root: root, lock: lock}
	if err := safepath.VerifyOpenRoot(root, absolute); err != nil {
		return nil, errors.Join(err, owner.Close())
	}
	return owner, nil
}

func (owner *Owner) Close() error {
	if owner == nil {
		return nil
	}
	owner.once.Do(func() { owner.err = errors.Join(owner.lock.Close(), owner.root.Close()) })
	return owner.err
}
