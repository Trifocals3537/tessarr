package filelock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Lock owns both the locked file descriptor and its advisory lock. The file is
// opened through an os.Root so acquisition never re-resolves an absolute path.
type Lock struct {
	mu     sync.Mutex
	file   *os.File
	locked bool
}

// AcquireContext opens name beneath rooted, verifies that the opened file is
// still the visible regular non-symlink entry, and retries a non-blocking
// exclusive lock until ctx expires.
func AcquireContext(ctx context.Context, rooted *os.Root, name string, perm os.FileMode, retryDelay time.Duration) (*Lock, error) {
	if ctx == nil {
		return nil, fmt.Errorf("lock context is nil")
	}
	if rooted == nil {
		return nil, fmt.Errorf("lock filesystem root is nil")
	}
	if retryDelay <= 0 {
		return nil, fmt.Errorf("lock retry delay must be positive")
	}
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name || strings.ContainsAny(name, `/\`) {
		return nil, fmt.Errorf("lock file name %q is not a local component", name)
	}

	file, err := openVerified(rooted, name, perm)
	if err != nil {
		return nil, err
	}
	for {
		locked, lockErr := tryExclusive(file)
		if lockErr != nil {
			_ = file.Close()
			return nil, fmt.Errorf("lock rooted file %q: %w", name, lockErr)
		}
		if locked {
			return &Lock{file: file, locked: true}, nil
		}

		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			_ = file.Close()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func openVerified(rooted *os.Root, name string, perm os.FileMode) (*os.File, error) {
	for attempt := 0; attempt < 2; attempt++ {
		before, err := rooted.Lstat(name)
		if os.IsNotExist(err) {
			file, createErr := rooted.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, perm.Perm())
			if os.IsExist(createErr) {
				continue
			}
			if createErr != nil {
				return nil, fmt.Errorf("create rooted lock file %q: %w", name, createErr)
			}
			if err := verifyOpened(rooted, name, file, nil); err != nil {
				_ = file.Close()
				return nil, err
			}
			return file, nil
		}
		if err != nil {
			return nil, fmt.Errorf("inspect rooted lock file %q: %w", name, err)
		}
		if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() {
			return nil, fmt.Errorf("rooted lock file %q is not a regular file", name)
		}
		file, err := rooted.OpenFile(name, os.O_RDWR, perm.Perm())
		if err != nil {
			return nil, fmt.Errorf("open rooted lock file %q: %w", name, err)
		}
		if err := verifyOpened(rooted, name, file, before); err != nil {
			_ = file.Close()
			return nil, err
		}
		return file, nil
	}
	return nil, fmt.Errorf("rooted lock file %q changed while opening", name)
}

func verifyOpened(rooted *os.Root, name string, file *os.File, before os.FileInfo) error {
	opened, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat opened lock file %q: %w", name, err)
	}
	after, err := rooted.Lstat(name)
	if err != nil {
		return fmt.Errorf("reinspect rooted lock file %q: %w", name, err)
	}
	if after.Mode()&os.ModeSymlink != 0 || !after.Mode().IsRegular() || !os.SameFile(opened, after) {
		return fmt.Errorf("rooted lock file %q changed while opening", name)
	}
	if before != nil && !os.SameFile(before, opened) {
		return fmt.Errorf("rooted lock file %q changed while opening", name)
	}
	return nil
}

// Unlock releases the advisory lock and closes its file descriptor.
func (lock *Lock) Unlock() error {
	if lock == nil {
		return nil
	}
	lock.mu.Lock()
	defer lock.mu.Unlock()
	if lock.file == nil {
		return nil
	}
	var unlockErr error
	if lock.locked {
		unlockErr = unlockExclusive(lock.file)
		lock.locked = false
	}
	closeErr := lock.file.Close()
	lock.file = nil
	return errors.Join(unlockErr, closeErr)
}

// Close is equivalent to Unlock.
func (lock *Lock) Close() error {
	return lock.Unlock()
}
