package manager

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/Trifocals3537/tessarr/internal/filelock"
	"github.com/Trifocals3537/tessarr/internal/safepath"
)

// ownershipRoot keeps the validated directory capability and advisory lock
// alive together so mutations cannot be redirected by reopening the root path.
type ownershipRoot struct {
	absolute string
	root     *os.Root
	locks    []*filelock.Lock
}

func acquireOwnershipRoot(path, lockName string, create bool, timeout, retryDelay time.Duration) (*ownershipRoot, bool, error) {
	return acquireCompatibleOwnershipRoot(path, []string{lockName}, create, timeout, retryDelay)
}

func acquireCompatibleOwnershipRoot(path string, lockNames []string, create bool, timeout, retryDelay time.Duration) (*ownershipRoot, bool, error) {
	if len(lockNames) == 0 {
		return nil, false, fmt.Errorf("ownership lock name is required")
	}
	var (
		absolute string
		rooted   *os.Root
		err      error
	)
	if create {
		rooted, absolute, err = safepath.EnsureOpenRoot(path, 0o755)
	} else {
		absolute, err = safepath.ValidateRoot(path)
	}
	if err != nil {
		return nil, false, err
	}

	if rooted == nil {
		rooted, _, err = safepath.OpenRoot(absolute)
		if err != nil {
			if !create && errors.Is(err, os.ErrNotExist) {
				return nil, false, nil
			}
			return nil, false, fmt.Errorf("open ownership root: %w", err)
		}
	}

	locks := make([]*filelock.Lock, 0, len(lockNames))
	for _, lockName := range lockNames {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		locked, lockErr := filelock.AcquireContext(ctx, rooted, lockName, 0o600, retryDelay)
		cancel()
		if lockErr != nil {
			var releaseErr error
			for index := len(locks) - 1; index >= 0; index-- {
				releaseErr = errors.Join(releaseErr, locks[index].Unlock())
			}
			return nil, false, errors.Join(
				fmt.Errorf("lock ownership root with %q: %w", lockName, lockErr),
				releaseErr,
				rooted.Close(),
			)
		}
		locks = append(locks, locked)
	}
	return &ownershipRoot{absolute: absolute, root: rooted, locks: locks}, true, nil
}

func (ownership *ownershipRoot) close() error {
	if ownership == nil {
		return nil
	}
	var verifyErr, lockErr, rootErr error
	if ownership.root != nil {
		verifyErr = safepath.VerifyOpenRoot(ownership.root, ownership.absolute)
	}
	for index := len(ownership.locks) - 1; index >= 0; index-- {
		lockErr = errors.Join(lockErr, ownership.locks[index].Unlock())
	}
	ownership.locks = nil
	if ownership.root != nil {
		rootErr = ownership.root.Close()
		ownership.root = nil
	}
	return errors.Join(verifyErr, lockErr, rootErr)
}
