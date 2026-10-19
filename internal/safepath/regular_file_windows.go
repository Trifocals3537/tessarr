//go:build windows

package safepath

import (
	"errors"
	"os"
)

func replaceRootFile(rooted *os.Root, source, destination string) error {
	// os.Root.Rename uses directory handles plus replace-if-exists semantics on
	// Windows, so a renamed or swapped parent path cannot redirect replacement.
	// Syncing a newly opened destination handle after the rooted rename retains
	// the prior write-through guarantee without falling back to an absolute-path
	// MoveFileEx call that could be redirected by a parent-directory swap.
	if err := rooted.Rename(source, destination); err != nil {
		return err
	}
	file, err := rooted.OpenFile(destination, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

func syncOpenRoot(*os.Root) error {
	// Windows has no portable directory-fsync operation. The temporary file is
	// synced before the atomic rooted rename, and recovery files remain valid if
	// the namespace update is interrupted.
	return nil
}
