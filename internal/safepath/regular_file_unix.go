//go:build !windows

package safepath

import (
	"errors"
	"os"
)

func replaceRootFile(rooted *os.Root, source, destination string) error {
	return rooted.Rename(source, destination)
}

func syncOpenRoot(rooted *os.Root) error {
	directory, err := rooted.Open(".")
	if err != nil {
		return err
	}
	return errors.Join(directory.Sync(), directory.Close())
}
