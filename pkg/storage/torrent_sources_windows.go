//go:build windows

package storage

import (
	"errors"
	"os"
)

func replaceTorrentSource(rooted *os.Root, source, destination string) error {
	return replaceTorrentSourceAtRoot(rooted, source, destination)
}

type torrentSourceRoot interface {
	Rename(oldname, newname string) error
	OpenFile(name string, flag int, perm os.FileMode) (*os.File, error)
}

func replaceTorrentSourceAtRoot(rooted torrentSourceRoot, source, destination string) error {
	if err := rooted.Rename(source, destination); err != nil {
		return err
	}
	file, err := rooted.OpenFile(destination, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	return errors.Join(file.Sync(), file.Close())
}

func syncTorrentSourceDirectory(*os.Root) error {
	// The rooted replacement syncs the destination file before returning.
	return nil
}
