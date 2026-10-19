//go:build !windows

package storage

import "os"

func replaceTorrentSource(rooted *os.Root, source, destination string) error {
	return rooted.Rename(source, destination)
}

func syncTorrentSourceDirectory(rooted *os.Root) error {
	dir, err := rooted.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
