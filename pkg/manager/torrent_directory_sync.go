//go:build !windows

package manager

import "os"

func syncTorrentDirectory(directory *os.File) error {
	return directory.Sync()
}
