package manager

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func syncTorrentDirectory(directory *os.File) error {
	err := directory.Sync()
	if errors.Is(err, windows.ERROR_ACCESS_DENIED) ||
		errors.Is(err, windows.ERROR_INVALID_HANDLE) {
		// Go can open directories on Windows but FlushFileBuffers commonly
		// rejects those handles. Keep directory durability best-effort there;
		// every file and ownership marker is still flushed before this call.
		return nil
	}
	return err
}
