//go:build !unix && !windows

package manager

import (
	"fmt"
	"os"
)

func torrentOpenFileLinkCount(*os.File) (uint64, error) {
	return 0, fmt.Errorf("file link count is unavailable on this platform")
}
