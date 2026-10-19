//go:build unix

package manager

import (
	"fmt"
	"os"
	"syscall"
)

func torrentOpenFileLinkCount(file *os.File) (uint64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("opened file does not expose a Unix link count")
	}
	return uint64(stat.Nlink), nil
}
