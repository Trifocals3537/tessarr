//go:build unix

package manager

import (
	"fmt"
	"os"
	"syscall"
)

func legacyUsenetLinkCount(file *os.File) (uint64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("read artifact metadata: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("filesystem link count is unavailable")
	}
	return uint64(stat.Nlink), nil
}

func legacyUsenetSymlinkLinkCount(_ *os.Root, _ string, info os.FileInfo) (uint64, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("filesystem link count is unavailable")
	}
	return uint64(stat.Nlink), nil
}
