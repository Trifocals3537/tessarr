//go:build !unix && !windows

package manager

import (
	"fmt"
	"os"
)

func legacyUsenetLinkCount(_ *os.File) (uint64, error) {
	return 0, fmt.Errorf("filesystem link-count inspection is unsupported on this platform")
}

func legacyUsenetSymlinkLinkCount(_ *os.Root, _ string, _ os.FileInfo) (uint64, error) {
	return 0, fmt.Errorf("filesystem link-count inspection is unsupported on this platform")
}
