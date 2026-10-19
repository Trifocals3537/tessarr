//go:build darwin && amd64

package hanwen

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func mountPointActive(path string) (bool, error) {
	clean := filepath.Clean(path)
	info, err := os.Stat(clean)
	if err != nil {
		return false, fmt.Errorf("stat mount path: %w", err)
	}
	parent, err := os.Stat(filepath.Dir(clean))
	if err != nil {
		return false, fmt.Errorf("stat mount parent: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false, fmt.Errorf("mount path has unsupported stat type %T", info.Sys())
	}
	parentStat, ok := parent.Sys().(*syscall.Stat_t)
	if !ok {
		return false, fmt.Errorf("mount parent has unsupported stat type %T", parent.Sys())
	}
	return stat.Dev != parentStat.Dev, nil
}
