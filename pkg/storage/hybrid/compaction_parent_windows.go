package hybrid

import (
	"fmt"
	"os"
	"path/filepath"
)

func resolveCompactionParent(parent string) (string, error) {
	resolved, err := filepath.EvalSymlinks(parent)
	if err == nil {
		return resolved, nil
	}

	// EvalSymlinks can return Access Denied for otherwise usable Windows
	// sandbox and user-profile directories. A direct parent that is itself a
	// reparse link still fails closed; a regular opened directory may use its
	// stable configured path because every compaction artifact remains an
	// exact sibling and is identity-checked before replacement.
	info, inspectErr := os.Lstat(parent)
	if inspectErr != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("append log directory link could not be resolved: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("append log parent is not a directory: %s", parent)
	}
	return filepath.Clean(parent), nil
}
