package manager

import (
	"fmt"
	"path/filepath"
)

// symlinkTarget returns the value to store in a symlink at linkPath. Relative
// targets are resolved from the link's parent, as required by the filesystem.
func symlinkTarget(linkPath, target string, relative bool) (string, error) {
	absoluteTarget, err := filepath.Abs(target)
	if err != nil {
		return "", fmt.Errorf("resolve symlink target %q: %w", target, err)
	}
	absoluteTarget = filepath.Clean(absoluteTarget)
	if !relative {
		return absoluteTarget, nil
	}

	absoluteLink, err := filepath.Abs(linkPath)
	if err != nil {
		return "", fmt.Errorf("resolve symlink path %q: %w", linkPath, err)
	}
	storedTarget, err := filepath.Rel(filepath.Dir(filepath.Clean(absoluteLink)), absoluteTarget)
	if err != nil {
		return "", fmt.Errorf("make symlink target %q relative to %q: %w", absoluteTarget, linkPath, err)
	}
	if filepath.IsAbs(storedTarget) {
		return "", fmt.Errorf("make symlink target %q relative to %q: result is absolute", absoluteTarget, linkPath)
	}
	return filepath.Clean(storedTarget), nil
}

func resolveSymlinkTarget(linkPath, target string) (string, error) {
	if target == "" {
		return "", fmt.Errorf("symlink target is empty")
	}
	if filepath.IsAbs(target) {
		return filepath.Clean(target), nil
	}
	absoluteLink, err := filepath.Abs(linkPath)
	if err != nil {
		return "", err
	}
	return filepath.Clean(filepath.Join(filepath.Dir(absoluteLink), target)), nil
}

// sameSymlinkTarget compares where two stored link targets resolve, rather
// than comparing their textual absolute/relative representation.
func sameSymlinkTarget(linkPath, left, right string) bool {
	resolvedLeft, leftErr := resolveSymlinkTarget(linkPath, left)
	resolvedRight, rightErr := resolveSymlinkTarget(linkPath, right)
	return leftErr == nil && rightErr == nil && sameFilesystemPath(resolvedLeft, resolvedRight)
}
