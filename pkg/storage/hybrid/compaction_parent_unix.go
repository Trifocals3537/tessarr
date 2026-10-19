//go:build !windows

package hybrid

import "path/filepath"

func resolveCompactionParent(parent string) (string, error) {
	// Resolve configured directory links once and retain the concrete path for
	// the lifetime of the store, so a later link swap cannot redirect rename.
	return filepath.EvalSymlinks(parent)
}
