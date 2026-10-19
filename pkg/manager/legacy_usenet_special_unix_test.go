//go:build unix

package manager

import "syscall"

func createLegacySpecialArtifact(path string) error {
	return syscall.Mkfifo(path, 0o600)
}
