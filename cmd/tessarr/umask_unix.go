//go:build !windows

package tessarr

import "syscall"

func SetUmask(umask int) {
	syscall.Umask(umask)
}
