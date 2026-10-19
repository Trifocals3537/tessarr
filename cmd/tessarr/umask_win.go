//go:build windows

package tessarr

func SetUmask(umask int) {
	// No-op on Windows
}
