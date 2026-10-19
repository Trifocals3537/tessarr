//go:build !windows

package safepath

import "os"

func isNotExistError(err error) bool {
	return os.IsNotExist(err)
}
