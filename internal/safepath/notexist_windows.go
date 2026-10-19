//go:build windows

package safepath

import (
	"errors"
	"os"
	"syscall"
)

func isNotExistError(err error) bool {
	return os.IsNotExist(err) ||
		errors.Is(err, syscall.ERROR_FILE_NOT_FOUND) ||
		errors.Is(err, syscall.ERROR_PATH_NOT_FOUND)
}
