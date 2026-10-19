//go:build windows

package manager

import (
	"fmt"
	"os"

	"golang.org/x/sys/windows"
)

func torrentOpenFileLinkCount(file *os.File) (uint64, error) {
	if file == nil {
		return 0, fmt.Errorf("opened file is nil")
	}
	var information windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(windows.Handle(file.Fd()), &information); err != nil {
		return 0, fmt.Errorf("query Windows file link count: %w", err)
	}
	return uint64(information.NumberOfLinks), nil
}
