//go:build windows

package manager

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func legacyUsenetLinkCount(file *os.File) (uint64, error) {
	if file == nil {
		return 0, fmt.Errorf("artifact handle is nil")
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("access artifact handle: %w", err)
	}
	var info windows.ByHandleFileInformation
	var inspectErr error
	if err := raw.Control(func(handle uintptr) {
		inspectErr = windows.GetFileInformationByHandle(windows.Handle(handle), &info)
	}); err != nil {
		return 0, fmt.Errorf("control artifact handle: %w", err)
	}
	if inspectErr != nil {
		return 0, fmt.Errorf("read artifact link count: %w", inspectErr)
	}
	return uint64(info.NumberOfLinks), nil
}

func legacyUsenetSymlinkLinkCount(rooted *os.Root, name string, expected os.FileInfo) (uint64, error) {
	if rooted == nil {
		return 0, fmt.Errorf("artifact root is nil")
	}
	directory, err := rooted.Open(".")
	if err != nil {
		return 0, fmt.Errorf("open artifact root handle: %w", err)
	}
	defer directory.Close()
	raw, err := directory.SyscallConn()
	if err != nil {
		return 0, fmt.Errorf("access artifact root handle: %w", err)
	}
	objectName, err := windows.NewNTUnicodeString(name)
	if err != nil {
		return 0, fmt.Errorf("encode artifact name: %w", err)
	}
	var handle windows.Handle
	var openErr error
	if err := raw.Control(func(rootHandle uintptr) {
		attributes := &windows.OBJECT_ATTRIBUTES{
			Length:        uint32(unsafe.Sizeof(windows.OBJECT_ATTRIBUTES{})),
			RootDirectory: windows.Handle(rootHandle),
			ObjectName:    objectName,
			Attributes:    windows.OBJ_CASE_INSENSITIVE,
		}
		var status windows.IO_STATUS_BLOCK
		openErr = windows.NtCreateFile(
			&handle,
			windows.FILE_READ_ATTRIBUTES,
			attributes,
			&status,
			nil,
			0,
			windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE|windows.FILE_SHARE_DELETE,
			windows.FILE_OPEN,
			windows.FILE_OPEN_REPARSE_POINT|windows.FILE_OPEN_FOR_BACKUP_INTENT,
			0,
			0,
		)
	}); err != nil {
		return 0, fmt.Errorf("control artifact root handle: %w", err)
	}
	if openErr != nil {
		return 0, fmt.Errorf("open symlink for link-count inspection: %w", openErr)
	}
	file := os.NewFile(uintptr(handle), name)
	if file == nil {
		_ = windows.CloseHandle(handle)
		return 0, fmt.Errorf("wrap symlink inspection handle")
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("inspect opened symlink: %w", err)
	}
	if expected == nil || opened.Mode()&os.ModeSymlink == 0 || !os.SameFile(expected, opened) {
		return 0, fmt.Errorf("symlink changed while opening for link-count inspection")
	}
	var info windows.ByHandleFileInformation
	if err := windows.GetFileInformationByHandle(handle, &info); err != nil {
		return 0, fmt.Errorf("read symlink link count: %w", err)
	}
	return uint64(info.NumberOfLinks), nil
}
