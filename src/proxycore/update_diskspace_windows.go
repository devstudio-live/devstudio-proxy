//go:build windows

package proxycore

import (
	"fmt"
	"syscall"
	"unsafe"
)

// checkDiskSpace verifies that the directory at path has at least
// requiredBytes of free space using GetDiskFreeSpaceExW.
// Returns nil if the check passes or if the API call fails (fail-open).
func checkDiskSpace(path string, requiredBytes int64) error {
	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil // can't convert path — proceed anyway
	}

	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	getDiskFreeSpaceEx := kernel32.NewProc("GetDiskFreeSpaceExW")

	var freeBytesAvailable uint64
	ret, _, _ := getDiskFreeSpaceEx.Call(
		uintptr(unsafe.Pointer(pathPtr)),
		uintptr(unsafe.Pointer(&freeBytesAvailable)),
		0, // totalBytes — not needed
		0, // totalFreeBytes — not needed
	)
	if ret == 0 {
		return nil // API call failed — proceed anyway (fail-open)
	}

	if int64(freeBytesAvailable) < requiredBytes {
		return fmt.Errorf("insufficient disk space: need %d MB free, have %d MB",
			requiredBytes/(1024*1024), int64(freeBytesAvailable)/(1024*1024))
	}
	return nil
}
