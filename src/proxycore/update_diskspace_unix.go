//go:build !windows

package proxycore

import (
	"fmt"
	"syscall"
)

// checkDiskSpace verifies that the directory at path has at least
// requiredBytes of free space. Returns nil if the check passes or
// if the filesystem stats cannot be read (fail-open).
func checkDiskSpace(path string, requiredBytes int64) error {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return nil // can't check — proceed anyway
	}
	available := int64(stat.Bavail) * int64(stat.Bsize)
	if available < requiredBytes {
		return fmt.Errorf("insufficient disk space: need %d MB free, have %d MB",
			requiredBytes/(1024*1024), available/(1024*1024))
	}
	return nil
}
