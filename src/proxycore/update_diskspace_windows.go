//go:build windows

package proxycore

// checkDiskSpace is a no-op stub on Windows (Phase 3).
func checkDiskSpace(_ string, _ int64) error {
	return nil
}
