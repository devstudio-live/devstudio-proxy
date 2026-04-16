//go:build windows

package proxycore

import "fmt"

// Restart on Windows is implemented in Phase 3.
func Restart() error {
	return fmt.Errorf("Windows self-update restart not yet supported (Phase 3)")
}
