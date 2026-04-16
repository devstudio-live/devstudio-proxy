//go:build linux

package proxycore

import (
	"fmt"
	"os"
	"syscall"
)

// Restart re-execs the current binary, preserving PID so that systemd
// doesn't see a crash. Uses syscall.Exec for an in-place process replacement.
func Restart() error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("executable path: %w", err)
	}

	return syscall.Exec(exe, os.Args, os.Environ())
}
