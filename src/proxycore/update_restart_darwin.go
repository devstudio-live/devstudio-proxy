//go:build darwin

package proxycore

import (
	"fmt"
	"os"
	"syscall"
)

// Restart re-execs the current binary, preserving PID so that launchd /
// brew-services don't see a crash. For CLI binaries (BuildSource "release"
// or "standalone"), this is a direct syscall.Exec.
//
// Wails .app bundle restart (BuildSource "wails-app") is deferred to Phase 5B.
func Restart() error {
	if BuildSource == "wails-app" {
		return fmt.Errorf("wails .app bundle restart not yet supported")
	}

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("executable path: %w", err)
	}

	return syscall.Exec(exe, os.Args, os.Environ())
}
