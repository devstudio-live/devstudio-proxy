//go:build darwin

package proxycore

import (
	"fmt"
	"os"
	"syscall"
)

// Restart re-execs the current binary. For CLI builds (BuildSource "release"
// or "standalone"), it uses syscall.Exec to preserve PID so launchd /
// brew-services don't see a crash.
//
// For Wails single-file builds (BuildSource "wails-exe"), it spawns a detached
// child and exits — syscall.Exec would kill the Wails webview abruptly.
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

	if BuildSource == "wails-exe" {
		return restartDetached(exe)
	}

	return syscall.Exec(exe, os.Args, os.Environ())
}

// restartDetached spawns the binary in a new session and exits the current
// process. The new process gets its own session (Setsid) so it survives
// the parent's exit.
func restartDetached(exe string) error {
	attr := &os.ProcAttr{
		Dir:   ".",
		Env:   os.Environ(),
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
		Sys:   &syscall.SysProcAttr{Setsid: true},
	}

	proc, err := os.StartProcess(exe, os.Args, attr)
	if err != nil {
		return fmt.Errorf("start new process: %w", err)
	}
	proc.Release()

	os.Exit(0)
	return nil // unreachable
}
