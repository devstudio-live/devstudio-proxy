//go:build linux

package proxycore

import (
	"fmt"
	"os"
	"syscall"
)

// Restart re-execs the current binary. For CLI builds (BuildSource "release"),
// it uses syscall.Exec to preserve PID so systemd doesn't see a crash.
// For Wails desktop builds (BuildSource "wails-exe"), it spawns a detached
// child and exits — syscall.Exec would kill the Wails webview abruptly.
func Restart() error {
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
