//go:build windows

package proxycore

import (
	"fmt"
	"os"
	"syscall"
)

// Restart on Windows spawns a detached copy of the current binary then exits.
// Windows cannot replace a running .exe in-place via syscall.Exec, so we use
// the standard detached-spawn pattern (same as Caddy, ollama, etc.).
//
// The new process inherits the original command-line arguments. The old process
// exits cleanly so the .exe file lock is released and the new process can bind
// the same port.
func Restart() error {
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("executable path: %w", err)
	}

	// DETACHED_PROCESS (0x00000008) tells Windows to give the child its own
	// console session so it survives the parent's exit.
	attr := &os.ProcAttr{
		Dir:   ".",
		Env:   os.Environ(),
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
		Sys: &syscall.SysProcAttr{
			CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | 0x00000008, // DETACHED_PROCESS
		},
	}

	proc, err := os.StartProcess(exe, os.Args, attr)
	if err != nil {
		return fmt.Errorf("start new process: %w", err)
	}
	// Release the Process handle — we don't need to wait on the child.
	proc.Release()

	// Exit the old process so the port and .exe file lock are freed.
	os.Exit(0)
	return nil // unreachable
}
