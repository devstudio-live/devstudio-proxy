package proxycore

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
)

// Update states reported via /admin/update/status.
const (
	UpdateStateIdle        = "idle"
	UpdateStateChecking    = "checking"
	UpdateStateDownloading = "downloading"
	UpdateStateVerifying   = "verifying"
	UpdateStateApplying    = "applying"
	UpdateStateRestarting  = "restarting"
	UpdateStateError       = "error"
)

type updateStatusInfo struct {
	mu       sync.RWMutex
	state    string
	progress float64
	errMsg   string
}

var (
	currentUpdateStatus = &updateStatusInfo{state: UpdateStateIdle}
	applyMu             sync.Mutex // single-flight guard
)

func setUpdateStatus(state string, progress float64, errMsg string) {
	currentUpdateStatus.mu.Lock()
	currentUpdateStatus.state = state
	currentUpdateStatus.progress = progress
	currentUpdateStatus.errMsg = errMsg
	currentUpdateStatus.mu.Unlock()
}

// UpdateStatus returns the current update state for /admin/update/status.
func UpdateStatus() map[string]interface{} {
	currentUpdateStatus.mu.RLock()
	defer currentUpdateStatus.mu.RUnlock()
	return map[string]interface{}{
		"state":    currentUpdateStatus.state,
		"progress": currentUpdateStatus.progress,
		"error":    currentUpdateStatus.errMsg,
	}
}

// ApplyUpdate downloads, verifies, and replaces the current binary with
// the specified version. Caller is responsible for triggering Restart()
// after a successful return.
func ApplyUpdate(ctx context.Context, targetVersion string) error {
	if !applyMu.TryLock() {
		return fmt.Errorf("update already in progress")
	}
	defer applyMu.Unlock()

	if BuildSource == "source" {
		return fmt.Errorf("self-update not supported for dev builds")
	}

	setUpdateStatus(UpdateStateChecking, 0, "")

	// Force a fresh check so we have the latest release metadata.
	info, err := CheckForUpdate(ctx, true)
	if err != nil {
		setUpdateStatus(UpdateStateError, 0, err.Error())
		return err
	}

	// TOCTOU guard: requested version must match what GitHub reports.
	if info.Latest != targetVersion {
		err := fmt.Errorf("version mismatch: requested %s but latest is %s", targetVersion, info.Latest)
		setUpdateStatus(UpdateStateError, 0, err.Error())
		return err
	}

	if !info.UpdateAvailable {
		err := fmt.Errorf("no update available")
		setUpdateStatus(UpdateStateError, 0, err.Error())
		return err
	}

	// Dispatch: bundle-swap for Wails .app, binary-replace for everything else.
	if BuildSource == "wails-app" {
		// Disk-space precheck is inside applyBundleUpdate (uses bundle size).
		setUpdateStatus(UpdateStateDownloading, 0.1, "")
		if err := applyBundleUpdate(ctx, targetVersion); err != nil {
			setUpdateStatus(UpdateStateError, 0, err.Error())
			return err
		}
	} else {
		// Disk-space precheck: need ~3x the current binary size.
		exe, err := os.Executable()
		if err != nil {
			setUpdateStatus(UpdateStateError, 0, err.Error())
			return fmt.Errorf("executable path: %w", err)
		}
		if fi, statErr := os.Stat(exe); statErr == nil {
			needed := fi.Size() * 3
			if dsErr := checkDiskSpace(filepath.Dir(exe), needed); dsErr != nil {
				setUpdateStatus(UpdateStateError, 0, dsErr.Error())
				return dsErr
			}
		}

		setUpdateStatus(UpdateStateDownloading, 0.1, "")

		if err := downloadAndApply(ctx, targetVersion); err != nil {
			setUpdateStatus(UpdateStateError, 0, err.Error())
			return err
		}
	}

	setUpdateStatus(UpdateStateRestarting, 1.0, "")
	return nil
}

// rollbackUpdate restores the pre-update binary from the .old backup
// created by go-selfupdate.
func rollbackUpdate() {
	exe, err := os.Executable()
	if err != nil {
		log.Printf("proxy: rollback failed — cannot determine executable: %v", err)
		return
	}
	oldPath := exe + ".old"
	if _, err := os.Stat(oldPath); err != nil {
		log.Printf("proxy: rollback failed — no .old backup at %s", oldPath)
		return
	}
	if err := os.Rename(oldPath, exe); err != nil {
		log.Printf("proxy: rollback failed — rename %s → %s: %v", oldPath, exe, err)
		return
	}
	log.Printf("proxy: rollback succeeded — restored %s from .old backup", exe)
}
