package proxycore

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Build-time variables set via -ldflags.
var (
	Version     = "dev"
	Commit      = "unknown"
	BuildDate   = "unknown"
	BuildSource = "source" // "source" | "release" | "wails-exe" | "wails-app"
)

// InstallKind returns a cosmetic label for how the binary was installed.
// Detected at runtime via path inspection — not stamped at build time.
func InstallKind() string {
	exe, err := os.Executable()
	if err != nil {
		return "unknown"
	}
	resolved, err := filepath.EvalSymlinks(exe)
	if err != nil {
		resolved = exe
	}

	// macOS .app bundle
	if strings.Contains(resolved, ".app/Contents/MacOS/") {
		return "wails-app"
	}

	// Homebrew (macOS or Linuxbrew)
	if strings.Contains(resolved, "/Cellar/") || strings.Contains(resolved, "/linuxbrew/") {
		return "homebrew"
	}

	return "standalone"
}

// VersionInfo returns the full version metadata map for /admin/config.
func VersionInfo() map[string]interface{} {
	return map[string]interface{}{
		"version":      Version,
		"commit":       Commit,
		"build_date":   BuildDate,
		"build_source": BuildSource,
		"install_kind": InstallKind(),
		"go_os":        runtime.GOOS,
		"go_arch":      runtime.GOARCH,
	}
}
