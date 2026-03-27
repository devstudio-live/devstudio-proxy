package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// AppConfig holds all configurable values with their resolved defaults.
type AppConfig struct {
	Port        int
	Log         bool
	Verbose     bool
	MCPRefresh  time.Duration
	MCPFallback bool
}

func defaultConfig() AppConfig {
	return AppConfig{
		Port:        7700,
		Log:         false,
		Verbose:     false,
		MCPRefresh:  30 * time.Minute,
		MCPFallback: false,
	}
}

// configPaths returns candidate config file paths in ascending priority order
// (system-level first, user-level last). Missing files are silently skipped.
//
// macOS:   /opt/homebrew/etc/devstudio-proxy.conf  (Apple Silicon)
//
//	/usr/local/etc/devstudio-proxy.conf      (Intel)
//	~/Library/Application Support/devstudio-proxy/config.conf
//
// Linux:   /etc/devstudio-proxy/devstudio-proxy.conf
//
//	~/.config/devstudio-proxy/config.conf
//
// Windows: %PROGRAMDATA%\devstudio-proxy\config.conf
//
//	%AppData%\devstudio-proxy\config.conf
func configPaths() []string {
	var paths []string

	// System-level path
	switch runtime.GOOS {
	case "darwin":
		// Both Homebrew prefix locations are probed; whichever exists is applied.
		// On Apple Silicon only /opt/homebrew/etc/ exists; on Intel only /usr/local/etc/.
		paths = append(paths,
			"/opt/homebrew/etc/devstudio-proxy.conf",
			"/usr/local/etc/devstudio-proxy.conf",
		)
	case "windows":
		if pd := os.Getenv("PROGRAMDATA"); pd != "" {
			paths = append(paths, filepath.Join(pd, "devstudio-proxy", "config.conf"))
		}
	default: // linux, freebsd, etc.
		paths = append(paths, "/etc/devstudio-proxy/devstudio-proxy.conf")
	}

	// User-level path (os.UserConfigDir is cross-platform stdlib)
	if dir, err := os.UserConfigDir(); err == nil {
		paths = append(paths, filepath.Join(dir, "devstudio-proxy", "config.conf"))
	}

	return paths
}

// loadConfig builds the final config by layering:
//
//	compiled defaults → system config file → user config file → env vars
//
// CLI flags are applied on top by main() via the flag package.
func loadConfig() (AppConfig, error) {
	cfg := defaultConfig()

	for _, path := range configPaths() {
		if err := applyConfigFile(&cfg, path); err != nil {
			return cfg, fmt.Errorf("config file %s: %w", path, err)
		}
	}

	applyEnvVars(&cfg)
	return cfg, nil
}

// applyConfigFile reads a KEY=VALUE file and overlays recognised keys onto cfg.
// Missing files are silently skipped. Malformed lines return an error.
func applyConfigFile(cfg *AppConfig, path string) error {
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			return fmt.Errorf("line %d: missing '=' in %q", lineNum, line)
		}
		key = strings.TrimSpace(key)
		val = strings.TrimSpace(val)
		// Strip optional inline comment: PORT=7700 # some note
		if idx := strings.Index(val, " #"); idx >= 0 {
			val = strings.TrimSpace(val[:idx])
		}
		if err := applyKeyValue(cfg, key, val); err != nil {
			return fmt.Errorf("line %d: %w", lineNum, err)
		}
	}
	return scanner.Err()
}

// applyEnvVars overlays DEVPROXY_* environment variables onto cfg.
// Errors from malformed env var values are silently ignored.
func applyEnvVars(cfg *AppConfig) {
	for key, val := range map[string]string{
		"PORT":         os.Getenv("DEVPROXY_PORT"),
		"LOG":          os.Getenv("DEVPROXY_LOG"),
		"VERBOSE":      os.Getenv("DEVPROXY_VERBOSE"),
		"MCP_REFRESH":  os.Getenv("DEVPROXY_MCP_REFRESH"),
		"MCP_FALLBACK": os.Getenv("DEVPROXY_MCP_FALLBACK"),
	} {
		if val != "" {
			_ = applyKeyValue(cfg, key, val)
		}
	}
}

// applyKeyValue sets a single key on cfg. Unknown keys are silently ignored
// for forward-compatibility. Bad values return a descriptive error.
func applyKeyValue(cfg *AppConfig, key, val string) error {
	switch strings.ToUpper(key) {
	case "PORT":
		var n int
		if _, err := fmt.Sscanf(val, "%d", &n); err != nil {
			return fmt.Errorf("PORT: expected integer, got %q", val)
		}
		cfg.Port = n
	case "LOG":
		cfg.Log = strings.ToLower(val) == "true" || val == "1"
	case "VERBOSE":
		cfg.Verbose = strings.ToLower(val) == "true" || val == "1"
	case "MCP_REFRESH":
		d, err := time.ParseDuration(val)
		if err != nil {
			return fmt.Errorf("MCP_REFRESH: %w", err)
		}
		cfg.MCPRefresh = d
	case "MCP_FALLBACK":
		cfg.MCPFallback = strings.ToLower(val) == "true" || val == "1"
	// unknown keys intentionally ignored
	}
	return nil
}
