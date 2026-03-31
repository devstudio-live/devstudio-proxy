package proxycore

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
	HTTPS       bool
}

// DefaultConfig returns an AppConfig with compiled-in defaults.
func DefaultConfig() AppConfig {
	return AppConfig{
		Port:        7700,
		Log:         false,
		Verbose:     false,
		MCPRefresh:  30 * time.Minute,
		MCPFallback: false,
	}
}

func configPaths() []string {
	var paths []string

	switch runtime.GOOS {
	case "darwin":
		paths = append(paths,
			"/opt/homebrew/etc/devstudio-proxy.conf",
			"/usr/local/etc/devstudio-proxy.conf",
		)
	case "windows":
		if pd := os.Getenv("PROGRAMDATA"); pd != "" {
			paths = append(paths, filepath.Join(pd, "devstudio-proxy", "config.conf"))
		}
	default:
		paths = append(paths, "/etc/devstudio-proxy/devstudio-proxy.conf")
	}

	if dir, err := os.UserConfigDir(); err == nil {
		paths = append(paths, filepath.Join(dir, "devstudio-proxy", "config.conf"))
	}

	return paths
}

// LoadConfig builds the final config by layering:
// compiled defaults -> system config file -> user config file -> env vars
func LoadConfig() (AppConfig, error) {
	cfg := DefaultConfig()

	for _, path := range configPaths() {
		if err := applyConfigFile(&cfg, path); err != nil {
			return cfg, fmt.Errorf("config file %s: %w", path, err)
		}
	}

	applyEnvVars(&cfg)
	return cfg, nil
}

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
		if idx := strings.Index(val, " #"); idx >= 0 {
			val = strings.TrimSpace(val[:idx])
		}
		if err := ApplyKeyValue(cfg, key, val); err != nil {
			return fmt.Errorf("line %d: %w", lineNum, err)
		}
	}
	return scanner.Err()
}

func applyEnvVars(cfg *AppConfig) {
	for key, val := range map[string]string{
		"PORT":         os.Getenv("DEVPROXY_PORT"),
		"LOG":          os.Getenv("DEVPROXY_LOG"),
		"VERBOSE":      os.Getenv("DEVPROXY_VERBOSE"),
		"MCP_REFRESH":  os.Getenv("DEVPROXY_MCP_REFRESH"),
		"MCP_FALLBACK": os.Getenv("DEVPROXY_MCP_FALLBACK"),
		"HTTPS":        os.Getenv("DEVPROXY_HTTPS"),
	} {
		if val != "" {
			_ = ApplyKeyValue(cfg, key, val)
		}
	}
}

// UserConfigPath returns the user-level config file path.
func UserConfigPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "devstudio-proxy", "config.conf"), nil
}

// PersistConfigKey writes or updates a KEY=VALUE pair in the user config file.
func PersistConfigKey(key, value string) error {
	path, err := UserConfigPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	var lines []string
	if data, err := os.ReadFile(path); err == nil {
		lines = strings.Split(string(data), "\n")
	}

	found := false
	upper := strings.ToUpper(key)
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		k, _, ok := strings.Cut(trimmed, "=")
		if ok && strings.TrimSpace(strings.ToUpper(k)) == upper {
			lines[i] = key + "=" + value
			found = true
			break
		}
	}
	if !found {
		if len(lines) > 0 && lines[len(lines)-1] == "" {
			lines = append(lines[:len(lines)-1], key+"="+value, "")
		} else {
			lines = append(lines, key+"="+value)
		}
	}

	content := strings.Join(lines, "\n")
	tmp, err := os.CreateTemp(filepath.Dir(path), ".config-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.WriteString(content); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, path)
}

// ApplyKeyValue sets a single key on cfg.
func ApplyKeyValue(cfg *AppConfig, key, val string) error {
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
	case "HTTPS":
		cfg.HTTPS = strings.ToLower(val) == "true" || val == "1"
	}
	return nil
}
