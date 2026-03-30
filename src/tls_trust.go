package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

// installCATrust installs caPath into the OS trust store.
// A ca-trusted.flag marker in the same directory prevents repeated prompts.
func installCATrust(caPath string) error {
	dir := filepath.Dir(caPath)
	flagPath := filepath.Join(dir, "ca-trusted.flag")
	if _, err := os.Stat(flagPath); err == nil {
		return nil // already installed in a previous run
	}

	var installErr error
	switch runtime.GOOS {
	case "darwin":
		installErr = installCATrustMacOS(caPath)
	case "windows":
		installErr = installCATrustWindows(caPath)
	default:
		// Linux: non-fatal, log manual instructions.
		log.Printf("proxy: to trust the CA on Linux: sudo cp %s /usr/local/share/ca-certificates/devstudio-proxy-ca.crt && sudo update-ca-certificates", caPath)
		return nil
	}

	if installErr != nil {
		log.Printf("proxy: CA trust installation failed: %v", installErr)
		return installErr
	}

	// Write flag to prevent the OS dialog from appearing on future startups.
	_ = os.WriteFile(flagPath, []byte("ok"), 0644)
	return nil
}

// installCATrustMacOS uses osascript to invoke security(1) with
// administrator privileges via a native macOS GUI elevation dialog —
// no terminal needed.
func installCATrustMacOS(caPath string) error {
	script := fmt.Sprintf(
		`do shell script "security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain %s" with administrator privileges`,
		shellQuote(caPath),
	)
	out, err := exec.Command("osascript", "-e", script).CombinedOutput()
	if err != nil {
		return fmt.Errorf("osascript: %w: %s", err, out)
	}
	return nil
}

// installCATrustWindows uses PowerShell to invoke certutil with UAC elevation.
func installCATrustWindows(caPath string) error {
	out, err := exec.Command(
		"powershell", "-NoProfile", "-NonInteractive", "-Command",
		fmt.Sprintf(`Start-Process certutil -ArgumentList '-addstore','-f','ROOT','%s' -Verb RunAs -Wait`, winQuote(caPath)),
	).CombinedOutput()
	if err != nil {
		return fmt.Errorf("powershell certutil: %w: %s", err, out)
	}
	return nil
}

// shellQuote single-quotes a path for use in a POSIX shell argument.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// winQuote escapes single quotes for use in a PowerShell string literal.
func winQuote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
