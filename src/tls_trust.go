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

// installCATrustMacOS opens a Terminal window to run the security(1) trust
// command. The proxy runs as a Chrome native messaging host and therefore
// lacks a GUI security session, which causes osascript "with administrator
// privileges" to fail with "no user interaction was possible". Terminal.app
// always runs in the user's GUI session, so sudo prompts work there.
//
// The call returns as soon as Terminal opens (async); the flag is written
// optimistically. If the user cancels, Re-trust Certificate removes the flag
// and opens Terminal again.
func installCATrustMacOS(caPath string) error {
	// Shell command that Terminal will run. shellQuote handles the path.
	shellCmd := fmt.Sprintf(
		"sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain %s && echo 'Certificate trusted. You can close this window.' || echo 'Trust failed.'; exit",
		shellQuote(caPath),
	)
	// AppleScript string literal: double-quoted, internal " and \ escaped.
	script := fmt.Sprintf(
		`tell application "Terminal"
    activate
    do script %s
end tell`,
		asQuote(shellCmd),
	)
	out, err := exec.Command("osascript", "-e", script).CombinedOutput()
	if err != nil {
		return fmt.Errorf("osascript: %w: %s", err, out)
	}
	return nil
}

// asQuote wraps s in AppleScript double-quote delimiters, escaping any
// internal backslashes and double quotes.
func asQuote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return `"` + s + `"`
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
