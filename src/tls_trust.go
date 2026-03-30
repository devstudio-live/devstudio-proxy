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

// installCATrustMacOS trusts the CA in the user's login keychain.
//
// The proxy runs as a Chrome native messaging host without a GUI security
// session. Targeting the System keychain requires sudo and a GUI auth dialog,
// which fails in this context. Instead we add to the login keychain, which
// Chrome also reads for root CA trust and does not require root or a GUI
// session. A macOS keychain unlock dialog may still appear on the first run.
func installCATrustMacOS(caPath string) error {
	// Expand ~ manually since exec.Command does not invoke a shell.
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("could not determine home directory: %w", err)
	}
	loginKeychain := filepath.Join(homeDir, "Library", "Keychains", "login.keychain-db")

	out, err := exec.Command(
		"security", "add-trusted-cert",
		"-d", "-r", "trustRoot",
		"-k", loginKeychain,
		caPath,
	).CombinedOutput()
	if err != nil {
		// Fall back to Terminal+sudo for System keychain if login keychain fails.
		return installCATrustMacOSTerminal(caPath, fmt.Sprintf("login keychain failed (%s), trying system keychain", strings.TrimSpace(string(out))))
	}
	return nil
}

// installCATrustMacOSTerminal is the fallback: opens a Terminal window to run
// the security command with sudo against the System keychain.
func installCATrustMacOSTerminal(caPath string, reason string) error {
	log.Printf("proxy: %s", reason)
	shellCmd := fmt.Sprintf(
		"sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain %s && echo 'Certificate trusted. You can close this window.' || echo 'Trust failed — try Re-trust Certificate in DevStudio.'; exit",
		shellQuote(caPath),
	)
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
