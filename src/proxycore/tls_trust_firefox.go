package proxycore

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

// installFirefoxEnterpriseRoots drops a policies.json into the Firefox
// installation directory that enables ImportEnterpriseRoots, causing Firefox
// to read the OS trust store (the same store InstallCATrust populates).
//
// Non-fatal: if Firefox is not installed or the write fails, we log and move on.
func installFirefoxEnterpriseRoots() {
	dir, err := CertDir()
	if err != nil {
		log.Printf("proxy: firefox policy: %v", err)
		return
	}
	flagPath := filepath.Join(dir, "firefox-policy.flag")
	if _, err := os.Stat(flagPath); err == nil {
		return // already installed
	}

	var installErr error
	switch runtime.GOOS {
	case "darwin":
		installErr = installFirefoxPolicyDarwin()
	case "windows":
		installErr = installFirefoxPolicyWindows()
	default:
		installFirefoxPolicyLinuxHint()
		return
	}

	if installErr != nil {
		log.Printf("proxy: firefox policy install failed: %v", installErr)
		return
	}

	_ = os.WriteFile(flagPath, []byte("ok"), 0644)
}

// firefoxPolicyJSON builds the policies.json content, merging with any
// existing file at path so we don't clobber enterprise policies.
func firefoxPolicyJSON(existingPath string) ([]byte, error) {
	root := map[string]interface{}{
		"policies": map[string]interface{}{
			"Certificates": map[string]interface{}{
				"ImportEnterpriseRoots": true,
			},
		},
	}

	data, err := os.ReadFile(existingPath)
	if err == nil && len(data) > 0 {
		var existing map[string]interface{}
		if json.Unmarshal(data, &existing) == nil {
			// Merge: ensure policies.Certificates.ImportEnterpriseRoots = true
			policies, _ := existing["policies"].(map[string]interface{})
			if policies == nil {
				policies = map[string]interface{}{}
				existing["policies"] = policies
			}
			certs, _ := policies["Certificates"].(map[string]interface{})
			if certs == nil {
				certs = map[string]interface{}{}
				policies["Certificates"] = certs
			}
			certs["ImportEnterpriseRoots"] = true
			root = existing
		}
	}

	return json.MarshalIndent(root, "", "  ")
}

// ── macOS ───────────────────────────────────────────────────────────────────

func installFirefoxPolicyDarwin() error {
	// Use /Library/Application Support/Mozilla/ — the system-wide policy
	// directory that Firefox reads. Unlike the distribution/ dir inside the
	// .app bundle, this path is NOT blocked by SIP.
	policyDir := "/Library/Application Support/Mozilla"
	policyPath := filepath.Join(policyDir, "policies.json")

	content, err := firefoxPolicyJSON(policyPath)
	if err != nil {
		return fmt.Errorf("building policies.json: %w", err)
	}

	// The directory is owned by root, so we need sudo.
	// Write to a temp file, then sudo cp.
	tmpFile, err := os.CreateTemp("", "devstudio-firefox-policy-*.json")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	_, _ = tmpFile.Write(content)
	tmpFile.Close()

	shellCmd := fmt.Sprintf(
		"sudo mkdir -p %s && sudo cp %s %s && sudo chmod 644 %s && rm -f %s && echo 'Firefox HTTPS policy installed. You can close this window.' || echo 'Failed — try Re-trust Certificate in DevStudio.'; exit",
		shellQuote(policyDir),
		shellQuote(tmpPath),
		shellQuote(policyPath),
		shellQuote(policyPath),
		shellQuote(tmpPath),
	)
	script := fmt.Sprintf(
		`tell application "Terminal"
    activate
    do script %s
end tell`,
		asQuote(shellCmd),
	)

	log.Printf("proxy: installing Firefox enterprise roots policy (requires sudo)")
	out, err := exec.Command("osascript", "-e", script).CombinedOutput()
	if err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("osascript: %w: %s", err, out)
	}
	return nil
}

// ── Windows ─────────────────────────────────────────────────────────────────

func installFirefoxPolicyWindows() error {
	candidates := []string{
		`C:\Program Files\Mozilla Firefox\distribution`,
		`C:\Program Files (x86)\Mozilla Firefox\distribution`,
	}

	var distDir string
	for _, c := range candidates {
		if _, err := os.Stat(filepath.Dir(c)); err == nil {
			distDir = c
			break
		}
	}
	if distDir == "" {
		return fmt.Errorf("Firefox not found in Program Files")
	}

	policyPath := filepath.Join(distDir, "policies.json")
	content, err := firefoxPolicyJSON(policyPath)
	if err != nil {
		return fmt.Errorf("building policies.json: %w", err)
	}

	// Write to temp, then use PowerShell with elevation to mkdir + copy.
	tmpFile, err := os.CreateTemp("", "devstudio-firefox-policy-*.json")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpPath := tmpFile.Name()
	_, _ = tmpFile.Write(content)
	tmpFile.Close()

	psCmd := fmt.Sprintf(
		`$d='%s'; $s='%s'; $t='%s'; if (!(Test-Path $d)) { New-Item -ItemType Directory -Path $d -Force }; Copy-Item $s $t -Force; Remove-Item $s -Force`,
		winQuote(distDir), winQuote(tmpPath), winQuote(policyPath),
	)

	log.Printf("proxy: installing Firefox enterprise roots policy (requires admin)")
	out, err := exec.Command(
		"powershell", "-NoProfile", "-NonInteractive", "-Command",
		fmt.Sprintf(`Start-Process powershell -ArgumentList '-NoProfile','-Command','%s' -Verb RunAs -Wait`, winQuote(psCmd)),
	).CombinedOutput()
	if err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("powershell: %w: %s", err, out)
	}
	return nil
}

// ── Linux ───────────────────────────────────────────────────────────────────

func installFirefoxPolicyLinuxHint() {
	candidates := []string{
		"/usr/lib/firefox/distribution",
		"/usr/lib64/firefox/distribution",
		"/usr/lib/firefox-esr/distribution",
	}
	for _, c := range candidates {
		if _, err := os.Stat(filepath.Dir(c)); err == nil {
			log.Printf("proxy: to enable Firefox HTTPS: sudo mkdir -p %s && echo '{\"policies\":{\"Certificates\":{\"ImportEnterpriseRoots\":true}}}' | sudo tee %s/policies.json", c, c)
			return
		}
	}
	log.Printf("proxy: Firefox not found; skip Firefox policy installation")
}

// FirefoxPolicyInstalled reports whether the firefox-policy.flag exists.
func FirefoxPolicyInstalled() bool {
	dir, err := CertDir()
	if err != nil {
		return false
	}
	_, err = os.Stat(filepath.Join(dir, "firefox-policy.flag"))
	return err == nil
}

// RemoveFirefoxPolicyFlag removes the flag so the next trust attempt re-runs.
func RemoveFirefoxPolicyFlag() {
	dir, err := CertDir()
	if err != nil {
		return
	}
	_ = os.Remove(filepath.Join(dir, "firefox-policy.flag"))
}
