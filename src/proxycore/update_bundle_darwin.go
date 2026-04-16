//go:build darwin

package proxycore

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// appBundlePath returns the .app bundle directory path if the current
// binary is running inside one, or empty string otherwise.
// Example: /Users/foo/Applications/devproxy.app
func appBundlePath() string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	p := exe
	for p != "/" && p != "." {
		if strings.HasSuffix(p, ".app") {
			return p
		}
		p = filepath.Dir(p)
	}
	return ""
}

// checkBundleWritable verifies the parent directory of the .app bundle is
// writable. Refuses with a user-friendly message if not (e.g. /Applications/).
func checkBundleWritable(dir string) error {
	probe := filepath.Join(dir, ".devproxy-write-probe")
	f, err := os.Create(probe)
	if err != nil {
		return fmt.Errorf("install location %q is not writable — move the app to ~/Applications/ or drag-install the new version manually", dir)
	}
	f.Close()
	os.Remove(probe)
	return nil
}

// bundleAssetName returns the .app.zip release asset name for the current arch.
func bundleAssetName() string {
	return fmt.Sprintf("devproxy-darwin-%s.app.zip", runtime.GOARCH)
}

// releaseAssetURL constructs the GitHub release download URL for an asset.
func releaseAssetURL(version, assetName string) string {
	tag := version
	if !strings.HasPrefix(tag, "v") {
		tag = "v" + tag
	}
	return fmt.Sprintf("https://github.com/%s/%s/releases/download/%s/%s",
		updateOwner, updateRepo, tag, assetName)
}

// applyBundleUpdate downloads the .app.zip, verifies SHA256, extracts, and
// atomically swaps the current .app bundle. Called instead of downloadAndApply
// when BuildSource == "wails-app".
func applyBundleUpdate(ctx context.Context, targetVersion string) error {
	currentApp := appBundlePath()
	if currentApp == "" {
		return fmt.Errorf("not running inside a .app bundle")
	}

	parentDir := filepath.Dir(currentApp)

	// Refuse if install location is not writable.
	if err := checkBundleWritable(parentDir); err != nil {
		return err
	}

	// Disk-space precheck: 3× current bundle size.
	if size, err := dirSize(currentApp); err == nil && size > 0 {
		if dsErr := checkDiskSpace(parentDir, size*3); dsErr != nil {
			return dsErr
		}
	}

	// Fetch expected SHA256 from checksums.txt.
	expectedSHA, err := fetchBundleSHA(ctx, targetVersion)
	if err != nil {
		return fmt.Errorf("fetch checksum: %w", err)
	}

	setUpdateStatus(UpdateStateDownloading, 0.3, "")

	// Download .app.zip to a temp file next to the current .app.
	ts := time.Now().Unix()
	tmpZip := filepath.Join(parentDir, fmt.Sprintf(".devproxy-update-%d.zip", ts))
	assetURL := releaseAssetURL(targetVersion, bundleAssetName())
	if err := downloadFile(ctx, assetURL, tmpZip); err != nil {
		os.Remove(tmpZip)
		return fmt.Errorf("download bundle: %w", err)
	}
	defer os.Remove(tmpZip)

	setUpdateStatus(UpdateStateVerifying, 0.6, "")

	// Verify SHA256.
	if err := verifySHA256(tmpZip, expectedSHA); err != nil {
		return fmt.Errorf("SHA256 verification failed: %w", err)
	}

	// Extract to a staging directory.
	stagingDir := filepath.Join(parentDir, fmt.Sprintf(".devproxy-staging-%d", ts))
	if err := extractZip(tmpZip, stagingDir); err != nil {
		os.RemoveAll(stagingDir)
		return fmt.Errorf("extract bundle: %w", err)
	}
	defer os.RemoveAll(stagingDir)

	// Locate the .app inside the extraction.
	newApp, err := findAppInDir(stagingDir)
	if err != nil {
		return fmt.Errorf("find extracted .app: %w", err)
	}

	// Verify code signature on the extracted bundle (non-fatal warning).
	if err := verifyCodesign(newApp); err != nil {
		log.Printf("proxy: codesign verification warning (non-fatal): %v", err)
	}

	setUpdateStatus(UpdateStateApplying, 0.8, "")

	// Atomic swap: current → .old, new → current.
	oldApp := currentApp + ".old"
	os.RemoveAll(oldApp) // clean up any previous .old

	if err := os.Rename(currentApp, oldApp); err != nil {
		return fmt.Errorf("move current bundle to .old: %w", err)
	}

	if err := os.Rename(newApp, currentApp); err != nil {
		// Rollback: restore .old → current.
		if rbErr := os.Rename(oldApp, currentApp); rbErr != nil {
			log.Printf("proxy: CRITICAL — rollback of rename also failed: %v", rbErr)
		}
		return fmt.Errorf("move new bundle into place: %w", err)
	}

	log.Printf("proxy: bundle swap complete: %s updated to %s", filepath.Base(currentApp), targetVersion)
	return nil
}

// rollbackBundleUpdate restores the .old backup of the .app bundle.
func rollbackBundleUpdate() {
	// After swap, os.Executable() points into the .old bundle.
	// Derive the canonical .app path by stripping ".old".
	exe, err := os.Executable()
	if err != nil {
		log.Printf("proxy: bundle rollback failed — cannot determine executable: %v", err)
		return
	}

	var currentApp string
	p := exe
	for p != "/" && p != "." {
		if strings.HasSuffix(p, ".app.old") {
			currentApp = strings.TrimSuffix(p, ".old")
			break
		}
		if strings.HasSuffix(p, ".app") {
			currentApp = p
			break
		}
		p = filepath.Dir(p)
	}

	if currentApp == "" {
		log.Printf("proxy: bundle rollback failed — cannot determine .app path from %s", exe)
		return
	}

	oldApp := currentApp + ".old"
	if _, err := os.Stat(oldApp); err != nil {
		log.Printf("proxy: bundle rollback — no .old backup at %s", oldApp)
		return
	}

	os.RemoveAll(currentApp)
	if err := os.Rename(oldApp, currentApp); err != nil {
		log.Printf("proxy: bundle rollback failed — rename %s → %s: %v", oldApp, currentApp, err)
		return
	}
	log.Printf("proxy: bundle rollback succeeded — restored %s", currentApp)
}

// restartBundleApp re-launches the .app bundle via "open -a" and exits.
func restartBundleApp() error {
	// After bundle swap, os.Executable() may point into .app.old.
	// Derive the canonical .app path.
	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("executable path: %w", err)
	}

	var appPath string
	p := exe
	for p != "/" && p != "." {
		if strings.HasSuffix(p, ".app.old") {
			appPath = strings.TrimSuffix(p, ".old")
			break
		}
		if strings.HasSuffix(p, ".app") {
			appPath = p
			break
		}
		p = filepath.Dir(p)
	}

	if appPath == "" {
		return fmt.Errorf("cannot determine .app bundle path from %s", exe)
	}

	log.Printf("proxy: relaunching bundle: open -a %s", appPath)
	cmd := exec.Command("open", "-a", appPath)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("relaunch bundle: %w", err)
	}

	os.Exit(0)
	return nil // unreachable
}

// fetchBundleSHA downloads checksums.txt from the release and extracts the
// SHA256 for the .app.zip asset.
func fetchBundleSHA(ctx context.Context, version string) (string, error) {
	url := releaseAssetURL(version, "checksums.txt")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("checksums.txt: HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	assetName := bundleAssetName()
	for _, line := range strings.Split(string(body), "\n") {
		parts := strings.Fields(line)
		if len(parts) >= 2 && parts[1] == assetName {
			return parts[0], nil
		}
	}

	return "", fmt.Errorf("SHA256 for %s not found in checksums.txt", assetName)
}

// downloadFile downloads a URL to a local file path.
func downloadFile(ctx context.Context, url, dest string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}

	f, err := os.Create(dest)
	if err != nil {
		return err
	}

	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// verifySHA256 computes the SHA256 of a file and compares it to the expected
// hex digest.
func verifySHA256(path, expected string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}

	got := hex.EncodeToString(h.Sum(nil))
	if !strings.EqualFold(got, expected) {
		return fmt.Errorf("expected %s, got %s", expected, got)
	}
	return nil
}

// extractZip extracts a zip archive to destDir.
func extractZip(zipPath, destDir string) error {
	r, err := zip.OpenReader(zipPath)
	if err != nil {
		return err
	}
	defer r.Close()

	cleanDest := filepath.Clean(destDir)

	for _, f := range r.File {
		target := filepath.Join(destDir, f.Name) //nolint:gosec // zip contents from our own signed release
		cleanTarget := filepath.Clean(target)

		// Zip-slip guard.
		if cleanTarget != cleanDest && !strings.HasPrefix(cleanTarget, cleanDest+string(os.PathSeparator)) {
			return fmt.Errorf("illegal path in zip: %s", f.Name)
		}

		if f.FileInfo().IsDir() {
			os.MkdirAll(target, f.Mode()) //nolint:errcheck
			continue
		}

		os.MkdirAll(filepath.Dir(target), 0o755) //nolint:errcheck

		out, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			return err
		}

		rc, err := f.Open()
		if err != nil {
			out.Close()
			return err
		}

		_, copyErr := io.Copy(out, rc)
		rc.Close()
		out.Close()
		if copyErr != nil {
			return copyErr
		}
	}
	return nil
}

// findAppInDir finds the first .app bundle directory inside dir.
func findAppInDir(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}

	for _, e := range entries {
		if e.IsDir() && strings.HasSuffix(e.Name(), ".app") {
			return filepath.Join(dir, e.Name()), nil
		}
	}

	// ditto --keepParent may nest the .app one level deep.
	if len(entries) == 1 && entries[0].IsDir() {
		subEntries, err := os.ReadDir(filepath.Join(dir, entries[0].Name()))
		if err == nil {
			for _, e := range subEntries {
				if e.IsDir() && strings.HasSuffix(e.Name(), ".app") {
					return filepath.Join(dir, entries[0].Name(), e.Name()), nil
				}
			}
		}
	}

	return "", fmt.Errorf("no .app bundle found in %s", dir)
}

// verifyCodesign runs codesign --verify on the bundle.
func verifyCodesign(appPath string) error {
	cmd := exec.Command("codesign", "--verify", "--deep", "--strict", appPath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %w", strings.TrimSpace(string(output)), err)
	}
	return nil
}

// dirSize recursively sums the size of all files in a directory.
func dirSize(path string) (int64, error) {
	var total int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}
