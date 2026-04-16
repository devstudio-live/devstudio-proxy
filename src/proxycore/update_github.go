package proxycore

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/creativeprojects/go-selfupdate"
)

// downloadAndApply uses go-selfupdate to download, verify (SHA256 via
// checksums.txt), and atomically replace the current binary.
func downloadAndApply(ctx context.Context, targetVersion string) error {
	source, err := selfupdate.NewGitHubSource(selfupdate.GitHubConfig{})
	if err != nil {
		return fmt.Errorf("github source: %w", err)
	}

	updater, err := selfupdate.NewUpdater(selfupdate.Config{
		Source:    source,
		Validator: &selfupdate.ChecksumValidator{UniqueFilename: "checksums.txt"},
	})
	if err != nil {
		return fmt.Errorf("updater: %w", err)
	}

	current := strings.TrimPrefix(Version, "v")

	release, found, err := updater.DetectLatest(ctx, selfupdate.ParseSlug(updateOwner+"/"+updateRepo))
	if err != nil {
		return fmt.Errorf("detect latest: %w", err)
	}
	if !found || release == nil {
		return fmt.Errorf("no release found")
	}

	// Verify we're still downloading the expected version.
	if release.Version() != targetVersion {
		return fmt.Errorf("version drift: expected %s, got %s", targetVersion, release.Version())
	}

	// Double-check the release is actually newer (unless transitioning from "dev").
	if Version != "dev" && !release.GreaterThan(current) {
		return fmt.Errorf("release %s is not newer than current %s", release.Version(), Version)
	}

	setUpdateStatus(UpdateStateDownloading, 0.3, "")

	exe, err := os.Executable()
	if err != nil {
		return fmt.Errorf("executable path: %w", err)
	}

	setUpdateStatus(UpdateStateVerifying, 0.6, "")

	// UpdateTo downloads the asset, validates via ChecksumValidator,
	// writes to a temp file, then atomically renames to exe.
	// The old binary is preserved as exe + ".old".
	setUpdateStatus(UpdateStateApplying, 0.8, "")
	if err := updater.UpdateTo(ctx, release, exe); err != nil {
		return fmt.Errorf("apply update: %w", err)
	}

	return nil
}
