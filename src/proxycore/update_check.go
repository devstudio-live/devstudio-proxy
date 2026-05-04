package proxycore

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/creativeprojects/go-selfupdate"
)

const (
	updateOwner    = "devstudio-live"
	updateRepo     = "devstudio-proxy"
	updateCacheTTL = 10 * time.Minute
)

// UpdateInfo holds the result of a version check against GitHub releases.
type UpdateInfo struct {
	Current         string `json:"current"`
	Latest          string `json:"latest"`
	UpdateAvailable bool   `json:"update_available"`
	PublishedAt     string `json:"published_at,omitempty"`
	Notes           string `json:"notes,omitempty"`
	AssetURL        string `json:"asset_url,omitempty"`
}

var (
	updateCacheMu   sync.RWMutex
	updateCacheInfo *UpdateInfo
	updateCacheTime time.Time
)

// CheckForUpdate queries GitHub for the latest release and compares it to
// the current version. Results are cached for 10 minutes unless force is true.
func CheckForUpdate(ctx context.Context, force bool) (*UpdateInfo, error) {
	if !force {
		updateCacheMu.RLock()
		if updateCacheInfo != nil && time.Since(updateCacheTime) < updateCacheTTL {
			info := *updateCacheInfo
			updateCacheMu.RUnlock()
			return &info, nil
		}
		updateCacheMu.RUnlock()
	}

	current := strings.TrimPrefix(Version, "v")

	source, err := selfupdate.NewGitHubSource(selfupdate.GitHubConfig{})
	if err != nil {
		return nil, fmt.Errorf("github source: %w", err)
	}

	updater, err := selfupdate.NewUpdater(selfupdate.Config{
		Source: source,
	})
	if err != nil {
		return nil, fmt.Errorf("updater: %w", err)
	}

	release, found, err := updater.DetectLatest(ctx, selfupdate.ParseSlug(updateOwner+"/"+updateRepo))
	if err != nil {
		return nil, fmt.Errorf("detect latest: %w", err)
	}

	info := &UpdateInfo{
		Current: Version,
	}

	if !found || release == nil {
		info.Latest = Version
		info.UpdateAvailable = false
	} else {
		info.Latest = release.Version()
		info.Notes = release.ReleaseNotes
		info.AssetURL = release.AssetURL
		if !release.PublishedAt.IsZero() {
			info.PublishedAt = release.PublishedAt.Format(time.RFC3339)
		}
		// For "dev" version, any release is an update
		if Version == "dev" {
			info.UpdateAvailable = true
		} else {
			info.UpdateAvailable = release.GreaterThan(current)
		}
	}

	updateCacheMu.Lock()
	updateCacheInfo = info
	updateCacheTime = time.Now()
	updateCacheMu.Unlock()

	return info, nil
}

// CachedUpdateAvailable returns whether an update is available based on the
// last cached check. Returns false if no check has been performed yet.
func CachedUpdateAvailable() bool {
	updateCacheMu.RLock()
	defer updateCacheMu.RUnlock()
	if updateCacheInfo == nil {
		return false
	}
	return updateCacheInfo.UpdateAvailable
}

// BackgroundUpdateCheck performs a non-blocking version check on startup.
// Network errors are swallowed to avoid spamming logs when offline.
func BackgroundUpdateCheck() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	info, err := CheckForUpdate(ctx, false)
	if err != nil {
		// Swallow network errors silently — don't spam the log when offline.
		return
	}
	if info.UpdateAvailable {
		log.Printf("proxy: update available: %s → %s", info.Current, info.Latest)
	}
}
