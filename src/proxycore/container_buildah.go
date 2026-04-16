package proxycore

import (
	"context"
	"encoding/json"
	"os/exec"
	"strings"
	"time"
)

// BuildahAdapter communicates with Buildah via its CLI, parsing JSON output.
// Buildah is image-focused: it manages "working containers" for building images
// but does not run long-lived containers. Its primary value is listing images
// that share the Podman/CRI-O storage backend.
type BuildahAdapter struct {
	binaryPath string
}

// NewBuildahAdapter creates an adapter for buildah.
func NewBuildahAdapter(binaryPath string) *BuildahAdapter {
	return &BuildahAdapter{binaryPath: binaryPath}
}

func (b *BuildahAdapter) Name() string { return "buildah" }

func (b *BuildahAdapter) run(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, b.binaryPath, args...)
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, &containerError{msg: "buildah: " + strings.TrimSpace(string(exitErr.Stderr))}
		}
		return nil, err
	}
	return out, nil
}

// ── Detect ──────────────────────────────────────────────────────────────────

func (b *BuildahAdapter) Detect() (*RuntimeInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := b.run(ctx, "version")
	if err != nil {
		return &RuntimeInfo{
			Name:       "buildah",
			BinaryPath: b.binaryPath,
			Available:  false,
		}, err
	}

	// Parse "Version:         1.33.0\nGo Version:      go1.21\n..." text output
	version := ""
	for _, line := range strings.Split(string(out), "\n") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 && strings.TrimSpace(parts[0]) == "Version" {
			version = strings.TrimSpace(parts[1])
			break
		}
	}

	return &RuntimeInfo{
		Name:        "buildah",
		DisplayName: "Buildah",
		Version:     version,
		BinaryPath:  b.binaryPath,
		Available:   true,
	}, nil
}

// ── Containers (working containers) ─────────────────────────────────────────

func (b *BuildahAdapter) ListContainers(filters map[string]string) ([]ContainerInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := b.run(ctx, "containers", "--json")
	if err != nil {
		return nil, err
	}

	var raw []buildahContainerJSON
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}

	containers := make([]ContainerInfo, 0, len(raw))
	for _, c := range raw {
		containers = append(containers, ContainerInfo{
			ID:      truncateID(c.ID),
			Name:    c.ContainerName,
			Image:   c.ImageName,
			ImageID: truncateID(c.ImageID),
			State:   "building", // Buildah containers are always in a "building" state
			Status:  "Building",
			Runtime: "buildah",
		})
	}
	return containers, nil
}

func (b *BuildahAdapter) InspectContainer(id string) (*ContainerDetail, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := b.run(ctx, "inspect", "--type", "container", id)
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}

	return &ContainerDetail{
		ContainerInfo: ContainerInfo{
			ID:      truncateID(getStr(raw, "ContainerID")),
			Name:    getStr(raw, "Container"),
			Image:   getStr(raw, "FromImage"),
			State:   "building",
			Status:  "Building",
			Runtime: "buildah",
		},
		Raw: raw,
	}, nil
}

// ── Images ──────────────────────────────────────────────────────────────────

func (b *BuildahAdapter) ListImages(filters map[string]string) ([]ImageInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	out, err := b.run(ctx, "images", "--json")
	if err != nil {
		return nil, err
	}

	var raw []buildahImageJSON
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}

	images := make([]ImageInfo, 0, len(raw))
	for _, img := range raw {
		repo := "<none>"
		tag := "<none>"
		dangling := true
		if len(img.Names) > 0 {
			repo, tag = splitRepoTag(img.Names[0])
			dangling = false
		}

		created := time.Time{}
		if img.CreatedAt != "" {
			if t, err := time.Parse(time.RFC3339Nano, img.CreatedAt); err == nil {
				created = t
			}
		}

		images = append(images, ImageInfo{
			ID:         truncateID(img.ID),
			Repository: repo,
			Tag:        tag,
			Created:    created,
			Size:       img.Size,
			Dangling:   dangling,
			Runtime:    "buildah",
		})
	}
	return images, nil
}

func (b *BuildahAdapter) InspectImage(id string) (*ImageDetail, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := b.run(ctx, "inspect", "--type", "image", id)
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}

	detail := &ImageDetail{Raw: raw}
	detail.Runtime = "buildah"
	detail.ID = truncateID(getStr(raw, "FromImageID"))

	if docker, ok := raw["Docker"].(map[string]any); ok {
		detail.Architecture = getStr(docker, "architecture")
		detail.OS = getStr(docker, "os")
		detail.Author = getStr(docker, "author")
	}

	return detail, nil
}

// ── Volumes (not supported by buildah) ──────────────────────────────────────

func (b *BuildahAdapter) ListVolumes() ([]VolumeInfo, error) {
	return []VolumeInfo{}, nil
}

// ── Networks (not supported by buildah) ─────────────────────────────────────

func (b *BuildahAdapter) ListNetworks() ([]NetworkInfo, error) {
	return []NetworkInfo{}, nil
}

// ── System Info ─────────────────────────────────────────────────────────────

func (b *BuildahAdapter) SystemInfo() (*SystemInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := b.run(ctx, "info", "--format", "json")
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}

	info := &SystemInfo{Runtime: "buildah"}

	if host, ok := raw["host"].(map[string]any); ok {
		info.OS = getStr(host, "os")
		info.Arch = getStr(host, "arch")
		info.KernelVersion = getStr(host, "kernel")
	}

	if store, ok := raw["store"].(map[string]any); ok {
		info.StorageDriver = getStr(store, "GraphDriverName")
		info.Images = getInt(store, "ImageStore")
		info.Containers = getInt(store, "ContainerStore")
	}

	return info, nil
}

// ── Buildah JSON models (internal) ──────────────────────────────────────────

type buildahContainerJSON struct {
	ID            string `json:"id"`
	Builder       bool   `json:"builder"`
	ImageID       string `json:"imageid"`
	ImageName     string `json:"imagename"`
	ContainerName string `json:"containername"`
}

type buildahImageJSON struct {
	ID        string   `json:"id"`
	Names     []string `json:"names"`
	Size      int64    `json:"size"`
	CreatedAt string   `json:"createdat"`
}
