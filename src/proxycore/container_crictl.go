package proxycore

import (
	"context"
	"encoding/json"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// CrictlAdapter communicates with CRI-O (or any CRI-compatible runtime) via
// the crictl CLI, parsing JSON output from commands like `crictl ps -o json`.
type CrictlAdapter struct {
	binaryPath string
}

// NewCrictlAdapter creates an adapter for crictl.
func NewCrictlAdapter(binaryPath string) *CrictlAdapter {
	return &CrictlAdapter{binaryPath: binaryPath}
}

func (c *CrictlAdapter) Name() string { return "crictl" }

func (c *CrictlAdapter) run(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, c.binaryPath, args...)
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, &containerError{msg: "crictl: " + strings.TrimSpace(string(exitErr.Stderr))}
		}
		return nil, err
	}
	return out, nil
}

// ── Detect ──────────────────────────────────────────────────────────────────

func (c *CrictlAdapter) Detect() (*RuntimeInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := c.run(ctx, "version")
	if err != nil {
		return &RuntimeInfo{
			Name:       "crictl",
			BinaryPath: c.binaryPath,
			Available:  false,
		}, err
	}

	// Parse "Version:  0.1.0\nRuntimeName:  cri-o\n..." text output
	version := ""
	runtimeName := ""
	for _, line := range strings.Split(string(out), "\n") {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		switch key {
		case "Version":
			version = val
		case "RuntimeName":
			runtimeName = val
		}
	}

	return &RuntimeInfo{
		Name:        "crictl",
		DisplayName: "crictl (" + runtimeName + ")",
		Version:     version,
		BinaryPath:  c.binaryPath,
		Available:   true,
	}, nil
}

// ── Containers ──────────────────────────────────────────────────────────────

func (c *CrictlAdapter) ListContainers(filters map[string]string) ([]ContainerInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	args := []string{"ps", "-a", "-o", "json"}
	if name, ok := filters["name"]; ok {
		args = append(args, "--name", name)
	}
	if state, ok := filters["state"]; ok {
		args = append(args, "--state", state)
	}

	out, err := c.run(ctx, args...)
	if err != nil {
		return nil, err
	}

	var raw struct {
		Containers []crictlContainerJSON `json:"containers"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}

	containers := make([]ContainerInfo, 0, len(raw.Containers))
	for _, ct := range raw.Containers {
		containers = append(containers, c.normalizeContainer(ct))
	}
	return containers, nil
}

func (c *CrictlAdapter) InspectContainer(id string) (*ContainerDetail, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := c.run(ctx, "inspect", id, "-o", "json")
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}

	detail := &ContainerDetail{Raw: raw}
	detail.Runtime = "crictl"

	if info, ok := raw["info"].(map[string]any); ok {
		if config, ok := info["config"].(map[string]any); ok {
			if metadata, ok := config["metadata"].(map[string]any); ok {
				detail.Name = getStr(metadata, "name")
			}
			if image, ok := config["image"].(map[string]any); ok {
				detail.Image = getStr(image, "image")
			}
			if envs, ok := config["envs"].([]any); ok {
				for _, e := range envs {
					if env, ok := e.(map[string]any); ok {
						detail.Env = append(detail.Env, getStr(env, "key")+"="+getStr(env, "value"))
					}
				}
			}
		}
	}

	if status, ok := raw["status"].(map[string]any); ok {
		detail.ID = truncateID(getStr(status, "id"))
		detail.State = crictlStateToString(getStr(status, "state"))
		detail.Status = detail.State
	}

	return detail, nil
}

// ── Images ──────────────────────────────────────────────────────────────────

func (c *CrictlAdapter) ListImages(filters map[string]string) ([]ImageInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	out, err := c.run(ctx, "images", "-o", "json")
	if err != nil {
		return nil, err
	}

	var raw struct {
		Images []crictlImageJSON `json:"images"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}

	images := make([]ImageInfo, 0, len(raw.Images))
	for _, img := range raw.Images {
		images = append(images, c.normalizeImage(img))
	}
	return images, nil
}

func (c *CrictlAdapter) InspectImage(id string) (*ImageDetail, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := c.run(ctx, "inspecti", id, "-o", "json")
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}

	detail := &ImageDetail{Raw: raw}
	detail.Runtime = "crictl"

	if info, ok := raw["info"].(map[string]any); ok {
		if imageSpec, ok := info["imageSpec"].(map[string]any); ok {
			if config, ok := imageSpec["config"].(map[string]any); ok {
				if env, ok := config["Env"].([]any); ok {
					for _, e := range env {
						if s, ok := e.(string); ok {
							detail.Env = append(detail.Env, s)
						}
					}
				}
			}
		}
	}

	if status, ok := raw["status"].(map[string]any); ok {
		detail.ID = truncateID(getStr(status, "id"))
		if repoTags, ok := status["repoTags"].([]any); ok && len(repoTags) > 0 {
			if tag, ok := repoTags[0].(string); ok {
				detail.Repository, detail.Tag = splitRepoTag(tag)
			}
		}
	}

	return detail, nil
}

// ── Container lifecycle (Phase 2A) ─────────────────────────────────────────

func (c *CrictlAdapter) StartContainer(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := c.run(ctx, "start", id)
	return err
}

func (c *CrictlAdapter) StopContainer(id string, timeout int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout+10)*time.Second)
	defer cancel()
	_, err := c.run(ctx, "stop", "--timeout", strconv.Itoa(timeout), id)
	return err
}

func (c *CrictlAdapter) RestartContainer(id string, timeout int) error {
	// crictl has no restart; stop then start
	if err := c.StopContainer(id, timeout); err != nil {
		return err
	}
	return c.StartContainer(id)
}

func (c *CrictlAdapter) RemoveContainer(id string, force bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	args := []string{"rm", id}
	if force {
		args = []string{"rm", "-f", id}
	}
	_, err := c.run(ctx, args...)
	return err
}

func (c *CrictlAdapter) PauseContainer(_ string) error {
	return &containerError{msg: "crictl does not support pause"}
}

func (c *CrictlAdapter) UnpauseContainer(_ string) error {
	return &containerError{msg: "crictl does not support unpause"}
}

// ── Image write operations (Phase 2A) ──────────────────────────────────────

func (c *CrictlAdapter) PullImage(ref string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, err := c.run(ctx, "pull", ref)
	return err
}

func (c *CrictlAdapter) RemoveImage(id string, _ bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := c.run(ctx, "rmi", id)
	return err
}

func (c *CrictlAdapter) PruneImages(_ bool) (*PruneResult, error) {
	return &PruneResult{}, &containerError{msg: "crictl does not support image prune"}
}

func (c *CrictlAdapter) TagImage(_, _ string) error {
	return &containerError{msg: "crictl does not support image tagging"}
}

// ── Volume write operations (Phase 2A) — not supported ─────────────────────

func (c *CrictlAdapter) CreateVolume(_, _ string, _ map[string]string) (*VolumeInfo, error) {
	return nil, &containerError{msg: "crictl does not support volume management"}
}

func (c *CrictlAdapter) RemoveVolume(_ string, _ bool) error {
	return &containerError{msg: "crictl does not support volume management"}
}

func (c *CrictlAdapter) PruneVolumes() (*PruneResult, error) {
	return nil, &containerError{msg: "crictl does not support volume management"}
}

// ── Network write operations (Phase 2A) — not supported ────────────────────

func (c *CrictlAdapter) CreateNetwork(_ string, _ string, _ map[string]string) (*NetworkInfo, error) {
	return nil, &containerError{msg: "crictl does not support network management"}
}

func (c *CrictlAdapter) RemoveNetwork(_ string) error {
	return &containerError{msg: "crictl does not support network management"}
}

func (c *CrictlAdapter) PruneNetworks() (*PruneResult, error) {
	return nil, &containerError{msg: "crictl does not support network management"}
}

// ── Volumes (not supported by crictl) ───────────────────────────────────────

func (c *CrictlAdapter) ListVolumes() ([]VolumeInfo, error) {
	return []VolumeInfo{}, nil
}

// ── Networks (not supported by crictl) ──────────────────────────────────────

func (c *CrictlAdapter) ListNetworks() ([]NetworkInfo, error) {
	return []NetworkInfo{}, nil
}

// ── System Info ─────────────────────────────────────────────────────────────

func (c *CrictlAdapter) SystemInfo() (*SystemInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := c.run(ctx, "info", "-o", "json")
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}

	info := &SystemInfo{Runtime: "crictl"}

	if status, ok := raw["status"].(map[string]any); ok {
		if conditions, ok := status["conditions"].([]any); ok {
			_ = conditions // CRI status conditions
		}
	}

	return info, nil
}

// ── crictl JSON models (internal) ───────────────────────────────────────────

type crictlContainerJSON struct {
	ID          string            `json:"id"`
	PodSandboxID string          `json:"podSandboxId"`
	Metadata    struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Image struct {
		Image string `json:"image"`
	} `json:"image"`
	ImageRef    string            `json:"imageRef"`
	State       string            `json:"state"`
	CreatedAt   string            `json:"createdAt"` // nanoseconds as string
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
}

type crictlImageJSON struct {
	ID       string   `json:"id"`
	RepoTags []string `json:"repoTags"`
	Size     string   `json:"size"` // bytes as string
}

// ── Normalization helpers ───────────────────────────────────────────────────

func (c *CrictlAdapter) normalizeContainer(ct crictlContainerJSON) ContainerInfo {
	created := time.Time{}
	// createdAt is nanoseconds since epoch as string
	if ct.CreatedAt != "" {
		if ns, err := time.Parse(time.RFC3339Nano, ct.CreatedAt); err == nil {
			created = ns
		}
	}

	state := crictlStateToString(ct.State)

	return ContainerInfo{
		ID:      truncateID(ct.ID),
		Name:    ct.Metadata.Name,
		Image:   ct.Image.Image,
		ImageID: truncateID(ct.ImageRef),
		Created: created,
		State:   state,
		Status:  state,
		Labels:  ct.Labels,
		Runtime: "crictl",
	}
}

func (c *CrictlAdapter) normalizeImage(img crictlImageJSON) ImageInfo {
	repo := "<none>"
	tag := "<none>"
	dangling := true
	if len(img.RepoTags) > 0 && img.RepoTags[0] != "" {
		repo, tag = splitRepoTag(img.RepoTags[0])
		dangling = false
	}

	var size int64
	if img.Size != "" {
		// size is bytes as string
		for _, c := range img.Size {
			if c >= '0' && c <= '9' {
				size = size*10 + int64(c-'0')
			} else {
				break
			}
		}
	}

	return ImageInfo{
		ID:         truncateID(img.ID),
		Repository: repo,
		Tag:        tag,
		Size:       size,
		Dangling:   dangling,
		Runtime:    "crictl",
	}
}

func crictlStateToString(state string) string {
	switch strings.ToUpper(state) {
	case "CONTAINER_RUNNING":
		return "running"
	case "CONTAINER_EXITED":
		return "exited"
	case "CONTAINER_CREATED":
		return "created"
	case "CONTAINER_UNKNOWN":
		return "unknown"
	default:
		return strings.ToLower(state)
	}
}
