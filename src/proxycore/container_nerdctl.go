package proxycore

import (
	"context"
	"encoding/json"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// NerdctlAdapter communicates with containerd via the nerdctl or finch CLI,
// parsing JSON output from commands like `nerdctl ps --format json`.
type NerdctlAdapter struct {
	binaryPath  string
	runtimeName string // "nerdctl" or "finch"
	namespace   string // containerd namespace, empty = default
}

// NewNerdctlAdapter creates an adapter for nerdctl or finch.
func NewNerdctlAdapter(binaryPath string, runtimeName string) *NerdctlAdapter {
	return &NerdctlAdapter{
		binaryPath:  binaryPath,
		runtimeName: runtimeName,
	}
}

func (n *NerdctlAdapter) Name() string { return n.runtimeName }

// SetNamespace sets the containerd namespace for subsequent commands.
func (n *NerdctlAdapter) SetNamespace(ns string) {
	n.namespace = ns
}

func (n *NerdctlAdapter) run(ctx context.Context, args ...string) ([]byte, error) {
	if n.namespace != "" {
		args = append([]string{"--namespace", n.namespace}, args...)
	}
	cmd := exec.CommandContext(ctx, n.binaryPath, args...)
	out, err := cmd.Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			return nil, &containerError{msg: n.runtimeName + ": " + strings.TrimSpace(string(exitErr.Stderr))}
		}
		return nil, err
	}
	return out, nil
}

// runJSONLines parses nerdctl's line-delimited JSON output (one JSON object per line).
func (n *NerdctlAdapter) runJSONLines(ctx context.Context, args ...string) ([]map[string]any, error) {
	out, err := n.run(ctx, args...)
	if err != nil {
		return nil, err
	}

	var results []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			continue
		}
		results = append(results, obj)
	}
	return results, nil
}

// ── Detect ──────────────────────────────────────────────────────────────────

func (n *NerdctlAdapter) Detect() (*RuntimeInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := n.run(ctx, "version", "--format", "json")
	if err != nil {
		return &RuntimeInfo{
			Name:       n.runtimeName,
			BinaryPath: n.binaryPath,
			Available:  false,
		}, err
	}

	var ver struct {
		Client struct {
			Version string `json:"Version"`
			OS      string `json:"Os"`
			Arch    string `json:"Arch"`
		} `json:"Client"`
	}
	json.Unmarshal(out, &ver)

	displayName := "nerdctl (containerd)"
	if n.runtimeName == "finch" {
		displayName = "Finch"
	}

	return &RuntimeInfo{
		Name:        n.runtimeName,
		DisplayName: displayName,
		Version:     ver.Client.Version,
		BinaryPath:  n.binaryPath,
		OS:          ver.Client.OS,
		Arch:        ver.Client.Arch,
		Available:   true,
	}, nil
}

// ── Containers ──────────────────────────────────────────────────────────────

func (n *NerdctlAdapter) ListContainers(filters map[string]string) ([]ContainerInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	args := []string{"ps", "-a", "--format", "json"}
	for k, v := range filters {
		args = append(args, "--filter", k+"="+v)
	}

	lines, err := n.runJSONLines(ctx, args...)
	if err != nil {
		return nil, err
	}

	containers := make([]ContainerInfo, 0, len(lines))
	for _, obj := range lines {
		containers = append(containers, n.normalizeContainer(obj))
	}
	return containers, nil
}

func (n *NerdctlAdapter) InspectContainer(id string) (*ContainerDetail, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := n.run(ctx, "inspect", id)
	if err != nil {
		return nil, err
	}

	// nerdctl inspect returns a JSON array
	var raw []map[string]any
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, &containerError{msg: "container not found: " + id}
	}

	detail := &ContainerDetail{Raw: raw[0]}
	detail.Runtime = n.runtimeName
	detail.ID = getStr(raw[0], "Id")
	detail.Name = strings.TrimPrefix(getStr(raw[0], "Name"), "/")

	if state, ok := raw[0]["State"].(map[string]any); ok {
		detail.State = strings.ToLower(getStr(state, "Status"))
		detail.Status = getStr(state, "Status")
		detail.ExitCode = getInt(state, "ExitCode")
	}

	if config, ok := raw[0]["Config"].(map[string]any); ok {
		detail.Image = getStr(config, "Image")
		if env, ok := config["Env"].([]any); ok {
			for _, e := range env {
				if s, ok := e.(string); ok {
					detail.Env = append(detail.Env, s)
				}
			}
		}
		detail.Labels = getStrMap(config, "Labels")
	}

	return detail, nil
}

// ── Images ──────────────────────────────────────────────────────────────────

func (n *NerdctlAdapter) ListImages(filters map[string]string) ([]ImageInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	args := []string{"images", "--format", "json"}
	if v, ok := filters["dangling"]; ok && v == "true" {
		args = append(args, "--filter", "dangling=true")
	}

	lines, err := n.runJSONLines(ctx, args...)
	if err != nil {
		return nil, err
	}

	images := make([]ImageInfo, 0, len(lines))
	for _, obj := range lines {
		images = append(images, n.normalizeImage(obj))
	}
	return images, nil
}

func (n *NerdctlAdapter) InspectImage(id string) (*ImageDetail, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := n.run(ctx, "image", "inspect", id)
	if err != nil {
		return nil, err
	}

	var raw []map[string]any
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, &containerError{msg: "image not found: " + id}
	}

	detail := &ImageDetail{Raw: raw[0]}
	detail.Runtime = n.runtimeName
	detail.ID = truncateID(getStr(raw[0], "Id"))
	detail.Architecture = getStr(raw[0], "Architecture")
	detail.OS = getStr(raw[0], "Os")

	if repoTags, ok := raw[0]["RepoTags"].([]any); ok && len(repoTags) > 0 {
		if tag, ok := repoTags[0].(string); ok {
			detail.Repository, detail.Tag = splitRepoTag(tag)
		}
	}

	return detail, nil
}

// ── Volumes ─────────────────────────────────────────────────────────────────

func (n *NerdctlAdapter) ListVolumes() ([]VolumeInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lines, err := n.runJSONLines(ctx, "volume", "ls", "--format", "json")
	if err != nil {
		return nil, err
	}

	volumes := make([]VolumeInfo, 0, len(lines))
	for _, obj := range lines {
		volumes = append(volumes, VolumeInfo{
			Name:       getStr(obj, "Name"),
			MountPoint: getStr(obj, "Mountpoint"),
			Runtime:    n.runtimeName,
			Size:       -1,
		})
	}
	return volumes, nil
}

// ── Networks ────────────────────────────────────────────────────────────────

func (n *NerdctlAdapter) ListNetworks() ([]NetworkInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lines, err := n.runJSONLines(ctx, "network", "ls", "--format", "json")
	if err != nil {
		return nil, err
	}

	networks := make([]NetworkInfo, 0, len(lines))
	for _, obj := range lines {
		networks = append(networks, NetworkInfo{
			ID:      truncateID(getStr(obj, "ID")),
			Name:    getStr(obj, "Name"),
			Driver:  getStr(obj, "Driver"),
			Scope:   getStr(obj, "Scope"),
			Runtime: n.runtimeName,
		})
	}
	return networks, nil
}

// ── System Info ─────────────────────────────────────────────────────────────

func (n *NerdctlAdapter) SystemInfo() (*SystemInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := n.run(ctx, "info", "--format", "json")
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}

	return &SystemInfo{
		Runtime:       n.runtimeName,
		Version:       getStr(raw, "ServerVersion"),
		OS:            getStr(raw, "OperatingSystem"),
		Arch:          getStr(raw, "Architecture"),
		KernelVersion: getStr(raw, "KernelVersion"),
	}, nil
}

// ── Container lifecycle (Phase 2A) ─────────────────────────────────────────

func (n *NerdctlAdapter) StartContainer(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := n.run(ctx, "start", id)
	return err
}

func (n *NerdctlAdapter) StopContainer(id string, timeout int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout+10)*time.Second)
	defer cancel()
	_, err := n.run(ctx, "stop", "-t", strconv.Itoa(timeout), id)
	return err
}

func (n *NerdctlAdapter) RestartContainer(id string, timeout int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout+10)*time.Second)
	defer cancel()
	_, err := n.run(ctx, "restart", "-t", strconv.Itoa(timeout), id)
	return err
}

func (n *NerdctlAdapter) RemoveContainer(id string, force bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	args := []string{"rm", id}
	if force {
		args = []string{"rm", "-f", id}
	}
	_, err := n.run(ctx, args...)
	return err
}

func (n *NerdctlAdapter) PauseContainer(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := n.run(ctx, "pause", id)
	return err
}

func (n *NerdctlAdapter) UnpauseContainer(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := n.run(ctx, "unpause", id)
	return err
}

// ── Image write operations (Phase 2A) ──────────────────────────────────────

func (n *NerdctlAdapter) PullImage(ref string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, err := n.run(ctx, "pull", ref)
	return err
}

func (n *NerdctlAdapter) RemoveImage(id string, force bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	args := []string{"rmi", id}
	if force {
		args = []string{"rmi", "-f", id}
	}
	_, err := n.run(ctx, args...)
	return err
}

func (n *NerdctlAdapter) PruneImages(dangling bool) (*PruneResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// nerdctl image prune -f (force to skip confirmation)
	out, err := n.run(ctx, "image", "prune", "-f")
	if err != nil {
		return nil, err
	}
	deleted := parseDeletedLines(string(out))
	return &PruneResult{ItemsDeleted: deleted}, nil
}

func (n *NerdctlAdapter) TagImage(source, target string) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := n.run(ctx, "tag", source, target)
	return err
}

// ── Volume write operations (Phase 2A) ─────────────────────────────────────

func (n *NerdctlAdapter) CreateVolume(name string, driver string, _ map[string]string) (*VolumeInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	args := []string{"volume", "create", name}
	if driver != "" {
		args = []string{"volume", "create", "--driver", driver, name}
	}
	_, err := n.run(ctx, args...)
	if err != nil {
		return nil, err
	}
	return &VolumeInfo{Name: name, Driver: driver, Runtime: n.runtimeName, Size: -1}, nil
}

func (n *NerdctlAdapter) RemoveVolume(name string, force bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	args := []string{"volume", "rm", name}
	if force {
		args = []string{"volume", "rm", "-f", name}
	}
	_, err := n.run(ctx, args...)
	return err
}

func (n *NerdctlAdapter) PruneVolumes() (*PruneResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := n.run(ctx, "volume", "prune", "-f")
	if err != nil {
		return nil, err
	}
	deleted := parseDeletedLines(string(out))
	return &PruneResult{ItemsDeleted: deleted}, nil
}

// ── Network write operations (Phase 2A) ────────────────────────────────────

func (n *NerdctlAdapter) CreateNetwork(name string, driver string, _ map[string]string) (*NetworkInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	args := []string{"network", "create", name}
	if driver != "" {
		args = []string{"network", "create", "--driver", driver, name}
	}
	out, err := n.run(ctx, args...)
	if err != nil {
		return nil, err
	}
	netID := strings.TrimSpace(string(out))
	return &NetworkInfo{ID: truncateID(netID), Name: name, Driver: driver, Runtime: n.runtimeName}, nil
}

func (n *NerdctlAdapter) RemoveNetwork(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := n.run(ctx, "network", "rm", id)
	return err
}

func (n *NerdctlAdapter) PruneNetworks() (*PruneResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := n.run(ctx, "network", "prune", "-f")
	if err != nil {
		return nil, err
	}
	deleted := parseDeletedLines(string(out))
	return &PruneResult{ItemsDeleted: deleted}, nil
}

// parseDeletedLines extracts non-empty lines from CLI prune output (typically IDs or names).
func parseDeletedLines(output string) []string {
	var items []string
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "Total") && !strings.HasPrefix(line, "Deleted") {
			items = append(items, line)
		}
	}
	return items
}

// ── Normalization helpers ───────────────────────────────────────────────────

func (n *NerdctlAdapter) normalizeContainer(obj map[string]any) ContainerInfo {
	created := time.Time{}
	if cs := getStr(obj, "CreatedAt"); cs != "" {
		if t, err := time.Parse(time.RFC3339Nano, cs); err == nil {
			created = t
		}
	}

	name := getStr(obj, "Names")
	// nerdctl may return space-separated names
	if idx := strings.Index(name, " "); idx > 0 {
		name = name[:idx]
	}

	return ContainerInfo{
		ID:      truncateID(getStr(obj, "ID")),
		Name:    name,
		Image:   getStr(obj, "Image"),
		Command: getStr(obj, "Command"),
		Created: created,
		State:   strings.ToLower(getStr(obj, "Status")),
		Status:  getStr(obj, "Status"),
		Runtime: n.runtimeName,
	}
}

func (n *NerdctlAdapter) normalizeImage(obj map[string]any) ImageInfo {
	created := time.Time{}
	if cs := getStr(obj, "CreatedAt"); cs != "" {
		if t, err := time.Parse(time.RFC3339Nano, cs); err == nil {
			created = t
		}
	}

	repo := getStr(obj, "Repository")
	tag := getStr(obj, "Tag")
	dangling := repo == "<none>" && tag == "<none>"

	return ImageInfo{
		ID:         truncateID(getStr(obj, "ID")),
		Repository: repo,
		Tag:        tag,
		Created:    created,
		Dangling:   dangling,
		Runtime:    n.runtimeName,
	}
}
