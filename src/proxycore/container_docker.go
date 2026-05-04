package proxycore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// DockerAdapter communicates with Docker or Podman via the compatible REST API
// over a Unix socket. Both runtimes expose the same /v1.43/ API surface.
type DockerAdapter struct {
	socketPath  string
	runtimeName string // "docker" or "podman"
	client      *http.Client
	apiScheme   string // "http" or "https"; empty defaults to "http" (Phase 4B)
}

const dockerAPIVersion = "v1.43"

// NewDockerAdapter creates an adapter that talks to the Docker/Podman API
// over the given Unix socket.
func NewDockerAdapter(socketPath string, runtimeName string) *DockerAdapter {
	transport := &http.Transport{
		DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return net.DialTimeout("unix", socketPath, 5*time.Second)
		},
	}
	return &DockerAdapter{
		socketPath:  socketPath,
		runtimeName: runtimeName,
		client: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
	}
}

func (d *DockerAdapter) Name() string { return d.runtimeName }

func (d *DockerAdapter) apiURL(path string) string {
	scheme := d.apiScheme
	if scheme == "" {
		scheme = "http"
	}
	return fmt.Sprintf("%s://localhost/%s%s", scheme, dockerAPIVersion, path)
}

func (d *DockerAdapter) get(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.apiURL(path), nil)
	if err != nil {
		return nil, err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		// Try to extract error message from JSON body
		var errResp struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(body, &errResp) == nil && errResp.Message != "" {
			return nil, &containerError{msg: errResp.Message}
		}
		return nil, &containerError{msg: fmt.Sprintf("API error %d: %s", resp.StatusCode, string(body))}
	}

	return body, nil
}

func (d *DockerAdapter) post(ctx context.Context, path string, body []byte) ([]byte, error) {
	var reqBody io.Reader
	if body != nil {
		reqBody = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.apiURL(path), reqBody)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		var errResp struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(respBody, &errResp) == nil && errResp.Message != "" {
			return nil, &containerError{msg: errResp.Message}
		}
		return nil, &containerError{msg: fmt.Sprintf("API error %d: %s", resp.StatusCode, string(respBody))}
	}

	return respBody, nil
}

func (d *DockerAdapter) delete(ctx context.Context, path string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, d.apiURL(path), nil)
	if err != nil {
		return nil, err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		var errResp struct {
			Message string `json:"message"`
		}
		if json.Unmarshal(respBody, &errResp) == nil && errResp.Message != "" {
			return nil, &containerError{msg: errResp.Message}
		}
		return nil, &containerError{msg: fmt.Sprintf("API error %d: %s", resp.StatusCode, string(respBody))}
	}

	return respBody, nil
}

// ── Detect ──────────────────────────────────────────────────────────────────

func (d *DockerAdapter) Detect() (*RuntimeInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body, err := d.get(ctx, "/version")
	if err != nil {
		return &RuntimeInfo{
			Name:       d.runtimeName,
			SocketPath: d.socketPath,
			Available:  false,
		}, err
	}

	var ver struct {
		Version    string `json:"Version"`
		APIVersion string `json:"ApiVersion"`
		OS         string `json:"Os"`
		Arch       string `json:"Arch"`
	}
	if err := json.Unmarshal(body, &ver); err != nil {
		return nil, err
	}

	displayName := "Docker Engine"
	if d.runtimeName == "podman" {
		displayName = "Podman"
	}

	return &RuntimeInfo{
		Name:        d.runtimeName,
		DisplayName: displayName,
		Version:     ver.Version,
		APIVersion:  ver.APIVersion,
		SocketPath:  d.socketPath,
		OS:          ver.OS,
		Arch:        ver.Arch,
		Available:   true,
	}, nil
}

// ── Containers ──────────────────────────────────────────────────────────────

func (d *DockerAdapter) ListContainers(filters map[string]string) ([]ContainerInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	path := "/containers/json?all=true"

	// Build filters JSON if provided
	if len(filters) > 0 {
		filterMap := map[string][]string{}
		for k, v := range filters {
			filterMap[k] = []string{v}
		}
		fb, _ := json.Marshal(filterMap)
		path += "&filters=" + string(fb)
	}

	body, err := d.get(ctx, path)
	if err != nil {
		return nil, err
	}

	var raw []dockerContainerJSON
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	containers := make([]ContainerInfo, 0, len(raw))
	for _, c := range raw {
		containers = append(containers, d.normalizeContainer(c))
	}
	return containers, nil
}

func (d *DockerAdapter) InspectContainer(id string) (*ContainerDetail, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	body, err := d.get(ctx, "/containers/"+id+"/json")
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	return d.normalizeContainerDetail(raw), nil
}

// ── Images ──────────────────────────────────────────────────────────────────

func (d *DockerAdapter) ListImages(filters map[string]string) ([]ImageInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	path := "/images/json?all=false"

	if len(filters) > 0 {
		filterMap := map[string][]string{}
		for k, v := range filters {
			filterMap[k] = []string{v}
		}
		fb, _ := json.Marshal(filterMap)
		path += "&filters=" + string(fb)
	}

	body, err := d.get(ctx, path)
	if err != nil {
		return nil, err
	}

	var raw []dockerImageJSON
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	images := make([]ImageInfo, 0, len(raw))
	for _, img := range raw {
		images = append(images, d.normalizeImage(img)...)
	}
	return images, nil
}

func (d *DockerAdapter) InspectImage(id string) (*ImageDetail, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	body, err := d.get(ctx, "/images/"+id+"/json")
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	// Also fetch history
	histBody, _ := d.get(ctx, "/images/"+id+"/history")
	var histRaw []map[string]any
	if histBody != nil {
		json.Unmarshal(histBody, &histRaw)
	}

	return d.normalizeImageDetail(raw, histRaw), nil
}

// ── Volumes ─────────────────────────────────────────────────────────────────

func (d *DockerAdapter) ListVolumes() ([]VolumeInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	body, err := d.get(ctx, "/volumes")
	if err != nil {
		return nil, err
	}

	var raw struct {
		Volumes []dockerVolumeJSON `json:"Volumes"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	volumes := make([]VolumeInfo, 0, len(raw.Volumes))
	for _, v := range raw.Volumes {
		volumes = append(volumes, d.normalizeVolume(v))
	}
	return volumes, nil
}

// ── Networks ────────────────────────────────────────────────────────────────

func (d *DockerAdapter) ListNetworks() ([]NetworkInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	body, err := d.get(ctx, "/networks")
	if err != nil {
		return nil, err
	}

	var raw []dockerNetworkJSON
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	networks := make([]NetworkInfo, 0, len(raw))
	for _, n := range raw {
		networks = append(networks, d.normalizeNetwork(n))
	}
	return networks, nil
}

// ── System Info ─────────────────────────────────────────────────────────────

func (d *DockerAdapter) SystemInfo() (*SystemInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	body, err := d.get(ctx, "/info")
	if err != nil {
		return nil, err
	}

	var raw dockerInfoJSON
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	return &SystemInfo{
		Runtime:         d.runtimeName,
		Version:         raw.ServerVersion,
		OS:              raw.OperatingSystem,
		Arch:            raw.Architecture,
		KernelVersion:   raw.KernelVersion,
		Rootless:        raw.SecurityOptions != nil && containsStr(raw.SecurityOptions, "rootless"),
		Containers:      raw.Containers,
		Running:         raw.ContainersRunning,
		Paused:          raw.ContainersPaused,
		Stopped:         raw.ContainersStopped,
		Images:          raw.Images,
		StorageDriver:   raw.Driver,
		CgroupDriver:    raw.CgroupDriver,
		CgroupVersion:   raw.CgroupVersion,
		MemoryTotal:     raw.MemTotal,
		CPUs:            raw.NCPU,
		SecurityOptions: raw.SecurityOptions,
	}, nil
}

// ── Container lifecycle (Phase 2A) ─────────────────────────────────────────

func (d *DockerAdapter) StartContainer(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := d.post(ctx, "/containers/"+id+"/start", nil)
	return err
}

func (d *DockerAdapter) StopContainer(id string, timeout int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout+10)*time.Second)
	defer cancel()
	path := fmt.Sprintf("/containers/%s/stop?t=%d", id, timeout)
	_, err := d.post(ctx, path, nil)
	return err
}

func (d *DockerAdapter) RestartContainer(id string, timeout int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout+10)*time.Second)
	defer cancel()
	path := fmt.Sprintf("/containers/%s/restart?t=%d", id, timeout)
	_, err := d.post(ctx, path, nil)
	return err
}

func (d *DockerAdapter) RemoveContainer(id string, force bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	path := fmt.Sprintf("/containers/%s?force=%t", id, force)
	_, err := d.delete(ctx, path)
	return err
}

func (d *DockerAdapter) PauseContainer(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := d.post(ctx, "/containers/"+id+"/pause", nil)
	return err
}

func (d *DockerAdapter) UnpauseContainer(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := d.post(ctx, "/containers/"+id+"/unpause", nil)
	return err
}

// ── Image write operations (Phase 2A) ──────────────────────────────────────

func (d *DockerAdapter) PullImage(ref string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	// POST /images/create?fromImage=ref — reads full body to wait for completion
	path := "/images/create?fromImage=" + ref
	_, err := d.post(ctx, path, nil)
	return err
}

func (d *DockerAdapter) RemoveImage(id string, force bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	path := fmt.Sprintf("/images/%s?force=%t", id, force)
	_, err := d.delete(ctx, path)
	return err
}

func (d *DockerAdapter) PruneImages(dangling bool) (*PruneResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	path := "/images/prune"
	if dangling {
		fb, _ := json.Marshal(map[string][]string{"dangling": {"true"}})
		path += "?filters=" + string(fb)
	}

	body, err := d.post(ctx, path, nil)
	if err != nil {
		return nil, err
	}

	var raw struct {
		ImagesDeleted []struct {
			Untagged string `json:"Untagged"`
			Deleted  string `json:"Deleted"`
		} `json:"ImagesDeleted"`
		SpaceReclaimed int64 `json:"SpaceReclaimed"`
	}
	json.Unmarshal(body, &raw)

	result := &PruneResult{SpaceFreed: raw.SpaceReclaimed}
	for _, img := range raw.ImagesDeleted {
		if img.Deleted != "" {
			result.ItemsDeleted = append(result.ItemsDeleted, img.Deleted)
		} else if img.Untagged != "" {
			result.ItemsDeleted = append(result.ItemsDeleted, img.Untagged)
		}
	}
	return result, nil
}

func (d *DockerAdapter) TagImage(source, target string) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()

	repo, tag := splitRepoTag(target)
	path := fmt.Sprintf("/images/%s/tag?repo=%s&tag=%s", source, repo, tag)
	_, err := d.post(ctx, path, nil)
	return err
}

// ── Volume write operations (Phase 2A) ─────────────────────────────────────

func (d *DockerAdapter) CreateVolume(name string, driver string, opts map[string]string) (*VolumeInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()

	payload := map[string]any{"Name": name}
	if driver != "" {
		payload["Driver"] = driver
	}
	if len(opts) > 0 {
		payload["DriverOpts"] = opts
	}
	reqBody, _ := json.Marshal(payload)

	body, err := d.post(ctx, "/volumes/create", reqBody)
	if err != nil {
		return nil, err
	}

	var raw dockerVolumeJSON
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	vol := d.normalizeVolume(raw)
	return &vol, nil
}

func (d *DockerAdapter) RemoveVolume(name string, force bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	path := fmt.Sprintf("/volumes/%s?force=%t", name, force)
	_, err := d.delete(ctx, path)
	return err
}

func (d *DockerAdapter) PruneVolumes() (*PruneResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	body, err := d.post(ctx, "/volumes/prune", nil)
	if err != nil {
		return nil, err
	}

	var raw struct {
		VolumesDeleted []string `json:"VolumesDeleted"`
		SpaceReclaimed int64    `json:"SpaceReclaimed"`
	}
	json.Unmarshal(body, &raw)

	return &PruneResult{ItemsDeleted: raw.VolumesDeleted, SpaceFreed: raw.SpaceReclaimed}, nil
}

// ── Network write operations (Phase 2A) ────────────────────────────────────

func (d *DockerAdapter) CreateNetwork(name string, driver string, opts map[string]string) (*NetworkInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()

	payload := map[string]any{"Name": name, "CheckDuplicate": true}
	if driver != "" {
		payload["Driver"] = driver
	}
	if len(opts) > 0 {
		payload["Options"] = opts
	}
	reqBody, _ := json.Marshal(payload)

	body, err := d.post(ctx, "/networks/create", reqBody)
	if err != nil {
		return nil, err
	}

	var raw struct {
		ID string `json:"Id"`
	}
	json.Unmarshal(body, &raw)

	// Fetch full network info
	netBody, err := d.get(ctx, "/networks/"+raw.ID)
	if err != nil {
		return &NetworkInfo{ID: truncateID(raw.ID), Name: name, Driver: driver, Runtime: d.runtimeName}, nil
	}
	var netRaw dockerNetworkJSON
	if err := json.Unmarshal(netBody, &netRaw); err != nil {
		return &NetworkInfo{ID: truncateID(raw.ID), Name: name, Driver: driver, Runtime: d.runtimeName}, nil
	}
	net := d.normalizeNetwork(netRaw)
	return &net, nil
}

func (d *DockerAdapter) RemoveNetwork(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := d.delete(ctx, "/networks/"+id)
	return err
}

func (d *DockerAdapter) PruneNetworks() (*PruneResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	body, err := d.post(ctx, "/networks/prune", nil)
	if err != nil {
		return nil, err
	}

	var raw struct {
		NetworksDeleted []string `json:"NetworksDeleted"`
	}
	json.Unmarshal(body, &raw)

	return &PruneResult{ItemsDeleted: raw.NetworksDeleted}, nil
}

// ── Docker API JSON models (internal) ───────────────────────────────────────

type dockerContainerJSON struct {
	ID      string `json:"Id"`
	Names   []string
	Image   string
	ImageID string
	Command string
	Created int64
	State   string
	Status  string
	Ports   []struct {
		IP          string `json:"IP"`
		PrivatePort int    `json:"PrivatePort"`
		PublicPort  int    `json:"PublicPort"`
		Type        string `json:"Type"`
	}
	Labels map[string]string
	Mounts []struct {
		Name        string
		Source      string
		Destination string
		Driver      string
		Mode        string
		RW          bool
	}
	NetworkSettings struct {
		Networks map[string]struct {
			NetworkID string
		}
	}
	SizeRw     int64
	SizeRootFs int64
}

type dockerImageJSON struct {
	ID          string `json:"Id"`
	RepoTags    []string
	RepoDigests []string
	Created     int64
	Size        int64
	Labels      map[string]string
}

type dockerVolumeJSON struct {
	Name       string
	Driver     string
	Mountpoint string
	CreatedAt  string
	Labels     map[string]string
	Scope      string
	UsageData  *struct {
		Size int64
	}
}

type dockerNetworkJSON struct {
	ID     string `json:"Id"`
	Name   string
	Driver string
	Scope  string
	IPAM   struct {
		Config []struct {
			Subnet  string
			Gateway string
		}
	}
	Internal   bool
	Labels     map[string]string
	Containers map[string]any
}

type dockerInfoJSON struct {
	ServerVersion     string
	OperatingSystem   string
	Architecture      string
	KernelVersion     string
	Containers        int
	ContainersRunning int
	ContainersPaused  int
	ContainersStopped int
	Images            int
	Driver            string
	CgroupDriver      string
	CgroupVersion     string
	MemTotal          int64
	NCPU              int
	SecurityOptions   []string
}

// ── Normalization helpers ───────────────────────────────────────────────────

func (d *DockerAdapter) normalizeContainer(c dockerContainerJSON) ContainerInfo {
	name := ""
	if len(c.Names) > 0 {
		name = strings.TrimPrefix(c.Names[0], "/")
	}

	ports := make([]PortBinding, 0, len(c.Ports))
	for _, p := range c.Ports {
		ports = append(ports, PortBinding{
			ContainerPort: p.PrivatePort,
			HostPort:      p.PublicPort,
			HostIP:        p.IP,
			Protocol:      p.Type,
		})
	}

	mounts := make([]string, 0, len(c.Mounts))
	for _, m := range c.Mounts {
		mounts = append(mounts, m.Destination)
	}

	networks := make([]string, 0, len(c.NetworkSettings.Networks))
	for netName := range c.NetworkSettings.Networks {
		networks = append(networks, netName)
	}

	return ContainerInfo{
		ID:       truncateID(c.ID),
		Name:     name,
		Image:    c.Image,
		ImageID:  truncateID(c.ImageID),
		Command:  c.Command,
		Created:  time.Unix(c.Created, 0),
		State:    strings.ToLower(c.State),
		Status:   c.Status,
		Ports:    ports,
		Labels:   c.Labels,
		Mounts:   mounts,
		Networks: networks,
		Runtime:  d.runtimeName,
		SizeRw:   c.SizeRw,
		SizeRoot: c.SizeRootFs,
	}
}

func (d *DockerAdapter) normalizeContainerDetail(raw map[string]any) *ContainerDetail {
	detail := &ContainerDetail{Raw: raw}

	detail.ID = getStr(raw, "Id")
	detail.Name = strings.TrimPrefix(getStr(raw, "Name"), "/")
	detail.Runtime = d.runtimeName

	if state, ok := raw["State"].(map[string]any); ok {
		detail.State = strings.ToLower(getStr(state, "Status"))
		detail.Status = getStr(state, "Status")
		detail.ExitCode = getInt(state, "ExitCode")
		if t, err := time.Parse(time.RFC3339Nano, getStr(state, "StartedAt")); err == nil && !t.IsZero() {
			detail.StartedAt = &t
		}
		if t, err := time.Parse(time.RFC3339Nano, getStr(state, "FinishedAt")); err == nil && !t.IsZero() {
			detail.FinishedAt = &t
		}
		if health, ok := state["Health"].(map[string]any); ok {
			detail.HealthStatus = getStr(health, "Status")
		}
	}

	if config, ok := raw["Config"].(map[string]any); ok {
		detail.Image = getStr(config, "Image")
		detail.Hostname = getStr(config, "Hostname")
		detail.WorkingDir = getStr(config, "WorkingDir")
		detail.User = getStr(config, "User")
		if env, ok := config["Env"].([]any); ok {
			for _, e := range env {
				if s, ok := e.(string); ok {
					detail.Env = append(detail.Env, s)
				}
			}
		}
		if ep, ok := config["Entrypoint"].([]any); ok {
			for _, e := range ep {
				if s, ok := e.(string); ok {
					detail.Entrypoint = append(detail.Entrypoint, s)
				}
			}
		}
		if cmd, ok := config["Cmd"].([]any); ok {
			for _, c := range cmd {
				if s, ok := c.(string); ok {
					detail.Cmd = append(detail.Cmd, s)
				}
			}
		}
		detail.Labels = getStrMap(config, "Labels")
	}

	if created, err := time.Parse(time.RFC3339Nano, getStr(raw, "Created")); err == nil {
		detail.Created = created
	}

	if hostConfig, ok := raw["HostConfig"].(map[string]any); ok {
		detail.Privileged, _ = hostConfig["Privileged"].(bool)
		detail.RestartCount = getInt(raw, "RestartCount")
	}

	// Mounts
	if mounts, ok := raw["Mounts"].([]any); ok {
		for _, m := range mounts {
			if mount, ok := m.(map[string]any); ok {
				rw, _ := mount["RW"].(bool)
				detail.MountDetails = append(detail.MountDetails, MountDetail{
					Type:        getStr(mount, "Type"),
					Source:      getStr(mount, "Source"),
					Destination: getStr(mount, "Destination"),
					Mode:        getStr(mount, "Mode"),
					RW:          rw,
				})
			}
		}
	}

	// Networks
	if ns, ok := raw["NetworkSettings"].(map[string]any); ok {
		if nets, ok := ns["Networks"].(map[string]any); ok {
			detail.NetworkDetail = map[string]NetworkEndpoint{}
			for name, v := range nets {
				if net, ok := v.(map[string]any); ok {
					detail.NetworkDetail[name] = NetworkEndpoint{
						NetworkID:  getStr(net, "NetworkID"),
						IPAddress:  getStr(net, "IPAddress"),
						Gateway:    getStr(net, "Gateway"),
						MacAddress: getStr(net, "MacAddress"),
					}
				}
			}
		}
	}

	return detail
}

func (d *DockerAdapter) normalizeImage(img dockerImageJSON) []ImageInfo {
	// An image can have multiple repo:tag pairs; emit one ImageInfo per tag
	if len(img.RepoTags) == 0 {
		return []ImageInfo{{
			ID:       truncateID(img.ID),
			Created:  time.Unix(img.Created, 0),
			Size:     img.Size,
			Labels:   img.Labels,
			Dangling: true,
			Runtime:  d.runtimeName,
		}}
	}

	var images []ImageInfo
	for _, tag := range img.RepoTags {
		repo, t := splitRepoTag(tag)
		images = append(images, ImageInfo{
			ID:         truncateID(img.ID),
			Repository: repo,
			Tag:        t,
			Created:    time.Unix(img.Created, 0),
			Size:       img.Size,
			Labels:     img.Labels,
			Dangling:   tag == "<none>:<none>",
			Runtime:    d.runtimeName,
		})
	}
	return images
}

func (d *DockerAdapter) normalizeImageDetail(raw map[string]any, histRaw []map[string]any) *ImageDetail {
	detail := &ImageDetail{Raw: raw}

	detail.ID = truncateID(getStr(raw, "Id"))
	detail.Architecture = getStr(raw, "Architecture")
	detail.OS = getStr(raw, "Os")
	detail.Author = getStr(raw, "Author")
	detail.Size = getInt64(raw, "Size")
	detail.Runtime = d.runtimeName

	if created, err := time.Parse(time.RFC3339Nano, getStr(raw, "Created")); err == nil {
		detail.Created = created
	}

	if repoTags, ok := raw["RepoTags"].([]any); ok && len(repoTags) > 0 {
		if tag, ok := repoTags[0].(string); ok {
			detail.Repository, detail.Tag = splitRepoTag(tag)
		}
	}

	if config, ok := raw["Config"].(map[string]any); ok {
		if env, ok := config["Env"].([]any); ok {
			for _, e := range env {
				if s, ok := e.(string); ok {
					detail.Env = append(detail.Env, s)
				}
			}
		}
		if ep, ok := config["Entrypoint"].([]any); ok {
			for _, e := range ep {
				if s, ok := e.(string); ok {
					detail.Entrypoint = append(detail.Entrypoint, s)
				}
			}
		}
		if cmd, ok := config["Cmd"].([]any); ok {
			for _, c := range cmd {
				if s, ok := c.(string); ok {
					detail.Cmd = append(detail.Cmd, s)
				}
			}
		}
		detail.WorkingDir = getStr(config, "WorkingDir")
		if ports, ok := config["ExposedPorts"].(map[string]any); ok {
			for p := range ports {
				detail.ExposedPorts = append(detail.ExposedPorts, p)
			}
		}
		if vols, ok := config["Volumes"].(map[string]any); ok {
			for v := range vols {
				detail.Volumes = append(detail.Volumes, v)
			}
		}
		detail.Labels = getStrMap(config, "Labels")
	}

	if rootFS, ok := raw["RootFS"].(map[string]any); ok {
		if layers, ok := rootFS["Layers"].([]any); ok {
			for _, l := range layers {
				if s, ok := l.(string); ok {
					detail.Layers = append(detail.Layers, s)
				}
			}
		}
	}

	for _, h := range histRaw {
		created := time.Time{}
		if c, ok := h["Created"].(float64); ok {
			created = time.Unix(int64(c), 0)
		}
		size := int64(0)
		if s, ok := h["Size"].(float64); ok {
			size = int64(s)
		}
		empty := false
		if s, ok := h["Size"].(float64); ok {
			empty = int64(s) == 0
		}
		detail.History = append(detail.History, ImageHistory{
			CreatedBy: getStr(h, "CreatedBy"),
			Created:   created,
			Size:      size,
			Comment:   getStr(h, "Comment"),
			Empty:     empty,
		})
	}

	return detail
}

func (d *DockerAdapter) normalizeVolume(v dockerVolumeJSON) VolumeInfo {
	created := time.Time{}
	if v.CreatedAt != "" {
		if t, err := time.Parse(time.RFC3339Nano, v.CreatedAt); err == nil {
			created = t
		}
	}
	size := int64(-1)
	if v.UsageData != nil {
		size = v.UsageData.Size
	}
	return VolumeInfo{
		Name:       v.Name,
		Driver:     v.Driver,
		MountPoint: v.Mountpoint,
		Created:    created,
		Labels:     v.Labels,
		Scope:      v.Scope,
		Size:       size,
		Runtime:    d.runtimeName,
	}
}

func (d *DockerAdapter) normalizeNetwork(n dockerNetworkJSON) NetworkInfo {
	subnet := ""
	gateway := ""
	if len(n.IPAM.Config) > 0 {
		subnet = n.IPAM.Config[0].Subnet
		gateway = n.IPAM.Config[0].Gateway
	}
	return NetworkInfo{
		ID:         truncateID(n.ID),
		Name:       n.Name,
		Driver:     n.Driver,
		Scope:      n.Scope,
		Subnet:     subnet,
		Gateway:    gateway,
		Internal:   n.Internal,
		Labels:     n.Labels,
		Containers: len(n.Containers),
		Runtime:    d.runtimeName,
	}
}

// ── Shared helpers ──────────────────────────────────────────────────────────

func truncateID(id string) string {
	id = strings.TrimPrefix(id, "sha256:")
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

func splitRepoTag(repoTag string) (string, string) {
	if repoTag == "<none>:<none>" {
		return "<none>", "<none>"
	}
	// Handle digest references
	if idx := strings.Index(repoTag, "@"); idx >= 0 {
		return repoTag[:idx], repoTag[idx:]
	}
	// repo:tag — find the last colon that isn't part of a port or registry
	lastColon := strings.LastIndex(repoTag, ":")
	if lastColon < 0 {
		return repoTag, "latest"
	}
	return repoTag[:lastColon], repoTag[lastColon+1:]
}

func containsStr(ss []string, target string) bool {
	for _, s := range ss {
		if strings.Contains(s, target) {
			return true
		}
	}
	return false
}

// getStr safely extracts a string from a map.
func getStr(m map[string]any, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// getInt safely extracts an int from a map (JSON numbers are float64).
func getInt(m map[string]any, key string) int {
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		case json.Number:
			i, _ := strconv.Atoi(n.String())
			return i
		}
	}
	return 0
}

// getInt64 safely extracts an int64 from a map.
func getInt64(m map[string]any, key string) int64 {
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case float64:
			return int64(n)
		case int64:
			return n
		case json.Number:
			i, _ := strconv.ParseInt(n.String(), 10, 64)
			return i
		}
	}
	return 0
}

// getStrMap safely extracts a map[string]string from a map.
func getStrMap(m map[string]any, key string) map[string]string {
	if v, ok := m[key]; ok {
		if sm, ok := v.(map[string]any); ok {
			result := make(map[string]string, len(sm))
			for k, v := range sm {
				if s, ok := v.(string); ok {
					result[k] = s
				}
			}
			return result
		}
	}
	return nil
}
