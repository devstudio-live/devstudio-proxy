package proxycore

import "time"

// ── Request / Response ──────────────────────────────────────────────────────

// ContainerRequest is the unified request body for all container gateway endpoints.
type ContainerRequest struct {
	Runtime        string            `json:"runtime,omitempty"`        // auto-detect if empty
	SocketPath     string            `json:"socketPath,omitempty"`     // custom socket
	Resource       string            `json:"resource"`                 // containers|images|volumes|networks|compose|pods|system
	Action         string            `json:"action"`                   // list|inspect|stats|logs|start|stop|remove|pull|build|prune
	ID             string            `json:"id,omitempty"`             // container/image/volume ID
	Name           string            `json:"name,omitempty"`
	Namespace      string            `json:"namespace,omitempty"`      // containerd namespace
	Filters        map[string]string `json:"filters,omitempty"`
	ConnectionMode string            `json:"connectionMode,omitempty"` // "local" | "ssh" | "remote-tcp"
	SSHConnection  *SSHConnection    `json:"sshConnection,omitempty"`
	RemoteHost     string            `json:"remoteHost,omitempty"`     // tcp://host:2376

	// Phase 2 — write operations
	Timeout  int               `json:"timeout,omitempty"`  // stop/restart timeout in seconds
	Force    bool              `json:"force,omitempty"`    // force remove
	Ref      string            `json:"ref,omitempty"`      // image reference for pull (e.g. "nginx:latest")
	Target   string            `json:"target,omitempty"`   // target reference for image tag
	Driver   string            `json:"driver,omitempty"`   // driver for volume/network create
	Options  map[string]string `json:"options,omitempty"`  // driver options for volume/network create
	Dangling bool              `json:"dangling,omitempty"` // prune only dangling images

	// Phase 2B — logs
	TailLines int   `json:"tailLines,omitempty"` // number of tail lines for logs
	Follow    bool  `json:"follow,omitempty"`    // follow/stream logs
}

// ContainerResponse is the unified response body for all container gateway endpoints.
type ContainerResponse struct {
	Containers []ContainerInfo  `json:"containers,omitempty"`
	Images     []ImageInfo      `json:"images,omitempty"`
	Volumes    []VolumeInfo     `json:"volumes,omitempty"`
	Networks   []NetworkInfo    `json:"networks,omitempty"`
	Container  *ContainerDetail `json:"container,omitempty"`
	Image      *ImageDetail     `json:"image,omitempty"`
	Volume     *VolumeInfo      `json:"volume,omitempty"`
	Network    *NetworkInfo     `json:"network,omitempty"`
	Runtimes   []RuntimeInfo    `json:"runtimes,omitempty"`
	System     *SystemInfo      `json:"system,omitempty"`
	Prune      *PruneResult     `json:"prune,omitempty"`
	Logs       string           `json:"logs,omitempty"`
	OK         bool             `json:"ok,omitempty"`
	Error      string           `json:"error,omitempty"`
	DurationMs float64          `json:"durationMs"`
}

// ── Unified data models ─────────────────────────────────────────────────────

// ContainerInfo is the normalized container summary returned by all adapters.
type ContainerInfo struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Image     string            `json:"image"`
	ImageID   string            `json:"imageId,omitempty"`
	Command   string            `json:"command,omitempty"`
	Created   time.Time         `json:"created"`
	State     string            `json:"state"`  // running|exited|paused|created|restarting|removing|dead
	Status    string            `json:"status"` // human-readable, e.g. "Up 2 hours"
	Ports     []PortBinding     `json:"ports,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	Mounts    []string          `json:"mounts,omitempty"`
	Networks  []string          `json:"networks,omitempty"`
	Runtime   string            `json:"runtime"` // which adapter produced this
	Platform  string            `json:"platform,omitempty"`
	SizeRw    int64             `json:"sizeRw,omitempty"`
	SizeRoot  int64             `json:"sizeRoot,omitempty"`
}

// ContainerDetail is the full inspect output for a single container.
type ContainerDetail struct {
	ContainerInfo
	Env          []string          `json:"env,omitempty"`
	Entrypoint   []string          `json:"entrypoint,omitempty"`
	Cmd          []string          `json:"cmd,omitempty"`
	WorkingDir   string            `json:"workingDir,omitempty"`
	User         string            `json:"user,omitempty"`
	Hostname     string            `json:"hostname,omitempty"`
	RestartCount int               `json:"restartCount"`
	MountDetails []MountDetail     `json:"mountDetails,omitempty"`
	NetworkDetail map[string]NetworkEndpoint `json:"networkDetail,omitempty"`
	HealthStatus string            `json:"healthStatus,omitempty"` // healthy|unhealthy|starting|none
	StartedAt    *time.Time        `json:"startedAt,omitempty"`
	FinishedAt   *time.Time        `json:"finishedAt,omitempty"`
	ExitCode     int               `json:"exitCode"`
	Privileged   bool              `json:"privileged"`
	Raw          map[string]any    `json:"raw,omitempty"` // full inspect JSON
}

// PortBinding maps a container port to a host port.
type PortBinding struct {
	ContainerPort int    `json:"containerPort"`
	HostPort      int    `json:"hostPort,omitempty"`
	HostIP        string `json:"hostIp,omitempty"`
	Protocol      string `json:"protocol"` // tcp|udp
}

// MountDetail describes a single mount/bind/volume.
type MountDetail struct {
	Type        string `json:"type"`        // bind|volume|tmpfs
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Mode        string `json:"mode,omitempty"` // rw|ro
	RW          bool   `json:"rw"`
}

// NetworkEndpoint describes a container's connection to a network.
type NetworkEndpoint struct {
	NetworkID string `json:"networkId"`
	IPAddress string `json:"ipAddress"`
	Gateway   string `json:"gateway"`
	MacAddress string `json:"macAddress,omitempty"`
}

// ImageInfo is the normalized image summary.
type ImageInfo struct {
	ID         string    `json:"id"`
	Repository string    `json:"repository"`
	Tag        string    `json:"tag"`
	Digest     string    `json:"digest,omitempty"`
	Created    time.Time `json:"created"`
	Size       int64     `json:"size"`
	Labels     map[string]string `json:"labels,omitempty"`
	Dangling   bool      `json:"dangling"`
	Runtime    string    `json:"runtime"`
}

// ImageDetail is the full inspect output for a single image.
type ImageDetail struct {
	ImageInfo
	Architecture string            `json:"architecture"`
	OS           string            `json:"os"`
	Author       string            `json:"author,omitempty"`
	Env          []string          `json:"env,omitempty"`
	Entrypoint   []string          `json:"entrypoint,omitempty"`
	Cmd          []string          `json:"cmd,omitempty"`
	ExposedPorts []string          `json:"exposedPorts,omitempty"`
	Volumes      []string          `json:"volumes,omitempty"`
	WorkingDir   string            `json:"workingDir,omitempty"`
	Layers       []string          `json:"layers,omitempty"`
	History      []ImageHistory    `json:"history,omitempty"`
	Raw          map[string]any    `json:"raw,omitempty"`
}

// ImageHistory is a single layer in the image build history.
type ImageHistory struct {
	CreatedBy string    `json:"createdBy"`
	Created   time.Time `json:"created"`
	Size      int64     `json:"size"`
	Comment   string    `json:"comment,omitempty"`
	Empty     bool      `json:"empty"`
}

// VolumeInfo is the normalized volume summary.
type VolumeInfo struct {
	Name       string            `json:"name"`
	Driver     string            `json:"driver"`
	MountPoint string            `json:"mountPoint"`
	Created    time.Time         `json:"created"`
	Labels     map[string]string `json:"labels,omitempty"`
	Scope      string            `json:"scope,omitempty"` // local|global
	Size       int64             `json:"size,omitempty"`   // -1 if unknown
	Runtime    string            `json:"runtime"`
}

// NetworkInfo is the normalized network summary.
type NetworkInfo struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Driver     string            `json:"driver"`
	Scope      string            `json:"scope"` // local|swarm|global
	Subnet     string            `json:"subnet,omitempty"`
	Gateway    string            `json:"gateway,omitempty"`
	Internal   bool              `json:"internal"`
	Labels     map[string]string `json:"labels,omitempty"`
	Containers int               `json:"containers"` // count of connected containers
	Runtime    string            `json:"runtime"`
}

// RuntimeInfo describes a detected container runtime.
type RuntimeInfo struct {
	Name        string `json:"name"`        // docker|podman|nerdctl|crictl|buildah|finch|colima|rancher-desktop|lima
	DisplayName string `json:"displayName"` // e.g. "Docker Engine"
	Version     string `json:"version"`
	APIVersion  string `json:"apiVersion,omitempty"`
	SocketPath  string `json:"socketPath,omitempty"`
	BinaryPath  string `json:"binaryPath,omitempty"`
	OS          string `json:"os,omitempty"`
	Arch        string `json:"arch,omitempty"`
	Rootless    bool   `json:"rootless"`
	Available   bool   `json:"available"`
	Recommended bool   `json:"recommended"` // auto-detected best choice
}

// SystemInfo is the runtime-level system information.
type SystemInfo struct {
	Runtime        string `json:"runtime"`
	Version        string `json:"version"`
	APIVersion     string `json:"apiVersion,omitempty"`
	OS             string `json:"os"`
	Arch           string `json:"arch"`
	KernelVersion  string `json:"kernelVersion,omitempty"`
	Rootless       bool   `json:"rootless"`
	Containers     int    `json:"containers"`
	Running        int    `json:"running"`
	Paused         int    `json:"paused"`
	Stopped        int    `json:"stopped"`
	Images         int    `json:"images"`
	StorageDriver  string `json:"storageDriver,omitempty"`
	CgroupDriver   string `json:"cgroupDriver,omitempty"`
	CgroupVersion  string `json:"cgroupVersion,omitempty"`
	MemoryTotal    int64  `json:"memoryTotal,omitempty"`
	CPUs           int    `json:"cpus,omitempty"`
	SecurityOptions []string `json:"securityOptions,omitempty"`
}

// PruneResult summarizes the outcome of a prune (cleanup) operation.
type PruneResult struct {
	ItemsDeleted []string `json:"itemsDeleted,omitempty"`
	SpaceFreed   int64    `json:"spaceFreed"` // bytes
}

// ContainerStats holds a single stats snapshot for a running container.
type ContainerStats struct {
	ID            string  `json:"id"`
	Name          string  `json:"name"`
	CPUPercent    float64 `json:"cpuPercent"`
	MemoryUsage   int64   `json:"memoryUsage"`   // bytes
	MemoryLimit   int64   `json:"memoryLimit"`   // bytes
	MemoryPercent float64 `json:"memoryPercent"`
	NetInput      int64   `json:"netInput"`      // bytes
	NetOutput     int64   `json:"netOutput"`     // bytes
	BlockInput    int64   `json:"blockInput"`    // bytes
	BlockOutput   int64   `json:"blockOutput"`   // bytes
	PIDs          int     `json:"pids"`
	Timestamp     int64   `json:"timestamp"`     // unix ms
}

// ── Adapter interface ───────────────────────────────────────────────────────

// ContainerAdapter is implemented by each runtime backend (docker, podman,
// nerdctl, crictl, buildah).
type ContainerAdapter interface {
	// Read operations (Phase 1A)
	Name() string
	Detect() (*RuntimeInfo, error)
	ListContainers(filters map[string]string) ([]ContainerInfo, error)
	InspectContainer(id string) (*ContainerDetail, error)
	ListImages(filters map[string]string) ([]ImageInfo, error)
	InspectImage(id string) (*ImageDetail, error)
	ListVolumes() ([]VolumeInfo, error)
	ListNetworks() ([]NetworkInfo, error)
	SystemInfo() (*SystemInfo, error)

	// Write operations — container lifecycle (Phase 2A)
	StartContainer(id string) error
	StopContainer(id string, timeout int) error
	RestartContainer(id string, timeout int) error
	RemoveContainer(id string, force bool) error
	PauseContainer(id string) error
	UnpauseContainer(id string) error

	// Write operations — images (Phase 2A)
	PullImage(ref string) error
	RemoveImage(id string, force bool) error
	PruneImages(dangling bool) (*PruneResult, error)
	TagImage(source, target string) error

	// Write operations — volumes (Phase 2A)
	CreateVolume(name string, driver string, opts map[string]string) (*VolumeInfo, error)
	RemoveVolume(name string, force bool) error
	PruneVolumes() (*PruneResult, error)

	// Write operations — networks (Phase 2A)
	CreateNetwork(name string, driver string, opts map[string]string) (*NetworkInfo, error)
	RemoveNetwork(id string) error
	PruneNetworks() (*PruneResult, error)
}
