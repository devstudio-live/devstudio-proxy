package proxycore

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// ── Runtime detection ───────────────────────────────────────────────────────

// detectionCache holds the cached results of runtime detection.
type detectionCache struct {
	mu       sync.Mutex
	runtimes []RuntimeInfo
	expiry   time.Time
}

const detectionCacheTTL = 60 * time.Second

var globalDetectionCache = &detectionCache{}

// DetectRuntimes probes for available container runtimes.
// Results are cached with a 60-second TTL.
func DetectRuntimes() []RuntimeInfo {
	globalDetectionCache.mu.Lock()
	defer globalDetectionCache.mu.Unlock()

	if time.Now().Before(globalDetectionCache.expiry) && len(globalDetectionCache.runtimes) > 0 {
		return globalDetectionCache.runtimes
	}

	runtimes := detectAllRuntimes()
	globalDetectionCache.runtimes = runtimes
	globalDetectionCache.expiry = time.Now().Add(detectionCacheTTL)
	return runtimes
}

// InvalidateDetectionCache forces the next DetectRuntimes call to re-probe.
func InvalidateDetectionCache() {
	globalDetectionCache.mu.Lock()
	defer globalDetectionCache.mu.Unlock()
	globalDetectionCache.expiry = time.Time{}
}

// ResolveAdapter returns the appropriate ContainerAdapter for the given request.
// If req.Runtime is empty, it picks the recommended runtime.
func ResolveAdapter(req ContainerRequest) (ContainerAdapter, error) {
	runtimes := DetectRuntimes()

	// If a specific runtime was requested, find its adapter.
	if req.Runtime != "" {
		for _, rt := range runtimes {
			if rt.Name == req.Runtime && rt.Available {
				return adapterForRuntime(rt, req.SocketPath)
			}
		}
		return nil, &containerError{msg: "requested runtime not available: " + req.Runtime}
	}

	// Auto-select: pick the recommended runtime, or the first available.
	for _, rt := range runtimes {
		if rt.Recommended && rt.Available {
			return adapterForRuntime(rt, req.SocketPath)
		}
	}
	for _, rt := range runtimes {
		if rt.Available {
			return adapterForRuntime(rt, req.SocketPath)
		}
	}

	return nil, &containerError{msg: "no container runtime detected"}
}

// containerError is a simple error type for container operations.
type containerError struct {
	msg string
}

func (e *containerError) Error() string { return e.msg }

// adapterForRuntime creates the appropriate adapter for a given RuntimeInfo.
func adapterForRuntime(rt RuntimeInfo, socketOverride string) (ContainerAdapter, error) {
	socket := rt.SocketPath
	if socketOverride != "" {
		socket = socketOverride
	}

	switch rt.Name {
	case "docker":
		return NewDockerAdapter(socket, "docker"), nil
	case "podman":
		return NewDockerAdapter(socket, "podman"), nil
	case "nerdctl":
		return NewNerdctlAdapter(rt.BinaryPath, "nerdctl"), nil
	case "finch":
		return NewNerdctlAdapter(rt.BinaryPath, "finch"), nil
	case "crictl":
		return NewCrictlAdapter(rt.BinaryPath), nil
	case "buildah":
		return NewBuildahAdapter(rt.BinaryPath), nil
	default:
		return nil, &containerError{msg: "unsupported runtime: " + rt.Name}
	}
}

// ── Detection probes ────────────────────────────────────────────────────────

func detectAllRuntimes() []RuntimeInfo {
	var runtimes []RuntimeInfo

	// 1. Check environment variables
	envRuntimes := detectFromEnv()

	// 2. Check well-known sockets
	socketRuntimes := detectFromSockets()

	// 3. Check binaries on PATH
	binaryRuntimes := detectFromBinaries()

	// Merge: env > socket > binary, deduplicate by name
	seen := map[string]bool{}
	for _, lists := range [][]RuntimeInfo{envRuntimes, socketRuntimes, binaryRuntimes} {
		for _, rt := range lists {
			if !seen[rt.Name] {
				seen[rt.Name] = true
				runtimes = append(runtimes, rt)
			}
		}
	}

	// Mark recommended runtime (preference order: docker > podman > nerdctl > finch > crictl)
	preference := []string{"docker", "podman", "nerdctl", "finch", "crictl"}
	for _, pref := range preference {
		for i := range runtimes {
			if runtimes[i].Name == pref && runtimes[i].Available {
				runtimes[i].Recommended = true
				goto done
			}
		}
	}
done:

	return runtimes
}

func detectFromEnv() []RuntimeInfo {
	var runtimes []RuntimeInfo

	if dockerHost := os.Getenv("DOCKER_HOST"); dockerHost != "" {
		// Could be unix:///path or tcp://host:port
		if strings.HasPrefix(dockerHost, "unix://") {
			sock := strings.TrimPrefix(dockerHost, "unix://")
			if socketExists(sock) {
				runtimes = append(runtimes, RuntimeInfo{
					Name:        "docker",
					DisplayName: "Docker (DOCKER_HOST)",
					SocketPath:  sock,
					Available:   true,
				})
			}
		}
	}

	if containerHost := os.Getenv("CONTAINER_HOST"); containerHost != "" {
		if strings.HasPrefix(containerHost, "unix://") {
			sock := strings.TrimPrefix(containerHost, "unix://")
			if socketExists(sock) {
				runtimes = append(runtimes, RuntimeInfo{
					Name:        "podman",
					DisplayName: "Podman (CONTAINER_HOST)",
					SocketPath:  sock,
					Available:   true,
				})
			}
		}
	}

	return runtimes
}

func detectFromSockets() []RuntimeInfo {
	var runtimes []RuntimeInfo

	type socketProbe struct {
		name        string
		displayName string
		paths       []string
	}

	probes := []socketProbe{
		{"docker", "Docker Engine", dockerSocketPaths()},
		{"podman", "Podman", podmanSocketPaths()},
	}

	for _, probe := range probes {
		for _, path := range probe.paths {
			if socketExists(path) {
				runtimes = append(runtimes, RuntimeInfo{
					Name:        probe.name,
					DisplayName: probe.displayName,
					SocketPath:  path,
					Available:   true,
				})
				break // take first match per runtime
			}
		}
	}

	return runtimes
}

func detectFromBinaries() []RuntimeInfo {
	var runtimes []RuntimeInfo

	type binaryProbe struct {
		name        string
		displayName string
		binary      string
	}

	probes := []binaryProbe{
		{"docker", "Docker Engine", "docker"},
		{"podman", "Podman", "podman"},
		{"nerdctl", "nerdctl (containerd)", "nerdctl"},
		{"finch", "Finch", "finch"},
		{"crictl", "crictl (CRI-O)", "crictl"},
		{"buildah", "Buildah", "buildah"},
	}

	for _, probe := range probes {
		if path, err := exec.LookPath(probe.binary); err == nil {
			runtimes = append(runtimes, RuntimeInfo{
				Name:        probe.name,
				DisplayName: probe.displayName,
				BinaryPath:  path,
				Available:   true,
			})
		}
	}

	return runtimes
}

// ── Platform-specific socket paths ──────────────────────────────────────────

func dockerSocketPaths() []string {
	paths := []string{"/var/run/docker.sock"}
	if runtime.GOOS == "darwin" {
		home, _ := os.UserHomeDir()
		if home != "" {
			paths = append(paths,
				filepath.Join(home, ".docker/run/docker.sock"),
				filepath.Join(home, "Library/Containers/com.docker.docker/Data/docker.sock"),
			)
		}
	}
	// Colima socket
	home, _ := os.UserHomeDir()
	if home != "" {
		paths = append(paths, filepath.Join(home, ".colima/default/docker.sock"))
	}
	return paths
}

func podmanSocketPaths() []string {
	paths := []string{"/run/podman/podman.sock"}
	if xdg := os.Getenv("XDG_RUNTIME_DIR"); xdg != "" {
		paths = append(paths, filepath.Join(xdg, "podman/podman.sock"))
	}
	// macOS: podman machine socket
	if runtime.GOOS == "darwin" {
		home, _ := os.UserHomeDir()
		if home != "" {
			paths = append(paths, filepath.Join(home, ".local/share/containers/podman/machine/podman.sock"))
		}
	}
	return paths
}

func socketExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	// On Unix, check for socket type
	return info.Mode()&os.ModeSocket != 0 || info.Mode().IsRegular()
}
