package proxycore

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

// ── Remote container host support (Phase 4B) ─────────────────────────────────
//
// Two connection modes:
//
// 1. SSH — two sub-strategies:
//    a. Docker/Podman: SSH-tunnelled HTTP to the remote Docker/Podman REST API
//       socket. The SSH client dials the remote Unix socket, providing full API
//       support (streaming stats, logs, etc.) through the existing DockerAdapter.
//    b. nerdctl/crictl/buildah: CLI command execution over SSH sessions, same
//       pattern as k8s_ssh.go uses for kubectl.
//
// 2. Remote TCP — HTTP(S) to a Docker/Podman API on tcp://host:port, with
//    optional TLS client certificate authentication.

// ── SSH Docker/Podman adapter ────────────────────────────────────────────────

// NewSSHDockerAdapter creates a DockerAdapter that talks to a remote
// Docker/Podman REST API over an SSH-tunnelled connection to the remote
// Unix socket. This reuses the entire DockerAdapter implementation; only
// the transport layer changes (SSH tunnel instead of local Unix dial).
func NewSSHDockerAdapter(sshClient *ssh.Client, remoteSocketPath string, runtimeName string) *DockerAdapter {
	transport := &http.Transport{
		DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return sshClient.Dial("unix", remoteSocketPath)
		},
	}
	return &DockerAdapter{
		socketPath:  remoteSocketPath,
		runtimeName: runtimeName,
		client: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
	}
}

// ── TCP Docker/Podman adapter ────────────────────────────────────────────────

// NewTCPDockerAdapter creates a DockerAdapter that talks to a remote
// Docker/Podman REST API over TCP. Supports optional TLS with client
// certificate authentication.
func NewTCPDockerAdapter(hostAddr string, runtimeName string, tlsConfig *tls.Config) *DockerAdapter {
	transport := &http.Transport{
		DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
			return net.DialTimeout("tcp", hostAddr, 5*time.Second)
		},
	}

	scheme := "http"
	if tlsConfig != nil {
		transport.TLSClientConfig = tlsConfig
		scheme = "https"
	}

	return &DockerAdapter{
		socketPath:  hostAddr,
		runtimeName: runtimeName,
		apiScheme:   scheme,
		client: &http.Client{
			Transport: transport,
			Timeout:   30 * time.Second,
		},
	}
}

// buildTLSConfig constructs a tls.Config from PEM-encoded cert material.
// All three fields are optional: without TLS fields, returns nil (plain HTTP).
func buildTLSConfig(caCert, clientCert, clientKey string) (*tls.Config, error) {
	if caCert == "" && clientCert == "" && clientKey == "" {
		return nil, nil
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// CA certificate
	if caCert != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(caCert)) {
			return nil, &containerError{msg: "failed to parse TLS CA certificate"}
		}
		tlsCfg.RootCAs = pool
	}

	// Client certificate + key
	if clientCert != "" && clientKey != "" {
		cert, err := tls.X509KeyPair([]byte(clientCert), []byte(clientKey))
		if err != nil {
			return nil, &containerError{msg: "failed to parse TLS client certificate: " + err.Error()}
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

// parseTCPHost extracts host:port from a Docker-style DOCKER_HOST URL.
// Accepts: "tcp://host:port", "host:port", or "host" (default port 2375/2376).
func parseTCPHost(remoteHost string, useTLS bool) string {
	host := remoteHost
	host = strings.TrimPrefix(host, "tcp://")
	host = strings.TrimPrefix(host, "http://")
	host = strings.TrimPrefix(host, "https://")
	// Strip trailing path
	if idx := strings.Index(host, "/"); idx >= 0 {
		host = host[:idx]
	}

	// If no port, add default
	if _, _, err := net.SplitHostPort(host); err != nil {
		if useTLS {
			host = host + ":2376"
		} else {
			host = host + ":2375"
		}
	}

	return host
}

// ── SSH CLI adapter ──────────────────────────────────────────────────────────

// SSHCLIAdapter implements ContainerAdapter by executing container CLI
// commands (nerdctl, crictl, buildah) over SSH sessions. This mirrors the
// NerdctlAdapter pattern but uses SSH exec instead of local exec.
type SSHCLIAdapter struct {
	sshClient   *ssh.Client
	binaryName  string // "nerdctl", "finch", "crictl", "buildah"
	runtimeName string // same as binaryName
}

// NewSSHCLIAdapter creates an adapter that runs container commands over SSH.
func NewSSHCLIAdapter(sshClient *ssh.Client, binaryName, runtimeName string) *SSHCLIAdapter {
	return &SSHCLIAdapter{
		sshClient:   sshClient,
		binaryName:  binaryName,
		runtimeName: runtimeName,
	}
}

func (a *SSHCLIAdapter) Name() string { return a.runtimeName }

// run executes a command over SSH and returns stdout.
func (a *SSHCLIAdapter) run(ctx context.Context, args ...string) ([]byte, string, error) {
	sess, err := a.sshClient.NewSession()
	if err != nil {
		return nil, "", fmt.Errorf("ssh session: %w", err)
	}

	var stdout, stderr bytes.Buffer
	sess.Stdout = &stdout
	sess.Stderr = &stderr

	cmd := a.binaryName + " " + strings.Join(args, " ")

	done := make(chan error, 1)
	go func() {
		done <- sess.Run(cmd)
	}()

	select {
	case <-ctx.Done():
		sess.Close()
		return nil, stderr.String(), ctx.Err()
	case err := <-done:
		sess.Close()
		return stdout.Bytes(), stderr.String(), err
	}
}

// runOut is a convenience that returns only stdout and wraps stderr into the error.
func (a *SSHCLIAdapter) runOut(ctx context.Context, args ...string) ([]byte, error) {
	stdout, stderr, err := a.run(ctx, args...)
	if err != nil {
		msg := strings.TrimSpace(stderr)
		if msg == "" {
			msg = err.Error()
		}
		return nil, &containerError{msg: a.runtimeName + ": " + msg}
	}
	return stdout, nil
}

// runJSONLines parses NDJSON output (one JSON object per line).
func (a *SSHCLIAdapter) runJSONLines(ctx context.Context, args ...string) ([]map[string]any, error) {
	out, err := a.runOut(ctx, args...)
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

// ── SSHCLIAdapter: Detect ───────────────────────────────────────────────────

func (a *SSHCLIAdapter) Detect() (*RuntimeInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := a.runOut(ctx, "version", "--format", "json")
	if err != nil {
		return &RuntimeInfo{
			Name:      a.runtimeName,
			Available: false,
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

	displayName := a.runtimeName
	switch a.runtimeName {
	case "nerdctl":
		displayName = "nerdctl (containerd)"
	case "finch":
		displayName = "Finch"
	case "crictl":
		displayName = "crictl (CRI-O)"
	case "buildah":
		displayName = "Buildah"
	}

	return &RuntimeInfo{
		Name:        a.runtimeName,
		DisplayName: displayName + " [SSH]",
		Version:     ver.Client.Version,
		OS:          ver.Client.OS,
		Arch:        ver.Client.Arch,
		Available:   true,
	}, nil
}

// ── SSHCLIAdapter: Containers ───────────────────────────────────────────────

func (a *SSHCLIAdapter) ListContainers(filters map[string]string) ([]ContainerInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	args := []string{"ps", "-a", "--format", "json"}
	for k, v := range filters {
		args = append(args, "--filter", k+"="+v)
	}

	lines, err := a.runJSONLines(ctx, args...)
	if err != nil {
		return nil, err
	}

	containers := make([]ContainerInfo, 0, len(lines))
	for _, obj := range lines {
		containers = append(containers, a.normalizeContainer(obj))
	}
	return containers, nil
}

func (a *SSHCLIAdapter) InspectContainer(id string) (*ContainerDetail, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := a.runOut(ctx, "inspect", id)
	if err != nil {
		return nil, err
	}

	var raw []map[string]any
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, &containerError{msg: "container not found: " + id}
	}

	detail := &ContainerDetail{Raw: raw[0]}
	detail.Runtime = a.runtimeName
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

// ── SSHCLIAdapter: Images ───────────────────────────────────────────────────

func (a *SSHCLIAdapter) ListImages(filters map[string]string) ([]ImageInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	args := []string{"images", "--format", "json"}
	if v, ok := filters["dangling"]; ok && v == "true" {
		args = append(args, "--filter", "dangling=true")
	}

	lines, err := a.runJSONLines(ctx, args...)
	if err != nil {
		return nil, err
	}

	images := make([]ImageInfo, 0, len(lines))
	for _, obj := range lines {
		images = append(images, a.normalizeImage(obj))
	}
	return images, nil
}

func (a *SSHCLIAdapter) InspectImage(id string) (*ImageDetail, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := a.runOut(ctx, "image", "inspect", id)
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
	detail.Runtime = a.runtimeName
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

// ── SSHCLIAdapter: Volumes ──────────────────────────────────────────────────

func (a *SSHCLIAdapter) ListVolumes() ([]VolumeInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lines, err := a.runJSONLines(ctx, "volume", "ls", "--format", "json")
	if err != nil {
		return nil, err
	}

	volumes := make([]VolumeInfo, 0, len(lines))
	for _, obj := range lines {
		volumes = append(volumes, VolumeInfo{
			Name:       getStr(obj, "Name"),
			MountPoint: getStr(obj, "Mountpoint"),
			Runtime:    a.runtimeName,
			Size:       -1,
		})
	}
	return volumes, nil
}

// ── SSHCLIAdapter: Networks ─────────────────────────────────────────────────

func (a *SSHCLIAdapter) ListNetworks() ([]NetworkInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lines, err := a.runJSONLines(ctx, "network", "ls", "--format", "json")
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
			Runtime: a.runtimeName,
		})
	}
	return networks, nil
}

// ── SSHCLIAdapter: System Info ──────────────────────────────────────────────

func (a *SSHCLIAdapter) SystemInfo() (*SystemInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := a.runOut(ctx, "info", "--format", "json")
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}

	return &SystemInfo{
		Runtime:       a.runtimeName,
		Version:       getStr(raw, "ServerVersion"),
		OS:            getStr(raw, "OperatingSystem"),
		Arch:          getStr(raw, "Architecture"),
		KernelVersion: getStr(raw, "KernelVersion"),
	}, nil
}

// ── SSHCLIAdapter: Container lifecycle (Phase 2A) ───────────────────────────

func (a *SSHCLIAdapter) StartContainer(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := a.runOut(ctx, "start", id)
	return err
}

func (a *SSHCLIAdapter) StopContainer(id string, timeout int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout+10)*time.Second)
	defer cancel()
	_, err := a.runOut(ctx, "stop", "-t", strconv.Itoa(timeout), id)
	return err
}

func (a *SSHCLIAdapter) RestartContainer(id string, timeout int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout+10)*time.Second)
	defer cancel()
	_, err := a.runOut(ctx, "restart", "-t", strconv.Itoa(timeout), id)
	return err
}

func (a *SSHCLIAdapter) RemoveContainer(id string, force bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	args := []string{"rm", id}
	if force {
		args = []string{"rm", "-f", id}
	}
	_, err := a.runOut(ctx, args...)
	return err
}

func (a *SSHCLIAdapter) PauseContainer(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := a.runOut(ctx, "pause", id)
	return err
}

func (a *SSHCLIAdapter) UnpauseContainer(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := a.runOut(ctx, "unpause", id)
	return err
}

// ── SSHCLIAdapter: Image write operations (Phase 2A) ────────────────────────

func (a *SSHCLIAdapter) PullImage(ref string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, err := a.runOut(ctx, "pull", ref)
	return err
}

func (a *SSHCLIAdapter) RemoveImage(id string, force bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	args := []string{"rmi", id}
	if force {
		args = []string{"rmi", "-f", id}
	}
	_, err := a.runOut(ctx, args...)
	return err
}

func (a *SSHCLIAdapter) PruneImages(_ bool) (*PruneResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := a.runOut(ctx, "image", "prune", "-f")
	if err != nil {
		return nil, err
	}
	deleted := parseDeletedLines(string(out))
	return &PruneResult{ItemsDeleted: deleted}, nil
}

func (a *SSHCLIAdapter) TagImage(source, target string) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := a.runOut(ctx, "tag", source, target)
	return err
}

// ── SSHCLIAdapter: Volume write operations (Phase 2A) ───────────────────────

func (a *SSHCLIAdapter) CreateVolume(name string, driver string, _ map[string]string) (*VolumeInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	args := []string{"volume", "create", name}
	if driver != "" {
		args = []string{"volume", "create", "--driver", driver, name}
	}
	_, err := a.runOut(ctx, args...)
	if err != nil {
		return nil, err
	}
	return &VolumeInfo{Name: name, Driver: driver, Runtime: a.runtimeName, Size: -1}, nil
}

func (a *SSHCLIAdapter) RemoveVolume(name string, force bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	args := []string{"volume", "rm", name}
	if force {
		args = []string{"volume", "rm", "-f", name}
	}
	_, err := a.runOut(ctx, args...)
	return err
}

func (a *SSHCLIAdapter) PruneVolumes() (*PruneResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := a.runOut(ctx, "volume", "prune", "-f")
	if err != nil {
		return nil, err
	}
	deleted := parseDeletedLines(string(out))
	return &PruneResult{ItemsDeleted: deleted}, nil
}

// ── SSHCLIAdapter: Network write operations (Phase 2A) ──────────────────────

func (a *SSHCLIAdapter) CreateNetwork(name string, driver string, _ map[string]string) (*NetworkInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	args := []string{"network", "create", name}
	if driver != "" {
		args = []string{"network", "create", "--driver", driver, name}
	}
	out, err := a.runOut(ctx, args...)
	if err != nil {
		return nil, err
	}
	netID := strings.TrimSpace(string(out))
	return &NetworkInfo{ID: truncateID(netID), Name: name, Driver: driver, Runtime: a.runtimeName}, nil
}

func (a *SSHCLIAdapter) RemoveNetwork(id string) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := a.runOut(ctx, "network", "rm", id)
	return err
}

func (a *SSHCLIAdapter) PruneNetworks() (*PruneResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := a.runOut(ctx, "network", "prune", "-f")
	if err != nil {
		return nil, err
	}
	deleted := parseDeletedLines(string(out))
	return &PruneResult{ItemsDeleted: deleted}, nil
}

// ── SSHCLIAdapter: Normalization ────────────────────────────────────────────

func (a *SSHCLIAdapter) normalizeContainer(obj map[string]any) ContainerInfo {
	created := time.Time{}
	if cs := getStr(obj, "CreatedAt"); cs != "" {
		if t, err := time.Parse(time.RFC3339Nano, cs); err == nil {
			created = t
		}
	}

	name := getStr(obj, "Names")
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
		Runtime: a.runtimeName,
	}
}

func (a *SSHCLIAdapter) normalizeImage(obj map[string]any) ImageInfo {
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
		Runtime:    a.runtimeName,
	}
}

// ── Remote runtime detection ─────────────────────────────────────────────────

// sshExecSimple runs a simple command over SSH and returns stdout.
func sshExecSimple(sshClient *ssh.Client, cmd string) (string, error) {
	sess, err := sshClient.NewSession()
	if err != nil {
		return "", err
	}
	defer sess.Close()
	var stdout bytes.Buffer
	sess.Stdout = &stdout
	err = sess.Run(cmd)
	return strings.TrimSpace(stdout.String()), err
}

// remoteSocketPaths returns the Docker/Podman socket paths to probe on a remote host.
var remoteDockerSockets = []string{
	"/var/run/docker.sock",
	"/run/docker.sock",
}

var remotePodmanSockets = []string{
	"/run/podman/podman.sock",
	"/var/run/podman/podman.sock",
}

// DetectRemoteRuntimes probes a remote host (over SSH) for available
// container runtimes. Returns RuntimeInfo entries for each detected runtime.
func DetectRemoteRuntimes(sshClient *ssh.Client) []RuntimeInfo {
	var runtimes []RuntimeInfo

	// Probe for Docker socket
	for _, sock := range remoteDockerSockets {
		if out, err := sshExecSimple(sshClient, "test -S "+sock+" && echo ok"); err == nil && out == "ok" {
			runtimes = append(runtimes, RuntimeInfo{
				Name:        "docker",
				DisplayName: "Docker Engine [SSH]",
				SocketPath:  sock,
				Available:   true,
			})
			break
		}
	}

	// Probe for Podman socket
	for _, sock := range remotePodmanSockets {
		if out, err := sshExecSimple(sshClient, "test -S "+sock+" && echo ok"); err == nil && out == "ok" {
			runtimes = append(runtimes, RuntimeInfo{
				Name:        "podman",
				DisplayName: "Podman [SSH]",
				SocketPath:  sock,
				Available:   true,
			})
			break
		}
	}

	// Probe for CLI binaries
	type binaryProbe struct {
		name        string
		displayName string
		binary      string
	}
	probes := []binaryProbe{
		{"docker", "Docker Engine [SSH]", "docker"},
		{"podman", "Podman [SSH]", "podman"},
		{"nerdctl", "nerdctl (containerd) [SSH]", "nerdctl"},
		{"finch", "Finch [SSH]", "finch"},
		{"crictl", "crictl (CRI-O) [SSH]", "crictl"},
		{"buildah", "Buildah [SSH]", "buildah"},
	}

	seen := map[string]bool{}
	for _, rt := range runtimes {
		seen[rt.Name] = true
	}

	for _, probe := range probes {
		if seen[probe.name] {
			continue
		}
		if path, err := sshExecSimple(sshClient, "which "+probe.binary+" 2>/dev/null"); err == nil && path != "" {
			runtimes = append(runtimes, RuntimeInfo{
				Name:        probe.name,
				DisplayName: probe.displayName,
				BinaryPath:  path,
				Available:   true,
			})
			seen[probe.name] = true
		}
	}

	// Mark recommended runtime (same preference as local detection)
	preference := []string{"docker", "podman", "nerdctl", "finch", "crictl"}
	for _, pref := range preference {
		for i := range runtimes {
			if runtimes[i].Name == pref && runtimes[i].Available {
				runtimes[i].Recommended = true
				return runtimes
			}
		}
	}

	return runtimes
}

// DetectTCPRuntime probes a remote Docker/Podman TCP endpoint for version info.
func DetectTCPRuntime(hostAddr string, tlsConfig *tls.Config) []RuntimeInfo {
	adapter := NewTCPDockerAdapter(hostAddr, "docker", tlsConfig)

	info, err := adapter.Detect()
	if err != nil {
		return nil
	}

	info.DisplayName += " [TCP]"
	info.Recommended = true
	return []RuntimeInfo{*info}
}

// ── Master adapter resolver ──────────────────────────────────────────────────

// resolveContainerAdapter returns the appropriate ContainerAdapter for the
// given request, handling local, SSH, and TCP connection modes.
func (s *Server) resolveContainerAdapter(req ContainerRequest) (ContainerAdapter, error) {
	switch req.ConnectionMode {
	case "ssh":
		return s.resolveSSHAdapter(req)
	case "remote-tcp":
		return s.resolveTCPAdapter(req)
	default:
		return ResolveAdapter(req)
	}
}

// resolveSSHAdapter creates an adapter for SSH connection mode.
func (s *Server) resolveSSHAdapter(req ContainerRequest) (ContainerAdapter, error) {
	if req.SSHConnection == nil {
		return nil, &containerError{msg: "sshConnection is required for SSH mode"}
	}

	sshClient, err := s.getPooledSSHClient(*req.SSHConnection)
	if err != nil {
		return nil, &containerError{msg: "ssh connect: " + err.Error()}
	}

	// If a specific runtime was requested, create the appropriate adapter.
	if req.Runtime != "" {
		return s.sshAdapterForRuntime(sshClient, req)
	}

	// Auto-detect: probe the remote host for available runtimes.
	runtimes := DetectRemoteRuntimes(sshClient)

	// Pick the recommended runtime, or first available.
	for _, rt := range runtimes {
		if rt.Recommended && rt.Available {
			req.Runtime = rt.Name
			req.SocketPath = rt.SocketPath
			return s.sshAdapterForRuntime(sshClient, req)
		}
	}
	for _, rt := range runtimes {
		if rt.Available {
			req.Runtime = rt.Name
			req.SocketPath = rt.SocketPath
			return s.sshAdapterForRuntime(sshClient, req)
		}
	}

	return nil, &containerError{msg: "no container runtime detected on remote host"}
}

// sshAdapterForRuntime creates the right SSH adapter type based on runtime name.
// Docker/Podman with a socket get the SSH-tunnelled REST API adapter (full API).
// CLI-only runtimes (nerdctl, finch, crictl, buildah) get the SSH CLI adapter.
func (s *Server) sshAdapterForRuntime(sshClient *ssh.Client, req ContainerRequest) (ContainerAdapter, error) {
	switch req.Runtime {
	case "docker":
		sock := req.SocketPath
		if sock == "" {
			// Try to find a Docker socket on the remote host
			for _, s := range remoteDockerSockets {
				if out, err := sshExecSimple(sshClient, "test -S "+s+" && echo ok"); err == nil && out == "ok" {
					sock = s
					break
				}
			}
		}
		if sock != "" {
			return NewSSHDockerAdapter(sshClient, sock, "docker"), nil
		}
		// Fall back to CLI
		return NewSSHCLIAdapter(sshClient, "docker", "docker"), nil

	case "podman":
		sock := req.SocketPath
		if sock == "" {
			for _, s := range remotePodmanSockets {
				if out, err := sshExecSimple(sshClient, "test -S "+s+" && echo ok"); err == nil && out == "ok" {
					sock = s
					break
				}
			}
		}
		if sock != "" {
			return NewSSHDockerAdapter(sshClient, sock, "podman"), nil
		}
		return NewSSHCLIAdapter(sshClient, "podman", "podman"), nil

	case "nerdctl":
		return NewSSHCLIAdapter(sshClient, "nerdctl", "nerdctl"), nil
	case "finch":
		return NewSSHCLIAdapter(sshClient, "finch", "finch"), nil
	case "crictl":
		return NewSSHCLIAdapter(sshClient, "crictl", "crictl"), nil
	case "buildah":
		return NewSSHCLIAdapter(sshClient, "buildah", "buildah"), nil
	default:
		return nil, &containerError{msg: "unsupported remote runtime: " + req.Runtime}
	}
}

// resolveTCPAdapter creates an adapter for remote TCP connection mode.
func (s *Server) resolveTCPAdapter(req ContainerRequest) (ContainerAdapter, error) {
	if req.RemoteHost == "" {
		return nil, &containerError{msg: "remoteHost is required for remote-tcp mode (e.g. tcp://host:2376)"}
	}

	tlsConfig, err := buildTLSConfig(req.TLSCACert, req.TLSCert, req.TLSKey)
	if err != nil {
		return nil, err
	}

	hostAddr := parseTCPHost(req.RemoteHost, tlsConfig != nil)
	runtimeName := "docker" // TCP mode is Docker/Podman API only
	if req.Runtime != "" {
		runtimeName = req.Runtime
	}

	return NewTCPDockerAdapter(hostAddr, runtimeName, tlsConfig), nil
}

// ── Remote detection for /detect endpoint ────────────────────────────────────

// detectRemoteRuntimes handles runtime detection for SSH and TCP modes.
func (s *Server) detectRemoteRuntimes(req ContainerRequest) ([]RuntimeInfo, error) {
	switch req.ConnectionMode {
	case "ssh":
		if req.SSHConnection == nil {
			return nil, &containerError{msg: "sshConnection is required for SSH mode"}
		}
		sshClient, err := s.getPooledSSHClient(*req.SSHConnection)
		if err != nil {
			return nil, &containerError{msg: "ssh connect: " + err.Error()}
		}
		return DetectRemoteRuntimes(sshClient), nil

	case "remote-tcp":
		if req.RemoteHost == "" {
			return nil, &containerError{msg: "remoteHost is required for remote-tcp mode"}
		}
		tlsConfig, err := buildTLSConfig(req.TLSCACert, req.TLSCert, req.TLSKey)
		if err != nil {
			return nil, err
		}
		hostAddr := parseTCPHost(req.RemoteHost, tlsConfig != nil)
		runtimes := DetectTCPRuntime(hostAddr, tlsConfig)
		if len(runtimes) == 0 {
			return nil, &containerError{msg: "could not connect to remote host: " + req.RemoteHost}
		}
		return runtimes, nil

	default:
		return DetectRuntimes(), nil
	}
}
