package proxycore

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ── VM-aware management (Phase 4A) ────────────────────────────────────────
//
// Detects VM-backed container runtimes:
//   - Lima:            limactl list --json
//   - Colima:          colima list --json
//   - Finch:           finch vm status (uses Lima underneath)
//   - Rancher Desktop: rdctl list-settings + rdctl api /v1/settings

// ── VM detection cache ─────────────────────────────────────────────────────

type vmDetectionCache struct {
	mu     sync.Mutex
	vms    []VMInfo
	expiry time.Time
}

const vmCacheTTL = 60 * time.Second

var globalVMCache = &vmDetectionCache{}

// DetectVMs probes for VM-backed container runtimes.
// Results are cached with a 60-second TTL.
func DetectVMs() []VMInfo {
	globalVMCache.mu.Lock()
	defer globalVMCache.mu.Unlock()

	if time.Now().Before(globalVMCache.expiry) && globalVMCache.vms != nil {
		return globalVMCache.vms
	}

	vms := detectAllVMs()
	globalVMCache.vms = vms
	globalVMCache.expiry = time.Now().Add(vmCacheTTL)
	return vms
}

// InvalidateVMCache forces the next DetectVMs call to re-probe.
func InvalidateVMCache() {
	globalVMCache.mu.Lock()
	defer globalVMCache.mu.Unlock()
	globalVMCache.expiry = time.Time{}
}

func detectAllVMs() []VMInfo {
	var vms []VMInfo

	// Detect in parallel to avoid sequential CLI latency
	type result struct {
		vms []VMInfo
	}
	ch := make(chan result, 4)

	go func() { ch <- result{detectLimaVMs()} }()
	go func() { ch <- result{detectColimaVMs()} }()
	go func() { ch <- result{detectFinchVMs()} }()
	go func() { ch <- result{detectRancherDesktopVMs()} }()

	for i := 0; i < 4; i++ {
		r := <-ch
		vms = append(vms, r.vms...)
	}

	return vms
}

// ── Lima detection ─────────────────────────────────────────────────────────

// limaListEntry represents one instance in `limactl list --json` output.
// Lima outputs one JSON object per line (NDJSON), not a JSON array.
type limaListEntry struct {
	Name    string `json:"name"`
	Status  string `json:"status"` // Running, Stopped
	Dir     string `json:"dir"`
	Arch    string `json:"arch"`
	CPUs    int    `json:"cpus"`
	Memory  int64  `json:"memory"` // bytes
	Disk    int64  `json:"disk"`   // bytes
	VMType  string `json:"vmType"`
	MountType string `json:"mountType,omitempty"`
}

func detectLimaVMs() []VMInfo {
	path, err := exec.LookPath("limactl")
	if err != nil {
		return nil
	}

	out, err := exec.Command(path, "list", "--json").Output()
	if err != nil {
		return nil
	}

	var vms []VMInfo

	// limactl list --json outputs NDJSON (one JSON object per line)
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var entry limaListEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}

		// Skip instances that are managed by Colima or Finch — they
		// will be reported by their own detectors with richer context.
		if isColimaInstance(entry.Name, entry.Dir) || isFinchInstance(entry.Name, entry.Dir) {
			continue
		}

		vm := VMInfo{
			Name:        entry.Name,
			Type:        "lima",
			DisplayName: fmt.Sprintf("Lima (%s)", entry.Name),
			Status:      normVMStatus(entry.Status),
			CPUs:        entry.CPUs,
			Memory:      entry.Memory,
			Disk:        entry.Disk,
			Arch:        entry.Arch,
			VMType:      entry.VMType,
			MountType:   entry.MountType,
			Dir:         entry.Dir,
		}

		// Try to determine the container runtime inside the VM
		vm.Runtime = limaGuessRuntime(entry.Dir)

		// Get PID if running
		if vm.Status == "Running" {
			vm.PID = limaPID(entry.Dir)
		}

		vms = append(vms, vm)
	}

	return vms
}

// isColimaInstance checks if a Lima instance was created by Colima.
func isColimaInstance(name, dir string) bool {
	if strings.HasPrefix(name, "colima") {
		return true
	}
	if strings.Contains(dir, "colima") {
		return true
	}
	return false
}

// isFinchInstance checks if a Lima instance was created by Finch.
func isFinchInstance(name, dir string) bool {
	if strings.Contains(dir, "finch") {
		return true
	}
	if name == "finch" {
		return true
	}
	return false
}

// limaGuessRuntime reads the Lima instance config to determine the container runtime.
func limaGuessRuntime(dir string) string {
	if dir == "" {
		return ""
	}
	data, err := os.ReadFile(filepath.Join(dir, "lima.yaml"))
	if err != nil {
		return ""
	}
	content := string(data)
	// Simple heuristic: check for runtime mentions in the config
	if strings.Contains(content, "docker") {
		return "docker"
	}
	if strings.Contains(content, "containerd") || strings.Contains(content, "nerdctl") {
		return "containerd"
	}
	if strings.Contains(content, "podman") {
		return "podman"
	}
	return "containerd" // Lima default
}

// limaPID reads the PID file for a running Lima instance.
func limaPID(dir string) int {
	if dir == "" {
		return 0
	}

	// Lima stores PID files as <dir>/qemu.pid or <dir>/vz.pid
	for _, name := range []string{"qemu.pid", "vz.pid"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err == nil && pid > 0 {
			return pid
		}
	}

	// Also try the ssh.pid or ha.pid (Lima's host agent)
	data, err := os.ReadFile(filepath.Join(dir, "ha.pid"))
	if err == nil {
		pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
		if err == nil && pid > 0 {
			return pid
		}
	}
	return 0
}

// ── Colima detection ───────────────────────────────────────────────────────

// colimaListEntry represents one instance in `colima list --json` output.
type colimaListEntry struct {
	Name    string `json:"name"`
	Status  string `json:"status"` // Running, Stopped
	Arch    string `json:"arch"`
	CPUs    int    `json:"cpus"`
	Memory  int64  `json:"memory"` // bytes
	Disk    int64  `json:"disk"`   // bytes
	Runtime string `json:"runtime"`
	VMType  string `json:"vmType,omitempty"`
	MountType string `json:"mountType,omitempty"`
	Address string `json:"address,omitempty"`
}

func detectColimaVMs() []VMInfo {
	path, err := exec.LookPath("colima")
	if err != nil {
		return nil
	}

	out, err := exec.Command(path, "list", "--json").Output()
	if err != nil {
		return nil
	}

	var vms []VMInfo

	// colima list --json outputs NDJSON (one JSON object per line)
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var entry colimaListEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			continue
		}

		displayName := "Colima"
		if entry.Name != "" && entry.Name != "default" {
			displayName = fmt.Sprintf("Colima (%s)", entry.Name)
		}

		vm := VMInfo{
			Name:        entry.Name,
			Type:        "colima",
			DisplayName: displayName,
			Status:      normVMStatus(entry.Status),
			CPUs:        entry.CPUs,
			Memory:      entry.Memory,
			Disk:        entry.Disk,
			Arch:        entry.Arch,
			Runtime:     entry.Runtime,
			VMType:      entry.VMType,
			MountType:   entry.MountType,
		}

		// Resolve Colima instance directory
		vm.Dir = colimaDir(entry.Name)

		// Get PID if running
		if vm.Status == "Running" {
			vm.PID = colimaPID(entry.Name)
		}

		vms = append(vms, vm)
	}

	return vms
}

// colimaDir returns the directory for a Colima instance.
func colimaDir(name string) string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	if name == "" || name == "default" {
		return filepath.Join(home, ".colima", "default")
	}
	return filepath.Join(home, ".colima", name)
}

// colimaPID reads the PID from a Colima instance.
func colimaPID(name string) int {
	dir := colimaDir(name)
	if dir == "" {
		return 0
	}
	// Colima uses Lima underneath, so PID is in the Lima directory
	home, _ := os.UserHomeDir()
	if home == "" {
		return 0
	}
	limaName := "colima"
	if name != "" && name != "default" {
		limaName = "colima-" + name
	}
	limaDir := filepath.Join(home, ".lima", limaName)
	return limaPID(limaDir)
}

// ── Finch detection ────────────────────────────────────────────────────────

func detectFinchVMs() []VMInfo {
	path, err := exec.LookPath("finch")
	if err != nil {
		return nil
	}

	// Finch uses Lima underneath. `finch vm status` returns the status.
	out, err := exec.Command(path, "vm", "status").Output()
	if err != nil {
		// If the command fails, check if Finch is installed but VM not initialized
		return []VMInfo{{
			Name:        "finch",
			Type:        "finch",
			DisplayName: "Finch",
			Status:      "Stopped",
		}}
	}

	status := normVMStatus(strings.TrimSpace(string(out)))

	vm := VMInfo{
		Name:        "finch",
		Type:        "finch",
		DisplayName: "Finch",
		Status:      status,
		Runtime:     "containerd", // Finch always uses containerd/nerdctl
	}

	// Try to get resource info from the Finch Lima config
	finchDir := finchLimaDir()
	if finchDir != "" {
		vm.Dir = finchDir

		// Read the Lima config for resource info
		data, err := os.ReadFile(filepath.Join(finchDir, "lima.yaml"))
		if err == nil {
			parseFinchResources(&vm, string(data))
		}

		if status == "Running" {
			vm.PID = limaPID(finchDir)
		}
	}

	// Get arch from runtime
	if runtime.GOARCH == "arm64" {
		vm.Arch = "aarch64"
	} else {
		vm.Arch = "x86_64"
	}

	return []VMInfo{vm}
}

// finchLimaDir returns the Lima instance directory used by Finch.
func finchLimaDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	// Finch stores its Lima instance at ~/.finch/lima/data/_config or ~/.lima/finch
	candidates := []string{
		filepath.Join(home, ".lima", "finch"),
		filepath.Join(home, ".finch", "lima", "data", "finch"),
	}
	for _, c := range candidates {
		if fi, err := os.Stat(c); err == nil && fi.IsDir() {
			return c
		}
	}
	return ""
}

// parseFinchResources extracts CPU/memory/disk from a Lima YAML config.
// Uses simple line scanning instead of a YAML parser to avoid dependencies.
func parseFinchResources(vm *VMInfo, content string) {
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "cpus:") {
			if v, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "cpus:"))); err == nil {
				vm.CPUs = v
			}
		} else if strings.HasPrefix(line, "memory:") {
			val := strings.TrimSpace(strings.TrimPrefix(line, "memory:"))
			vm.Memory = parseMemoryString(val)
		} else if strings.HasPrefix(line, "disk:") {
			val := strings.TrimSpace(strings.TrimPrefix(line, "disk:"))
			vm.Disk = parseDiskString(val)
		} else if strings.HasPrefix(line, "arch:") {
			vm.Arch = strings.TrimSpace(strings.TrimPrefix(line, "arch:"))
		} else if strings.HasPrefix(line, "vmType:") {
			vm.VMType = strings.TrimSpace(strings.TrimPrefix(line, "vmType:"))
		} else if strings.HasPrefix(line, "mountType:") {
			vm.MountType = strings.TrimSpace(strings.TrimPrefix(line, "mountType:"))
		}
	}
}

// ── Rancher Desktop detection ──────────────────────────────────────────────

func detectRancherDesktopVMs() []VMInfo {
	path, err := exec.LookPath("rdctl")
	if err != nil {
		return nil
	}

	out, err := exec.Command(path, "list-settings").Output()
	if err != nil {
		// rdctl exists but may not be running
		return []VMInfo{{
			Name:        "rancher-desktop",
			Type:        "rancher-desktop",
			DisplayName: "Rancher Desktop",
			Status:      "Stopped",
		}}
	}

	vm := VMInfo{
		Name:        "rancher-desktop",
		Type:        "rancher-desktop",
		DisplayName: "Rancher Desktop",
		Status:      "Running", // if rdctl list-settings succeeds, it's running
	}

	// Parse settings JSON for resource info
	var settings map[string]any
	if err := json.Unmarshal(out, &settings); err == nil {
		parseRancherSettings(&vm, settings)
	}

	// Get arch from runtime
	if runtime.GOARCH == "arm64" {
		vm.Arch = "aarch64"
	} else {
		vm.Arch = "x86_64"
	}

	return []VMInfo{vm}
}

// parseRancherSettings extracts VM resource configuration from rdctl settings.
func parseRancherSettings(vm *VMInfo, settings map[string]any) {
	// settings.virtualMachine.memoryInGB, .numberCPUs
	if vmSettings, ok := settings["virtualMachine"].(map[string]any); ok {
		if cpus, ok := vmSettings["numberCPUs"].(float64); ok {
			vm.CPUs = int(cpus)
		}
		if memGB, ok := vmSettings["memoryInGB"].(float64); ok {
			vm.Memory = int64(memGB * 1024 * 1024 * 1024)
		}
	}

	// settings.containerEngine.name (moby = docker, containerd)
	if engine, ok := settings["containerEngine"].(map[string]any); ok {
		if name, ok := engine["name"].(string); ok {
			if name == "moby" {
				vm.Runtime = "docker"
			} else {
				vm.Runtime = name
			}
		}
	}

	// Rancher Desktop on macOS uses Lima/QEMU or VZ
	if runtime.GOOS == "darwin" {
		vm.VMType = "lima"
	} else if runtime.GOOS == "windows" {
		vm.VMType = "wsl2"
	}
}

// ── VM lifecycle actions ───────────────────────────────────────────────────

// StartVM starts a VM-backed container runtime.
func StartVM(vmType, name string) error {
	switch vmType {
	case "lima":
		return runVMCmd("limactl", "start", name)
	case "colima":
		if name == "" || name == "default" {
			return runVMCmd("colima", "start")
		}
		return runVMCmd("colima", "start", "--profile", name)
	case "finch":
		return runVMCmd("finch", "vm", "start")
	case "rancher-desktop":
		return runVMCmd("rdctl", "start")
	default:
		return fmt.Errorf("cannot start VM: unknown type %q", vmType)
	}
}

// StopVM stops a VM-backed container runtime.
func StopVM(vmType, name string) error {
	switch vmType {
	case "lima":
		return runVMCmd("limactl", "stop", name)
	case "colima":
		if name == "" || name == "default" {
			return runVMCmd("colima", "stop")
		}
		return runVMCmd("colima", "stop", "--profile", name)
	case "finch":
		return runVMCmd("finch", "vm", "stop")
	case "rancher-desktop":
		return runVMCmd("rdctl", "shutdown")
	default:
		return fmt.Errorf("cannot stop VM: unknown type %q", vmType)
	}
}

func runVMCmd(name string, args ...string) error {
	path, err := exec.LookPath(name)
	if err != nil {
		return fmt.Errorf("%s not found on PATH", name)
	}
	cmd := exec.Command(path, args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("%s %s failed: %s", name, strings.Join(args, " "), strings.TrimSpace(string(out)))
	}
	return nil
}

// ── Helpers ────────────────────────────────────────────────────────────────

// normVMStatus normalizes the various status strings from different tools.
func normVMStatus(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	switch {
	case s == "running" || s == "started":
		return "Running"
	case s == "stopped" || s == "not running" || s == "broken":
		return "Stopped"
	default:
		return "Unknown"
	}
}

// parseMemoryString parses memory strings like "4GiB", "2048MiB", "4294967296".
func parseMemoryString(s string) int64 {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "\"'")
	// Try plain bytes
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return v
	}
	s = strings.ToUpper(s)
	multiplier := int64(1)
	if strings.HasSuffix(s, "GIB") || strings.HasSuffix(s, "GB") || strings.HasSuffix(s, "G") {
		multiplier = 1024 * 1024 * 1024
		s = strings.TrimRight(s, "GBIT")
	} else if strings.HasSuffix(s, "MIB") || strings.HasSuffix(s, "MB") || strings.HasSuffix(s, "M") {
		multiplier = 1024 * 1024
		s = strings.TrimRight(s, "MBIT")
	} else if strings.HasSuffix(s, "KIB") || strings.HasSuffix(s, "KB") || strings.HasSuffix(s, "K") {
		multiplier = 1024
		s = strings.TrimRight(s, "KBIT")
	}
	s = strings.TrimSpace(s)
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(v * float64(multiplier))
	}
	return 0
}

// parseDiskString parses disk size strings like "60GiB", "107374182400".
func parseDiskString(s string) int64 {
	return parseMemoryString(s) // same format
}

// ── Gateway handlers ──────────────────────────────────────────────────────

// handleVMList detects and returns all VM-backed container runtimes.
func (s *Server) handleVMList(w http.ResponseWriter, _ ContainerRequest) {
	t0 := time.Now()
	vms := DetectVMs()
	json.NewEncoder(w).Encode(ContainerResponse{VMs: vms, DurationMs: ms(t0)})
}

// handleVMStart starts a VM by type and name.
func (s *Server) handleVMStart(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	if req.VMType == "" {
		json.NewEncoder(w).Encode(ContainerResponse{Error: "vmType is required", DurationMs: ms(t0)})
		return
	}

	if err := StartVM(req.VMType, req.Name); err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	// Invalidate caches so next detect picks up the new state
	InvalidateVMCache()
	InvalidateDetectionCache()

	json.NewEncoder(w).Encode(ContainerResponse{OK: true, DurationMs: ms(t0)})
}

// handleVMStop stops a VM by type and name.
func (s *Server) handleVMStop(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	if req.VMType == "" {
		json.NewEncoder(w).Encode(ContainerResponse{Error: "vmType is required", DurationMs: ms(t0)})
		return
	}

	if err := StopVM(req.VMType, req.Name); err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	// Invalidate caches
	InvalidateVMCache()
	InvalidateDetectionCache()

	json.NewEncoder(w).Encode(ContainerResponse{OK: true, DurationMs: ms(t0)})
}
