package proxycore

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
)

// ── /container/export-run — generate docker run / podman run command ────────

func (s *Server) handleContainerExportRun(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	if req.ID == "" && req.Name == "" {
		json.NewEncoder(w).Encode(ContainerResponse{Error: "id or name is required", DurationMs: ms(t0)})
		return
	}

	adapter, err := s.resolveContainerAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	target := req.ID
	if target == "" {
		target = req.Name
	}

	detail, err := adapter.InspectContainer(target)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	runtime := adapter.Name()
	cmd := buildRunCommand(detail, runtime)

	json.NewEncoder(w).Encode(ContainerResponse{ExportRun: cmd, DurationMs: ms(t0)})
}

// ── /container/export-compose — generate compose YAML ──────────────────────

func (s *Server) handleContainerExportCompose(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	if req.ID == "" && req.Name == "" {
		json.NewEncoder(w).Encode(ContainerResponse{Error: "id or name is required", DurationMs: ms(t0)})
		return
	}

	adapter, err := s.resolveContainerAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	target := req.ID
	if target == "" {
		target = req.Name
	}

	detail, err := adapter.InspectContainer(target)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	yaml := buildComposeYAML(detail)

	json.NewEncoder(w).Encode(ContainerResponse{ExportCompose: yaml, DurationMs: ms(t0)})
}

// ── Run command builder ────────────────────────────────────────────────────

func buildRunCommand(d *ContainerDetail, runtime string) string {
	var parts []string

	// Base command
	if runtime == "podman" {
		parts = append(parts, "podman run")
	} else {
		parts = append(parts, "docker run")
	}

	// Detached
	parts = append(parts, "-d")

	// Container name
	name := strings.TrimPrefix(d.Name, "/")
	if name != "" {
		parts = append(parts, "--name", ctShellQuote(name))
	}

	// Hostname
	if d.Hostname != "" && d.Hostname != name {
		parts = append(parts, "--hostname", ctShellQuote(d.Hostname))
	}

	// User
	if d.User != "" {
		parts = append(parts, "--user", ctShellQuote(d.User))
	}

	// Working dir
	if d.WorkingDir != "" {
		parts = append(parts, "--workdir", ctShellQuote(d.WorkingDir))
	}

	// Restart policy from raw HostConfig
	restartPolicy := extractRestartPolicy(d.Raw)
	if restartPolicy != "" && restartPolicy != "no" {
		parts = append(parts, "--restart", restartPolicy)
	}

	// Privileged
	if d.Privileged {
		parts = append(parts, "--privileged")
	}

	// Network mode
	networkMode := getNestedStr(d.Raw, "HostConfig", "NetworkMode")
	if networkMode != "" && networkMode != "default" && networkMode != "bridge" {
		parts = append(parts, "--network", ctShellQuote(networkMode))
	}

	// PID mode
	pidMode := getNestedStr(d.Raw, "HostConfig", "PidMode")
	if pidMode != "" {
		parts = append(parts, "--pid", ctShellQuote(pidMode))
	}

	// IPC mode
	ipcMode := getNestedStr(d.Raw, "HostConfig", "IpcMode")
	if ipcMode != "" && ipcMode != "private" && ipcMode != "shareable" {
		parts = append(parts, "--ipc", ctShellQuote(ipcMode))
	}

	// Memory limit
	memLimit := getNestedInt(d.Raw, "HostConfig", "Memory")
	if memLimit > 0 {
		parts = append(parts, "--memory", fmt.Sprintf("%d", memLimit))
	}

	// CPU shares
	cpuShares := getNestedInt(d.Raw, "HostConfig", "CpuShares")
	if cpuShares > 0 && cpuShares != 1024 {
		parts = append(parts, "--cpu-shares", fmt.Sprintf("%d", cpuShares))
	}

	// CPU period + quota → --cpus
	nanoCpus := getNestedInt(d.Raw, "HostConfig", "NanoCpus")
	if nanoCpus > 0 {
		cpus := float64(nanoCpus) / 1e9
		parts = append(parts, "--cpus", fmt.Sprintf("%.2f", cpus))
	}

	// Read-only rootfs
	readOnly := getNestedBool(d.Raw, "HostConfig", "ReadonlyRootfs")
	if readOnly {
		parts = append(parts, "--read-only")
	}

	// Capabilities
	capAdd := getNestedStrSlice(d.Raw, "HostConfig", "CapAdd")
	for _, cap := range capAdd {
		parts = append(parts, "--cap-add", cap)
	}
	capDrop := getNestedStrSlice(d.Raw, "HostConfig", "CapDrop")
	for _, cap := range capDrop {
		parts = append(parts, "--cap-drop", cap)
	}

	// Environment variables
	for _, e := range d.Env {
		parts = append(parts, "-e", ctShellQuote(e))
	}

	// Labels (skip compose labels to reduce noise)
	if d.Labels != nil {
		keys := make([]string, 0, len(d.Labels))
		for k := range d.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if strings.HasPrefix(k, "com.docker.compose.") {
				continue
			}
			parts = append(parts, "--label", ctShellQuote(k+"="+d.Labels[k]))
		}
	}

	// Port bindings
	for _, p := range d.Ports {
		if p.HostPort > 0 {
			portStr := fmt.Sprintf("%d:%d", p.HostPort, p.ContainerPort)
			if p.HostIP != "" && p.HostIP != "0.0.0.0" {
				portStr = p.HostIP + ":" + portStr
			}
			if p.Protocol == "udp" {
				portStr += "/udp"
			}
			parts = append(parts, "-p", portStr)
		}
	}

	// Volumes / mounts
	for _, m := range d.MountDetails {
		if m.Type == "volume" || m.Type == "bind" {
			mountStr := m.Source + ":" + m.Destination
			if !m.RW {
				mountStr += ":ro"
			}
			parts = append(parts, "-v", ctShellQuote(mountStr))
		} else if m.Type == "tmpfs" {
			parts = append(parts, "--tmpfs", m.Destination)
		}
	}

	// Entrypoint (only if explicitly set, not from image)
	if len(d.Entrypoint) > 0 {
		ep := strings.Join(d.Entrypoint, " ")
		parts = append(parts, "--entrypoint", ctShellQuote(ep))
	}

	// Image
	parts = append(parts, ctShellQuote(d.Image))

	// Command
	if len(d.Cmd) > 0 {
		for _, c := range d.Cmd {
			parts = append(parts, ctShellQuote(c))
		}
	}

	return formatMultilineCommand(parts)
}

// ── Compose YAML builder ───────────────────────────────────────────────────

func buildComposeYAML(d *ContainerDetail) string {
	var b strings.Builder
	i1 := "  "
	i2 := "    "
	i3 := "      "

	name := strings.TrimPrefix(d.Name, "/")
	if name == "" {
		name = "service"
	}

	// Sanitize service name for compose (alphanumeric, hyphens, underscores)
	svcName := sanitizeServiceName(name)

	b.WriteString("services:\n")
	b.WriteString(i1 + svcName + ":\n")

	// Image
	b.WriteString(i2 + "image: " + d.Image + "\n")

	// Container name
	if name != svcName {
		b.WriteString(i2 + "container_name: " + name + "\n")
	}

	// Hostname
	if d.Hostname != "" && d.Hostname != name {
		b.WriteString(i2 + "hostname: " + d.Hostname + "\n")
	}

	// User
	if d.User != "" {
		b.WriteString(i2 + "user: " + yamlQuote(d.User) + "\n")
	}

	// Working dir
	if d.WorkingDir != "" {
		b.WriteString(i2 + "working_dir: " + d.WorkingDir + "\n")
	}

	// Entrypoint
	if len(d.Entrypoint) > 0 {
		b.WriteString(i2 + "entrypoint:\n")
		for _, e := range d.Entrypoint {
			b.WriteString(i3 + "- " + yamlQuote(e) + "\n")
		}
	}

	// Command
	if len(d.Cmd) > 0 {
		b.WriteString(i2 + "command:\n")
		for _, c := range d.Cmd {
			b.WriteString(i3 + "- " + yamlQuote(c) + "\n")
		}
	}

	// Environment
	if len(d.Env) > 0 {
		b.WriteString(i2 + "environment:\n")
		for _, e := range d.Env {
			b.WriteString(i3 + "- " + yamlQuote(e) + "\n")
		}
	}

	// Ports
	if len(d.Ports) > 0 {
		b.WriteString(i2 + "ports:\n")
		for _, p := range d.Ports {
			if p.HostPort > 0 {
				portStr := fmt.Sprintf("%d:%d", p.HostPort, p.ContainerPort)
				if p.HostIP != "" && p.HostIP != "0.0.0.0" {
					portStr = p.HostIP + ":" + portStr
				}
				if p.Protocol == "udp" {
					portStr += "/udp"
				}
				b.WriteString(i3 + "- " + yamlQuote(portStr) + "\n")
			}
		}
	}

	// Volumes
	if len(d.MountDetails) > 0 {
		hasVolumeMounts := false
		for _, m := range d.MountDetails {
			if m.Type == "volume" || m.Type == "bind" || m.Type == "tmpfs" {
				hasVolumeMounts = true
				break
			}
		}
		if hasVolumeMounts {
			b.WriteString(i2 + "volumes:\n")
			for _, m := range d.MountDetails {
				if m.Type == "volume" || m.Type == "bind" {
					mountStr := m.Source + ":" + m.Destination
					if !m.RW {
						mountStr += ":ro"
					}
					b.WriteString(i3 + "- " + yamlQuote(mountStr) + "\n")
				} else if m.Type == "tmpfs" {
					b.WriteString(i3 + "- type: tmpfs\n")
					b.WriteString(i3 + "  target: " + m.Destination + "\n")
				}
			}
		}
	}

	// Networks
	if len(d.Networks) > 0 {
		b.WriteString(i2 + "networks:\n")
		for _, n := range d.Networks {
			if n != "bridge" && n != "host" && n != "none" {
				b.WriteString(i3 + "- " + n + "\n")
			}
		}
	}

	// Restart policy
	restartPolicy := extractRestartPolicy(d.Raw)
	if restartPolicy != "" && restartPolicy != "no" {
		b.WriteString(i2 + "restart: " + restartPolicy + "\n")
	}

	// Privileged
	if d.Privileged {
		b.WriteString(i2 + "privileged: true\n")
	}

	// Read-only rootfs
	readOnly := getNestedBool(d.Raw, "HostConfig", "ReadonlyRootfs")
	if readOnly {
		b.WriteString(i2 + "read_only: true\n")
	}

	// Network mode
	networkMode := getNestedStr(d.Raw, "HostConfig", "NetworkMode")
	if networkMode == "host" || networkMode == "none" {
		b.WriteString(i2 + "network_mode: " + networkMode + "\n")
	}

	// PID mode
	pidMode := getNestedStr(d.Raw, "HostConfig", "PidMode")
	if pidMode == "host" {
		b.WriteString(i2 + "pid: " + pidMode + "\n")
	}

	// Capabilities
	capAdd := getNestedStrSlice(d.Raw, "HostConfig", "CapAdd")
	if len(capAdd) > 0 {
		b.WriteString(i2 + "cap_add:\n")
		for _, c := range capAdd {
			b.WriteString(i3 + "- " + c + "\n")
		}
	}
	capDrop := getNestedStrSlice(d.Raw, "HostConfig", "CapDrop")
	if len(capDrop) > 0 {
		b.WriteString(i2 + "cap_drop:\n")
		for _, c := range capDrop {
			b.WriteString(i3 + "- " + c + "\n")
		}
	}

	// Memory limit
	memLimit := getNestedInt(d.Raw, "HostConfig", "Memory")
	if memLimit > 0 {
		b.WriteString(i2 + "mem_limit: " + fmt.Sprintf("%d", memLimit) + "\n")
	}

	// CPU
	nanoCpus := getNestedInt(d.Raw, "HostConfig", "NanoCpus")
	if nanoCpus > 0 {
		cpus := float64(nanoCpus) / 1e9
		b.WriteString(i2 + fmt.Sprintf("cpus: %.2f\n", cpus))
	}

	// Labels (skip compose labels)
	if d.Labels != nil {
		filteredLabels := map[string]string{}
		for k, v := range d.Labels {
			if !strings.HasPrefix(k, "com.docker.compose.") {
				filteredLabels[k] = v
			}
		}
		if len(filteredLabels) > 0 {
			b.WriteString(i2 + "labels:\n")
			keys := make([]string, 0, len(filteredLabels))
			for k := range filteredLabels {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				b.WriteString(i3 + yamlQuote(k) + ": " + yamlQuote(filteredLabels[k]) + "\n")
			}
		}
	}

	// Named volumes section (if any volume mounts use named volumes)
	namedVolumes := []string{}
	for _, m := range d.MountDetails {
		if m.Type == "volume" && m.Source != "" && !strings.HasPrefix(m.Source, "/") {
			namedVolumes = append(namedVolumes, m.Source)
		}
	}
	if len(namedVolumes) > 0 {
		b.WriteString("\nvolumes:\n")
		seen := map[string]bool{}
		for _, v := range namedVolumes {
			if !seen[v] {
				b.WriteString(i1 + v + ":\n")
				seen[v] = true
			}
		}
	}

	// External networks
	extNetworks := []string{}
	for _, n := range d.Networks {
		if n != "bridge" && n != "host" && n != "none" && n != "default" {
			extNetworks = append(extNetworks, n)
		}
	}
	if len(extNetworks) > 0 {
		b.WriteString("\nnetworks:\n")
		for _, n := range extNetworks {
			b.WriteString(i1 + n + ":\n")
			b.WriteString(i2 + "external: true\n")
		}
	}

	return b.String()
}

// ── Helpers ────────────────────────────────────────────────────────────────

func ctShellQuote(s string) string {
	// If the string is simple, return it unquoted
	safe := true
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '/' || c == ':' || c == '=' || c == ',') {
			safe = false
			break
		}
	}
	if safe && len(s) > 0 {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

func yamlQuote(s string) string {
	// Check if quoting is needed
	needsQuote := false
	if s == "" || s == "true" || s == "false" || s == "null" || s == "yes" || s == "no" {
		needsQuote = true
	}
	for _, c := range s {
		if c == ':' || c == '#' || c == '[' || c == ']' || c == '{' || c == '}' ||
			c == ',' || c == '&' || c == '*' || c == '!' || c == '|' || c == '>' ||
			c == '\'' || c == '"' || c == '%' || c == '@' || c == '`' {
			needsQuote = true
			break
		}
	}
	if needsQuote {
		return "\"" + strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "\"", "\\\"") + "\""
	}
	return s
}

func sanitizeServiceName(name string) string {
	var b strings.Builder
	for _, c := range name {
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			b.WriteRune(c)
		} else if c >= 'A' && c <= 'Z' {
			b.WriteRune(c - 'A' + 'a')
		} else {
			b.WriteRune('-')
		}
	}
	result := b.String()
	result = strings.Trim(result, "-")
	if result == "" {
		return "service"
	}
	return result
}

func formatMultilineCommand(parts []string) string {
	if len(parts) <= 4 {
		return strings.Join(parts, " ")
	}

	var b strings.Builder
	b.WriteString(parts[0]) // docker run / podman run
	for i := 1; i < len(parts); i++ {
		b.WriteString(" \\\n  " + parts[i])
	}
	return b.String()
}

func extractRestartPolicy(raw map[string]any) string {
	hc, ok := raw["HostConfig"]
	if !ok {
		return ""
	}
	hcMap, ok := hc.(map[string]any)
	if !ok {
		return ""
	}
	rp, ok := hcMap["RestartPolicy"]
	if !ok {
		return ""
	}
	rpMap, ok := rp.(map[string]any)
	if !ok {
		return ""
	}
	name, _ := rpMap["Name"].(string)
	if name == "" {
		return ""
	}
	maxRetry, _ := rpMap["MaximumRetryCount"].(float64)
	if name == "on-failure" && maxRetry > 0 {
		return fmt.Sprintf("on-failure:%d", int(maxRetry))
	}
	return name
}

func getNestedStr(raw map[string]any, keys ...string) string {
	current := any(raw)
	for _, k := range keys[:len(keys)-1] {
		m, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current = m[k]
	}
	m, ok := current.(map[string]any)
	if !ok {
		return ""
	}
	v, _ := m[keys[len(keys)-1]].(string)
	return v
}

func getNestedInt(raw map[string]any, keys ...string) int64 {
	current := any(raw)
	for _, k := range keys[:len(keys)-1] {
		m, ok := current.(map[string]any)
		if !ok {
			return 0
		}
		current = m[k]
	}
	m, ok := current.(map[string]any)
	if !ok {
		return 0
	}
	v := m[keys[len(keys)-1]]
	switch n := v.(type) {
	case float64:
		return int64(n)
	case int64:
		return n
	case int:
		return int64(n)
	}
	return 0
}

func getNestedBool(raw map[string]any, keys ...string) bool {
	current := any(raw)
	for _, k := range keys[:len(keys)-1] {
		m, ok := current.(map[string]any)
		if !ok {
			return false
		}
		current = m[k]
	}
	m, ok := current.(map[string]any)
	if !ok {
		return false
	}
	v, _ := m[keys[len(keys)-1]].(bool)
	return v
}

func getNestedStrSlice(raw map[string]any, keys ...string) []string {
	current := any(raw)
	for _, k := range keys[:len(keys)-1] {
		m, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = m[k]
	}
	m, ok := current.(map[string]any)
	if !ok {
		return nil
	}
	arr, ok := m[keys[len(keys)-1]].([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(arr))
	for _, v := range arr {
		if s, ok := v.(string); ok {
			result = append(result, s)
		}
	}
	return result
}
