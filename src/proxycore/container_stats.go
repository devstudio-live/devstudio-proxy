package proxycore

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// handleContainerStats streams live container stats via SSE (Server-Sent Events).
// The client POSTs a ContainerRequest with the container ID; the response is
// an SSE stream of ContainerStats snapshots until the client disconnects.
func (s *Server) handleContainerStats(w http.ResponseWriter, r *http.Request, req ContainerRequest) {
	if req.ID == "" && req.Name == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ContainerResponse{Error: "id or name is required"})
		return
	}

	adapter, err := ResolveAdapter(req)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error()})
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	setCORS(w, r)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher.Flush()

	target := req.ID
	if target == "" {
		target = req.Name
	}

	ctx := r.Context()

	// Docker/Podman adapters use the streaming REST API
	if da, ok := adapter.(*DockerAdapter); ok {
		streamDockerStats(ctx, da, target, w, flusher)
		return
	}

	// CLI-based adapters: poll at 2-second intervals
	pollContainerStats(ctx, adapter, target, w, flusher)
}

// ── Docker/Podman streaming stats ──────────────────────────────────────────

// streamDockerStats connects to the Docker stats API which streams NDJSON
// and converts each snapshot to an SSE event.
func streamDockerStats(ctx context.Context, da *DockerAdapter, id string, w http.ResponseWriter, flusher http.Flusher) {
	statsCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	url := da.apiURL(fmt.Sprintf("/containers/%s/stats?stream=true&no-trunc=false", id))
	req, err := http.NewRequestWithContext(statsCtx, http.MethodGet, url, nil)
	if err != nil {
		writeSSEError(w, flusher, err.Error())
		return
	}

	resp, err := da.client.Do(req)
	if err != nil {
		writeSSEError(w, flusher, err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		writeSSEError(w, flusher, fmt.Sprintf("stats API error: %d", resp.StatusCode))
		return
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 256*1024)

	var prevCPU, prevSystem uint64

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var raw dockerStatsJSON
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			continue
		}

		stats := normalizeDockerStats(raw, &prevCPU, &prevSystem)
		b, _ := json.Marshal(stats)
		_, _ = w.Write([]byte("data: " + string(b) + "\n\n"))
		flusher.Flush()
	}
}

// ── CLI polling stats ──────────────────────────────────────────────────────

// pollContainerStats periodically runs the adapter's CLI stats command and
// streams results as SSE events. Used for nerdctl, crictl, etc.
func pollContainerStats(ctx context.Context, adapter ContainerAdapter, id string, w http.ResponseWriter, flusher http.Flusher) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Send an initial stats snapshot immediately
	if stats := cliStats(adapter, id); stats != nil {
		b, _ := json.Marshal(stats)
		_, _ = w.Write([]byte("data: " + string(b) + "\n\n"))
		flusher.Flush()
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats := cliStats(adapter, id)
			if stats == nil {
				continue
			}
			b, _ := json.Marshal(stats)
			_, _ = w.Write([]byte("data: " + string(b) + "\n\n"))
			flusher.Flush()
		}
	}
}

// cliStats fetches a single stats snapshot via the adapter. Returns nil if
// stats are not available. This works for nerdctl adapters that support
// `nerdctl stats --no-stream --format json`.
func cliStats(adapter ContainerAdapter, id string) *ContainerStats {
	if na, ok := adapter.(*NerdctlAdapter); ok {
		return nerdctlOneStats(na, id)
	}
	// crictl and buildah don't have per-container stats
	return nil
}

func nerdctlOneStats(n *NerdctlAdapter, id string) *ContainerStats {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	lines, err := n.runJSONLines(ctx, "stats", "--no-stream", "--format", "json", id)
	if err != nil || len(lines) == 0 {
		return nil
	}

	obj := lines[0]
	return &ContainerStats{
		ID:          truncateID(getStr(obj, "ID")),
		Name:        getStr(obj, "Name"),
		CPUPercent:  parsePercent(getStr(obj, "CPUPerc")),
		MemoryUsage: parseByteSize(getStr(obj, "MemUsage")),
		MemoryLimit: parseByteSize(getStr(obj, "MemLimit")),
		NetInput:    parseByteSize(getStr(obj, "NetInput")),
		NetOutput:   parseByteSize(getStr(obj, "NetOutput")),
		BlockInput:  parseByteSize(getStr(obj, "BlockInput")),
		BlockOutput: parseByteSize(getStr(obj, "BlockOutput")),
		PIDs:        getInt(obj, "PIDs"),
		Timestamp:   time.Now().UnixMilli(),
	}
}

// ── Docker stats JSON model ────────────────────────────────────────────────

type dockerStatsJSON struct {
	ID   string `json:"id"`
	Name string `json:"name"`

	CPUStats struct {
		CPUUsage struct {
			TotalUsage uint64 `json:"total_usage"`
		} `json:"cpu_usage"`
		SystemCPUUsage uint64 `json:"system_cpu_usage"`
		OnlineCPUs     int    `json:"online_cpus"`
	} `json:"cpu_stats"`

	PreCPUStats struct {
		CPUUsage struct {
			TotalUsage uint64 `json:"total_usage"`
		} `json:"cpu_usage"`
		SystemCPUUsage uint64 `json:"system_cpu_usage"`
		OnlineCPUs     int    `json:"online_cpus"`
	} `json:"precpu_stats"`

	MemoryStats struct {
		Usage int64 `json:"usage"`
		Limit int64 `json:"limit"`
	} `json:"memory_stats"`

	Networks map[string]struct {
		RxBytes uint64 `json:"rx_bytes"`
		TxBytes uint64 `json:"tx_bytes"`
	} `json:"networks"`

	BlkioStats struct {
		IOServiceBytesRecursive []struct {
			Op    string `json:"op"`
			Value uint64 `json:"value"`
		} `json:"io_service_bytes_recursive"`
	} `json:"blkio_stats"`

	PidsStats struct {
		Current int `json:"current"`
	} `json:"pids_stats"`
}

func normalizeDockerStats(raw dockerStatsJSON, prevCPU, prevSystem *uint64) ContainerStats {
	// CPU percentage: delta(container CPU) / delta(system CPU) * numCPUs * 100
	cpuPercent := 0.0
	cpuDelta := float64(raw.CPUStats.CPUUsage.TotalUsage - raw.PreCPUStats.CPUUsage.TotalUsage)
	sysDelta := float64(raw.CPUStats.SystemCPUUsage - raw.PreCPUStats.SystemCPUUsage)
	numCPUs := raw.CPUStats.OnlineCPUs
	if numCPUs == 0 {
		numCPUs = 1
	}
	if sysDelta > 0 && cpuDelta >= 0 {
		cpuPercent = (cpuDelta / sysDelta) * float64(numCPUs) * 100.0
	}

	*prevCPU = raw.CPUStats.CPUUsage.TotalUsage
	*prevSystem = raw.CPUStats.SystemCPUUsage

	// Memory
	memPercent := 0.0
	if raw.MemoryStats.Limit > 0 {
		memPercent = float64(raw.MemoryStats.Usage) / float64(raw.MemoryStats.Limit) * 100.0
	}

	// Network I/O (sum across all interfaces)
	var netIn, netOut int64
	for _, iface := range raw.Networks {
		netIn += int64(iface.RxBytes)
		netOut += int64(iface.TxBytes)
	}

	// Block I/O
	var blkIn, blkOut int64
	for _, entry := range raw.BlkioStats.IOServiceBytesRecursive {
		switch entry.Op {
		case "Read", "read":
			blkIn += int64(entry.Value)
		case "Write", "write":
			blkOut += int64(entry.Value)
		}
	}

	return ContainerStats{
		ID:            truncateID(raw.ID),
		Name:          strings.TrimPrefix(raw.Name, "/"),
		CPUPercent:    cpuPercent,
		MemoryUsage:   raw.MemoryStats.Usage,
		MemoryLimit:   raw.MemoryStats.Limit,
		MemoryPercent: memPercent,
		NetInput:      netIn,
		NetOutput:     netOut,
		BlockInput:    blkIn,
		BlockOutput:   blkOut,
		PIDs:          raw.PidsStats.Current,
		Timestamp:     time.Now().UnixMilli(),
	}
}

// ── Helpers ────────────────────────────────────────────────────────────────

func writeSSEError(w http.ResponseWriter, flusher http.Flusher, msg string) {
	b, _ := json.Marshal(map[string]string{"error": msg})
	_, _ = w.Write([]byte("data: " + string(b) + "\n\n"))
	flusher.Flush()
}

// parsePercent converts a string like "12.34%" to 12.34.
func parsePercent(s string) float64 {
	s = strings.TrimSuffix(strings.TrimSpace(s), "%")
	var f float64
	fmt.Sscanf(s, "%f", &f)
	return f
}

// parseByteSize extracts the numeric byte value from strings like "1.5GiB / 7.7GiB"
// (takes the first number), or "1024" (plain number).
func parseByteSize(s string) int64 {
	s = strings.TrimSpace(s)
	// Take only the portion before " / " if present
	if idx := strings.Index(s, " / "); idx >= 0 {
		s = s[:idx]
	}
	s = strings.TrimSpace(s)
	if s == "" || s == "--" {
		return 0
	}

	var value float64
	var unit string
	fmt.Sscanf(s, "%f%s", &value, &unit)

	unit = strings.ToUpper(strings.TrimSpace(unit))
	switch unit {
	case "KIB", "KB", "K":
		return int64(value * 1024)
	case "MIB", "MB", "M":
		return int64(value * 1024 * 1024)
	case "GIB", "GB", "G":
		return int64(value * 1024 * 1024 * 1024)
	case "TIB", "TB", "T":
		return int64(value * 1024 * 1024 * 1024 * 1024)
	case "B", "":
		return int64(value)
	default:
		return int64(value)
	}
}
