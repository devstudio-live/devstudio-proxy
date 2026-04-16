package proxycore

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"strconv"
	"time"
)

// handleContainerLogs returns container logs as a single response (one-shot).
func (s *Server) handleContainerLogs(w http.ResponseWriter, req ContainerRequest) {
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

	tailLines := req.TailLines
	if tailLines <= 0 {
		tailLines = 200
	}

	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()

	var logs string

	if da, ok := adapter.(*DockerAdapter); ok {
		logs, err = dockerContainerLogs(ctx, da, target, tailLines)
	} else if na, ok := adapter.(*NerdctlAdapter); ok {
		logs, err = nerdctlContainerLogs(ctx, na, target, tailLines)
	} else {
		err = fmt.Errorf("logs not supported for adapter %s", adapter.Name())
	}

	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{Logs: logs, DurationMs: ms(t0)})
}

// handleContainerLogStream streams container logs via SSE (Server-Sent Events).
func (s *Server) handleContainerLogStream(w http.ResponseWriter, r *http.Request, req ContainerRequest) {
	if req.ID == "" && req.Name == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ContainerResponse{Error: "id or name is required"})
		return
	}

	adapter, err := s.resolveContainerAdapter(req)
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

	tailLines := req.TailLines
	if tailLines <= 0 {
		tailLines = 200
	}

	ctx := r.Context()

	if da, ok := adapter.(*DockerAdapter); ok {
		streamDockerLogs(ctx, da, target, tailLines, w, flusher)
		return
	}

	if na, ok := adapter.(*NerdctlAdapter); ok {
		streamNerdctlLogs(ctx, na, target, tailLines, w, flusher)
		return
	}

	writeSSEError(w, flusher, fmt.Sprintf("log streaming not supported for adapter %s", adapter.Name()))
}

// ── Docker/Podman logs ────────────────────────────────────────────────────

func dockerContainerLogs(ctx context.Context, da *DockerAdapter, id string, tailLines int) (string, error) {
	url := da.apiURL(fmt.Sprintf("/containers/%s/logs?stdout=true&stderr=true&tail=%d", id, tailLines))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := da.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("logs API error: %d", resp.StatusCode)
	}

	return readDockerLogStream(resp.Body), nil
}

func streamDockerLogs(ctx context.Context, da *DockerAdapter, id string, tailLines int, w http.ResponseWriter, flusher http.Flusher) {
	logCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	url := da.apiURL(fmt.Sprintf("/containers/%s/logs?stdout=true&stderr=true&follow=true&tail=%d", id, tailLines))
	req, err := http.NewRequestWithContext(logCtx, http.MethodGet, url, nil)
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
		writeSSEError(w, flusher, fmt.Sprintf("logs API error: %d", resp.StatusCode))
		return
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 256*1024)

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		line := stripDockerLogHeader(scanner.Bytes())
		b, _ := json.Marshal(string(line))
		_, _ = w.Write([]byte("data: " + string(b) + "\n\n"))
		flusher.Flush()
	}
}

// ── nerdctl/CLI logs ──────────────────────────────────────────────────────

func nerdctlContainerLogs(ctx context.Context, na *NerdctlAdapter, id string, tailLines int) (string, error) {
	out, err := na.run(ctx, "logs", "--tail", strconv.Itoa(tailLines), id)
	if err != nil {
		return "", err
	}
	return string(out), nil
}

func streamNerdctlLogs(ctx context.Context, na *NerdctlAdapter, id string, tailLines int, w http.ResponseWriter, flusher http.Flusher) {
	var args []string
	if na.namespace != "" {
		args = append(args, "--namespace", na.namespace)
	}
	args = append(args, "logs", "--follow", "--tail", strconv.Itoa(tailLines), id)
	cmd := exec.CommandContext(ctx, na.binaryPath, args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		writeSSEError(w, flusher, err.Error())
		return
	}
	cmd.Stderr = cmd.Stdout // merge stderr into stdout

	if err := cmd.Start(); err != nil {
		writeSSEError(w, flusher, err.Error())
		return
	}

	scanner := bufio.NewScanner(stdout)
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			_ = cmd.Process.Kill()
			return
		default:
		}
		line := scanner.Text()
		b, _ := json.Marshal(line)
		_, _ = w.Write([]byte("data: " + string(b) + "\n\n"))
		flusher.Flush()
	}
	_ = cmd.Wait()
}

// ── Helpers ──────────────────────────────────────────────────────────────

// readDockerLogStream reads a Docker multiplexed log stream (with 8-byte
// header per frame) and returns the concatenated text content.
func readDockerLogStream(r interface{ Read([]byte) (int, error) }) string {
	var result []byte
	header := make([]byte, 8)
	for {
		_, err := fullRead(r, header)
		if err != nil {
			break
		}
		size := int(header[4])<<24 | int(header[5])<<16 | int(header[6])<<8 | int(header[7])
		if size <= 0 || size > 1<<20 {
			break
		}
		payload := make([]byte, size)
		_, err = fullRead(r, payload)
		if err != nil {
			break
		}
		result = append(result, payload...)
	}
	return string(result)
}

// stripDockerLogHeader strips the 8-byte Docker multiplexed log header if present.
func stripDockerLogHeader(line []byte) []byte {
	// Docker log stream frames: 8-byte header where byte[0] is stream type (1=stdout, 2=stderr)
	// and bytes [4:8] are big-endian uint32 payload length.
	// However in line-buffered scanner mode, the header appears at start of each chunk.
	if len(line) > 8 && (line[0] == 1 || line[0] == 2) && line[1] == 0 && line[2] == 0 && line[3] == 0 {
		return line[8:]
	}
	return line
}

func fullRead(r interface{ Read([]byte) (int, error) }, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
