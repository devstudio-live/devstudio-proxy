package proxycore

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// ── Multi-runtime unified view (Phase 4C) ──────────────────────────────────
//
// These handlers query ALL detected runtimes in parallel and merge results.
// Each item already carries a Runtime field set by its adapter.

// MultiRuntimeSummary provides per-runtime counts for the summary endpoint.
type MultiRuntimeSummary struct {
	Runtime    string `json:"runtime"`
	Containers int    `json:"containers"`
	Running    int    `json:"running"`
	Images     int    `json:"images"`
	Volumes    int    `json:"volumes"`
	Networks   int    `json:"networks"`
	Error      string `json:"error,omitempty"`
}

// MultiSummaryResponse is the response for multi/summary.
type MultiSummaryResponse struct {
	Summaries  []MultiRuntimeSummary `json:"summaries"`
	DurationMs float64               `json:"durationMs"`
	Error      string                `json:"error,omitempty"`
}

// resolveAllAdapters returns an adapter for each available runtime.
// For SSH/TCP modes, it detects runtimes on the remote host and creates adapters for each.
// For local mode, it uses the local detection cache.
func (s *Server) resolveAllAdapters(req ContainerRequest) []namedAdapter {
	var runtimes []RuntimeInfo

	switch req.ConnectionMode {
	case "ssh":
		if req.SSHConnection == nil {
			return nil
		}
		sshClient, err := s.getPooledSSHClient(*req.SSHConnection)
		if err != nil {
			return nil
		}
		runtimes = DetectRemoteRuntimes(sshClient)
	case "remote-tcp":
		if req.RemoteHost == "" {
			return nil
		}
		tlsConfig, err := buildTLSConfig(req.TLSCACert, req.TLSCert, req.TLSKey)
		if err != nil {
			return nil
		}
		hostAddr := parseTCPHost(req.RemoteHost, tlsConfig != nil)
		runtimes = DetectTCPRuntime(hostAddr, tlsConfig)
	default:
		runtimes = DetectRuntimes()
	}

	var adapters []namedAdapter
	seen := map[string]bool{}
	for _, rt := range runtimes {
		if !rt.Available || seen[rt.Name] {
			continue
		}
		seen[rt.Name] = true

		rReq := req
		rReq.Runtime = rt.Name
		rReq.SocketPath = rt.SocketPath

		adapter, err := s.resolveContainerAdapter(rReq)
		if err != nil {
			continue
		}
		adapters = append(adapters, namedAdapter{name: rt.Name, adapter: adapter})
	}
	return adapters
}

type namedAdapter struct {
	name    string
	adapter ContainerAdapter
}

// ── /multi/containers — containers across all runtimes ─────────────────────

func (s *Server) handleMultiContainers(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	adapters := s.resolveAllAdapters(req)
	if len(adapters) == 0 {
		json.NewEncoder(w).Encode(ContainerResponse{
			Error:      "no container runtimes available",
			DurationMs: ms(t0),
		})
		return
	}

	filters := req.Filters
	if req.Resource == "running" {
		if filters == nil {
			filters = map[string]string{}
		}
		filters["status"] = "running"
	}

	type result struct {
		containers []ContainerInfo
		err        error
	}
	results := make([]result, len(adapters))

	var wg sync.WaitGroup
	for i, na := range adapters {
		wg.Add(1)
		go func(idx int, a ContainerAdapter) {
			defer wg.Done()
			c, e := a.ListContainers(filters)
			results[idx] = result{containers: c, err: e}
		}(i, na.adapter)
	}
	wg.Wait()

	var all []ContainerInfo
	for _, r := range results {
		if r.err == nil {
			all = append(all, r.containers...)
		}
	}

	json.NewEncoder(w).Encode(ContainerResponse{Containers: all, DurationMs: ms(t0)})
}

// ── /multi/images — images across all runtimes ─────────────────────────────

func (s *Server) handleMultiImages(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	adapters := s.resolveAllAdapters(req)
	if len(adapters) == 0 {
		json.NewEncoder(w).Encode(ContainerResponse{
			Error:      "no container runtimes available",
			DurationMs: ms(t0),
		})
		return
	}

	filters := req.Filters
	if req.Resource == "dangling" {
		if filters == nil {
			filters = map[string]string{}
		}
		filters["dangling"] = "true"
	}

	type result struct {
		images []ImageInfo
		err    error
	}
	results := make([]result, len(adapters))

	var wg sync.WaitGroup
	for i, na := range adapters {
		wg.Add(1)
		go func(idx int, a ContainerAdapter) {
			defer wg.Done()
			imgs, e := a.ListImages(filters)
			results[idx] = result{images: imgs, err: e}
		}(i, na.adapter)
	}
	wg.Wait()

	var all []ImageInfo
	for _, r := range results {
		if r.err == nil {
			all = append(all, r.images...)
		}
	}

	json.NewEncoder(w).Encode(ContainerResponse{Images: all, DurationMs: ms(t0)})
}

// ── /multi/volumes — volumes across all runtimes ───────────────────────────

func (s *Server) handleMultiVolumes(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	adapters := s.resolveAllAdapters(req)
	if len(adapters) == 0 {
		json.NewEncoder(w).Encode(ContainerResponse{
			Error:      "no container runtimes available",
			DurationMs: ms(t0),
		})
		return
	}

	type result struct {
		volumes []VolumeInfo
		err     error
	}
	results := make([]result, len(adapters))

	var wg sync.WaitGroup
	for i, na := range adapters {
		wg.Add(1)
		go func(idx int, a ContainerAdapter) {
			defer wg.Done()
			v, e := a.ListVolumes()
			results[idx] = result{volumes: v, err: e}
		}(i, na.adapter)
	}
	wg.Wait()

	var all []VolumeInfo
	for _, r := range results {
		if r.err == nil {
			all = append(all, r.volumes...)
		}
	}

	json.NewEncoder(w).Encode(ContainerResponse{Volumes: all, DurationMs: ms(t0)})
}

// ── /multi/networks — networks across all runtimes ─────────────────────────

func (s *Server) handleMultiNetworks(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	adapters := s.resolveAllAdapters(req)
	if len(adapters) == 0 {
		json.NewEncoder(w).Encode(ContainerResponse{
			Error:      "no container runtimes available",
			DurationMs: ms(t0),
		})
		return
	}

	type result struct {
		networks []NetworkInfo
		err      error
	}
	results := make([]result, len(adapters))

	var wg sync.WaitGroup
	for i, na := range adapters {
		wg.Add(1)
		go func(idx int, a ContainerAdapter) {
			defer wg.Done()
			n, e := a.ListNetworks()
			results[idx] = result{networks: n, err: e}
		}(i, na.adapter)
	}
	wg.Wait()

	var all []NetworkInfo
	for _, r := range results {
		if r.err == nil {
			all = append(all, r.networks...)
		}
	}

	json.NewEncoder(w).Encode(ContainerResponse{Networks: all, DurationMs: ms(t0)})
}

// ── /multi/summary — per-runtime counts ────────────────────────────────────

func (s *Server) handleMultiSummary(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	adapters := s.resolveAllAdapters(req)
	if len(adapters) == 0 {
		json.NewEncoder(w).Encode(MultiSummaryResponse{
			Error:      "no container runtimes available",
			DurationMs: ms(t0),
		})
		return
	}

	summaries := make([]MultiRuntimeSummary, len(adapters))

	var wg sync.WaitGroup
	for i, na := range adapters {
		wg.Add(1)
		go func(idx int, na namedAdapter) {
			defer wg.Done()
			sum := MultiRuntimeSummary{Runtime: na.name}

			// Prefer SystemInfo for container counts (lighter than listing).
			if si, err := na.adapter.SystemInfo(); err == nil && si != nil {
				sum.Containers = si.Containers
				sum.Running = si.Running
				sum.Images = si.Images
			} else {
				// Fallback: list containers + images.
				if containers, err := na.adapter.ListContainers(nil); err == nil {
					sum.Containers = len(containers)
					for _, c := range containers {
						if c.State == "running" {
							sum.Running++
						}
					}
				} else {
					sum.Error = err.Error()
				}
				if images, err := na.adapter.ListImages(nil); err == nil {
					sum.Images = len(images)
				}
			}

			if volumes, err := na.adapter.ListVolumes(); err == nil {
				sum.Volumes = len(volumes)
			}
			if networks, err := na.adapter.ListNetworks(); err == nil {
				sum.Networks = len(networks)
			}

			summaries[idx] = sum
		}(i, na)
	}
	wg.Wait()

	json.NewEncoder(w).Encode(MultiSummaryResponse{
		Summaries:  summaries,
		DurationMs: ms(t0),
	})
}
