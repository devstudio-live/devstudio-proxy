package proxycore

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

// ── Gateway entry point ─────────────────────────────────────────────────────

const containerTimeout = 30 * time.Second

func (s *Server) handleContainerGateway(w http.ResponseWriter, r *http.Request) {
	r.Header.Del("X-DevStudio-Gateway-Route")
	r.Header.Del("X-DevStudio-Gateway-Protocol")

	setCORS(w, r)
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(ContainerResponse{Error: "only POST is accepted"})
		return
	}

	var req ContainerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(ContainerResponse{Error: "invalid JSON: " + err.Error()})
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/")

	switch path {
	case "detect":
		s.handleContainerDetect(w, req)
	case "containers":
		s.handleContainerList(w, req)
	case "container":
		s.handleContainerInspect(w, req)
	case "images":
		s.handleImageList(w, req)
	case "image":
		s.handleImageInspect(w, req)
	case "volumes":
		s.handleVolumeList(w, req)
	case "networks":
		s.handleNetworkList(w, req)
	case "system":
		s.handleContainerSystemInfo(w, req)
	default:
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(ContainerResponse{Error: "unknown container endpoint: " + path})
	}
}

// ── /detect — list available runtimes ───────────────────────────────────────

func (s *Server) handleContainerDetect(w http.ResponseWriter, _ ContainerRequest) {
	t0 := time.Now()
	runtimes := DetectRuntimes()
	json.NewEncoder(w).Encode(ContainerResponse{Runtimes: runtimes, DurationMs: ms(t0)})
}

// ── /containers — list containers ───────────────────────────────────────────

func (s *Server) handleContainerList(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	adapter, err := ResolveAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	filters := req.Filters
	// Convenience: if resource specifies "running" filter, inject state filter
	if req.Resource == "running" {
		if filters == nil {
			filters = map[string]string{}
		}
		filters["status"] = "running"
	}

	containers, err := adapter.ListContainers(filters)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{Containers: containers, DurationMs: ms(t0)})
}

// ── /container — inspect a single container ─────────────────────────────────

func (s *Server) handleContainerInspect(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	if req.ID == "" && req.Name == "" {
		json.NewEncoder(w).Encode(ContainerResponse{Error: "id or name is required", DurationMs: ms(t0)})
		return
	}

	adapter, err := ResolveAdapter(req)
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

	json.NewEncoder(w).Encode(ContainerResponse{Container: detail, DurationMs: ms(t0)})
}

// ── /images — list images ───────────────────────────────────────────────────

func (s *Server) handleImageList(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	adapter, err := ResolveAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	filters := req.Filters
	// Convenience: if resource specifies "dangling", inject dangling filter
	if req.Resource == "dangling" {
		if filters == nil {
			filters = map[string]string{}
		}
		filters["dangling"] = "true"
	}

	images, err := adapter.ListImages(filters)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{Images: images, DurationMs: ms(t0)})
}

// ── /image — inspect a single image ─────────────────────────────────────────

func (s *Server) handleImageInspect(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	if req.ID == "" && req.Name == "" {
		json.NewEncoder(w).Encode(ContainerResponse{Error: "id or name is required", DurationMs: ms(t0)})
		return
	}

	adapter, err := ResolveAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	target := req.ID
	if target == "" {
		target = req.Name
	}

	detail, err := adapter.InspectImage(target)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{Image: detail, DurationMs: ms(t0)})
}

// ── /volumes — list volumes ─────────────────────────────────────────────────

func (s *Server) handleVolumeList(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	adapter, err := ResolveAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	volumes, err := adapter.ListVolumes()
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{Volumes: volumes, DurationMs: ms(t0)})
}

// ── /networks — list networks ───────────────────────────────────────────────

func (s *Server) handleNetworkList(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	adapter, err := ResolveAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	networks, err := adapter.ListNetworks()
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{Networks: networks, DurationMs: ms(t0)})
}

// ── /system — system info ───────────────────────────────────────────────────

func (s *Server) handleContainerSystemInfo(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	adapter, err := ResolveAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	sysInfo, err := adapter.SystemInfo()
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{System: sysInfo, DurationMs: ms(t0)})
}
