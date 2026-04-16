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
	// ── Read operations (Phase 1A) ──────────────────────────────
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

	// ── Write operations — container lifecycle (Phase 2A) ───────
	case "container/start":
		s.handleContainerAction(w, req, func(a ContainerAdapter, id string) error {
			return a.StartContainer(id)
		})
	case "container/stop":
		s.handleContainerAction(w, req, func(a ContainerAdapter, id string) error {
			timeout := req.Timeout
			if timeout <= 0 {
				timeout = 10
			}
			return a.StopContainer(id, timeout)
		})
	case "container/restart":
		s.handleContainerAction(w, req, func(a ContainerAdapter, id string) error {
			timeout := req.Timeout
			if timeout <= 0 {
				timeout = 10
			}
			return a.RestartContainer(id, timeout)
		})
	case "container/remove":
		s.handleContainerAction(w, req, func(a ContainerAdapter, id string) error {
			return a.RemoveContainer(id, req.Force)
		})
	case "container/pause":
		s.handleContainerAction(w, req, func(a ContainerAdapter, id string) error {
			return a.PauseContainer(id)
		})
	case "container/unpause":
		s.handleContainerAction(w, req, func(a ContainerAdapter, id string) error {
			return a.UnpauseContainer(id)
		})

	// ── Write operations — images (Phase 2A) ────────────────────
	case "image/pull":
		s.handleImagePull(w, req)
	case "image/remove":
		s.handleImageRemove(w, req)
	case "image/prune":
		s.handleImagePrune(w, req)
	case "image/tag":
		s.handleImageTag(w, req)

	// ── Write operations — volumes (Phase 2A) ───────────────────
	case "volume/create":
		s.handleVolumeCreate(w, req)
	case "volume/remove":
		s.handleVolumeRemove(w, req)
	case "volume/prune":
		s.handleVolumePrune(w, req)

	// ── Write operations — networks (Phase 2A) ──────────────────
	case "network/create":
		s.handleNetworkCreate(w, req)
	case "network/remove":
		s.handleNetworkRemove(w, req)
	case "network/prune":
		s.handleNetworkPrune(w, req)

	// ── Stats streaming (Phase 2A) ──────────────────────────────
	case "container/stats":
		s.handleContainerStats(w, r, req)

	// ── Logs (Phase 2B) ─────────────────────────────────────────
	case "container/logs":
		s.handleContainerLogs(w, req)
	case "container/logs/stream":
		s.handleContainerLogStream(w, r, req)

	// ── Compose (Phase 2C) ──────────────────────────────────────
	case "compose":
		s.handleComposeList(w, req)
	case "compose/file":
		s.handleComposeFile(w, req)
	case "compose/up":
		s.handleComposeAction(w, req, "up")
	case "compose/down":
		s.handleComposeAction(w, req, "down")
	case "compose/restart":
		s.handleComposeAction(w, req, "restart")
	case "compose/stop":
		s.handleComposeAction(w, req, "stop")
	case "compose/start":
		s.handleComposeAction(w, req, "start")

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

// ── Phase 2A: Write operation handlers ─────────────────────────────────────

// handleContainerAction is a generic handler for container lifecycle actions
// (start, stop, restart, remove, pause, unpause).
func (s *Server) handleContainerAction(w http.ResponseWriter, req ContainerRequest, action func(ContainerAdapter, string) error) {
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

	if err := action(adapter, target); err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{OK: true, DurationMs: ms(t0)})
}

// ── Image write handlers ───────────────────────────────────────────────────

func (s *Server) handleImagePull(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	if req.Ref == "" {
		json.NewEncoder(w).Encode(ContainerResponse{Error: "ref is required (e.g. nginx:latest)", DurationMs: ms(t0)})
		return
	}

	adapter, err := ResolveAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	if err := adapter.PullImage(req.Ref); err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{OK: true, DurationMs: ms(t0)})
}

func (s *Server) handleImageRemove(w http.ResponseWriter, req ContainerRequest) {
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

	if err := adapter.RemoveImage(target, req.Force); err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{OK: true, DurationMs: ms(t0)})
}

func (s *Server) handleImagePrune(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	adapter, err := ResolveAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	result, err := adapter.PruneImages(req.Dangling)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{Prune: result, OK: true, DurationMs: ms(t0)})
}

func (s *Server) handleImageTag(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	source := req.ID
	if source == "" {
		source = req.Name
	}
	if source == "" {
		json.NewEncoder(w).Encode(ContainerResponse{Error: "id or name is required (source image)", DurationMs: ms(t0)})
		return
	}
	if req.Target == "" {
		json.NewEncoder(w).Encode(ContainerResponse{Error: "target is required (e.g. myrepo:v2)", DurationMs: ms(t0)})
		return
	}

	adapter, err := ResolveAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	if err := adapter.TagImage(source, req.Target); err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{OK: true, DurationMs: ms(t0)})
}

// ── Volume write handlers ──────────────────────────────────────────────────

func (s *Server) handleVolumeCreate(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	if req.Name == "" {
		json.NewEncoder(w).Encode(ContainerResponse{Error: "name is required", DurationMs: ms(t0)})
		return
	}

	adapter, err := ResolveAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	vol, err := adapter.CreateVolume(req.Name, req.Driver, req.Options)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{Volume: vol, OK: true, DurationMs: ms(t0)})
}

func (s *Server) handleVolumeRemove(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	if req.Name == "" && req.ID == "" {
		json.NewEncoder(w).Encode(ContainerResponse{Error: "name or id is required", DurationMs: ms(t0)})
		return
	}

	adapter, err := ResolveAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	target := req.Name
	if target == "" {
		target = req.ID
	}

	if err := adapter.RemoveVolume(target, req.Force); err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{OK: true, DurationMs: ms(t0)})
}

func (s *Server) handleVolumePrune(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	adapter, err := ResolveAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	result, err := adapter.PruneVolumes()
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{Prune: result, OK: true, DurationMs: ms(t0)})
}

// ── Network write handlers ─────────────────────────────────────────────────

func (s *Server) handleNetworkCreate(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	if req.Name == "" {
		json.NewEncoder(w).Encode(ContainerResponse{Error: "name is required", DurationMs: ms(t0)})
		return
	}

	adapter, err := ResolveAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	net, err := adapter.CreateNetwork(req.Name, req.Driver, req.Options)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{Network: net, OK: true, DurationMs: ms(t0)})
}

func (s *Server) handleNetworkRemove(w http.ResponseWriter, req ContainerRequest) {
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

	if err := adapter.RemoveNetwork(target); err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{OK: true, DurationMs: ms(t0)})
}

func (s *Server) handleNetworkPrune(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	adapter, err := ResolveAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	result, err := adapter.PruneNetworks()
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{Prune: result, OK: true, DurationMs: ms(t0)})
}
