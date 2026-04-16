package proxycore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// ── Pod gateway handlers ───────────────────────────────────────────────────
//
// Pod management is only available on runtimes that support native pods:
//   - Podman: via the /libpod/pods/ REST API
//   - crictl (CRI-O): via the crictl CLI (pods, inspectp, stopp, rmp)
//
// Other runtimes return "pods not supported by <runtime>".

// handlePodList lists pods for the resolved runtime.
func (s *Server) handlePodList(w http.ResponseWriter, req ContainerRequest) {
	t0 := time.Now()

	adapter, err := ResolveAdapter(req)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	podAdapter, ok := adapter.(PodCapable)
	if !ok {
		json.NewEncoder(w).Encode(ContainerResponse{
			Error:      "pods not supported by " + adapter.Name(),
			DurationMs: ms(t0),
		})
		return
	}

	pods, err := podAdapter.ListPods(req.Filters)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{Pods: pods, DurationMs: ms(t0)})
}

// handlePodInspect inspects a single pod.
func (s *Server) handlePodInspect(w http.ResponseWriter, req ContainerRequest) {
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

	podAdapter, ok := adapter.(PodCapable)
	if !ok {
		json.NewEncoder(w).Encode(ContainerResponse{
			Error:      "pods not supported by " + adapter.Name(),
			DurationMs: ms(t0),
		})
		return
	}

	target := req.ID
	if target == "" {
		target = req.Name
	}

	detail, err := podAdapter.InspectPod(target)
	if err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{Pod: detail, DurationMs: ms(t0)})
}

// handlePodAction handles pod lifecycle actions (start, stop, restart, remove, pause, unpause).
func (s *Server) handlePodAction(w http.ResponseWriter, req ContainerRequest, action func(PodCapable, string) error) {
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

	podAdapter, ok := adapter.(PodCapable)
	if !ok {
		json.NewEncoder(w).Encode(ContainerResponse{
			Error:      "pods not supported by " + adapter.Name(),
			DurationMs: ms(t0),
		})
		return
	}

	target := req.ID
	if target == "" {
		target = req.Name
	}

	if err := action(podAdapter, target); err != nil {
		json.NewEncoder(w).Encode(ContainerResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(ContainerResponse{OK: true, DurationMs: ms(t0)})
}

// ── Podman pod adapter (DockerAdapter, runtimeName == "podman") ────────────
//
// Podman exposes pods via the libpod API:
//   GET  /libpod/pods/json          — list pods
//   GET  /libpod/pods/{name}/json   — inspect pod
//   POST /libpod/pods/{name}/start  — start pod
//   POST /libpod/pods/{name}/stop   — stop pod
//   POST /libpod/pods/{name}/restart — restart pod
//   POST /libpod/pods/{name}/pause  — pause pod
//   POST /libpod/pods/{name}/unpause — unpause pod
//   DELETE /libpod/pods/{name}      — remove pod

func (d *DockerAdapter) ListPods(filters map[string]string) ([]PodInfo, error) {
	if d.runtimeName != "podman" {
		return nil, &containerError{msg: "pods not supported by " + d.runtimeName}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	path := "/libpod/pods/json"
	if len(filters) > 0 {
		filterMap := map[string][]string{}
		for k, v := range filters {
			filterMap[k] = []string{v}
		}
		fb, _ := json.Marshal(filterMap)
		path += "?filters=" + string(fb)
	}

	body, err := d.get(ctx, path)
	if err != nil {
		return nil, err
	}

	var raw []podmanPodJSON
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	pods := make([]PodInfo, 0, len(raw))
	for _, p := range raw {
		pods = append(pods, d.normalizePod(p))
	}
	return pods, nil
}

func (d *DockerAdapter) InspectPod(id string) (*PodDetail, error) {
	if d.runtimeName != "podman" {
		return nil, &containerError{msg: "pods not supported by " + d.runtimeName}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	body, err := d.get(ctx, "/libpod/pods/"+id+"/json")
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, err
	}

	return d.normalizePodDetail(raw), nil
}

func (d *DockerAdapter) StartPod(id string) error {
	if d.runtimeName != "podman" {
		return &containerError{msg: "pods not supported by " + d.runtimeName}
	}
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := d.post(ctx, "/libpod/pods/"+id+"/start", nil)
	return err
}

func (d *DockerAdapter) StopPod(id string, timeout int) error {
	if d.runtimeName != "podman" {
		return &containerError{msg: "pods not supported by " + d.runtimeName}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout+10)*time.Second)
	defer cancel()
	path := fmt.Sprintf("/libpod/pods/%s/stop?t=%d", id, timeout)
	_, err := d.post(ctx, path, nil)
	return err
}

func (d *DockerAdapter) RestartPod(id string) error {
	if d.runtimeName != "podman" {
		return &containerError{msg: "pods not supported by " + d.runtimeName}
	}
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := d.post(ctx, "/libpod/pods/"+id+"/restart", nil)
	return err
}

func (d *DockerAdapter) RemovePod(id string, force bool) error {
	if d.runtimeName != "podman" {
		return &containerError{msg: "pods not supported by " + d.runtimeName}
	}
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	path := fmt.Sprintf("/libpod/pods/%s?force=%t", id, force)
	_, err := d.delete(ctx, path)
	return err
}

func (d *DockerAdapter) PausePod(id string) error {
	if d.runtimeName != "podman" {
		return &containerError{msg: "pods not supported by " + d.runtimeName}
	}
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := d.post(ctx, "/libpod/pods/"+id+"/pause", nil)
	return err
}

func (d *DockerAdapter) UnpausePod(id string) error {
	if d.runtimeName != "podman" {
		return &containerError{msg: "pods not supported by " + d.runtimeName}
	}
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	_, err := d.post(ctx, "/libpod/pods/"+id+"/unpause", nil)
	return err
}

// ── Podman pod JSON models ────────────────────────────────────────────────

type podmanPodJSON struct {
	Cgroup     string `json:"Cgroup"`
	Containers []struct {
		ID     string `json:"Id"`
		Name   string `json:"Name"`
		Status string `json:"Status"`
	} `json:"Containers"`
	Created  time.Time         `json:"Created"`
	ID       string            `json:"Id"`
	InfraID  string            `json:"InfraId"`
	Labels   map[string]string `json:"Labels"`
	Name     string            `json:"Name"`
	Networks []string          `json:"Networks"`
	Status   string            `json:"Status"`
}

// ── Podman pod normalization ──────────────────────────────────────────────

func (d *DockerAdapter) normalizePod(p podmanPodJSON) PodInfo {
	containers := make([]PodContainerInfo, 0, len(p.Containers))
	running, paused, stopped := 0, 0, 0
	for _, c := range p.Containers {
		status := strings.ToLower(c.Status)
		containers = append(containers, PodContainerInfo{
			ID:     truncateID(c.ID),
			Name:   c.Name,
			Status: status,
		})
		switch status {
		case "running":
			running++
		case "paused":
			paused++
		default:
			stopped++
		}
	}

	return PodInfo{
		ID:         truncateID(p.ID),
		Name:       p.Name,
		Status:     p.Status,
		Created:    p.Created,
		Labels:     p.Labels,
		Containers: containers,
		Running:    running,
		Paused:     paused,
		Stopped:    stopped,
		Total:      len(p.Containers),
		Runtime:    d.runtimeName,
		InfraID:    truncateID(p.InfraID),
		Networks:   p.Networks,
	}
}

func (d *DockerAdapter) normalizePodDetail(raw map[string]any) *PodDetail {
	detail := &PodDetail{Raw: raw}
	detail.Runtime = d.runtimeName

	detail.ID = truncateID(getStr(raw, "Id"))
	detail.Name = getStr(raw, "Name")
	detail.Status = getStr(raw, "State")
	detail.Hostname = getStr(raw, "Hostname")
	detail.CgroupPath = getStr(raw, "CgroupPath")
	detail.Labels = getStrMap(raw, "Labels")
	detail.InfraID = truncateID(getStr(raw, "InfraContainerId"))
	detail.Total = getInt(raw, "NumContainers")

	if created, err := time.Parse(time.RFC3339Nano, getStr(raw, "Created")); err == nil {
		detail.Created = created
	}

	if ns, ok := raw["SharedNamespaces"].([]any); ok {
		for _, n := range ns {
			if s, ok := n.(string); ok {
				detail.SharedNamespaces = append(detail.SharedNamespaces, s)
			}
		}
	}

	// Parse containers list
	if ctrs, ok := raw["Containers"].([]any); ok {
		running, paused, stopped := 0, 0, 0
		for _, c := range ctrs {
			if ctr, ok := c.(map[string]any); ok {
				status := strings.ToLower(getStr(ctr, "State"))
				detail.Containers = append(detail.Containers, PodContainerInfo{
					ID:     truncateID(getStr(ctr, "Id")),
					Name:   getStr(ctr, "Name"),
					Status: status,
				})
				switch status {
				case "running":
					running++
				case "paused":
					paused++
				default:
					stopped++
				}
			}
		}
		detail.Running = running
		detail.Paused = paused
		detail.Stopped = stopped
		if detail.Total == 0 {
			detail.Total = len(detail.Containers)
		}
	}

	return detail
}

// ── crictl pod adapter (CRI-O pod sandboxes) ──────────────────────────────
//
// crictl exposes pod sandboxes via:
//   crictl pods -o json                      — list pods
//   crictl inspectp <pod-id> -o json          — inspect pod
//   crictl stopp <pod-id>                     — stop pod
//   crictl rmp <pod-id>                       — remove pod
//
// crictl does not support start, pause, or unpause for existing pods.

func (c *CrictlAdapter) ListPods(filters map[string]string) ([]PodInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	args := []string{"pods", "-o", "json"}
	if name, ok := filters["name"]; ok {
		args = append(args, "--name", name)
	}
	if ns, ok := filters["namespace"]; ok {
		args = append(args, "--namespace", ns)
	}
	if state, ok := filters["state"]; ok {
		args = append(args, "--state", state)
	}

	out, err := c.run(ctx, args...)
	if err != nil {
		return nil, err
	}

	var raw struct {
		Items []crictlPodJSON `json:"items"`
	}
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}

	// Also list containers to map pod→container relationships
	containersByPod := map[string][]PodContainerInfo{}
	ctrOut, ctrErr := c.run(ctx, "ps", "-a", "-o", "json")
	if ctrErr == nil {
		var ctrRaw struct {
			Containers []crictlContainerJSON `json:"containers"`
		}
		if json.Unmarshal(ctrOut, &ctrRaw) == nil {
			for _, ct := range ctrRaw.Containers {
				podID := ct.PodSandboxID
				if podID == "" {
					continue
				}
				containersByPod[podID] = append(containersByPod[podID], PodContainerInfo{
					ID:     truncateID(ct.ID),
					Name:   ct.Metadata.Name,
					Status: crictlStateToString(ct.State),
				})
			}
		}
	}

	pods := make([]PodInfo, 0, len(raw.Items))
	for _, p := range raw.Items {
		pod := c.normalizePodSandbox(p)
		// Attach containers belonging to this pod (match on full ID prefix)
		for fullID, ctrs := range containersByPod {
			if strings.HasPrefix(fullID, p.ID) || strings.HasPrefix(p.ID, fullID) {
				pod.Containers = ctrs
				for _, ctr := range ctrs {
					switch ctr.Status {
					case "running":
						pod.Running++
					default:
						pod.Stopped++
					}
				}
				pod.Total = len(ctrs)
				break
			}
		}
		pods = append(pods, pod)
	}
	return pods, nil
}

func (c *CrictlAdapter) InspectPod(id string) (*PodDetail, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := c.run(ctx, "inspectp", id, "-o", "json")
	if err != nil {
		return nil, err
	}

	var raw map[string]any
	if err := json.Unmarshal(out, &raw); err != nil {
		return nil, err
	}

	detail := &PodDetail{Raw: raw}
	detail.Runtime = "crictl"

	if status, ok := raw["status"].(map[string]any); ok {
		detail.ID = truncateID(getStr(status, "id"))
		detail.Status = crictlPodStateToString(getStr(status, "state"))

		if meta, ok := status["metadata"].(map[string]any); ok {
			detail.Name = getStr(meta, "name")
			detail.Namespace = getStr(meta, "namespace")
		}

		if createdAt := getStr(status, "createdAt"); createdAt != "" {
			if t, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
				detail.Created = t
			}
		}

		if linux, ok := status["linux"].(map[string]any); ok {
			if ns, ok := linux["namespaces"].(map[string]any); ok {
				if opts, ok := ns["options"].(map[string]any); ok {
					for k := range opts {
						detail.SharedNamespaces = append(detail.SharedNamespaces, k)
					}
				}
			}
		}

		detail.Labels = getStrMap(status, "labels")
	}

	// Fetch containers belonging to this pod
	ctrOut, ctrErr := c.run(ctx, "ps", "-a", "--pod", id, "-o", "json")
	if ctrErr == nil {
		var ctrRaw struct {
			Containers []crictlContainerJSON `json:"containers"`
		}
		if json.Unmarshal(ctrOut, &ctrRaw) == nil {
			for _, ct := range ctrRaw.Containers {
				status := crictlStateToString(ct.State)
				detail.Containers = append(detail.Containers, PodContainerInfo{
					ID:     truncateID(ct.ID),
					Name:   ct.Metadata.Name,
					Status: status,
				})
				switch status {
				case "running":
					detail.Running++
				default:
					detail.Stopped++
				}
			}
			detail.Total = len(detail.Containers)
		}
	}

	return detail, nil
}

func (c *CrictlAdapter) StartPod(_ string) error {
	return &containerError{msg: "crictl does not support starting an existing pod sandbox"}
}

func (c *CrictlAdapter) StopPod(id string, timeout int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout+10)*time.Second)
	defer cancel()
	_, err := c.run(ctx, "stopp", id)
	return err
}

func (c *CrictlAdapter) RestartPod(id string) error {
	// crictl has no pod restart; stop then note that start is not supported
	return &containerError{msg: "crictl does not support restarting a pod sandbox"}
}

func (c *CrictlAdapter) RemovePod(id string, force bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), containerTimeout)
	defer cancel()
	args := []string{"rmp", id}
	if force {
		args = []string{"rmp", "-f", id}
	}
	_, err := c.run(ctx, args...)
	return err
}

func (c *CrictlAdapter) PausePod(_ string) error {
	return &containerError{msg: "crictl does not support pod pause"}
}

func (c *CrictlAdapter) UnpausePod(_ string) error {
	return &containerError{msg: "crictl does not support pod unpause"}
}

// ── crictl pod JSON models ────────────────────────────────────────────────

type crictlPodJSON struct {
	ID       string `json:"id"`
	Metadata struct {
		Name      string `json:"name"`
		UID       string `json:"uid"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	State       string            `json:"state"`
	CreatedAt   string            `json:"createdAt"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
}

// ── crictl pod normalization ──────────────────────────────────────────────

func (c *CrictlAdapter) normalizePodSandbox(p crictlPodJSON) PodInfo {
	created := time.Time{}
	if p.CreatedAt != "" {
		if t, err := time.Parse(time.RFC3339Nano, p.CreatedAt); err == nil {
			created = t
		}
	}

	return PodInfo{
		ID:        truncateID(p.ID),
		Name:      p.Metadata.Name,
		Status:    crictlPodStateToString(p.State),
		Created:   created,
		Labels:    p.Labels,
		Runtime:   "crictl",
		Namespace: p.Metadata.Namespace,
	}
}

func crictlPodStateToString(state string) string {
	switch strings.ToUpper(state) {
	case "SANDBOX_READY":
		return "Running"
	case "SANDBOX_NOTREADY":
		return "Stopped"
	default:
		return strings.ToLower(state)
	}
}
