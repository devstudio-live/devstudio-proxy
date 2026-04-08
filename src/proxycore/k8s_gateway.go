package proxycore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"
)

// ── Request / Response ──────────────────────────────────────────────────────

// K8sRequest is the unified request body for all K8s gateway endpoints.
type K8sRequest struct {
	Kubeconfig    string `json:"kubeconfig"`
	Context       string `json:"context,omitempty"`
	Namespace     string `json:"namespace,omitempty"`
	Resource      string `json:"resource,omitempty"`
	Name          string `json:"name,omitempty"`
	Container     string `json:"container,omitempty"`
	Follow        bool   `json:"follow,omitempty"`
	TailLines     int64  `json:"tailLines,omitempty"`
	LabelSelector string `json:"labelSelector,omitempty"`
	FieldSelector string `json:"fieldSelector,omitempty"`
	YAML          string `json:"yaml,omitempty"`
	Limit         int64  `json:"limit,omitempty"`
}

// K8sResponse is the unified response body for all K8s gateway endpoints.
type K8sResponse struct {
	Items      []map[string]any `json:"items,omitempty"`
	Item       map[string]any   `json:"item,omitempty"`
	Contexts   []K8sContext     `json:"contexts,omitempty"`
	Namespaces []string         `json:"namespaces,omitempty"`
	Logs       string           `json:"logs,omitempty"`
	YAML       string           `json:"yaml,omitempty"`
	Events     []map[string]any `json:"events,omitempty"`
	Error      string           `json:"error,omitempty"`
	DurationMs float64          `json:"durationMs"`
	Version    string           `json:"version,omitempty"`
	Resources  []K8sAPIResource `json:"resources,omitempty"`
}

// K8sContext describes a context entry from a kubeconfig file.
type K8sContext struct {
	Name      string `json:"name"`
	Cluster   string `json:"cluster"`
	User      string `json:"user"`
	Namespace string `json:"namespace"`
	Current   bool   `json:"current"`
}

// K8sAPIResource describes an available API resource type.
type K8sAPIResource struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	Namespaced bool   `json:"namespaced"`
	Group      string `json:"group,omitempty"`
	Version    string `json:"version"`
}

// ── Gateway entry point ─────────────────────────────────────────────────────

const k8sTimeout = 30 * time.Second

func (s *Server) handleK8sGateway(w http.ResponseWriter, r *http.Request) {
	r.Header.Del("X-DevStudio-Gateway-Route")
	r.Header.Del("X-DevStudio-Gateway-Protocol")

	setCORS(w, r)
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(K8sResponse{Error: "only POST is accepted"})
		return
	}

	var req K8sRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(K8sResponse{Error: "invalid JSON: " + err.Error()})
		return
	}

	if req.Limit <= 0 {
		req.Limit = 500
	}

	path := strings.TrimPrefix(r.URL.Path, "/")
	switch path {
	case "test":
		s.handleK8sTest(w, r, req)
	case "contexts":
		s.handleK8sContexts(w, r, req)
	case "namespaces":
		s.handleK8sNamespaces(w, r, req)
	case "resources":
		s.handleK8sResources(w, r, req)
	case "resource":
		s.handleK8sResource(w, r, req)
	case "describe":
		s.handleK8sDescribe(w, r, req)
	case "yaml":
		s.handleK8sYAML(w, r, req)
	case "logs":
		s.handleK8sLogs(w, r, req)
	case "logs/stream":
		s.handleK8sLogStream(w, r, req)
	case "events":
		s.handleK8sEvents(w, r, req)
	case "api-resources":
		s.handleK8sAPIResources(w, r, req)
	case "top/pods":
		s.handleK8sTopPods(w, r, req)
	case "top/nodes":
		s.handleK8sTopNodes(w, r, req)
	default:
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(K8sResponse{Error: "unknown k8s endpoint: " + path})
	}
}

// ── /test ───────────────────────────────────────────────────────────────────

func (s *Server) handleK8sTest(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	cs, _, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), k8sTimeout)
	defer cancel()

	info, err := cs.Discovery().ServerVersion()
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}
	json.NewEncoder(w).Encode(K8sResponse{Version: info.GitVersion, DurationMs: ms(t0), Item: map[string]any{"platform": info.Platform, "goVersion": info.GoVersion}})
	_ = ctx
}

// ── /contexts ───────────────────────────────────────────────────────────────

func (s *Server) handleK8sContexts(w http.ResponseWriter, _ *http.Request, req K8sRequest) {
	t0 := time.Now()
	kubeconfigPath := resolveKubeconfig(req.Kubeconfig)
	if kubeconfigPath == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "could not resolve kubeconfig path", DurationMs: ms(t0)})
		return
	}

	cfg, err := clientcmd.LoadFromFile(kubeconfigPath)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "load kubeconfig: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	contexts := make([]K8sContext, 0, len(cfg.Contexts))
	for name, ctx := range cfg.Contexts {
		contexts = append(contexts, K8sContext{
			Name:      name,
			Cluster:   ctx.Cluster,
			User:      ctx.AuthInfo,
			Namespace: ctx.Namespace,
			Current:   name == cfg.CurrentContext,
		})
	}
	json.NewEncoder(w).Encode(K8sResponse{Contexts: contexts, DurationMs: ms(t0)})
}

// ── /namespaces ─────────────────────────────────────────────────────────────

func (s *Server) handleK8sNamespaces(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	cs, _, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), k8sTimeout)
	defer cancel()

	nsList, err := cs.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	names := make([]string, len(nsList.Items))
	for i, ns := range nsList.Items {
		names[i] = ns.Name
	}
	json.NewEncoder(w).Encode(K8sResponse{Namespaces: names, DurationMs: ms(t0)})
}

// ── /resources ──────────────────────────────────────────────────────────────

func (s *Server) handleK8sResources(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	cs, _, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	ns := req.Namespace
	if ns == "" {
		ns = "default"
	}

	ctx, cancel := context.WithTimeout(r.Context(), k8sTimeout)
	defer cancel()

	opts := metav1.ListOptions{}
	if req.LabelSelector != "" {
		opts.LabelSelector = req.LabelSelector
	}
	if req.FieldSelector != "" {
		opts.FieldSelector = req.FieldSelector
	}
	if req.Limit > 0 {
		opts.Limit = req.Limit
	}

	var items []map[string]any

	switch req.Resource {
	case "pods":
		list, err := cs.CoreV1().Pods(ns).List(ctx, opts)
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
		for _, p := range list.Items {
			ready := 0
			for _, cs := range p.Status.ContainerStatuses {
				if cs.Ready {
					ready++
				}
			}
			restarts := int32(0)
			for _, cs := range p.Status.ContainerStatuses {
				restarts += cs.RestartCount
			}
			containers := make([]string, len(p.Spec.Containers))
			for i, c := range p.Spec.Containers {
				containers[i] = c.Name
			}
			items = append(items, map[string]any{
				"name":       p.Name,
				"namespace":  p.Namespace,
				"status":     string(p.Status.Phase),
				"ready":      fmt.Sprintf("%d/%d", ready, len(p.Spec.Containers)),
				"restarts":   restarts,
				"age":        time.Since(p.CreationTimestamp.Time).Truncate(time.Second).String(),
				"node":       p.Spec.NodeName,
				"ip":         p.Status.PodIP,
				"labels":     p.Labels,
				"containers": containers,
				"createdAt":  p.CreationTimestamp.Time,
			})
		}

	case "deployments":
		list, err := cs.AppsV1().Deployments(ns).List(ctx, opts)
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
		for _, d := range list.Items {
			items = append(items, map[string]any{
				"name":      d.Name,
				"namespace": d.Namespace,
				"ready":     fmt.Sprintf("%d/%d", d.Status.ReadyReplicas, *d.Spec.Replicas),
				"upToDate":  d.Status.UpdatedReplicas,
				"available": d.Status.AvailableReplicas,
				"age":       time.Since(d.CreationTimestamp.Time).Truncate(time.Second).String(),
				"labels":    d.Labels,
				"createdAt": d.CreationTimestamp.Time,
			})
		}

	case "services":
		list, err := cs.CoreV1().Services(ns).List(ctx, opts)
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
		for _, svc := range list.Items {
			ports := make([]string, len(svc.Spec.Ports))
			for i, p := range svc.Spec.Ports {
				ports[i] = fmt.Sprintf("%d/%s", p.Port, p.Protocol)
			}
			extIPs := svc.Spec.ExternalIPs
			if len(extIPs) == 0 {
				for _, ing := range svc.Status.LoadBalancer.Ingress {
					if ing.IP != "" {
						extIPs = append(extIPs, ing.IP)
					} else if ing.Hostname != "" {
						extIPs = append(extIPs, ing.Hostname)
					}
				}
			}
			items = append(items, map[string]any{
				"name":       svc.Name,
				"namespace":  svc.Namespace,
				"type":       string(svc.Spec.Type),
				"clusterIP":  svc.Spec.ClusterIP,
				"externalIP": strings.Join(extIPs, ","),
				"ports":      strings.Join(ports, ","),
				"age":        time.Since(svc.CreationTimestamp.Time).Truncate(time.Second).String(),
				"labels":     svc.Labels,
				"createdAt":  svc.CreationTimestamp.Time,
			})
		}

	case "statefulsets":
		list, err := cs.AppsV1().StatefulSets(ns).List(ctx, opts)
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
		for _, ss := range list.Items {
			items = append(items, map[string]any{
				"name":      ss.Name,
				"namespace": ss.Namespace,
				"ready":     fmt.Sprintf("%d/%d", ss.Status.ReadyReplicas, *ss.Spec.Replicas),
				"age":       time.Since(ss.CreationTimestamp.Time).Truncate(time.Second).String(),
				"labels":    ss.Labels,
				"createdAt": ss.CreationTimestamp.Time,
			})
		}

	case "daemonsets":
		list, err := cs.AppsV1().DaemonSets(ns).List(ctx, opts)
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
		for _, ds := range list.Items {
			items = append(items, map[string]any{
				"name":         ds.Name,
				"namespace":    ds.Namespace,
				"desired":      ds.Status.DesiredNumberScheduled,
				"current":      ds.Status.CurrentNumberScheduled,
				"ready":        ds.Status.NumberReady,
				"upToDate":     ds.Status.UpdatedNumberScheduled,
				"available":    ds.Status.NumberAvailable,
				"nodeSelector": ds.Spec.Template.Spec.NodeSelector,
				"age":          time.Since(ds.CreationTimestamp.Time).Truncate(time.Second).String(),
				"labels":       ds.Labels,
				"createdAt":    ds.CreationTimestamp.Time,
			})
		}

	case "jobs":
		list, err := cs.BatchV1().Jobs(ns).List(ctx, opts)
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
		for _, j := range list.Items {
			status := "Running"
			if j.Status.Succeeded > 0 {
				status = "Complete"
			} else if j.Status.Failed > 0 {
				status = "Failed"
			}
			items = append(items, map[string]any{
				"name":        j.Name,
				"namespace":   j.Namespace,
				"completions": fmt.Sprintf("%d/%d", j.Status.Succeeded, *j.Spec.Completions),
				"status":      status,
				"age":         time.Since(j.CreationTimestamp.Time).Truncate(time.Second).String(),
				"labels":      j.Labels,
				"createdAt":   j.CreationTimestamp.Time,
			})
		}

	case "cronjobs":
		list, err := cs.BatchV1().CronJobs(ns).List(ctx, opts)
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
		for _, cj := range list.Items {
			lastSchedule := ""
			if cj.Status.LastScheduleTime != nil {
				lastSchedule = time.Since(cj.Status.LastScheduleTime.Time).Truncate(time.Second).String()
			}
			suspended := false
			if cj.Spec.Suspend != nil {
				suspended = *cj.Spec.Suspend
			}
			items = append(items, map[string]any{
				"name":         cj.Name,
				"namespace":    cj.Namespace,
				"schedule":     cj.Spec.Schedule,
				"suspended":    suspended,
				"active":       len(cj.Status.Active),
				"lastSchedule": lastSchedule,
				"age":          time.Since(cj.CreationTimestamp.Time).Truncate(time.Second).String(),
				"labels":       cj.Labels,
				"createdAt":    cj.CreationTimestamp.Time,
			})
		}

	case "configmaps":
		list, err := cs.CoreV1().ConfigMaps(ns).List(ctx, opts)
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
		for _, cm := range list.Items {
			items = append(items, map[string]any{
				"name":      cm.Name,
				"namespace": cm.Namespace,
				"dataCount": len(cm.Data),
				"age":       time.Since(cm.CreationTimestamp.Time).Truncate(time.Second).String(),
				"labels":    cm.Labels,
				"createdAt": cm.CreationTimestamp.Time,
			})
		}

	case "secrets":
		list, err := cs.CoreV1().Secrets(ns).List(ctx, opts)
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
		for _, sec := range list.Items {
			items = append(items, map[string]any{
				"name":      sec.Name,
				"namespace": sec.Namespace,
				"type":      string(sec.Type),
				"dataCount": len(sec.Data),
				"age":       time.Since(sec.CreationTimestamp.Time).Truncate(time.Second).String(),
				"labels":    sec.Labels,
				"createdAt": sec.CreationTimestamp.Time,
			})
		}

	case "ingresses":
		list, err := cs.NetworkingV1().Ingresses(ns).List(ctx, opts)
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
		for _, ing := range list.Items {
			hosts := []string{}
			for _, rule := range ing.Spec.Rules {
				if rule.Host != "" {
					hosts = append(hosts, rule.Host)
				}
			}
			items = append(items, map[string]any{
				"name":      ing.Name,
				"namespace": ing.Namespace,
				"class":     ing.Spec.IngressClassName,
				"hosts":     strings.Join(hosts, ","),
				"age":       time.Since(ing.CreationTimestamp.Time).Truncate(time.Second).String(),
				"labels":    ing.Labels,
				"createdAt": ing.CreationTimestamp.Time,
			})
		}

	case "endpoints":
		list, err := cs.CoreV1().Endpoints(ns).List(ctx, opts)
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
		for _, ep := range list.Items {
			addrs := 0
			for _, sub := range ep.Subsets {
				addrs += len(sub.Addresses)
			}
			items = append(items, map[string]any{
				"name":      ep.Name,
				"namespace": ep.Namespace,
				"endpoints": addrs,
				"age":       time.Since(ep.CreationTimestamp.Time).Truncate(time.Second).String(),
				"labels":    ep.Labels,
				"createdAt": ep.CreationTimestamp.Time,
			})
		}

	case "pvcs":
		list, err := cs.CoreV1().PersistentVolumeClaims(ns).List(ctx, opts)
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
		for _, pvc := range list.Items {
			capacity := ""
			if q, ok := pvc.Status.Capacity["storage"]; ok {
				capacity = q.String()
			}
			scName := ""
			if pvc.Spec.StorageClassName != nil {
				scName = *pvc.Spec.StorageClassName
			}
			items = append(items, map[string]any{
				"name":         pvc.Name,
				"namespace":    pvc.Namespace,
				"status":       string(pvc.Status.Phase),
				"volume":       pvc.Spec.VolumeName,
				"capacity":     capacity,
				"accessModes":  pvc.Spec.AccessModes,
				"storageClass": scName,
				"age":          time.Since(pvc.CreationTimestamp.Time).Truncate(time.Second).String(),
				"labels":       pvc.Labels,
				"createdAt":    pvc.CreationTimestamp.Time,
			})
		}

	case "pvs":
		list, err := cs.CoreV1().PersistentVolumes().List(ctx, opts)
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
		for _, pv := range list.Items {
			capacity := ""
			if q, ok := pv.Spec.Capacity["storage"]; ok {
				capacity = q.String()
			}
			scName := pv.Spec.StorageClassName
			items = append(items, map[string]any{
				"name":            pv.Name,
				"capacity":        capacity,
				"accessModes":     pv.Spec.AccessModes,
				"reclaimPolicy":   string(pv.Spec.PersistentVolumeReclaimPolicy),
				"status":          string(pv.Status.Phase),
				"claim":           pv.Spec.ClaimRef,
				"storageClass":    scName,
				"age":             time.Since(pv.CreationTimestamp.Time).Truncate(time.Second).String(),
				"labels":          pv.Labels,
				"createdAt":       pv.CreationTimestamp.Time,
			})
		}

	case "nodes":
		list, err := cs.CoreV1().Nodes().List(ctx, opts)
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
		for _, n := range list.Items {
			status := "NotReady"
			for _, c := range n.Status.Conditions {
				if c.Type == "Ready" && c.Status == "True" {
					status = "Ready"
				}
			}
			internalIP := ""
			for _, addr := range n.Status.Addresses {
				if addr.Type == "InternalIP" {
					internalIP = addr.Address
					break
				}
			}
			items = append(items, map[string]any{
				"name":      n.Name,
				"status":    status,
				"roles":     nodeRoles(n.Labels),
				"age":       time.Since(n.CreationTimestamp.Time).Truncate(time.Second).String(),
				"version":   n.Status.NodeInfo.KubeletVersion,
				"os":        n.Status.NodeInfo.OSImage,
				"arch":      n.Status.NodeInfo.Architecture,
				"internalIP": internalIP,
				"labels":    n.Labels,
				"createdAt": n.CreationTimestamp.Time,
			})
		}

	case "namespaces":
		list, err := cs.CoreV1().Namespaces().List(ctx, opts)
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
		for _, ns := range list.Items {
			items = append(items, map[string]any{
				"name":      ns.Name,
				"status":    string(ns.Status.Phase),
				"age":       time.Since(ns.CreationTimestamp.Time).Truncate(time.Second).String(),
				"labels":    ns.Labels,
				"createdAt": ns.CreationTimestamp.Time,
			})
		}

	default:
		json.NewEncoder(w).Encode(K8sResponse{Error: "unsupported resource type: " + req.Resource, DurationMs: ms(t0)})
		return
	}

	if items == nil {
		items = []map[string]any{}
	}
	json.NewEncoder(w).Encode(K8sResponse{Items: items, DurationMs: ms(t0)})
}

// ── /resource ───────────────────────────────────────────────────────────────

func (s *Server) handleK8sResource(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	if req.Resource == "" || req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "resource and name are required", DurationMs: ms(t0)})
		return
	}

	cs, _, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	ns := req.Namespace
	if ns == "" {
		ns = "default"
	}

	ctx, cancel := context.WithTimeout(r.Context(), k8sTimeout)
	defer cancel()

	getOpts := metav1.GetOptions{}
	var raw any

	switch req.Resource {
	case "pods":
		raw, err = cs.CoreV1().Pods(ns).Get(ctx, req.Name, getOpts)
	case "deployments":
		raw, err = cs.AppsV1().Deployments(ns).Get(ctx, req.Name, getOpts)
	case "services":
		raw, err = cs.CoreV1().Services(ns).Get(ctx, req.Name, getOpts)
	case "statefulsets":
		raw, err = cs.AppsV1().StatefulSets(ns).Get(ctx, req.Name, getOpts)
	case "daemonsets":
		raw, err = cs.AppsV1().DaemonSets(ns).Get(ctx, req.Name, getOpts)
	case "jobs":
		raw, err = cs.BatchV1().Jobs(ns).Get(ctx, req.Name, getOpts)
	case "cronjobs":
		raw, err = cs.BatchV1().CronJobs(ns).Get(ctx, req.Name, getOpts)
	case "configmaps":
		raw, err = cs.CoreV1().ConfigMaps(ns).Get(ctx, req.Name, getOpts)
	case "secrets":
		raw, err = cs.CoreV1().Secrets(ns).Get(ctx, req.Name, getOpts)
	case "ingresses":
		raw, err = cs.NetworkingV1().Ingresses(ns).Get(ctx, req.Name, getOpts)
	case "endpoints":
		raw, err = cs.CoreV1().Endpoints(ns).Get(ctx, req.Name, getOpts)
	case "pvcs":
		raw, err = cs.CoreV1().PersistentVolumeClaims(ns).Get(ctx, req.Name, getOpts)
	case "pvs":
		raw, err = cs.CoreV1().PersistentVolumes().Get(ctx, req.Name, getOpts)
	case "nodes":
		raw, err = cs.CoreV1().Nodes().Get(ctx, req.Name, getOpts)
	case "namespaces":
		raw, err = cs.CoreV1().Namespaces().Get(ctx, req.Name, getOpts)
	default:
		json.NewEncoder(w).Encode(K8sResponse{Error: "unsupported resource type: " + req.Resource, DurationMs: ms(t0)})
		return
	}

	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	// Convert to generic map
	b, _ := json.Marshal(raw)
	var item map[string]any
	json.Unmarshal(b, &item)

	json.NewEncoder(w).Encode(K8sResponse{Item: item, DurationMs: ms(t0)})
}

// ── /describe ───────────────────────────────────────────────────────────────

func (s *Server) handleK8sDescribe(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	if req.Resource == "" || req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "resource and name are required", DurationMs: ms(t0)})
		return
	}

	cs, _, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	ns := req.Namespace
	if ns == "" {
		ns = "default"
	}

	ctx, cancel := context.WithTimeout(r.Context(), k8sTimeout)
	defer cancel()

	// Get the resource
	getOpts := metav1.GetOptions{}
	var raw any
	switch req.Resource {
	case "pods":
		raw, err = cs.CoreV1().Pods(ns).Get(ctx, req.Name, getOpts)
	case "deployments":
		raw, err = cs.AppsV1().Deployments(ns).Get(ctx, req.Name, getOpts)
	case "services":
		raw, err = cs.CoreV1().Services(ns).Get(ctx, req.Name, getOpts)
	case "nodes":
		raw, err = cs.CoreV1().Nodes().Get(ctx, req.Name, getOpts)
	default:
		raw = nil
		// For other types, we try to get via the resource endpoint
		s.handleK8sResource(w, r, req)
		return
	}

	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	b, _ := json.Marshal(raw)
	var item map[string]any
	json.Unmarshal(b, &item)

	// Get related events
	fieldSel := fmt.Sprintf("involvedObject.name=%s", req.Name)
	eventList, err := cs.CoreV1().Events(ns).List(ctx, metav1.ListOptions{FieldSelector: fieldSel})
	var events []map[string]any
	if err == nil {
		for _, e := range eventList.Items {
			events = append(events, map[string]any{
				"type":      e.Type,
				"reason":    e.Reason,
				"message":   e.Message,
				"count":     e.Count,
				"firstSeen": e.FirstTimestamp.Time,
				"lastSeen":  e.LastTimestamp.Time,
				"source":    e.Source.Component,
			})
		}
	}

	json.NewEncoder(w).Encode(K8sResponse{Item: item, Events: events, DurationMs: ms(t0)})
}

// ── /yaml ───────────────────────────────────────────────────────────────────

func (s *Server) handleK8sYAML(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	if req.Resource == "" || req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "resource and name are required", DurationMs: ms(t0)})
		return
	}

	cs, _, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	ns := req.Namespace
	if ns == "" {
		ns = "default"
	}

	ctx, cancel := context.WithTimeout(r.Context(), k8sTimeout)
	defer cancel()

	getOpts := metav1.GetOptions{}
	var raw any

	switch req.Resource {
	case "pods":
		raw, err = cs.CoreV1().Pods(ns).Get(ctx, req.Name, getOpts)
	case "deployments":
		raw, err = cs.AppsV1().Deployments(ns).Get(ctx, req.Name, getOpts)
	case "services":
		raw, err = cs.CoreV1().Services(ns).Get(ctx, req.Name, getOpts)
	case "statefulsets":
		raw, err = cs.AppsV1().StatefulSets(ns).Get(ctx, req.Name, getOpts)
	case "daemonsets":
		raw, err = cs.AppsV1().DaemonSets(ns).Get(ctx, req.Name, getOpts)
	case "jobs":
		raw, err = cs.BatchV1().Jobs(ns).Get(ctx, req.Name, getOpts)
	case "cronjobs":
		raw, err = cs.BatchV1().CronJobs(ns).Get(ctx, req.Name, getOpts)
	case "configmaps":
		raw, err = cs.CoreV1().ConfigMaps(ns).Get(ctx, req.Name, getOpts)
	case "secrets":
		raw, err = cs.CoreV1().Secrets(ns).Get(ctx, req.Name, getOpts)
	case "ingresses":
		raw, err = cs.NetworkingV1().Ingresses(ns).Get(ctx, req.Name, getOpts)
	case "nodes":
		raw, err = cs.CoreV1().Nodes().Get(ctx, req.Name, getOpts)
	case "pvcs":
		raw, err = cs.CoreV1().PersistentVolumeClaims(ns).Get(ctx, req.Name, getOpts)
	case "pvs":
		raw, err = cs.CoreV1().PersistentVolumes().Get(ctx, req.Name, getOpts)
	case "namespaces":
		raw, err = cs.CoreV1().Namespaces().Get(ctx, req.Name, getOpts)
	default:
		json.NewEncoder(w).Encode(K8sResponse{Error: "unsupported resource type: " + req.Resource, DurationMs: ms(t0)})
		return
	}

	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	yamlBytes, err := yaml.Marshal(raw)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "yaml marshal: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{YAML: string(yamlBytes), DurationMs: ms(t0)})
}

// ── /events ─────────────────────────────────────────────────────────────────

func (s *Server) handleK8sEvents(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	cs, _, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	ns := req.Namespace
	if ns == "" {
		ns = "default"
	}

	ctx, cancel := context.WithTimeout(r.Context(), k8sTimeout)
	defer cancel()

	opts := metav1.ListOptions{}
	if req.FieldSelector != "" {
		opts.FieldSelector = req.FieldSelector
	}

	eventList, err := cs.CoreV1().Events(ns).List(ctx, opts)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	var events []map[string]any
	for _, e := range eventList.Items {
		events = append(events, map[string]any{
			"type":      e.Type,
			"reason":    e.Reason,
			"message":   e.Message,
			"object":    e.InvolvedObject.Kind + "/" + e.InvolvedObject.Name,
			"count":     e.Count,
			"firstSeen": e.FirstTimestamp.Time,
			"lastSeen":  e.LastTimestamp.Time,
			"source":    e.Source.Component,
		})
	}
	if events == nil {
		events = []map[string]any{}
	}

	json.NewEncoder(w).Encode(K8sResponse{Events: events, DurationMs: ms(t0)})
}

// ── /api-resources ──────────────────────────────────────────────────────────

func (s *Server) handleK8sAPIResources(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	cs, _, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), k8sTimeout)
	defer cancel()
	_ = ctx

	lists, err := cs.Discovery().ServerPreferredResources()
	if err != nil {
		// Partial results are common; continue if we got something
		if lists == nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
	}

	var resources []K8sAPIResource
	for _, list := range lists {
		gv := list.GroupVersion
		parts := strings.SplitN(gv, "/", 2)
		group := ""
		version := gv
		if len(parts) == 2 {
			group = parts[0]
			version = parts[1]
		}
		for _, res := range list.APIResources {
			if strings.Contains(res.Name, "/") {
				continue // skip subresources
			}
			resources = append(resources, K8sAPIResource{
				Name:       res.Name,
				Kind:       res.Kind,
				Namespaced: res.Namespaced,
				Group:      group,
				Version:    version,
			})
		}
	}

	json.NewEncoder(w).Encode(K8sResponse{Resources: resources, DurationMs: ms(t0)})
}

// ── /top/pods ───────────────────────────────────────────────────────────────

func (s *Server) handleK8sTopPods(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	cs, cfg, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}
	_ = cs

	ns := req.Namespace
	if ns == "" {
		ns = "default"
	}

	ctx, cancel := context.WithTimeout(r.Context(), k8sTimeout)
	defer cancel()

	// Use raw REST to query metrics API
	metricsPath := fmt.Sprintf("/apis/metrics.k8s.io/v1beta1/namespaces/%s/pods", ns)
	result, err := cs.RESTClient().Get().AbsPath(metricsPath).DoRaw(ctx)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "metrics-server may not be installed: " + err.Error(), DurationMs: ms(t0)})
		return
	}
	_ = cfg

	var metricsList map[string]any
	json.Unmarshal(result, &metricsList)

	var items []map[string]any
	if rawItems, ok := metricsList["items"].([]any); ok {
		for _, rawItem := range rawItems {
			if item, ok := rawItem.(map[string]any); ok {
				items = append(items, item)
			}
		}
	}
	if items == nil {
		items = []map[string]any{}
	}

	json.NewEncoder(w).Encode(K8sResponse{Items: items, DurationMs: ms(t0)})
}

// ── /top/nodes ──────────────────────────────────────────────────────────────

func (s *Server) handleK8sTopNodes(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	cs, _, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), k8sTimeout)
	defer cancel()

	result, err := cs.RESTClient().Get().AbsPath("/apis/metrics.k8s.io/v1beta1/nodes").DoRaw(ctx)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "metrics-server may not be installed: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	var metricsList map[string]any
	json.Unmarshal(result, &metricsList)

	var items []map[string]any
	if rawItems, ok := metricsList["items"].([]any); ok {
		for _, rawItem := range rawItems {
			if item, ok := rawItem.(map[string]any); ok {
				items = append(items, item)
			}
		}
	}
	if items == nil {
		items = []map[string]any{}
	}

	json.NewEncoder(w).Encode(K8sResponse{Items: items, DurationMs: ms(t0)})
}

// ── helpers ─────────────────────────────────────────────────────────────────

func nodeRoles(labels map[string]string) string {
	var roles []string
	for k := range labels {
		if strings.HasPrefix(k, "node-role.kubernetes.io/") {
			role := strings.TrimPrefix(k, "node-role.kubernetes.io/")
			if role != "" {
				roles = append(roles, role)
			}
		}
	}
	if len(roles) == 0 {
		return "<none>"
	}
	return strings.Join(roles, ",")
}
