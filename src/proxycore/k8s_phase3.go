package proxycore

import (
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	syaml "sigs.k8s.io/yaml"
)

// ── Phase 3: Helm Release Browser ──────────────────────────────────────────

// handleK8sHelmReleases lists Helm releases by reading Helm storage secrets.
func (s *Server) handleK8sHelmReleases(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	cs, _, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), k8sTimeout)
	defer cancel()

	// Helm stores releases as secrets with label owner=helm
	ns := req.Namespace
	if ns == "" {
		ns = "" // empty = all namespaces
	}

	opts := metav1.ListOptions{
		LabelSelector: "owner=helm",
	}

	secrets, err := cs.CoreV1().Secrets(ns).List(ctx, opts)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	// Group by release name, keep latest version per release
	type helmRelease struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
		Revision  string `json:"revision"`
		Status    string `json:"status"`
		Chart     string `json:"chart"`
		AppVer    string `json:"appVersion"`
		Updated   string `json:"updated"`
	}

	releaseMap := make(map[string]*helmRelease)

	for _, sec := range secrets.Items {
		labels := sec.Labels
		name := labels["name"]
		if name == "" {
			continue
		}
		key := sec.Namespace + "/" + name
		rev := labels["version"]
		status := labels["status"]

		// Decode release data to get chart info
		chartName := ""
		appVersion := ""
		if releaseData, ok := sec.Data["release"]; ok {
			if decoded := decodeHelmRelease(releaseData); decoded != nil {
				if ch, ok := decoded["chart"].(map[string]any); ok {
					if meta, ok := ch["metadata"].(map[string]any); ok {
						chartName, _ = meta["name"].(string)
						appVersion, _ = meta["appVersion"].(string)
						if ver, ok := meta["version"].(string); ok && chartName != "" {
							chartName = chartName + "-" + ver
						}
					}
				}
			}
		}

		existing, exists := releaseMap[key]
		if !exists || rev > existing.Revision {
			releaseMap[key] = &helmRelease{
				Name:      name,
				Namespace: sec.Namespace,
				Revision:  rev,
				Status:    status,
				Chart:     chartName,
				AppVer:    appVersion,
				Updated:   sec.CreationTimestamp.Format(time.RFC3339),
			}
		}
	}

	var items []map[string]any
	for _, rel := range releaseMap {
		items = append(items, map[string]any{
			"name":       rel.Name,
			"namespace":  rel.Namespace,
			"revision":   rel.Revision,
			"status":     rel.Status,
			"chart":      rel.Chart,
			"appVersion": rel.AppVer,
			"updated":    rel.Updated,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		a, _ := items[i]["name"].(string)
		b, _ := items[j]["name"].(string)
		return a < b
	})

	if items == nil {
		items = []map[string]any{}
	}
	json.NewEncoder(w).Encode(K8sResponse{Items: items, DurationMs: ms(t0)})
}

// handleK8sHelmRelease returns details of a specific Helm release.
func (s *Server) handleK8sHelmRelease(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	if req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "name is required", DurationMs: ms(t0)})
		return
	}

	cs, _, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), k8sTimeout)
	defer cancel()

	ns := req.Namespace
	if ns == "" {
		ns = "default"
	}

	opts := metav1.ListOptions{
		LabelSelector: "owner=helm,name=" + req.Name,
	}
	secrets, err := cs.CoreV1().Secrets(ns).List(ctx, opts)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	if len(secrets.Items) == 0 {
		json.NewEncoder(w).Encode(K8sResponse{Error: "release not found: " + req.Name, DurationMs: ms(t0)})
		return
	}

	// Find latest revision
	var latest = secrets.Items[0]
	for _, sec := range secrets.Items[1:] {
		if sec.Labels["version"] > latest.Labels["version"] {
			latest = sec
		}
	}

	item := map[string]any{
		"name":      latest.Labels["name"],
		"namespace": latest.Namespace,
		"revision":  latest.Labels["version"],
		"status":    latest.Labels["status"],
		"updated":   latest.CreationTimestamp.Format(time.RFC3339),
	}

	if releaseData, ok := latest.Data["release"]; ok {
		if decoded := decodeHelmRelease(releaseData); decoded != nil {
			item["info"] = decoded["info"]
			if ch, ok := decoded["chart"].(map[string]any); ok {
				item["chart"] = ch["metadata"]
				// Extract values
				if vals, ok := decoded["config"].(map[string]any); ok {
					yamlBytes, _ := syaml.Marshal(vals)
					item["values"] = string(yamlBytes)
				}
				// Extract manifest
				if manifest, ok := decoded["manifest"].(string); ok {
					item["manifest"] = manifest
				}
			}
		}
	}

	json.NewEncoder(w).Encode(K8sResponse{Item: item, DurationMs: ms(t0)})
}

// handleK8sHelmHistory returns the revision history of a Helm release.
func (s *Server) handleK8sHelmHistory(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	if req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "name is required", DurationMs: ms(t0)})
		return
	}

	cs, _, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), k8sTimeout)
	defer cancel()

	ns := req.Namespace
	if ns == "" {
		ns = "default"
	}

	opts := metav1.ListOptions{
		LabelSelector: "owner=helm,name=" + req.Name,
	}
	secrets, err := cs.CoreV1().Secrets(ns).List(ctx, opts)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	var items []map[string]any
	for _, sec := range secrets.Items {
		entry := map[string]any{
			"revision": sec.Labels["version"],
			"status":   sec.Labels["status"],
			"updated":  sec.CreationTimestamp.Format(time.RFC3339),
		}
		if releaseData, ok := sec.Data["release"]; ok {
			if decoded := decodeHelmRelease(releaseData); decoded != nil {
				if info, ok := decoded["info"].(map[string]any); ok {
					entry["description"], _ = info["description"].(string)
				}
				if ch, ok := decoded["chart"].(map[string]any); ok {
					if meta, ok := ch["metadata"].(map[string]any); ok {
						entry["chart"], _ = meta["name"].(string)
						entry["appVersion"], _ = meta["appVersion"].(string)
					}
				}
			}
		}
		items = append(items, entry)
	}

	sort.Slice(items, func(i, j int) bool {
		a, _ := items[i]["revision"].(string)
		b, _ := items[j]["revision"].(string)
		return a > b // newest first
	})

	if items == nil {
		items = []map[string]any{}
	}
	json.NewEncoder(w).Encode(K8sResponse{Items: items, DurationMs: ms(t0)})
}

// decodeHelmRelease decodes a Helm release secret value (base64 → gzip → JSON).
func decodeHelmRelease(data []byte) map[string]any {
	// Helm stores: base64(gzip(json))
	decoded, err := base64.StdEncoding.DecodeString(string(data))
	if err != nil {
		return nil
	}
	reader, err := gzip.NewReader(strings.NewReader(string(decoded)))
	if err != nil {
		return nil
	}
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	if err != nil {
		return nil
	}
	var result map[string]any
	json.Unmarshal(raw, &result)
	return result
}

// ── Phase 3: CRD Discovery & Custom Resource Browsing ──────────────────────

// handleK8sCRDs lists all CustomResourceDefinitions in the cluster.
func (s *Server) handleK8sCRDs(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	cs, cfg, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}
	_ = cs

	ctx, cancel := context.WithTimeout(r.Context(), k8sTimeout)
	defer cancel()

	// Use dynamic client to list CRDs from apiextensions.k8s.io/v1
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "failed to create dynamic client: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	crdGVR := schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}

	list, err := dynClient.Resource(crdGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	var items []map[string]any
	for _, crd := range list.Items {
		spec, _ := crd.Object["spec"].(map[string]any)
		names, _ := spec["names"].(map[string]any)
		group, _ := spec["group"].(string)
		scope, _ := spec["scope"].(string)

		// Get served version
		versions, _ := spec["versions"].([]any)
		servedVersion := ""
		for _, v := range versions {
			vm, _ := v.(map[string]any)
			if served, _ := vm["served"].(bool); served {
				if servedVersion == "" {
					servedVersion, _ = vm["name"].(string)
				}
				if storage, _ := vm["storage"].(bool); storage {
					servedVersion, _ = vm["name"].(string)
					break
				}
			}
		}

		kind, _ := names["kind"].(string)
		plural, _ := names["plural"].(string)
		singular, _ := names["singular"].(string)

		items = append(items, map[string]any{
			"name":     crd.GetName(),
			"group":    group,
			"version":  servedVersion,
			"kind":     kind,
			"plural":   plural,
			"singular": singular,
			"scope":    scope,
			"age":      time.Since(crd.GetCreationTimestamp().Time).Truncate(time.Second).String(),
			"labels":   crd.GetLabels(),
		})
	}

	if items == nil {
		items = []map[string]any{}
	}
	json.NewEncoder(w).Encode(K8sResponse{Items: items, DurationMs: ms(t0)})
}

// handleK8sCRDResources lists instances of a specific CRD.
func (s *Server) handleK8sCRDResources(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	if req.Resource == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "resource (plural name) is required", DurationMs: ms(t0)})
		return
	}

	_, cfg, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), k8sTimeout)
	defer cancel()

	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "failed to create dynamic client: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	// Parse group and resource from the request
	// req.Resource = plural name (e.g. "certificates")
	// req.Name is reused for group (e.g. "cert-manager.io")
	// req.YAML is reused for version (e.g. "v1")
	group := req.Name
	version := req.YAML
	if version == "" {
		version = "v1"
	}

	gvr := schema.GroupVersionResource{
		Group:    group,
		Version:  version,
		Resource: req.Resource,
	}

	ns := req.Namespace
	opts := metav1.ListOptions{}
	if req.LabelSelector != "" {
		opts.LabelSelector = req.LabelSelector
	}
	if req.Limit > 0 {
		opts.Limit = req.Limit
	}

	var items []map[string]any

	if ns != "" {
		result, err := dynClient.Resource(gvr).Namespace(ns).List(ctx, opts)
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
		for _, item := range result.Items {
			items = append(items, item.Object)
		}
	} else {
		result, err := dynClient.Resource(gvr).List(ctx, opts)
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
			return
		}
		for _, item := range result.Items {
			items = append(items, item.Object)
		}
	}

	if items == nil {
		items = []map[string]any{}
	}
	json.NewEncoder(w).Encode(K8sResponse{Items: items, DurationMs: ms(t0)})
}

// ── Phase 3: Resource Diff ─────────────────────────────────────────────────

// handleK8sDiff compares the YAML of a resource across two namespaces or contexts.
func (s *Server) handleK8sDiff(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	if req.Resource == "" || req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "resource and name are required", DurationMs: ms(t0)})
		return
	}

	// Parse diff parameters from Action field: "ns:other-ns" or "ctx:other-context"
	// Source is the current connection; target comes from Action
	if req.Action == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "action is required: 'ns:<namespace>' or 'ctx:<context>'", DurationMs: ms(t0)})
		return
	}

	parts := strings.SplitN(req.Action, ":", 2)
	if len(parts) != 2 || (parts[0] != "ns" && parts[0] != "ctx") {
		json.NewEncoder(w).Encode(K8sResponse{Error: "action must be 'ns:<namespace>' or 'ctx:<context>'", DurationMs: ms(t0)})
		return
	}

	diffType := parts[0]
	diffTarget := parts[1]

	// Get source YAML
	sourceYAML, err := s.getResourceYAML(r.Context(), req.Kubeconfig, req.Context, req.Namespace, req.Resource, req.Name)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "source: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	// Get target YAML
	var targetNS, targetCtx string
	if diffType == "ns" {
		targetNS = diffTarget
		targetCtx = req.Context
	} else {
		targetNS = req.Namespace
		targetCtx = diffTarget
	}

	targetYAML, err := s.getResourceYAML(r.Context(), req.Kubeconfig, targetCtx, targetNS, req.Resource, req.Name)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "target: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{
		Item: map[string]any{
			"sourceYaml": sourceYAML,
			"targetYaml": targetYAML,
		},
		DurationMs: ms(t0),
	})
}

// getResourceYAML fetches a resource and returns its YAML representation.
func (s *Server) getResourceYAML(parentCtx context.Context, kubeconfig, k8sContext, namespace, resource, name string) (string, error) {
	cs, _, err := s.getPooledK8sClient(kubeconfig, k8sContext)
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(parentCtx, k8sTimeout)
	defer cancel()

	ns := namespace
	if ns == "" {
		ns = "default"
	}

	var obj any

	switch resource {
	case "pods":
		obj, err = cs.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
	case "deployments":
		obj, err = cs.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	case "services":
		obj, err = cs.CoreV1().Services(ns).Get(ctx, name, metav1.GetOptions{})
	case "statefulsets":
		obj, err = cs.AppsV1().StatefulSets(ns).Get(ctx, name, metav1.GetOptions{})
	case "daemonsets":
		obj, err = cs.AppsV1().DaemonSets(ns).Get(ctx, name, metav1.GetOptions{})
	case "jobs":
		obj, err = cs.BatchV1().Jobs(ns).Get(ctx, name, metav1.GetOptions{})
	case "cronjobs":
		obj, err = cs.BatchV1().CronJobs(ns).Get(ctx, name, metav1.GetOptions{})
	case "configmaps":
		obj, err = cs.CoreV1().ConfigMaps(ns).Get(ctx, name, metav1.GetOptions{})
	case "secrets":
		obj, err = cs.CoreV1().Secrets(ns).Get(ctx, name, metav1.GetOptions{})
	case "ingresses":
		obj, err = cs.NetworkingV1().Ingresses(ns).Get(ctx, name, metav1.GetOptions{})
	case "pvcs":
		obj, err = cs.CoreV1().PersistentVolumeClaims(ns).Get(ctx, name, metav1.GetOptions{})
	case "pvs":
		obj, err = cs.CoreV1().PersistentVolumes().Get(ctx, name, metav1.GetOptions{})
	case "nodes":
		obj, err = cs.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
	case "namespaces":
		obj, err = cs.CoreV1().Namespaces().Get(ctx, name, metav1.GetOptions{})
	default:
		return "", err
	}

	if err != nil {
		return "", err
	}

	yamlBytes, err := syaml.Marshal(obj)
	if err != nil {
		return "", err
	}
	return string(yamlBytes), nil
}

// ── Phase 3: Topology ──────────────────────────────────────────────────────

// handleK8sTopology returns resource relationship data for topology graph rendering.
func (s *Server) handleK8sTopology(w http.ResponseWriter, r *http.Request, req K8sRequest) {
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

	// Fetch pods, deployments, replicasets, services, endpoints in parallel
	type fetchResult struct {
		key   string
		items []map[string]any
		err   error
	}
	ch := make(chan fetchResult, 5)

	go func() {
		list, err := cs.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
		var items []map[string]any
		if err == nil {
			for _, p := range list.Items {
				items = append(items, map[string]any{
					"kind":      "Pod",
					"name":      p.Name,
					"namespace": p.Namespace,
					"status":    string(p.Status.Phase),
					"labels":    p.Labels,
					"nodeName":  p.Spec.NodeName,
					"owners":    ownerRefs(p.OwnerReferences),
				})
			}
		}
		ch <- fetchResult{"pods", items, err}
	}()

	go func() {
		list, err := cs.AppsV1().Deployments(ns).List(ctx, metav1.ListOptions{})
		var items []map[string]any
		if err == nil {
			for _, d := range list.Items {
				sel := ""
				if d.Spec.Selector != nil {
					pairs := make([]string, 0, len(d.Spec.Selector.MatchLabels))
					for k, v := range d.Spec.Selector.MatchLabels {
						pairs = append(pairs, k+"="+v)
					}
					sel = strings.Join(pairs, ",")
				}
				items = append(items, map[string]any{
					"kind":      "Deployment",
					"name":      d.Name,
					"namespace": d.Namespace,
					"selector":  sel,
					"labels":    d.Labels,
				})
			}
		}
		ch <- fetchResult{"deployments", items, err}
	}()

	go func() {
		list, err := cs.AppsV1().ReplicaSets(ns).List(ctx, metav1.ListOptions{})
		var items []map[string]any
		if err == nil {
			for _, rs := range list.Items {
				if rs.Status.Replicas == 0 {
					continue // skip old/inactive replicasets
				}
				items = append(items, map[string]any{
					"kind":      "ReplicaSet",
					"name":      rs.Name,
					"namespace": rs.Namespace,
					"labels":    rs.Labels,
					"owners":    ownerRefs(rs.OwnerReferences),
				})
			}
		}
		ch <- fetchResult{"replicasets", items, err}
	}()

	go func() {
		list, err := cs.CoreV1().Services(ns).List(ctx, metav1.ListOptions{})
		var items []map[string]any
		if err == nil {
			for _, svc := range list.Items {
				sel := ""
				if svc.Spec.Selector != nil {
					pairs := make([]string, 0, len(svc.Spec.Selector))
					for k, v := range svc.Spec.Selector {
						pairs = append(pairs, k+"="+v)
					}
					sel = strings.Join(pairs, ",")
				}
				items = append(items, map[string]any{
					"kind":      "Service",
					"name":      svc.Name,
					"namespace": svc.Namespace,
					"type":      string(svc.Spec.Type),
					"selector":  sel,
					"labels":    svc.Labels,
				})
			}
		}
		ch <- fetchResult{"services", items, err}
	}()

	go func() {
		list, err := cs.NetworkingV1().Ingresses(ns).List(ctx, metav1.ListOptions{})
		var items []map[string]any
		if err == nil {
			for _, ing := range list.Items {
				backends := []string{}
				for _, rule := range ing.Spec.Rules {
					if rule.HTTP != nil {
						for _, path := range rule.HTTP.Paths {
							if path.Backend.Service != nil {
								backends = append(backends, path.Backend.Service.Name)
							}
						}
					}
				}
				items = append(items, map[string]any{
					"kind":      "Ingress",
					"name":      ing.Name,
					"namespace": ing.Namespace,
					"backends":  backends,
					"labels":    ing.Labels,
				})
			}
		}
		ch <- fetchResult{"ingresses", items, err}
	}()

	result := map[string]any{}
	for i := 0; i < 5; i++ {
		fr := <-ch
		if fr.err != nil && fr.items == nil {
			continue
		}
		if fr.items == nil {
			fr.items = []map[string]any{}
		}
		result[fr.key] = fr.items
	}

	json.NewEncoder(w).Encode(K8sResponse{Item: result, DurationMs: ms(t0)})
}

func ownerRefs(refs []metav1.OwnerReference) []map[string]string {
	var out []map[string]string
	for _, ref := range refs {
		out = append(out, map[string]string{
			"kind": ref.Kind,
			"name": ref.Name,
		})
	}
	return out
}
