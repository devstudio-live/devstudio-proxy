package proxycore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	sigyaml "sigs.k8s.io/yaml"
)

// ── /apply ─────────────────────────────────────────────────────────────────

func (s *Server) handleK8sApply(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	if req.YAML == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "yaml is required", DurationMs: ms(t0)})
		return
	}

	_, cfg, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), k8sTimeout)
	defer cancel()

	// Parse YAML into unstructured
	var obj unstructured.Unstructured
	jsonBytes, err := sigyaml.YAMLToJSON([]byte(req.YAML))
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "invalid YAML: " + err.Error(), DurationMs: ms(t0)})
		return
	}
	if err := json.Unmarshal(jsonBytes, &obj.Object); err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "invalid resource JSON: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	// Build dynamic client
	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "dynamic client: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	// Discover the GVR for this object
	gvk := obj.GroupVersionKind()
	gvr, err := discoverGVR(cfg, gvk)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "discover resource: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	ns := obj.GetNamespace()
	if ns == "" {
		ns = req.Namespace
	}
	if ns == "" {
		ns = "default"
	}

	var res *unstructured.Unstructured
	name := obj.GetName()

	// Try to get existing resource — update if exists, create if not
	if name != "" {
		_, getErr := dynClient.Resource(gvr).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
		if getErr == nil {
			// Update existing
			res, err = dynClient.Resource(gvr).Namespace(ns).Update(ctx, &obj, metav1.UpdateOptions{})
		} else {
			// Create new
			res, err = dynClient.Resource(gvr).Namespace(ns).Create(ctx, &obj, metav1.CreateOptions{})
		}
	} else {
		res, err = dynClient.Resource(gvr).Namespace(ns).Create(ctx, &obj, metav1.CreateOptions{})
	}

	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	// Convert result to generic map
	b, _ := json.Marshal(res.Object)
	var item map[string]any
	json.Unmarshal(b, &item)

	json.NewEncoder(w).Encode(K8sResponse{
		Success:    true,
		Message:    fmt.Sprintf("%s/%s applied", gvk.Kind, res.GetName()),
		Item:       item,
		DurationMs: ms(t0),
	})
}

// discoverGVR maps a GVK to a GVR using well-known mappings, falling back to naive pluralization.
func discoverGVR(_ *rest.Config, gvk schema.GroupVersionKind) (schema.GroupVersionResource, error) {
	// Use well-known mappings for core resources to avoid discovery round-trip
	known := map[schema.GroupVersionKind]schema.GroupVersionResource{
		{Group: "", Version: "v1", Kind: "Pod"}:                      {Group: "", Version: "v1", Resource: "pods"},
		{Group: "", Version: "v1", Kind: "Service"}:                  {Group: "", Version: "v1", Resource: "services"},
		{Group: "", Version: "v1", Kind: "ConfigMap"}:                {Group: "", Version: "v1", Resource: "configmaps"},
		{Group: "", Version: "v1", Kind: "Secret"}:                   {Group: "", Version: "v1", Resource: "secrets"},
		{Group: "", Version: "v1", Kind: "Namespace"}:                {Group: "", Version: "v1", Resource: "namespaces"},
		{Group: "", Version: "v1", Kind: "PersistentVolumeClaim"}:    {Group: "", Version: "v1", Resource: "persistentvolumeclaims"},
		{Group: "", Version: "v1", Kind: "PersistentVolume"}:         {Group: "", Version: "v1", Resource: "persistentvolumes"},
		{Group: "apps", Version: "v1", Kind: "Deployment"}:           {Group: "apps", Version: "v1", Resource: "deployments"},
		{Group: "apps", Version: "v1", Kind: "StatefulSet"}:          {Group: "apps", Version: "v1", Resource: "statefulsets"},
		{Group: "apps", Version: "v1", Kind: "DaemonSet"}:            {Group: "apps", Version: "v1", Resource: "daemonsets"},
		{Group: "batch", Version: "v1", Kind: "Job"}:                 {Group: "batch", Version: "v1", Resource: "jobs"},
		{Group: "batch", Version: "v1", Kind: "CronJob"}:             {Group: "batch", Version: "v1", Resource: "cronjobs"},
		{Group: "networking.k8s.io", Version: "v1", Kind: "Ingress"}: {Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"},
	}
	if gvr, ok := known[gvk]; ok {
		return gvr, nil
	}

	// Fallback: pluralize Kind naively (lowercase + "s")
	resource := strings.ToLower(gvk.Kind) + "s"
	return schema.GroupVersionResource{Group: gvk.Group, Version: gvk.Version, Resource: resource}, nil
}

// ── /delete ────────────────────────────────────────────────────────────────

func (s *Server) handleK8sDelete(w http.ResponseWriter, r *http.Request, req K8sRequest) {
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

	delOpts := metav1.DeleteOptions{}
	if req.GracePeriod != nil {
		delOpts.GracePeriodSeconds = req.GracePeriod
	}

	switch req.Resource {
	case "pods":
		err = cs.CoreV1().Pods(ns).Delete(ctx, req.Name, delOpts)
	case "deployments":
		err = cs.AppsV1().Deployments(ns).Delete(ctx, req.Name, delOpts)
	case "services":
		err = cs.CoreV1().Services(ns).Delete(ctx, req.Name, delOpts)
	case "statefulsets":
		err = cs.AppsV1().StatefulSets(ns).Delete(ctx, req.Name, delOpts)
	case "daemonsets":
		err = cs.AppsV1().DaemonSets(ns).Delete(ctx, req.Name, delOpts)
	case "jobs":
		err = cs.BatchV1().Jobs(ns).Delete(ctx, req.Name, delOpts)
	case "cronjobs":
		err = cs.BatchV1().CronJobs(ns).Delete(ctx, req.Name, delOpts)
	case "configmaps":
		err = cs.CoreV1().ConfigMaps(ns).Delete(ctx, req.Name, delOpts)
	case "secrets":
		err = cs.CoreV1().Secrets(ns).Delete(ctx, req.Name, delOpts)
	case "ingresses":
		err = cs.NetworkingV1().Ingresses(ns).Delete(ctx, req.Name, delOpts)
	case "endpoints":
		err = cs.CoreV1().Endpoints(ns).Delete(ctx, req.Name, delOpts)
	case "pvcs":
		err = cs.CoreV1().PersistentVolumeClaims(ns).Delete(ctx, req.Name, delOpts)
	case "pvs":
		err = cs.CoreV1().PersistentVolumes().Delete(ctx, req.Name, delOpts)
	case "nodes":
		err = cs.CoreV1().Nodes().Delete(ctx, req.Name, delOpts)
	case "namespaces":
		err = cs.CoreV1().Namespaces().Delete(ctx, req.Name, delOpts)
	default:
		json.NewEncoder(w).Encode(K8sResponse{Error: "unsupported resource type for delete: " + req.Resource, DurationMs: ms(t0)})
		return
	}

	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{
		Success:    true,
		Message:    fmt.Sprintf("%s/%s deleted", req.Resource, req.Name),
		DurationMs: ms(t0),
	})
}

// ── /scale ─────────────────────────────────────────────────────────────────

func (s *Server) handleK8sScale(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	if req.Resource == "" || req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "resource and name are required", DurationMs: ms(t0)})
		return
	}
	if req.Replicas == nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "replicas is required", DurationMs: ms(t0)})
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

	replicas := *req.Replicas
	patch := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)

	switch req.Resource {
	case "deployments":
		_, err = cs.AppsV1().Deployments(ns).Patch(ctx, req.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	case "statefulsets":
		_, err = cs.AppsV1().StatefulSets(ns).Patch(ctx, req.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	default:
		json.NewEncoder(w).Encode(K8sResponse{Error: "scale is only supported for deployments and statefulsets", DurationMs: ms(t0)})
		return
	}

	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{
		Success:    true,
		Message:    fmt.Sprintf("%s/%s scaled to %d replicas", req.Resource, req.Name, replicas),
		DurationMs: ms(t0),
	})
}

// ── /restart ───────────────────────────────────────────────────────────────

func (s *Server) handleK8sRestart(w http.ResponseWriter, r *http.Request, req K8sRequest) {
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

	// Rolling restart is done by patching the pod template annotation with a timestamp
	restartPatch := fmt.Sprintf(
		`{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":"%s"}}}}}`,
		time.Now().Format(time.RFC3339),
	)

	switch req.Resource {
	case "deployments":
		_, err = cs.AppsV1().Deployments(ns).Patch(ctx, req.Name, types.MergePatchType, []byte(restartPatch), metav1.PatchOptions{})
	case "statefulsets":
		_, err = cs.AppsV1().StatefulSets(ns).Patch(ctx, req.Name, types.MergePatchType, []byte(restartPatch), metav1.PatchOptions{})
	case "daemonsets":
		_, err = cs.AppsV1().DaemonSets(ns).Patch(ctx, req.Name, types.MergePatchType, []byte(restartPatch), metav1.PatchOptions{})
	default:
		json.NewEncoder(w).Encode(K8sResponse{Error: "restart is only supported for deployments, statefulsets, and daemonsets", DurationMs: ms(t0)})
		return
	}

	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{
		Success:    true,
		Message:    fmt.Sprintf("%s/%s rolling restart initiated", req.Resource, req.Name),
		DurationMs: ms(t0),
	})
}

// ── /cordon ────────────────────────────────────────────────────────────────

func (s *Server) handleK8sCordon(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	if req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "node name is required", DurationMs: ms(t0)})
		return
	}

	cs, _, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), k8sTimeout)
	defer cancel()

	unschedulable := !req.Uncordon // cordon = true, uncordon = false
	patch := fmt.Sprintf(`{"spec":{"unschedulable":%t}}`, unschedulable)

	_, err = cs.CoreV1().Nodes().Patch(ctx, req.Name, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	action := "cordoned"
	if req.Uncordon {
		action = "uncordoned"
	}
	json.NewEncoder(w).Encode(K8sResponse{
		Success:    true,
		Message:    fmt.Sprintf("node %s %s", req.Name, action),
		DurationMs: ms(t0),
	})
}

// ── /drain ─────────────────────────────────────────────────────────────────

func (s *Server) handleK8sDrain(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	if req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "node name is required", DurationMs: ms(t0)})
		return
	}

	cs, _, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute) // drain can be slow
	defer cancel()

	// Step 1: Cordon the node
	cordonPatch := `{"spec":{"unschedulable":true}}`
	_, err = cs.CoreV1().Nodes().Patch(ctx, req.Name, types.MergePatchType, []byte(cordonPatch), metav1.PatchOptions{})
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "cordon failed: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	// Step 2: Evict all non-DaemonSet pods on the node
	pods, err := cs.CoreV1().Pods("").List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + req.Name,
	})
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "list pods: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	evicted := 0
	var evictErrors []string
	for _, pod := range pods.Items {
		// Skip mirror pods and DaemonSet pods
		if _, isMirror := pod.Annotations["kubernetes.io/config.mirror"]; isMirror {
			continue
		}
		if isDaemonSetPod(cs, &pod) {
			continue
		}

		eviction := &policyv1.Eviction{
			ObjectMeta: metav1.ObjectMeta{
				Name:      pod.Name,
				Namespace: pod.Namespace,
			},
		}
		if req.GracePeriod != nil {
			eviction.DeleteOptions = &metav1.DeleteOptions{GracePeriodSeconds: req.GracePeriod}
		}

		if evErr := cs.CoreV1().Pods(pod.Namespace).EvictV1(ctx, eviction); evErr != nil {
			evictErrors = append(evictErrors, fmt.Sprintf("%s/%s: %s", pod.Namespace, pod.Name, evErr.Error()))
		} else {
			evicted++
		}
	}

	msg := fmt.Sprintf("node %s drained: %d pods evicted", req.Name, evicted)
	if len(evictErrors) > 0 {
		msg += fmt.Sprintf(", %d eviction errors", len(evictErrors))
	}

	resp := K8sResponse{
		Success:    len(evictErrors) == 0,
		Message:    msg,
		DurationMs: ms(t0),
	}
	if len(evictErrors) > 0 {
		resp.Error = fmt.Sprintf("%d eviction errors (node is cordoned)", len(evictErrors))
	}
	json.NewEncoder(w).Encode(resp)
}

// isDaemonSetPod checks if a pod is owned by a DaemonSet.
func isDaemonSetPod(cs *kubernetes.Clientset, pod *corev1.Pod) bool {
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}
