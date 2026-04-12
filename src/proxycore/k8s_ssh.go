package proxycore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

// ── SSH kubectl gateway ───────────────────────────────────────────────────────
//
// Translates each K8s gateway endpoint into a kubectl command executed over SSH.
// The remote host must have kubectl installed and a valid kubeconfig in place.

const kubectlTimeout = 30 * time.Second

// handleK8sSSHGateway routes SSH-mode requests to the appropriate kubectl handler.
func (s *Server) handleK8sSSHGateway(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()

	if req.SSHConnection == nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "sshConnection is required for SSH mode", DurationMs: ms(t0)})
		return
	}

	sshClient, err := s.getPooledSSHClient(*req.SSHConnection)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "ssh connect: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/")
	switch path {
	case "test":
		s.handleK8sSSHTest(w, r, req, sshClient)
	case "contexts":
		s.handleK8sSSHContexts(w, r, req, sshClient)
	case "namespaces":
		s.handleK8sSSHNamespaces(w, r, req, sshClient)
	case "resources":
		s.handleK8sSSHResources(w, r, req, sshClient)
	case "resource":
		s.handleK8sSSHResource(w, r, req, sshClient)
	case "describe":
		s.handleK8sSSHDescribe(w, r, req, sshClient)
	case "yaml":
		s.handleK8sSSHYAML(w, r, req, sshClient)
	case "logs":
		s.handleK8sSSHLogs(w, r, req, sshClient)
	case "logs/stream":
		s.handleK8sSSHLogStream(w, r, req, sshClient)
	case "events":
		s.handleK8sSSHEvents(w, r, req, sshClient)
	case "api-resources":
		s.handleK8sSSHAPIResources(w, r, req, sshClient)
	case "top/pods":
		s.handleK8sSSHTopPods(w, r, req, sshClient)
	case "top/nodes":
		s.handleK8sSSHTopNodes(w, r, req, sshClient)
	// Write operations
	case "apply":
		s.handleK8sSSHApply(w, r, req, sshClient)
	case "delete":
		s.handleK8sSSHDelete(w, r, req, sshClient)
	case "scale":
		s.handleK8sSSHScale(w, r, req, sshClient)
	case "restart":
		s.handleK8sSSHRestart(w, r, req, sshClient)
	case "cordon":
		s.handleK8sSSHCordon(w, r, req, sshClient)
	case "drain":
		s.handleK8sSSHDrain(w, r, req, sshClient)
	// Port-forward
	case "portforward/start":
		s.handleK8sSSHPortForwardStart(w, r, req, sshClient)
	case "portforward/stop":
		s.handleK8sSSHPortForwardStop(w, r, req)
	case "portforward/list":
		s.handleK8sSSHPortForwardList(w, r, req)
	// Advanced features
	case "helm/releases":
		s.handleK8sSSHHelmReleases(w, r, req, sshClient)
	case "crds":
		s.handleK8sSSHCRDs(w, r, req, sshClient)
	default:
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(K8sResponse{Error: "unknown k8s SSH endpoint: " + path})
	}
}

// ── kubectl execution helpers ─────────────────────────────────────────────────

// execKubectl runs a kubectl command over SSH and returns stdout, stderr, and error.
func execKubectl(sshClient *ssh.Client, args ...string) ([]byte, string, error) {
	sess, err := sshClient.NewSession()
	if err != nil {
		return nil, "", fmt.Errorf("ssh session: %w", err)
	}
	defer sess.Close()

	var stdout, stderr bytes.Buffer
	sess.Stdout = &stdout
	sess.Stderr = &stderr

	cmd := "kubectl " + strings.Join(args, " ")
	err = sess.Run(cmd)

	return stdout.Bytes(), stderr.String(), err
}

// execKubectlWithContext runs kubectl with a context timeout.
func execKubectlWithContext(ctx context.Context, sshClient *ssh.Client, args ...string) ([]byte, string, error) {
	sess, err := sshClient.NewSession()
	if err != nil {
		return nil, "", fmt.Errorf("ssh session: %w", err)
	}

	var stdout, stderr bytes.Buffer
	sess.Stdout = &stdout
	sess.Stderr = &stderr

	cmd := "kubectl " + strings.Join(args, " ")

	done := make(chan error, 1)
	go func() {
		done <- sess.Run(cmd)
	}()

	select {
	case <-ctx.Done():
		sess.Close()
		return nil, "", ctx.Err()
	case err := <-done:
		sess.Close()
		return stdout.Bytes(), stderr.String(), err
	}
}

// execKubectlStdin runs kubectl with stdin data piped in (for apply).
func execKubectlStdin(sshClient *ssh.Client, stdinData string, args ...string) ([]byte, string, error) {
	sess, err := sshClient.NewSession()
	if err != nil {
		return nil, "", fmt.Errorf("ssh session: %w", err)
	}
	defer sess.Close()

	var stdout, stderr bytes.Buffer
	sess.Stdout = &stdout
	sess.Stderr = &stderr
	sess.Stdin = strings.NewReader(stdinData)

	cmd := "kubectl " + strings.Join(args, " ")
	err = sess.Run(cmd)

	return stdout.Bytes(), stderr.String(), err
}

// kubectlContextArgs returns --context and --namespace args if set.
func kubectlContextArgs(req K8sRequest) []string {
	var args []string
	if req.Context != "" {
		args = append(args, "--context", req.Context)
	}
	return args
}

// kubectlNS returns the namespace or "default".
func kubectlNS(req K8sRequest) string {
	if req.Namespace == "" {
		return "default"
	}
	return req.Namespace
}

// parseKubectlList extracts .items from a JSON list response.
func parseKubectlList(raw []byte) ([]map[string]any, error) {
	var result struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	if result.Items == nil {
		result.Items = []map[string]any{}
	}
	return result.Items, nil
}

// kubectlErrorMessage extracts a useful error message from kubectl stderr.
func kubectlErrorMessage(stderr string, err error) string {
	s := strings.TrimSpace(stderr)
	if s != "" {
		// Common kubectl error prefixes
		s = strings.TrimPrefix(s, "error: ")
		s = strings.TrimPrefix(s, "Error from server: ")
		return s
	}
	if err != nil {
		return err.Error()
	}
	return "unknown kubectl error"
}

// ── /test ─────────────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHTest(w http.ResponseWriter, r *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), kubectlTimeout)
	defer cancel()

	args := append(kubectlContextArgs(req), "version", "-o", "json")
	stdout, stderr, err := execKubectlWithContext(ctx, sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	var ver struct {
		ServerVersion struct {
			GitVersion string `json:"gitVersion"`
			Platform   string `json:"platform"`
			GoVersion  string `json:"goVersion"`
		} `json:"serverVersion"`
	}
	if err := json.Unmarshal(stdout, &ver); err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "parse version: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{
		Version:    ver.ServerVersion.GitVersion,
		Item:       map[string]any{"platform": ver.ServerVersion.Platform, "goVersion": ver.ServerVersion.GoVersion},
		DurationMs: ms(t0),
	})
}

// ── /contexts ─────────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHContexts(w http.ResponseWriter, _ *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()

	// Get current context
	currentOut, _, _ := execKubectl(sshClient, "config", "current-context")
	currentCtx := strings.TrimSpace(string(currentOut))

	// Get all contexts as JSON
	args := []string{"config", "get-contexts", "-o", "name"}
	stdout, stderr, err := execKubectl(sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	var contexts []K8sContext
	for _, line := range strings.Split(strings.TrimSpace(string(stdout)), "\n") {
		name := strings.TrimSpace(line)
		if name == "" {
			continue
		}
		contexts = append(contexts, K8sContext{
			Name:    name,
			Current: name == currentCtx,
		})
	}

	if contexts == nil {
		contexts = []K8sContext{}
	}

	json.NewEncoder(w).Encode(K8sResponse{Contexts: contexts, DurationMs: ms(t0)})
}

// ── /namespaces ───────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHNamespaces(w http.ResponseWriter, r *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), kubectlTimeout)
	defer cancel()

	args := append(kubectlContextArgs(req), "get", "namespaces", "-o", "json")
	stdout, stderr, err := execKubectlWithContext(ctx, sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	items, err := parseKubectlList(stdout)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "parse namespaces: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	var namespaces []string
	for _, item := range items {
		if meta, ok := item["metadata"].(map[string]any); ok {
			if name, ok := meta["name"].(string); ok {
				namespaces = append(namespaces, name)
			}
		}
	}
	if namespaces == nil {
		namespaces = []string{}
	}

	json.NewEncoder(w).Encode(K8sResponse{Namespaces: namespaces, DurationMs: ms(t0)})
}

// ── /resources ────────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHResources(w http.ResponseWriter, r *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), kubectlTimeout)
	defer cancel()

	if req.Resource == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "resource type is required", DurationMs: ms(t0)})
		return
	}

	args := append(kubectlContextArgs(req), "get", req.Resource, "-n", kubectlNS(req), "-o", "json")
	if req.LabelSelector != "" {
		args = append(args, "--selector", req.LabelSelector)
	}
	if req.FieldSelector != "" {
		args = append(args, "--field-selector", req.FieldSelector)
	}
	if req.Limit > 0 {
		args = append(args, fmt.Sprintf("--chunk-size=%d", req.Limit))
	}

	stdout, stderr, err := execKubectlWithContext(ctx, sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	items, err := parseKubectlList(stdout)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "parse resources: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{Items: items, DurationMs: ms(t0)})
}

// ── /resource ─────────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHResource(w http.ResponseWriter, r *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), kubectlTimeout)
	defer cancel()

	if req.Resource == "" || req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "resource type and name are required", DurationMs: ms(t0)})
		return
	}

	args := append(kubectlContextArgs(req), "get", req.Resource, req.Name, "-n", kubectlNS(req), "-o", "json")
	stdout, stderr, err := execKubectlWithContext(ctx, sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	var item map[string]any
	if err := json.Unmarshal(stdout, &item); err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "parse resource: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{Item: item, DurationMs: ms(t0)})
}

// ── /describe ─────────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHDescribe(w http.ResponseWriter, r *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), kubectlTimeout)
	defer cancel()

	if req.Resource == "" || req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "resource type and name are required", DurationMs: ms(t0)})
		return
	}

	args := append(kubectlContextArgs(req), "describe", req.Resource, req.Name, "-n", kubectlNS(req))
	stdout, stderr, err := execKubectlWithContext(ctx, sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{Logs: string(stdout), DurationMs: ms(t0)})
	_ = stderr
}

// ── /yaml ─────────────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHYAML(w http.ResponseWriter, r *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), kubectlTimeout)
	defer cancel()

	if req.Resource == "" || req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "resource type and name are required", DurationMs: ms(t0)})
		return
	}

	args := append(kubectlContextArgs(req), "get", req.Resource, req.Name, "-n", kubectlNS(req), "-o", "yaml")
	stdout, stderr, err := execKubectlWithContext(ctx, sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{YAML: string(stdout), DurationMs: ms(t0)})
	_ = stderr
}

// ── /logs ─────────────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHLogs(w http.ResponseWriter, r *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), kubectlTimeout)
	defer cancel()

	if req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "pod name is required", DurationMs: ms(t0)})
		return
	}

	args := append(kubectlContextArgs(req), "logs", req.Name, "-n", kubectlNS(req))
	if req.Container != "" {
		args = append(args, "-c", req.Container)
	}
	if req.TailLines > 0 {
		args = append(args, fmt.Sprintf("--tail=%d", req.TailLines))
	}

	stdout, stderr, err := execKubectlWithContext(ctx, sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{Logs: string(stdout), DurationMs: ms(t0)})
	_ = stderr
}

// ── /events ───────────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHEvents(w http.ResponseWriter, r *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), kubectlTimeout)
	defer cancel()

	args := append(kubectlContextArgs(req), "get", "events", "-n", kubectlNS(req), "-o", "json")
	if req.FieldSelector != "" {
		args = append(args, "--field-selector", req.FieldSelector)
	}

	stdout, stderr, err := execKubectlWithContext(ctx, sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	items, err := parseKubectlList(stdout)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "parse events: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{Events: items, DurationMs: ms(t0)})
}

// ── /api-resources ────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHAPIResources(w http.ResponseWriter, r *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), kubectlTimeout)
	defer cancel()

	args := append(kubectlContextArgs(req), "api-resources", "-o", "wide", "--no-headers")
	stdout, stderr, err := execKubectlWithContext(ctx, sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	var resources []K8sAPIResource
	for _, line := range strings.Split(string(stdout), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		// api-resources -o wide --no-headers columns:
		// NAME SHORTNAMES APIVERSION NAMESPACED KIND VERBS
		// or: NAME SHORTNAMES APIGROUP APIVERSION NAMESPACED KIND VERBS
		//
		// The output varies by kubectl version. We parse based on NAMESPACED column (true/false).
		namespaced := false
		kind := ""
		group := ""
		version := ""

		// Find the "true"/"false" field to orient parsing
		for i, f := range fields {
			if f == "true" || f == "false" {
				namespaced = f == "true"
				if i+1 < len(fields) {
					kind = fields[i+1]
				}
				// APIVERSION is the field before NAMESPACED; may include group/version
				if i-1 >= 0 {
					parts := strings.SplitN(fields[i-1], "/", 2)
					if len(parts) == 2 {
						group = parts[0]
						version = parts[1]
					} else {
						version = parts[0]
					}
				}
				break
			}
		}

		if kind == "" {
			continue
		}

		resources = append(resources, K8sAPIResource{
			Name:       fields[0],
			Kind:       kind,
			Namespaced: namespaced,
			Group:      group,
			Version:    version,
		})
	}

	if resources == nil {
		resources = []K8sAPIResource{}
	}
	json.NewEncoder(w).Encode(K8sResponse{Resources: resources, DurationMs: ms(t0)})
}

// ── /top/pods ─────────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHTopPods(w http.ResponseWriter, r *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), kubectlTimeout)
	defer cancel()

	args := append(kubectlContextArgs(req), "top", "pods", "-n", kubectlNS(req), "--no-headers")
	stdout, stderr, err := execKubectlWithContext(ctx, sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	items := parseTopOutput(stdout)
	json.NewEncoder(w).Encode(K8sResponse{Items: items, DurationMs: ms(t0)})
}

// ── /top/nodes ────────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHTopNodes(w http.ResponseWriter, r *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), kubectlTimeout)
	defer cancel()

	args := append(kubectlContextArgs(req), "top", "nodes", "--no-headers")
	stdout, stderr, err := execKubectlWithContext(ctx, sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	items := parseTopOutput(stdout)
	json.NewEncoder(w).Encode(K8sResponse{Items: items, DurationMs: ms(t0)})
}

// parseTopOutput parses the tabular output of kubectl top (pods or nodes).
func parseTopOutput(raw []byte) []map[string]any {
	var items []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		item := map[string]any{"name": fields[0]}
		if len(fields) >= 3 {
			item["cpu"] = fields[1]
			item["memory"] = fields[2]
		}
		if len(fields) >= 5 {
			item["cpuPercent"] = fields[3]
			item["memoryPercent"] = fields[4]
		}
		items = append(items, item)
	}
	if items == nil {
		items = []map[string]any{}
	}
	return items
}

// ── Write operations ──────────────────────────────────────────────────────────

// ── /apply ────────────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHApply(w http.ResponseWriter, _ *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	if req.YAML == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "yaml is required", DurationMs: ms(t0)})
		return
	}

	args := append(kubectlContextArgs(req), "apply", "-f", "-", "-n", kubectlNS(req))
	stdout, stderr, err := execKubectlStdin(sshClient, req.YAML, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{
		Success:    true,
		Message:    strings.TrimSpace(string(stdout)),
		DurationMs: ms(t0),
	})
}

// ── /delete ───────────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHDelete(w http.ResponseWriter, _ *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	if req.Resource == "" || req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "resource type and name are required", DurationMs: ms(t0)})
		return
	}

	args := append(kubectlContextArgs(req), "delete", req.Resource, req.Name, "-n", kubectlNS(req))
	if req.Force {
		args = append(args, "--force")
	}
	if req.GracePeriod != nil {
		args = append(args, fmt.Sprintf("--grace-period=%d", *req.GracePeriod))
	}

	stdout, stderr, err := execKubectl(sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{
		Success:    true,
		Message:    strings.TrimSpace(string(stdout)),
		DurationMs: ms(t0),
	})
}

// ── /scale ────────────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHScale(w http.ResponseWriter, _ *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	if req.Resource == "" || req.Name == "" || req.Replicas == nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "resource, name, and replicas are required", DurationMs: ms(t0)})
		return
	}

	args := append(kubectlContextArgs(req), "scale",
		fmt.Sprintf("%s/%s", req.Resource, req.Name),
		"-n", kubectlNS(req),
		fmt.Sprintf("--replicas=%d", *req.Replicas))

	stdout, stderr, err := execKubectl(sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{
		Success:    true,
		Message:    strings.TrimSpace(string(stdout)),
		DurationMs: ms(t0),
	})
}

// ── /restart ──────────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHRestart(w http.ResponseWriter, _ *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	if req.Resource == "" || req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "resource type and name are required", DurationMs: ms(t0)})
		return
	}

	args := append(kubectlContextArgs(req), "rollout", "restart",
		fmt.Sprintf("%s/%s", req.Resource, req.Name),
		"-n", kubectlNS(req))

	stdout, stderr, err := execKubectl(sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{
		Success:    true,
		Message:    strings.TrimSpace(string(stdout)),
		DurationMs: ms(t0),
	})
}

// ── /cordon ───────────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHCordon(w http.ResponseWriter, _ *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	if req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "node name is required", DurationMs: ms(t0)})
		return
	}

	verb := "cordon"
	if req.Uncordon {
		verb = "uncordon"
	}
	args := append(kubectlContextArgs(req), verb, req.Name)

	stdout, stderr, err := execKubectl(sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{
		Success:    true,
		Message:    strings.TrimSpace(string(stdout)),
		DurationMs: ms(t0),
	})
}

// ── /drain ────────────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHDrain(w http.ResponseWriter, _ *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	if req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "node name is required", DurationMs: ms(t0)})
		return
	}

	args := append(kubectlContextArgs(req), "drain", req.Name, "--ignore-daemonsets", "--delete-emptydir-data")
	if req.Force {
		args = append(args, "--force")
	}
	if req.GracePeriod != nil {
		args = append(args, fmt.Sprintf("--grace-period=%d", *req.GracePeriod))
	}

	stdout, stderr, err := execKubectl(sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{
		Success:    true,
		Message:    strings.TrimSpace(string(stdout)),
		DurationMs: ms(t0),
	})
}

// ── /helm/releases ────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHHelmReleases(w http.ResponseWriter, r *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), kubectlTimeout)
	defer cancel()

	// Read helm release secrets via kubectl (no helm binary required)
	ns := req.Namespace
	args := kubectlContextArgs(req)
	if ns != "" {
		args = append(args, "get", "secrets", "-n", ns, "-l", "owner=helm", "-o", "json")
	} else {
		args = append(args, "get", "secrets", "--all-namespaces", "-l", "owner=helm", "-o", "json")
	}

	stdout, stderr, err := execKubectlWithContext(ctx, sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	items, err := parseKubectlList(stdout)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "parse helm secrets: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{Items: items, DurationMs: ms(t0)})
}

// ── /crds ─────────────────────────────────────────────────────────────────────

func (s *Server) handleK8sSSHCRDs(w http.ResponseWriter, r *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	ctx, cancel := context.WithTimeout(r.Context(), kubectlTimeout)
	defer cancel()

	args := append(kubectlContextArgs(req), "get", "crds", "-o", "json")
	stdout, stderr, err := execKubectlWithContext(ctx, sshClient, args...)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: kubectlErrorMessage(stderr, err), DurationMs: ms(t0)})
		return
	}

	items, err := parseKubectlList(stdout)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "parse crds: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{Items: items, DurationMs: ms(t0)})
}
