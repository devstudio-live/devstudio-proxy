package proxycore

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

// k8sPortForward represents an active port-forward session.
type k8sPortForward struct {
	ID        string    `json:"id"`
	Pod       string    `json:"pod"`
	Namespace string    `json:"namespace"`
	PodPort   int       `json:"podPort"`
	LocalPort int       `json:"localPort"`
	Context   string    `json:"context"`
	StartedAt time.Time `json:"startedAt"`
	stopCh    chan struct{}
}

// handleK8sPortForwardStart creates a port-forward session to a pod.
func (s *Server) handleK8sPortForwardStart(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	if req.Name == "" || req.PodPort == 0 {
		json.NewEncoder(w).Encode(K8sResponse{Error: "pod name and podPort are required", DurationMs: ms(t0)})
		return
	}

	_, cfg, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	ns := req.Namespace
	if ns == "" {
		ns = "default"
	}

	// Determine local port — use requested or find a free one
	localPort := req.LocalPort
	if localPort == 0 {
		localPort, err = findFreePort()
		if err != nil {
			json.NewEncoder(w).Encode(K8sResponse{Error: "find free port: " + err.Error(), DurationMs: ms(t0)})
			return
		}
	}

	// Build the port-forward URL
	pfURL := buildPortForwardURL(cfg, ns, req.Name)

	transport, upgrader, err := spdy.RoundTripperFor(cfg)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "spdy transport: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", pfURL)

	stopCh := make(chan struct{})
	readyCh := make(chan struct{})

	ports := []string{fmt.Sprintf("%d:%d", localPort, req.PodPort)}

	// portforward.New needs a writer for output; discard it
	pf, err := portforward.New(dialer, ports, stopCh, readyCh, discardWriter{}, discardWriter{})
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "portforward setup: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	// Run in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- pf.ForwardPorts()
	}()

	// Wait for ready or error
	select {
	case <-readyCh:
		// Port forward is active
	case err := <-errCh:
		json.NewEncoder(w).Encode(K8sResponse{Error: "portforward failed: " + err.Error(), DurationMs: ms(t0)})
		return
	case <-time.After(10 * time.Second):
		close(stopCh)
		json.NewEncoder(w).Encode(K8sResponse{Error: "portforward timed out waiting for ready", DurationMs: ms(t0)})
		return
	}

	// Get the actual forwarded ports (in case local port was 0)
	forwardedPorts, err := pf.GetPorts()
	if err == nil && len(forwardedPorts) > 0 {
		localPort = int(forwardedPorts[0].Local)
	}

	// Generate session ID and store
	id := fmt.Sprintf("pf-%s-%s-%d-%d", ns, req.Name, req.PodPort, localPort)

	fwd := &k8sPortForward{
		ID:        id,
		Pod:       req.Name,
		Namespace: ns,
		PodPort:   req.PodPort,
		LocalPort: localPort,
		Context:   req.Context,
		StartedAt: time.Now(),
		stopCh:    stopCh,
	}

	s.k8sPortForwards.Store(id, fwd)

	// Clean up when the port-forward stops
	go func() {
		<-stopCh
		s.k8sPortForwards.Delete(id)
	}()

	json.NewEncoder(w).Encode(K8sResponse{
		Success:    true,
		Message:    fmt.Sprintf("port-forward %s:%d -> %s/%s:%d", "localhost", localPort, ns, req.Name, req.PodPort),
		Item:       map[string]any{"id": id, "localPort": localPort, "podPort": req.PodPort, "pod": req.Name, "namespace": ns},
		DurationMs: ms(t0),
	})
}

// handleK8sPortForwardStop terminates an active port-forward session.
func (s *Server) handleK8sPortForwardStop(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	if req.ForwardID == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "forwardId is required", DurationMs: ms(t0)})
		return
	}

	v, ok := s.k8sPortForwards.Load(req.ForwardID)
	if !ok {
		json.NewEncoder(w).Encode(K8sResponse{Error: "port-forward session not found: " + req.ForwardID, DurationMs: ms(t0)})
		return
	}

	fwd := v.(*k8sPortForward)
	close(fwd.stopCh)
	s.k8sPortForwards.Delete(req.ForwardID)

	json.NewEncoder(w).Encode(K8sResponse{
		Success:    true,
		Message:    fmt.Sprintf("port-forward %s stopped", req.ForwardID),
		DurationMs: ms(t0),
	})
}

// handleK8sPortForwardList returns all active port-forward sessions.
func (s *Server) handleK8sPortForwardList(w http.ResponseWriter, _ *http.Request, req K8sRequest) {
	t0 := time.Now()
	var items []map[string]any

	s.k8sPortForwards.Range(func(_, v any) bool {
		fwd := v.(*k8sPortForward)
		items = append(items, map[string]any{
			"id":        fwd.ID,
			"pod":       fwd.Pod,
			"namespace": fwd.Namespace,
			"podPort":   fwd.PodPort,
			"localPort": fwd.LocalPort,
			"context":   fwd.Context,
			"startedAt": fwd.StartedAt,
		})
		return true
	})

	if items == nil {
		items = []map[string]any{}
	}
	json.NewEncoder(w).Encode(K8sResponse{Items: items, DurationMs: ms(t0)})
}

// ── helpers ────────────────────────────────────────────────────────────────

func buildPortForwardURL(cfg *rest.Config, namespace, podName string) *url.URL {
	host := cfg.Host
	if host == "" {
		host = "https://localhost:6443"
	}
	u, _ := url.Parse(host)
	u.Path = fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/portforward", namespace, podName)
	return u
}

func findFreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port, nil
}

// discardWriter is a no-op io.Writer for portforward output.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
