package proxycore

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// handleK8sLogs returns complete pod logs as a single response.
func (s *Server) handleK8sLogs(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	t0 := time.Now()
	if req.Name == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "pod name is required", DurationMs: ms(t0)})
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

	logOpts := &corev1.PodLogOptions{}
	if req.Container != "" {
		logOpts.Container = req.Container
	}
	if req.TailLines > 0 {
		logOpts.TailLines = &req.TailLines
	}

	stream, err := cs.CoreV1().Pods(ns).GetLogs(req.Name, logOpts).Stream(ctx)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}
	defer stream.Close()

	body, err := io.ReadAll(stream)
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error(), DurationMs: ms(t0)})
		return
	}

	json.NewEncoder(w).Encode(K8sResponse{Logs: string(body), DurationMs: ms(t0)})
}

// handleK8sLogStream streams pod logs via SSE (Server-Sent Events).
func (s *Server) handleK8sLogStream(w http.ResponseWriter, r *http.Request, req K8sRequest) {
	if req.Name == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(K8sResponse{Error: "pod name is required"})
		return
	}

	cs, _, err := s.getPooledK8sClient(req.Kubeconfig, req.Context)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error()})
		return
	}

	ns := req.Namespace
	if ns == "" {
		ns = "default"
	}

	logOpts := &corev1.PodLogOptions{Follow: true}
	if req.Container != "" {
		logOpts.Container = req.Container
	}
	if req.TailLines > 0 {
		logOpts.TailLines = &req.TailLines
	}

	stream, err := cs.CoreV1().Pods(ns).GetLogs(req.Name, logOpts).Stream(r.Context())
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(K8sResponse{Error: err.Error()})
		return
	}
	defer stream.Close()

	// Switch to SSE
	setCORS(w, r)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	scanner := bufio.NewScanner(stream)
	ctx := r.Context()
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		line := scanner.Text()
		b, _ := json.Marshal(line)
		_, _ = w.Write([]byte("data: " + string(b) + "\n\n"))
		flusher.Flush()
	}
}
