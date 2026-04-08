package proxycore

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
)

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// handleK8sExec upgrades the connection to WebSocket and attaches to a pod container.
// Routed directly (not through JSON-POST gateway) because WebSocket upgrade is needed.
//
// Query parameters: kubeconfig, context, namespace, name (pod), container, command (repeated), tty
func (s *Server) handleK8sExec(w http.ResponseWriter, r *http.Request) {
	setCORS(w, r)

	q := r.URL.Query()
	podName := q.Get("name")
	ns := q.Get("namespace")
	container := q.Get("container")
	kubeconfig := q.Get("kubeconfig")
	kctx := q.Get("context")
	tty := q.Get("tty") == "true"
	cmd := q["command"]

	if podName == "" {
		http.Error(w, `{"error":"pod name is required"}`, http.StatusBadRequest)
		return
	}
	if ns == "" {
		ns = "default"
	}
	if len(cmd) == 0 {
		cmd = []string{"/bin/sh"}
	}

	cs, cfg, err := s.getPooledK8sClient(kubeconfig, kctx)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadGateway)
		return
	}

	// Build SPDY exec request
	execReq := cs.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(ns).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   cmd,
			Stdin:     true,
			Stdout:    true,
			Stderr:    !tty,
			TTY:       tty,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(cfg, "POST", execReq.URL())
	if err != nil {
		http.Error(w, `{"error":"spdy executor: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	// Upgrade to WebSocket
	wsConn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already wrote HTTP error
	}
	defer wsConn.Close()

	// Bridge WebSocket <-> SPDY streams
	bridge := &wsBridge{conn: wsConn}

	streamErr := exec.StreamWithContext(r.Context(), remotecommand.StreamOptions{
		Stdin:  bridge,
		Stdout: bridge,
		Stderr: bridge,
		Tty:    tty,
	})

	if streamErr != nil {
		errMsg, _ := json.Marshal(map[string]string{"error": streamErr.Error()})
		wsConn.WriteMessage(websocket.TextMessage, errMsg)
	}
}

// wsBridge adapts a WebSocket connection to io.Reader/io.Writer for remotecommand.
type wsBridge struct {
	conn   *websocket.Conn
	mu     sync.Mutex
	reader io.Reader
}

// Read implements io.Reader — reads stdin from WebSocket binary messages.
func (b *wsBridge) Read(p []byte) (int, error) {
	for {
		if b.reader != nil {
			n, err := b.reader.Read(p)
			if n > 0 {
				return n, nil
			}
			if err != io.EOF {
				return 0, err
			}
			b.reader = nil
		}

		_, reader, err := b.conn.NextReader()
		if err != nil {
			return 0, err
		}
		b.reader = reader
	}
}

// Write implements io.Writer — sends stdout/stderr to WebSocket.
func (b *wsBridge) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.conn.WriteMessage(websocket.BinaryMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}
