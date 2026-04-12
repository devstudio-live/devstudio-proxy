package proxycore

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"

	"golang.org/x/crypto/ssh"
)

// handleK8sSSHLogStream streams pod logs via SSE using kubectl logs -f over SSH.
func (s *Server) handleK8sSSHLogStream(w http.ResponseWriter, r *http.Request, req K8sRequest, sshClient *ssh.Client) {
	if req.Name == "" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(K8sResponse{Error: "pod name is required"})
		return
	}

	ns := kubectlNS(req)

	// Build kubectl logs -f command
	args := kubectlContextArgs(req)
	cmd := fmt.Sprintf("kubectl %slogs -f %s -n %s",
		contextArgsString(args), req.Name, ns)
	if req.Container != "" {
		cmd += " -c " + req.Container
	}
	if req.TailLines > 0 {
		cmd += fmt.Sprintf(" --tail=%d", req.TailLines)
	}

	// Open SSH session
	sess, err := sshClient.NewSession()
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(K8sResponse{Error: "ssh session: " + err.Error()})
		return
	}

	stdout, err := sess.StdoutPipe()
	if err != nil {
		sess.Close()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(K8sResponse{Error: "stdout pipe: " + err.Error()})
		return
	}

	if err := sess.Start(cmd); err != nil {
		sess.Close()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(K8sResponse{Error: "start kubectl logs: " + err.Error()})
		return
	}

	// Switch to SSE
	setCORS(w, r)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		sess.Close()
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	scanner := bufio.NewScanner(stdout)

	// Clean up SSH session when done
	defer sess.Close()

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
