package proxycore

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

// k8sSSHPortForward represents an active port-forward session over SSH.
// It combines a remote kubectl port-forward process with a local SSH tunnel.
type k8sSSHPortForward struct {
	ID         string    `json:"id"`
	Pod        string    `json:"pod"`
	Namespace  string    `json:"namespace"`
	PodPort    int       `json:"podPort"`
	LocalPort  int       `json:"localPort"`
	RemotePort int       `json:"remotePort"` // port on SSH host where kubectl binds
	StartedAt  time.Time `json:"startedAt"`
	session    *ssh.Session
	listener   net.Listener
	cancel     context.CancelFunc
}

// handleK8sSSHPortForwardStart creates a port-forward session via kubectl over SSH.
//
// Flow:
//  1. Open SSH session and run: kubectl port-forward pod/<name> -n <ns> <remotePort>:<podPort>
//  2. Wait for the "Forwarding from 127.0.0.1:<remotePort>" line from kubectl stdout
//  3. Set up a local TCP listener that tunnels connections through SSH to the remote port
//  4. Return the local port to the caller
func (s *Server) handleK8sSSHPortForwardStart(w http.ResponseWriter, _ *http.Request, req K8sRequest, sshClient *ssh.Client) {
	t0 := time.Now()
	if req.Name == "" || req.PodPort == 0 {
		json.NewEncoder(w).Encode(K8sResponse{Error: "pod name and podPort are required", DurationMs: ms(t0)})
		return
	}

	ns := kubectlNS(req)

	// Pick a free ephemeral port on the remote host by using port 0 in kubectl
	// kubectl will pick a free port and report it in its output.
	remotePort := 0

	// Open SSH session for the kubectl port-forward process
	sess, err := sshClient.NewSession()
	if err != nil {
		json.NewEncoder(w).Encode(K8sResponse{Error: "ssh session: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	stdout, err := sess.StdoutPipe()
	if err != nil {
		sess.Close()
		json.NewEncoder(w).Encode(K8sResponse{Error: "stdout pipe: " + err.Error(), DurationMs: ms(t0)})
		return
	}
	stderr, err := sess.StderrPipe()
	if err != nil {
		sess.Close()
		json.NewEncoder(w).Encode(K8sResponse{Error: "stderr pipe: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	// Build kubectl command
	args := kubectlContextArgs(req)
	cmd := fmt.Sprintf("kubectl %sport-forward pod/%s -n %s :%d",
		contextArgsString(args), req.Name, ns, req.PodPort)

	if err := sess.Start(cmd); err != nil {
		sess.Close()
		json.NewEncoder(w).Encode(K8sResponse{Error: "start kubectl: " + err.Error(), DurationMs: ms(t0)})
		return
	}

	// Read stderr in background to capture errors
	stderrCh := make(chan string, 1)
	go func() {
		b, _ := io.ReadAll(stderr)
		stderrCh <- string(b)
	}()

	// Wait for the "Forwarding from" line to extract the bound port
	scanner := bufio.NewScanner(stdout)
	readyCh := make(chan int, 1)
	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			// Expected: "Forwarding from 127.0.0.1:XXXXX -> YYYYY"
			if strings.HasPrefix(line, "Forwarding from 127.0.0.1:") {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) == 2 {
					portStr := strings.Fields(parts[1])[0]
					var port int
					fmt.Sscanf(portStr, "%d", &port)
					if port > 0 {
						readyCh <- port
						return
					}
				}
			}
		}
		readyCh <- 0
	}()

	// Wait for ready or error with timeout
	select {
	case rp := <-readyCh:
		if rp == 0 {
			sess.Close()
			stderrMsg := ""
			select {
			case stderrMsg = <-stderrCh:
			default:
			}
			json.NewEncoder(w).Encode(K8sResponse{Error: "kubectl port-forward failed to bind: " + stderrMsg, DurationMs: ms(t0)})
			return
		}
		remotePort = rp
	case <-time.After(15 * time.Second):
		sess.Close()
		json.NewEncoder(w).Encode(K8sResponse{Error: "kubectl port-forward timed out waiting for ready", DurationMs: ms(t0)})
		return
	}

	// Set up local TCP listener that tunnels through SSH
	localListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		sess.Close()
		json.NewEncoder(w).Encode(K8sResponse{Error: "local listen: " + err.Error(), DurationMs: ms(t0)})
		return
	}
	localPort := localListener.Addr().(*net.TCPAddr).Port

	ctx, cancel := context.WithCancel(context.Background())

	id := fmt.Sprintf("ssh-pf-%s-%s-%d-%d", ns, req.Name, req.PodPort, localPort)

	fwd := &k8sSSHPortForward{
		ID:         id,
		Pod:        req.Name,
		Namespace:  ns,
		PodPort:    req.PodPort,
		LocalPort:  localPort,
		RemotePort: remotePort,
		StartedAt:  time.Now(),
		session:    sess,
		listener:   localListener,
		cancel:     cancel,
	}

	s.k8sSSHPortForwards.Store(id, fwd)

	// Accept loop: tunnel local connections to remote port via SSH
	go func() {
		for {
			conn, err := localListener.Accept()
			if err != nil {
				return // listener closed
			}
			go tunnelSSHPortForward(ctx, sshClient, conn, remotePort)
		}
	}()

	// Cleanup goroutine: waits for cancel or kubectl exit
	go func() {
		waitCh := make(chan error, 1)
		go func() { waitCh <- sess.Wait() }()

		select {
		case <-ctx.Done():
			// Cancelled externally (stop request)
			sess.Close()
		case <-waitCh:
			// kubectl exited on its own
			cancel()
		}
		localListener.Close()
		s.k8sSSHPortForwards.Delete(id)
	}()

	json.NewEncoder(w).Encode(K8sResponse{
		Success:    true,
		Message:    fmt.Sprintf("port-forward %s:%d -> %s/%s:%d (via SSH)", "localhost", localPort, ns, req.Name, req.PodPort),
		Item:       map[string]any{"id": id, "localPort": localPort, "podPort": req.PodPort, "pod": req.Name, "namespace": ns},
		DurationMs: ms(t0),
	})
}

// tunnelSSHPortForward bridges a local TCP connection to the remote port via SSH.
func tunnelSSHPortForward(ctx context.Context, sshClient *ssh.Client, local net.Conn, remotePort int) {
	defer local.Close()

	remote, err := sshClient.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", remotePort))
	if err != nil {
		return
	}
	defer remote.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	// local → remote
	go func() {
		defer wg.Done()
		io.Copy(remote, local) //nolint:errcheck
	}()

	// remote → local
	go func() {
		defer wg.Done()
		io.Copy(local, remote) //nolint:errcheck
	}()

	// Wait for either direction to finish or context cancel
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		local.Close()
		remote.Close()
	}
}

// handleK8sSSHPortForwardStop terminates an active SSH port-forward session.
func (s *Server) handleK8sSSHPortForwardStop(w http.ResponseWriter, _ *http.Request, req K8sRequest) {
	t0 := time.Now()
	if req.ForwardID == "" {
		json.NewEncoder(w).Encode(K8sResponse{Error: "forwardId is required", DurationMs: ms(t0)})
		return
	}

	v, ok := s.k8sSSHPortForwards.Load(req.ForwardID)
	if !ok {
		json.NewEncoder(w).Encode(K8sResponse{Error: "port-forward session not found: " + req.ForwardID, DurationMs: ms(t0)})
		return
	}

	fwd := v.(*k8sSSHPortForward)
	fwd.cancel() // triggers cleanup goroutine

	json.NewEncoder(w).Encode(K8sResponse{
		Success:    true,
		Message:    fmt.Sprintf("port-forward %s stopped", req.ForwardID),
		DurationMs: ms(t0),
	})
}

// handleK8sSSHPortForwardList returns all active SSH port-forward sessions.
func (s *Server) handleK8sSSHPortForwardList(w http.ResponseWriter, _ *http.Request, req K8sRequest) {
	t0 := time.Now()
	var items []map[string]any

	s.k8sSSHPortForwards.Range(func(_, v any) bool {
		fwd := v.(*k8sSSHPortForward)
		items = append(items, map[string]any{
			"id":        fwd.ID,
			"pod":       fwd.Pod,
			"namespace": fwd.Namespace,
			"podPort":   fwd.PodPort,
			"localPort": fwd.LocalPort,
			"startedAt": fwd.StartedAt,
		})
		return true
	})

	if items == nil {
		items = []map[string]any{}
	}
	json.NewEncoder(w).Encode(K8sResponse{Items: items, DurationMs: ms(t0)})
}

// contextArgsString formats kubectl context args as a command prefix string.
func contextArgsString(args []string) string {
	if len(args) == 0 {
		return ""
	}
	return strings.Join(args, " ") + " "
}
