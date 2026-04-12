package proxycore

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/gorilla/websocket"
	"golang.org/x/crypto/ssh"
)

// handleK8sSSHExec upgrades to WebSocket and runs kubectl exec over SSH with PTY.
// Routed by path (/k8s/ssh-exec) before gateway dispatch.
//
// Query parameters: host, port, user, authMethod, password, privateKey, passphrase,
//
//	jumpHosts, kiAnswers, context, namespace, name (pod), container, command (repeated), tty
func (s *Server) handleK8sSSHExec(w http.ResponseWriter, r *http.Request) {
	setCORS(w, r)

	q := r.URL.Query()

	// Parse SSH connection from query params
	conn := SSHConnection{
		Host:       q.Get("host"),
		User:       q.Get("user"),
		AuthMethod: q.Get("authMethod"),
		Password:   q.Get("password"),
		PrivateKey: q.Get("privateKey"),
		Passphrase: q.Get("passphrase"),
	}
	if p, err := strconv.Atoi(q.Get("port")); err == nil && p > 0 {
		conn.Port = p
	}
	if jh := q.Get("jumpHosts"); jh != "" {
		_ = json.Unmarshal([]byte(jh), &conn.JumpHosts)
	}
	if ki := q.Get("kiAnswers"); ki != "" {
		_ = json.Unmarshal([]byte(ki), &conn.KIAnswers)
	}

	// Parse kubectl params
	podName := q.Get("name")
	ns := q.Get("namespace")
	container := q.Get("container")
	kctx := q.Get("context")
	tty := q.Get("tty") == "true"
	cmd := q["command"]

	if conn.Host == "" || conn.User == "" {
		http.Error(w, `{"error":"ssh host and user are required"}`, http.StatusBadRequest)
		return
	}
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

	// Get pooled SSH client
	sshClient, err := s.getPooledSSHClient(conn)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadGateway)
		return
	}

	// Upgrade to WebSocket
	wsConn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer wsConn.Close()

	// Build kubectl exec command
	var kubectlCmd string
	{
		var parts []string
		parts = append(parts, "kubectl")
		if kctx != "" {
			parts = append(parts, "--context", kctx)
		}
		parts = append(parts, "exec")
		if tty {
			parts = append(parts, "-it")
		} else {
			parts = append(parts, "-i")
		}
		parts = append(parts, podName, "-n", ns)
		if container != "" {
			parts = append(parts, "-c", container)
		}
		parts = append(parts, "--")
		parts = append(parts, cmd...)
		kubectlCmd = strings.Join(parts, " ")
	}

	// Open SSH session
	sess, err := sshClient.NewSession()
	if err != nil {
		errMsg, _ := json.Marshal(map[string]string{"type": "error", "error": "ssh session: " + err.Error()})
		wsConn.WriteMessage(websocket.TextMessage, errMsg) //nolint:errcheck
		return
	}
	defer sess.Close()

	stdin, err := sess.StdinPipe()
	if err != nil {
		return
	}
	stdout, err := sess.StdoutPipe()
	if err != nil {
		return
	}
	stderr, err := sess.StderrPipe()
	if err != nil {
		return
	}

	if tty {
		modes := ssh.TerminalModes{
			ssh.ECHO:          1,
			ssh.TTY_OP_ISPEED: 14400,
			ssh.TTY_OP_OSPEED: 14400,
		}
		if err := sess.RequestPty("xterm-256color", 24, 80, modes); err != nil {
			errMsg, _ := json.Marshal(map[string]string{"type": "error", "error": "pty: " + err.Error()})
			wsConn.WriteMessage(websocket.TextMessage, errMsg) //nolint:errcheck
			return
		}
	}

	if err := sess.Start(kubectlCmd); err != nil {
		errMsg, _ := json.Marshal(map[string]string{"type": "error", "error": "start kubectl exec: " + err.Error()})
		wsConn.WriteMessage(websocket.TextMessage, errMsg) //nolint:errcheck
		return
	}

	var wsMu sync.Mutex

	// WebSocket → SSH stdin (with resize support)
	go func() {
		defer stdin.Close()
		for {
			msgType, data, err := wsConn.ReadMessage()
			if err != nil {
				return
			}
			switch msgType {
			case websocket.BinaryMessage:
				if _, err := stdin.Write(data); err != nil {
					return
				}
			case websocket.TextMessage:
				var ctrl struct {
					Type string `json:"type"`
					Cols int    `json:"cols"`
					Rows int    `json:"rows"`
				}
				if json.Unmarshal(data, &ctrl) == nil && ctrl.Type == "resize" && ctrl.Cols > 0 && ctrl.Rows > 0 {
					_ = sess.WindowChange(ctrl.Rows, ctrl.Cols)
				}
			}
		}
	}()

	// SSH stdout → WebSocket
	go func() {
		buf := make([]byte, 32*1024)
		for {
			n, err := stdout.Read(buf)
			if n > 0 {
				wsMu.Lock()
				wsConn.WriteMessage(websocket.BinaryMessage, buf[:n]) //nolint:errcheck
				wsMu.Unlock()
			}
			if err != nil {
				return
			}
		}
	}()

	// SSH stderr → WebSocket
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := stderr.Read(buf)
			if n > 0 {
				wsMu.Lock()
				wsConn.WriteMessage(websocket.BinaryMessage, buf[:n]) //nolint:errcheck
				wsMu.Unlock()
			}
			if err != nil {
				return
			}
		}
	}()

	// Block until kubectl exec finishes
	_ = sess.Wait()

	wsMu.Lock()
	exitMsg, _ := json.Marshal(map[string]string{"type": "exit"})
	wsConn.WriteMessage(websocket.TextMessage, exitMsg) //nolint:errcheck
	wsMu.Unlock()
}

// formatSSHExecQueryParams builds query string for SSH exec from an SSHConnection + K8s params.
// This is a utility for the frontend to construct the WebSocket URL.
func formatSSHExecQueryParams(conn SSHConnection, ns, pod, container, kctx string, tty bool, cmd []string) string {
	params := fmt.Sprintf("host=%s&port=%d&user=%s&authMethod=%s&name=%s&namespace=%s&tty=%v",
		conn.Host, sshPort(conn), conn.User, conn.AuthMethod, pod, ns, tty)
	if container != "" {
		params += "&container=" + container
	}
	if kctx != "" {
		params += "&context=" + kctx
	}
	for _, c := range cmd {
		params += "&command=" + c
	}
	return params
}
