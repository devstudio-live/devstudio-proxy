package proxycore

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"golang.org/x/crypto/ssh"
)

// sshTerminalSession tracks an active SSH terminal WebSocket session.
type sshTerminalSession struct {
	ID        string
	Host      string
	User      string
	ConnKey   string
	Session   *ssh.Session
	CreatedAt time.Time
	Cols      int
	Rows      int
}

// handleSSHTerminal upgrades the connection to WebSocket and bridges to an SSH PTY.
// Routed by path (/ssh/terminal) before gateway dispatch.
//
// Query parameters: host, port, user, authMethod, password, privateKey, passphrase, cols, rows
func (s *Server) handleSSHTerminal(w http.ResponseWriter, r *http.Request) {
	setCORS(w, r)

	q := r.URL.Query()
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

	cols, rows := 80, 24
	if c, err := strconv.Atoi(q.Get("cols")); err == nil && c > 0 {
		cols = c
	}
	if rr, err := strconv.Atoi(q.Get("rows")); err == nil && rr > 0 {
		rows = rr
	}

	if conn.Host == "" {
		http.Error(w, `{"error":"host is required"}`, http.StatusBadRequest)
		return
	}
	if conn.User == "" {
		http.Error(w, `{"error":"user is required"}`, http.StatusBadRequest)
		return
	}

	sshClient, err := s.getPooledSSHClient(conn)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadGateway)
		return
	}

	sess, err := sshClient.NewSession()
	if err != nil {
		http.Error(w, `{"error":"create session: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	stdin, err := sess.StdinPipe()
	if err != nil {
		sess.Close()
		http.Error(w, `{"error":"stdin pipe: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	stdout, err := sess.StdoutPipe()
	if err != nil {
		sess.Close()
		http.Error(w, `{"error":"stdout pipe: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	stderr, err := sess.StderrPipe()
	if err != nil {
		sess.Close()
		http.Error(w, `{"error":"stderr pipe: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	modes := ssh.TerminalModes{
		ssh.ECHO:          1,
		ssh.TTY_OP_ISPEED: 14400,
		ssh.TTY_OP_OSPEED: 14400,
	}
	if err := sess.RequestPty("xterm-256color", rows, cols, modes); err != nil {
		sess.Close()
		http.Error(w, `{"error":"request pty: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}
	if err := sess.Shell(); err != nil {
		sess.Close()
		http.Error(w, `{"error":"start shell: `+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	wsConn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		sess.Close()
		return // Upgrade already wrote HTTP error
	}

	sessionID := uuid.New().String()
	terminal := &sshTerminalSession{
		ID:        sessionID,
		Host:      conn.Host,
		User:      conn.User,
		ConnKey:   sshConnectionKey(conn),
		Session:   sess,
		CreatedAt: time.Now(),
		Cols:      cols,
		Rows:      rows,
	}
	s.sshSessions.Store(sessionID, terminal)

	defer func() {
		s.sshSessions.Delete(sessionID)
		sess.Close()
		wsConn.Close()
	}()

	var wsMu sync.Mutex

	// WebSocket → SSH stdin (also handles resize control frames)
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
					terminal.Cols = ctrl.Cols
					terminal.Rows = ctrl.Rows
					_ = sess.WindowChange(ctrl.Rows, ctrl.Cols)
				}
			}
		}
	}()

	// SSH stdout → WebSocket (binary frames)
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

	// SSH stderr → WebSocket (binary frames)
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

	// Block until SSH session ends (stdin EOF or remote exit)
	_ = sess.Wait()

	// Notify client that session has ended
	wsMu.Lock()
	msg, _ := json.Marshal(map[string]string{"type": "exit"})
	wsConn.WriteMessage(websocket.TextMessage, msg) //nolint:errcheck
	wsMu.Unlock()
}
