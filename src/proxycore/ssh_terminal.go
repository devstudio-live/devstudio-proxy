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
// Query parameters: host, port, user, authMethod, password, privateKey, passphrase,
//
//	jumpHosts (JSON array), kiAnswers (JSON array), cols, rows
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
	// Phase 5 — jump hosts & keyboard-interactive answers from URL params
	if jh := q.Get("jumpHosts"); jh != "" {
		_ = json.Unmarshal([]byte(jh), &conn.JumpHosts)
	}
	if ki := q.Get("kiAnswers"); ki != "" {
		_ = json.Unmarshal([]byte(ki), &conn.KIAnswers)
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

	// ── Keyboard-interactive: upgrade WebSocket first to relay challenge prompts ─

	if conn.AuthMethod == "keyboard-interactive" {
		key := sshConnectionKey(conn)
		if _, pooled := s.sshPool.Load(key); !pooled {
			// Not yet in pool — upgrade WS first so we can exchange challenge frames
			wsConn, err := wsUpgrader.Upgrade(w, r, nil)
			if err != nil {
				return // Upgrade already wrote HTTP error
			}
			var wsMu sync.Mutex

			challengeFn := func(name, instruction string, questions []string, echos []bool) ([]string, error) {
				wsMu.Lock()
				msg, _ := json.Marshal(map[string]any{
					"type":        "ki-challenge",
					"name":        name,
					"instruction": instruction,
					"questions":   questions,
					"echos":       echos,
				})
				wsConn.WriteMessage(websocket.TextMessage, msg) //nolint:errcheck
				wsMu.Unlock()

				_, data, err := wsConn.ReadMessage()
				if err != nil {
					return nil, err
				}
				var resp struct {
					Type    string   `json:"type"`
					Answers []string `json:"answers"`
				}
				if json.Unmarshal(data, &resp) == nil && len(resp.Answers) > 0 {
					return resp.Answers, nil
				}
				return make([]string, len(questions)), nil
			}

			sshClient, err := s.getOrDialSSHClient(conn, challengeFn)
			if err != nil {
				wsMu.Lock()
				errMsg, _ := json.Marshal(map[string]string{"type": "error", "error": err.Error()})
				wsConn.WriteMessage(websocket.TextMessage, errMsg) //nolint:errcheck
				wsMu.Unlock()
				wsConn.Close()
				return
			}

			s.runTerminalSession(wsConn, &wsMu, sshClient, conn, cols, rows)
			return
		}
	}

	// ── Normal flow (all auth methods except unestablished keyboard-interactive) ─

	sshClient, err := s.getPooledSSHClient(conn)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusBadGateway)
		return
	}

	wsConn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return // Upgrade already wrote HTTP error
	}
	var wsMu sync.Mutex
	s.runTerminalSession(wsConn, &wsMu, sshClient, conn, cols, rows)
}

// runTerminalSession wires an established SSH client + WebSocket into a PTY session.
func (s *Server) runTerminalSession(wsConn *websocket.Conn, wsMu *sync.Mutex, sshClient *ssh.Client, conn SSHConnection, cols, rows int) {
	sess, err := sshClient.NewSession()
	if err != nil {
		wsMu.Lock()
		errMsg, _ := json.Marshal(map[string]string{"type": "error", "error": "create session: " + err.Error()})
		wsConn.WriteMessage(websocket.TextMessage, errMsg) //nolint:errcheck
		wsMu.Unlock()
		wsConn.Close()
		return
	}

	stdin, err := sess.StdinPipe()
	if err != nil {
		sess.Close()
		wsConn.Close()
		return
	}
	stdout, err := sess.StdoutPipe()
	if err != nil {
		sess.Close()
		wsConn.Close()
		return
	}
	stderr, err := sess.StderrPipe()
	if err != nil {
		sess.Close()
		wsConn.Close()
		return
	}

	modes := ssh.TerminalModes{
		ssh.ECHO:          1,
		ssh.TTY_OP_ISPEED: 14400,
		ssh.TTY_OP_OSPEED: 14400,
	}
	if err := sess.RequestPty("xterm-256color", rows, cols, modes); err != nil {
		sess.Close()
		wsConn.Close()
		return
	}
	if err := sess.Shell(); err != nil {
		sess.Close()
		wsConn.Close()
		return
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

	// Block until SSH session ends
	_ = sess.Wait()

	wsMu.Lock()
	msg, _ := json.Marshal(map[string]string{"type": "exit"})
	wsConn.WriteMessage(websocket.TextMessage, msg) //nolint:errcheck
	wsMu.Unlock()
}
