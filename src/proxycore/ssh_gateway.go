package proxycore

import (
	"encoding/json"
	"net/http"
	"time"
)

// SSHRequest is the unified request body for all SSH gateway endpoints.
type SSHRequest struct {
	Connection SSHConnection `json:"connection"`
	// Phase 3 — tunnel operations
	TunnelType string `json:"tunnelType,omitempty"` // local | remote | dynamic
	BindAddr   string `json:"bindAddr,omitempty"`
	RemoteAddr string `json:"remoteAddr,omitempty"`
	TunnelID   string `json:"tunnelId,omitempty"`
	// Phase 4 — SFTP operations
	SFTPPath    string `json:"sftpPath,omitempty"`
	SFTPDest    string `json:"sftpDest,omitempty"`    // rename destination
	SFTPContent string `json:"sftpContent,omitempty"` // base64 file content for small uploads
}

// SSHResponse is the unified response body for all SSH gateway endpoints.
type SSHResponse struct {
	Success    bool             `json:"success,omitempty"`
	Message    string           `json:"message,omitempty"`
	Error      string           `json:"error,omitempty"`
	DurationMs float64          `json:"durationMs"`
	Sessions   *[]map[string]any `json:"sessions,omitempty"` // pointer so omitempty skips nil but not empty slice
	Item       map[string]any    `json:"item,omitempty"`
	Items      *[]map[string]any `json:"items,omitempty"` // pointer so omitempty skips nil but not empty slice
	Files      []map[string]any  `json:"files,omitempty"` // Phase 4 — SFTP directory listing
}

func (s *Server) handleSSHGateway(w http.ResponseWriter, r *http.Request) {
	r.Header.Del("X-DevStudio-Gateway-Route")
	r.Header.Del("X-DevStudio-Gateway-Protocol")

	setCORS(w, r)
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(SSHResponse{Error: "only POST is accepted"}) //nolint:errcheck
		return
	}

	var req SSHRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(SSHResponse{Error: "invalid request: " + err.Error()}) //nolint:errcheck
		return
	}

	start := time.Now()

	switch r.URL.Path {
	case "/test":
		s.handleSSHTest(w, req, start)
	case "/connect":
		s.handleSSHConnect(w, req, start)
	case "/disconnect":
		s.handleSSHDisconnect(w, req, start)
	case "/sessions":
		s.handleSSHSessions(w, start)
	case "/tunnel/start":
		s.handleSSHTunnelStart(w, req, start)
	case "/tunnel/stop":
		s.handleSSHTunnelStop(w, req, start)
	case "/tunnel/list":
		s.handleSSHTunnelList(w, start)
	// Phase 4 — SFTP
	case "/sftp/list":
		s.handleSFTPList(w, req, start)
	case "/sftp/stat":
		s.handleSFTPStat(w, req, start)
	case "/sftp/read":
		s.handleSFTPRead(w, req, start)
	case "/sftp/download":
		s.handleSFTPDownload(w, r, start)
	case "/sftp/write":
		s.handleSFTPWrite(w, r, req, start)
	case "/sftp/mkdir":
		s.handleSFTPMkdir(w, req, start)
	case "/sftp/delete":
		s.handleSFTPDelete(w, req, start)
	case "/sftp/rename":
		s.handleSFTPRename(w, req, start)
	default:
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(SSHResponse{Error: "unknown path: " + r.URL.Path}) //nolint:errcheck
	}
}

func (s *Server) handleSSHTest(w http.ResponseWriter, req SSHRequest, start time.Time) {
	client, err := s.getPooledSSHClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	sess, err := client.NewSession()
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: "session: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	defer sess.Close()
	if _, err := sess.Output("echo ok"); err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: "test failed: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	json.NewEncoder(w).Encode(SSHResponse{Success: true, Message: "Connected successfully", DurationMs: ms(start)}) //nolint:errcheck
}

func (s *Server) handleSSHConnect(w http.ResponseWriter, req SSHRequest, start time.Time) {
	if _, err := s.getPooledSSHClient(req.Connection); err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	json.NewEncoder(w).Encode(SSHResponse{Success: true, Message: "Connected", DurationMs: ms(start)}) //nolint:errcheck
}

func (s *Server) handleSSHDisconnect(w http.ResponseWriter, req SSHRequest, start time.Time) {
	s.closeSSHConnection(sshConnectionKey(req.Connection))
	json.NewEncoder(w).Encode(SSHResponse{Success: true, Message: "Disconnected", DurationMs: ms(start)}) //nolint:errcheck
}

func (s *Server) handleSSHSessions(w http.ResponseWriter, start time.Time) {
	var sessions []map[string]any
	s.sshSessions.Range(func(_, v any) bool {
		t := v.(*sshTerminalSession)
		sessions = append(sessions, map[string]any{
			"id":        t.ID,
			"host":      t.Host,
			"user":      t.User,
			"createdAt": t.CreatedAt,
			"cols":      t.Cols,
			"rows":      t.Rows,
		})
		return true
	})
	if sessions == nil {
		sessions = []map[string]any{}
	}
	json.NewEncoder(w).Encode(SSHResponse{Sessions: &sessions, DurationMs: ms(start)}) //nolint:errcheck
}
