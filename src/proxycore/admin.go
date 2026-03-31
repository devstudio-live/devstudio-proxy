package proxycore

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// handleAdmin routes /admin/* requests to the appropriate admin handler.
func (s *Server) handleAdmin(w http.ResponseWriter, r *http.Request) {
	setCORS(w, r)

	switch {
	case r.URL.Path == "/admin/config" && r.Method == http.MethodGet:
		s.adminGetConfig(w, r)
	case r.URL.Path == "/admin/config" && r.Method == http.MethodPost:
		s.adminPostConfig(w, r)
	case r.URL.Path == "/admin/restart" && r.Method == http.MethodPost:
		s.adminRestart(w, r)
	case r.URL.Path == "/admin/trust-cert" && r.Method == http.MethodPost:
		s.adminTrustCert(w, r)
	case r.URL.Path == "/admin/logs" && r.Method == http.MethodGet:
		s.adminLogs(w, r)
	case r.URL.Path == "/admin/logs" && r.Method == http.MethodPost:
		s.adminLogsJSON(w, r)
	case r.URL.Path == "/admin/events" && r.Method == http.MethodGet:
		s.adminEvents(w, r)
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "not found"}) //nolint:errcheck
	}
}

func (s *Server) adminGetConfig(w http.ResponseWriter, r *http.Request) {
	certTrusted := false
	if s.TLSCAPath != "" {
		flagPath := filepath.Join(filepath.Dir(s.TLSCAPath), "ca-trusted.flag")
		_, err := os.Stat(flagPath)
		certTrusted = err == nil
	}
	persistedCfg, _ := LoadConfig()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
		"port":            s.AdminPort,
		"log":             s.LogEnabled.Load(),
		"verbose":         s.VerboseEnabled.Load(),
		"tls":             s.TLSAvailable,
		"https_enabled":   persistedCfg.HTTPS,
		"cert_trusted":    certTrusted,
		"firefox_trusted": FirefoxPolicyInstalled(),
	})
}

func (s *Server) adminTrustCert(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.TLSCAPath == "" {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "error": "TLS not available"}) //nolint:errcheck
		return
	}
	flagPath := filepath.Join(filepath.Dir(s.TLSCAPath), "ca-trusted.flag")
	_ = os.Remove(flagPath)
	RemoveFirefoxPolicyFlag()
	if err := InstallCATrust(s.TLSCAPath); err != nil {
		log.Printf("proxy: re-trust failed: %v", err)
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "error": err.Error()}) //nolint:errcheck
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true}) //nolint:errcheck
}

func (s *Server) adminPostConfig(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Log     *bool `json:"log"`
		Verbose *bool `json:"verbose"`
		HTTPS   *bool `json:"https"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid json"}) //nolint:errcheck
		return
	}
	if body.Log != nil {
		s.LogEnabled.Store(*body.Log)
	}
	if body.Verbose != nil {
		s.VerboseEnabled.Store(*body.Verbose)
		if *body.Verbose {
			s.LogEnabled.Store(true)
		}
	}
	restartRequired := false
	if body.HTTPS != nil {
		val := "false"
		if *body.HTTPS {
			val = "true"
		}
		if err := PersistConfigKey("HTTPS", val); err != nil {
			log.Printf("proxy: failed to persist HTTPS config: %v", err)
		} else {
			restartRequired = true
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
		"ok":               true,
		"log":              s.LogEnabled.Load(),
		"verbose":          s.VerboseEnabled.Load(),
		"restart_required": restartRequired,
	})
}

func (s *Server) adminRestart(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true}) //nolint:errcheck
	log.Printf("proxy: shutdown requested on :%d", s.AdminPort)
	if s.AdminServer != nil {
		go func() {
			_ = s.AdminServer.Close()
		}()
	}
}

func (s *Server) adminLogs(w http.ResponseWriter, r *http.Request) {
	if wantsJSONLogs(r) {
		s.adminLogsJSON(w, r)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	s.LogBuf.Lock()
	buffered := s.LogBuf.Snapshot()
	s.LogBuf.Unlock()

	for _, entry := range buffered {
		b, _ := json.Marshal(entry.Line)
		_, _ = w.Write([]byte("data: " + string(b) + "\n\n"))
	}
	flusher.Flush()

	ch := make(chan string, 64)
	s.LogBuf.Subscribe(ch)
	defer s.LogBuf.Unsubscribe(ch)

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case line, ok := <-ch:
			if !ok {
				return
			}
			b, _ := json.Marshal(line)
			_, _ = w.Write([]byte("data: " + string(b) + "\n\n"))
			flusher.Flush()
		}
	}
}

func (s *Server) adminEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	s.EventBuf.Lock()
	buffered := s.EventBuf.Snapshot()
	s.EventBuf.Unlock()

	for _, evt := range buffered {
		_, _ = w.Write(MarshalSSEFrame(evt))
	}
	flusher.Flush()

	ch := make(chan ProxyEvent, 64)
	s.EventBuf.Subscribe(ch)
	defer s.EventBuf.Unsubscribe(ch)

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			_, _ = w.Write(MarshalSSEFrame(evt))
			flusher.Flush()
		}
	}
}

func wantsJSONLogs(r *http.Request) bool {
	if r.URL.Query().Get("format") == "json" {
		return true
	}
	return strings.Contains(r.Header.Get("Accept"), "application/json")
}

func (s *Server) adminLogsJSON(w http.ResponseWriter, r *http.Request) {
	var since int64
	if r.Method == http.MethodPost {
		var body struct {
			Since int64 `json:"since"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid json"}) //nolint:errcheck
			return
		}
		since = body.Since
	} else {
		since, _ = strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
	}

	s.LogBuf.Lock()
	lines := s.LogBuf.SnapshotSince(since)
	s.LogBuf.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"lines": lines,
	}) //nolint:errcheck
}

// isAdminPath returns true for /admin/* paths.
func isAdminPath(path string) bool {
	return strings.HasPrefix(path, "/admin/")
}
