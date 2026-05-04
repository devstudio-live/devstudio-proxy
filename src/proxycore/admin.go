package proxycore

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
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
	case r.URL.Path == "/admin/traffic" && r.Method == http.MethodGet:
		s.adminTraffic(w, r)
	case r.URL.Path == "/admin/traffic" && r.Method == http.MethodDelete:
		s.adminTrafficClear(w, r)
	case r.URL.Path == "/admin/traffic/stats" && r.Method == http.MethodGet:
		s.adminTrafficStats(w, r)
	case r.URL.Path == "/admin/internals" && r.Method == http.MethodGet:
		s.adminInternals(w, r)
	case r.URL.Path == "/admin/update/check" && r.Method == http.MethodGet:
		s.adminUpdateCheck(w, r)
	case r.URL.Path == "/admin/update/apply" && r.Method == http.MethodPost:
		s.adminUpdateApply(w, r)
	case r.URL.Path == "/admin/update/status" && r.Method == http.MethodGet:
		s.adminUpdateStatus(w, r)
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
	resp := map[string]interface{}{
		"port":             s.AdminPort,
		"log":              s.LogEnabled.Load(),
		"verbose":          s.VerboseEnabled.Load(),
		"tls":              s.TLSAvailable,
		"https_enabled":    persistedCfg.HTTPS,
		"cert_trusted":     certTrusted,
		"firefox_trusted":  FirefoxPolicyInstalled(),
		"update_available": CachedUpdateAvailable(),
	}
	for k, v := range VersionInfo() {
		resp[k] = v
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp) //nolint:errcheck
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
		Log               *bool `json:"log"`
		Verbose           *bool `json:"verbose"`
		HTTPS             *bool `json:"https"`
		VerboseTTLSeconds *int  `json:"verbose_ttl_seconds"`
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
	if body.VerboseTTLSeconds != nil && *body.VerboseTTLSeconds > 0 && s.VerboseEnabled.Load() {
		ttl := time.Duration(*body.VerboseTTLSeconds) * time.Second
		s.verboseRevertMu.Lock()
		if s.VerboseRevertTimer != nil {
			s.VerboseRevertTimer.Stop()
		}
		s.VerboseRevertTimer = time.AfterFunc(ttl, func() {
			s.VerboseEnabled.Store(false)
			log.Printf("proxy: verbose mode auto-reverted after TTL")
		})
		s.verboseRevertMu.Unlock()
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

func (s *Server) adminTraffic(w http.ResponseWriter, r *http.Request) {
	// JSON snapshot mode: ?format=json&since=ID
	if r.URL.Query().Get("format") == "json" {
		since, _ := strconv.ParseInt(r.URL.Query().Get("since"), 10, 64)
		records := s.TrafficBuf.SnapshotSince(since)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"records": records}) //nolint:errcheck
		return
	}

	// SSE stream mode.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Backfill buffered records.
	for _, rec := range s.TrafficBuf.Snapshot() {
		_, _ = w.Write(MarshalTrafficSSEFrame(rec))
	}
	flusher.Flush()

	ch := make(chan TrafficRecord, 64)
	s.TrafficBuf.Subscribe(ch)
	defer s.TrafficBuf.Unsubscribe(ch)

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case rec, ok := <-ch:
			if !ok {
				return
			}
			_, _ = w.Write(MarshalTrafficSSEFrame(rec))
			flusher.Flush()
		}
	}
}

func (s *Server) adminTrafficClear(w http.ResponseWriter, r *http.Request) {
	if s.TrafficBuf != nil {
		s.TrafficBuf.Clear()
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"ok": true}) //nolint:errcheck
}

func (s *Server) adminTrafficStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.TrafficBuf == nil {
		json.NewEncoder(w).Encode(TrafficStats{}) //nolint:errcheck
		return
	}
	json.NewEncoder(w).Encode(s.TrafficBuf.Stats()) //nolint:errcheck
}

func (s *Server) adminUpdateCheck(w http.ResponseWriter, r *http.Request) {
	force := r.URL.Query().Get("force") == "1"
	info, err := CheckForUpdate(r.Context(), force)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()}) //nolint:errcheck
		return
	}
	json.NewEncoder(w).Encode(info) //nolint:errcheck
}

func (s *Server) adminInternals(w http.ResponseWriter, r *http.Request) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	uptime := time.Since(s.ServerStartTime).Seconds()

	// Count active hprof jobs.
	activeJobs := 0
	s.hprofJobs.Range(func(_, _ any) bool {
		activeJobs++
		return true
	})

	s.mcpRuntime.mu.Lock()
	mcpReady := s.mcpRuntime.ready
	s.mcpRuntime.mu.Unlock()

	logLen := s.LogBuf.Len()
	evtLen := s.EventBuf.Len()
	trafficLen := s.TrafficBuf.Len()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
		"uptime_seconds": uptime,
		"goroutines":     runtime.NumGoroutine(),
		"memory": map[string]any{
			"alloc_mb":  float64(ms.Alloc) / (1024 * 1024),
			"sys_mb":    float64(ms.Sys) / (1024 * 1024),
			"gc_cycles": ms.NumGC,
		},
		"pools": map[string]any{
			"sql":     map[string]any{"active": s.sqlPoolSize, "max": maxPoolSize},
			"mongo":   map[string]any{"active": s.mongoPoolSize, "max": maxPoolSize},
			"redis":   map[string]any{"active": s.redisPoolSize, "max": maxPoolSize},
			"elastic": map[string]any{"active": s.elasticPoolSize, "max": maxPoolSize},
			"kafka":   map[string]any{"active": s.kafkaPoolSize, "max": maxPoolSize},
			"k8s":     map[string]any{"active": s.k8sPoolSize, "max": maxK8sPoolSize},
		},
		"context_cache": map[string]any{
			"entries":     s.contextCount.Load(),
			"max_entries": maxContextEntries,
			"total_bytes": s.contextTotalSize.Load(),
			"max_bytes":   maxTotalSize,
		},
		"hprof": map[string]any{
			"active_jobs":     activeJobs,
			"cached_sessions": s.hprofSessions.Len(),
		},
		"mcp": map[string]any{
			"ready":            mcpReady,
			"fallback_enabled": s.MCPFallbackEnabled,
		},
		"buffers": map[string]any{
			"log":     map[string]any{"capacity": 200, "current": logLen},
			"event":   map[string]any{"capacity": 100, "current": evtLen},
			"traffic": map[string]any{"capacity": 500, "current": trafficLen},
		},
	})
}
