package main

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
func handleAdmin(w http.ResponseWriter, r *http.Request) {
	setCORS(w, r)

	switch {
	case r.URL.Path == "/admin/config" && r.Method == http.MethodGet:
		adminGetConfig(w, r)
	case r.URL.Path == "/admin/config" && r.Method == http.MethodPost:
		adminPostConfig(w, r)
	case r.URL.Path == "/admin/restart" && r.Method == http.MethodPost:
		adminRestart(w, r)
	case r.URL.Path == "/admin/trust-cert" && r.Method == http.MethodPost:
		adminTrustCert(w, r)
	case r.URL.Path == "/admin/logs" && r.Method == http.MethodGet:
		adminLogs(w, r)
	case r.URL.Path == "/admin/logs" && r.Method == http.MethodPost:
		adminLogsJSON(w, r)
	case r.URL.Path == "/admin/events" && r.Method == http.MethodGet:
		adminEvents(w, r)
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "not found"}) //nolint:errcheck
	}
}

func adminGetConfig(w http.ResponseWriter, r *http.Request) {
	certTrusted := false
	if tlsCAPath != "" {
		flagPath := filepath.Join(filepath.Dir(tlsCAPath), "ca-trusted.flag")
		_, err := os.Stat(flagPath)
		certTrusted = err == nil
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
		"port":         adminPort,
		"log":          logEnabled.Load(),
		"verbose":      verboseEnabled.Load(),
		"tls":          tlsAvailable,
		"cert_trusted": certTrusted,
	})
}

func adminTrustCert(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if tlsCAPath == "" {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "error": "TLS not available"}) //nolint:errcheck
		return
	}
	// Remove the flag so installCATrust re-runs the OS dialog.
	flagPath := filepath.Join(filepath.Dir(tlsCAPath), "ca-trusted.flag")
	_ = os.Remove(flagPath)
	if err := installCATrust(tlsCAPath); err != nil {
		log.Printf("proxy: re-trust failed: %v", err)
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": false, "error": err.Error()}) //nolint:errcheck
		return
	}
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true}) //nolint:errcheck
}

func adminPostConfig(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Log     *bool `json:"log"`
		Verbose *bool `json:"verbose"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid json"}) //nolint:errcheck
		return
	}
	if body.Log != nil {
		logEnabled.Store(*body.Log)
	}
	if body.Verbose != nil {
		verboseEnabled.Store(*body.Verbose)
		if *body.Verbose {
			logEnabled.Store(true)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
		"ok":      true,
		"log":     logEnabled.Load(),
		"verbose": verboseEnabled.Load(),
	})
}

func adminRestart(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"ok": true}) //nolint:errcheck
	log.Printf("proxy: shutdown requested on :%d", adminPort)
	if adminServer != nil {
		go func() {
			_ = adminServer.Close()
		}()
	}
}

func adminLogs(w http.ResponseWriter, r *http.Request) {
	if wantsJSONLogs(r) {
		adminLogsJSON(w, r)
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

	// Send buffered lines
	logBuf.mu.Lock()
	buffered := logBuf.snapshot()
	logBuf.mu.Unlock()

	for _, entry := range buffered {
		b, _ := json.Marshal(entry.Line)
		_, _ = w.Write([]byte("data: " + string(b) + "\n\n"))
	}
	flusher.Flush()

	// Subscribe to live lines
	ch := make(chan string, 64)
	logBuf.subscribe(ch)
	defer logBuf.unsubscribe(ch)

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

func adminEvents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Send buffered events
	eventBuf.mu.Lock()
	buffered := eventBuf.snapshot()
	eventBuf.mu.Unlock()

	for _, evt := range buffered {
		_, _ = w.Write(marshalSSEFrame(evt))
	}
	flusher.Flush()

	// Subscribe to live events
	ch := make(chan proxyEvent, 64)
	eventBuf.subscribe(ch)
	defer eventBuf.unsubscribe(ch)

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			_, _ = w.Write(marshalSSEFrame(evt))
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

func adminLogsJSON(w http.ResponseWriter, r *http.Request) {
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

	logBuf.mu.Lock()
	lines := logBuf.snapshotSince(since)
	logBuf.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"lines": lines,
	}) //nolint:errcheck
}

// adminCORSPreflight handles OPTIONS for /admin/* in the main router.
func isAdminPath(path string) bool {
	return strings.HasPrefix(path, "/admin/")
}
