package main

import (
	"encoding/json"
	"net/http"
)

// Handler is the core HTTP proxy handler. It routes CONNECT requests to
// handleTunnel (for HTTPS tunneling) and all other methods to handleForward.
// It also handles health checks and CORS preflights for browser-based clients.
type Handler struct{}

func (h Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")

	// CORS preflight — allow browser-based clients to send any method/headers
	if r.Method == http.MethodOptions {
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			w.Header().Set("Access-Control-Max-Age", "86400")
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Health check (non-proxy relative-path request)
	if r.URL.Path == "/health" && r.URL.Host == "" {
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "mode": "forward-proxy"})
		return
	}

	if r.Method == http.MethodConnect {
		handleTunnel(w, r)
	} else {
		handleForward(w, r)
	}
}
