package proxycore

import (
	"encoding/json"
	"net/http"
	"strings"
)

// handler is the core HTTP proxy handler. It routes CONNECT requests to
// handleTunnel and all other methods to handleForward. It also handles
// health checks, admin endpoints, gateway dispatch, and MCP.
type handler struct {
	s *Server
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	origin := r.Header.Get("Origin")

	// CORS preflight
	if r.Method == http.MethodOptions {
		if origin != "" {
			w.Header().Add("Vary", "Origin")
			w.Header().Add("Vary", "Access-Control-Request-Method")
			w.Header().Add("Vary", "Access-Control-Request-Headers")
			w.Header().Add("Vary", "Access-Control-Request-Private-Network")
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "*")
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Set("Access-Control-Allow-Private-Network", "true")
			w.Header().Set("Access-Control-Max-Age", "86400")
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Health check
	if r.URL.Path == "/health" && r.URL.Host == "" {
		setCORS(w, r)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "mode": "forward-proxy"})
		return
	}

	// Admin endpoints
	if isAdminPath(r.URL.Path) && r.URL.Host == "" {
		h.s.handleAdmin(w, r)
		return
	}

	// MCP context cache
	if strings.HasPrefix(r.URL.Path, "/mcp/context") && r.URL.Host == "" {
		if r.Method == http.MethodPost && r.URL.Path == "/mcp/context" {
			h.s.handleContextStore(w, r)
		} else if r.Method == http.MethodGet && len(r.URL.Path) > len("/mcp/context/") {
			h.s.handleContextLoad(w, r)
		} else {
			setCORS(w, r)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "not found"}) //nolint:errcheck
		}
		return
	}

	// MCP gateway
	if r.URL.Path == "/mcp" && r.URL.Host == "" {
		h.s.handleMCPGateway(w, r)
		return
	}

	// URL Opener inspection (SSL cert, DNS/CDN, URLhaus phishing check)
	if r.URL.Path == "/urlopener/inspect" && r.URL.Host == "" {
		h.s.handleURLOpenerInspect(w, r)
		return
	}

	// K8s exec WebSocket endpoint (Phase 2) — must be before gateway dispatch
	if r.URL.Path == "/k8s/exec" && r.URL.Host == "" {
		h.s.handleK8sExec(w, r)
		return
	}

	// K8s SSH exec WebSocket endpoint — kubectl exec over SSH
	if r.URL.Path == "/k8s/ssh-exec" && r.URL.Host == "" {
		h.s.handleK8sSSHExec(w, r)
		return
	}

	// SSH terminal WebSocket endpoint — must be before gateway dispatch
	if r.URL.Path == "/ssh/terminal" && r.URL.Host == "" {
		h.s.handleSSHTerminal(w, r)
		return
	}

	// DevStudio gateway — header-based routing
	if r.Header.Get("X-DevStudio-Gateway-Route") != "" {
		switch r.Header.Get("X-DevStudio-Gateway-Protocol") {
		case "sql":
			h.s.handleDBGateway(w, r)
			return
		case "mongo":
			h.s.handleMongoGateway(w, r)
			return
		case "elastic":
			h.s.handleElasticGateway(w, r)
			return
		case "redis":
			h.s.handleRedisGateway(w, r)
			return
		case "fs":
			handleFSGateway(w, r)
			return
		case "hprof":
			h.s.handleHprofGateway(w, r)
			return
		case "k8s":
			h.s.handleK8sGateway(w, r)
			return
		case "ssh":
			h.s.handleSSHGateway(w, r)
			return
		case "kafka":
			h.s.handleKafkaGateway(w, r)
			return
		case "container":
			h.s.handleContainerGateway(w, r)
			return
		default:
			setCORS(w, r)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "unsupported gateway protocol"})
			return
		}
	}

	if r.Method == http.MethodConnect {
		handleTunnel(w, r)
	} else {
		handleForward(w, r)
	}
}
