package proxycore

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"time"
)

// isLoopback returns true if the request originates from 127.0.0.1 or ::1.
func isLoopback(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// adminUpdateApply handles POST /admin/update/apply.
// Security: loopback-only, CSRF header, single-flight, version-echo TOCTOU guard.
func (s *Server) adminUpdateApply(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Loopback guard.
	if !isLoopback(r) {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]string{"error": "loopback only"}) //nolint:errcheck
		return
	}

	// CSRF header guard.
	if r.Header.Get("X-DevStudio-Update") != "1" {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing X-DevStudio-Update header"}) //nolint:errcheck
		return
	}

	// BuildSource guard.
	if BuildSource == "source" {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]interface{}{"error": "self-update refused", "reason": "dev-build"}) //nolint:errcheck
		return
	}

	// Parse version from request body.
	var body struct {
		Version string `json:"version"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Version == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "version required"}) //nolint:errcheck
		return
	}

	// Apply the update (download + verify + replace binary).
	if err := ApplyUpdate(r.Context(), body.Version); err != nil {
		status := http.StatusInternalServerError
		switch {
		case contains(err.Error(), "version mismatch"):
			status = http.StatusConflict
		case contains(err.Error(), "already in progress"):
			status = http.StatusConflict
		case contains(err.Error(), "not supported"):
			status = http.StatusConflict
		}
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()}) //nolint:errcheck
		return
	}

	// Respond before restarting so the client sees the success response.
	json.NewEncoder(w).Encode(map[string]interface{}{ //nolint:errcheck
		"ok":            true,
		"applied":       body.Version,
		"restart_in_ms": 500,
	})

	// Flush the response to the client.
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	// Schedule restart after a short delay to let the HTTP response complete.
	go func() {
		time.Sleep(500 * time.Millisecond)
		log.Printf("proxy: restarting after update to %s", body.Version)
		if err := Restart(); err != nil {
			log.Printf("proxy: restart after update failed: %v — rolling back", err)
			setUpdateStatus(UpdateStateError, 0, "restart failed: "+err.Error())
			if BuildSource == "wails-app" {
				rollbackBundleUpdate()
			} else {
				rollbackUpdate()
			}
		}
	}()
}

// adminUpdateStatus handles GET /admin/update/status.
func (s *Server) adminUpdateStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(UpdateStatus()) //nolint:errcheck
}

// contains is a small helper (avoids importing strings in this file).
func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
