package proxycore

import (
	"encoding/json"
	"net/http"
	"strings"
)

// handleContextStore handles POST /mcp/context.
func (s *Server) handleContextStore(w http.ResponseWriter, r *http.Request) {
	setCORS(w, r)
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"}) //nolint:errcheck
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, maxEntrySize)
	var payload json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid JSON: " + err.Error()}) //nolint:errcheck
		return
	}

	id, err := s.StoreContext([]byte(payload), "snapshot")
	if err != nil {
		w.WriteHeader(http.StatusInsufficientStorage)
		json.NewEncoder(w).Encode(map[string]string{"error": err.Error()}) //nolint:errcheck
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"id": id}) //nolint:errcheck
}

// handleContextLoad handles GET /mcp/context/{uuid}.
func (s *Server) handleContextLoad(w http.ResponseWriter, r *http.Request) {
	setCORS(w, r)
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"}) //nolint:errcheck
		return
	}

	id := strings.TrimPrefix(r.URL.Path, "/mcp/context/")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "missing context id"}) //nolint:errcheck
		return
	}

	data, _, ok := s.LoadContext(id)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "context not found or expired"}) //nolint:errcheck
		return
	}

	w.Write(data) //nolint:errcheck
}
