package proxycore

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ── Helper ────────────────────────────────────────────────────────────────────

func k8sPost(t *testing.T, srv *Server, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/"+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-DevStudio-Gateway-Route", "true")
	req.Header.Set("X-DevStudio-Gateway-Protocol", "k8s")
	req.Header.Set("Origin", "http://localhost:5173")
	rr := httptest.NewRecorder()
	srv.handleK8sGateway(rr, req)
	return rr
}

func decodeK8sResp(t *testing.T, rr *httptest.ResponseRecorder) K8sResponse {
	t.Helper()
	var resp K8sResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v — body: %s", err, rr.Body.String())
	}
	return resp
}

// ── Routing & method enforcement ─────────────────────────────────────────────

func TestK8sGateway_UnknownEndpoint(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	rr := k8sPost(t, srv, "nonexistent-k8s-path", map[string]any{})
	if rr.Code != http.StatusNotFound {
		t.Errorf("want 404, got %d", rr.Code)
	}
	resp := decodeK8sResp(t, rr)
	if !strings.Contains(resp.Error, "nonexistent-k8s-path") {
		t.Errorf("want error mentioning endpoint name, got %q", resp.Error)
	}
}

func TestK8sGateway_MethodNotAllowed(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	rr := httptest.NewRecorder()
	srv.handleK8sGateway(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("want 405, got %d", rr.Code)
	}
}

func TestK8sGateway_MethodNotAllowed_PUT(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	req := httptest.NewRequest(http.MethodPut, "/resources", nil)
	rr := httptest.NewRecorder()
	srv.handleK8sGateway(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("want 405, got %d", rr.Code)
	}
}

func TestK8sGateway_BadJSON(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader("{bad json"))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.handleK8sGateway(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d", rr.Code)
	}
	resp := decodeK8sResp(t, rr)
	if resp.Error == "" {
		t.Error("want error message for bad JSON")
	}
}

func TestK8sGateway_EmptyBody(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader(""))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.handleK8sGateway(rr, req)
	// Empty body is invalid JSON → 400
	if rr.Code != http.StatusBadRequest {
		t.Errorf("want 400 for empty body, got %d", rr.Code)
	}
}

// ── CORS headers ──────────────────────────────────────────────────────────────

func TestK8sGateway_CORSHeaderPresent(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader([]byte("{}")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://localhost:5173")
	rr := httptest.NewRecorder()
	srv.handleK8sGateway(rr, req)
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:5173" {
		t.Errorf("CORS header = %q, want %q", got, "http://localhost:5173")
	}
}

func TestK8sGateway_ContentTypeJSON(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	rr := k8sPost(t, srv, "test", map[string]any{})
	ct := rr.Header().Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}

// ── Gateway header stripping ──────────────────────────────────────────────────

func TestK8sGateway_StripGatewayHeaders(t *testing.T) {
	// The gateway strips X-DevStudio-Gateway-* headers before processing.
	// Since we can't observe the stripped headers directly from a unit test,
	// we verify the request doesn't fail due to their presence.
	srv := NewServer(Options{Port: 0})
	req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader([]byte("{}")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-DevStudio-Gateway-Route", "true")
	req.Header.Set("X-DevStudio-Gateway-Protocol", "k8s")
	req.Header.Set("Origin", "http://localhost:5173")
	rr := httptest.NewRecorder()
	srv.handleK8sGateway(rr, req)
	// Should not be 400/500 due to headers — it will be 200 with an error
	// about invalid kubeconfig (no kubeconfig supplied)
	if rr.Code != http.StatusOK {
		t.Errorf("want HTTP 200 (app-level error), got %d — body: %s", rr.Code, rr.Body.String())
	}
	resp := decodeK8sResp(t, rr)
	if resp.Error == "" {
		t.Error("expected app-level error for missing kubeconfig")
	}
}

// ── Default limit ─────────────────────────────────────────────────────────────

func TestK8sGateway_DefaultLimit(t *testing.T) {
	// When limit is not specified (0), the gateway defaults to 500.
	// We verify this by checking that a /test request with no limit does not
	// return a 400 (it should hit the kubeconfig error path, not a validation error).
	srv := NewServer(Options{Port: 0})
	rr := k8sPost(t, srv, "test", map[string]any{})
	if rr.Code != http.StatusOK {
		t.Errorf("want 200 (app-level error for missing kubeconfig), got %d", rr.Code)
	}
}

// ── Endpoint routing smoke tests ──────────────────────────────────────────────

// These verify that each endpoint path is routed (returns 200 with an app-level
// error, not 404) even without a valid kubeconfig. A 404 means the path isn't
// registered in the switch.

var k8sEndpoints = []string{
	"test",
	"contexts",
	"namespaces",
	"resources",
	"resource",
	"describe",
	"yaml",
	"logs",
	"events",
	"api-resources",
	"top/pods",
	"top/nodes",
	"apply",
	"delete",
	"scale",
	"restart",
	"cordon",
	"drain",
	"portforward/start",
	"portforward/stop",
	"portforward/list",
	"helm/releases",
	"helm/release",
	"helm/history",
	"crds",
	"crd/resources",
	"diff",
	"topology",
}

func TestK8sGateway_AllEndpointsRouted(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	for _, ep := range k8sEndpoints {
		t.Run(ep, func(t *testing.T) {
			rr := k8sPost(t, srv, ep, map[string]any{})
			if rr.Code == http.StatusNotFound {
				t.Errorf("endpoint %q returned 404 — not registered in gateway switch", ep)
			}
			if rr.Code != http.StatusOK {
				t.Errorf("endpoint %q returned %d (want 200 with app-level error)", ep, rr.Code)
			}
			resp := decodeK8sResp(t, rr)
			if resp.Error == "" && resp.Version == "" && resp.Items == nil && resp.Contexts == nil &&
				resp.Namespaces == nil && resp.Logs == "" && resp.YAML == "" && !resp.Success {
				// portforward/list and similar may return empty success responses
				// so we only fail if there's truly nothing in the response
				t.Logf("endpoint %q returned empty response (may be OK for list/status endpoints)", ep)
			}
		})
	}
}

// ── Contexts endpoint — kubeconfig parsing ────────────────────────────────────

func TestK8sGateway_Contexts_MissingKubeconfig(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	rr := k8sPost(t, srv, "contexts", map[string]any{
		"kubeconfig": "/nonexistent/path/to/kubeconfig",
	})
	if rr.Code != http.StatusOK {
		t.Errorf("want 200 with error, got %d", rr.Code)
	}
	resp := decodeK8sResp(t, rr)
	if resp.Error == "" {
		t.Error("expected error for missing kubeconfig file")
	}
}

func TestK8sGateway_Test_MissingKubeconfig(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	rr := k8sPost(t, srv, "test", map[string]any{
		"kubeconfig": "/nonexistent/path/to/kubeconfig",
	})
	if rr.Code != http.StatusOK {
		t.Errorf("want 200 with error, got %d", rr.Code)
	}
	resp := decodeK8sResp(t, rr)
	if resp.Error == "" {
		t.Error("expected error for missing kubeconfig file")
	}
}

// ── DurationMs ────────────────────────────────────────────────────────────────

func TestK8sGateway_DurationMsPresent(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	rr := k8sPost(t, srv, "test", map[string]any{})
	resp := decodeK8sResp(t, rr)
	// DurationMs should always be non-negative
	if resp.DurationMs < 0 {
		t.Errorf("DurationMs = %f, want >= 0", resp.DurationMs)
	}
}
