// Package proxycore — DAG gateway acceptance tests (Phase 16A).
//
// Plan: devstudio/plans/workflow-wizard-plan.md §G16 / 16A acceptance.
// Each top-level test maps 1:1 to one acceptance bullet so that a
// failing run pinpoints which contract regressed.
package proxycore

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// startDAGTestServer brings up a fresh proxy + httptest.Server. Reaper
// is started lazily on first /dag/exec/start (matches production code
// path) so individual tests get isolated registries.
func startDAGTestServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	srv := NewServer(Options{Port: 0})
	hs := httptest.NewServer(srv.Handler)
	t.Cleanup(hs.Close)
	return srv, hs
}

// startDAGExecution is the happy-path POST helper: returns the parsed
// 202 response or fails the test on any non-202.
func startDAGExecution(t *testing.T, hs *httptest.Server, body string) map[string]string {
	t.Helper()
	resp, err := http.Post(hs.URL+"/dag/exec/start", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post /dag/exec/start: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 202 Accepted, got %d: %s", resp.StatusCode, raw)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("expected JSON content-type, got %q", got)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode 202 body: %v", err)
	}
	return out
}

// validDAGBody returns the smallest JSON object that passes 16A's
// validateDAGPayload check.
func validDAGBody() string {
	return `{"schemaVersion":"1.0","nodes":[{"id":"n1","kind":"noop"}],"edges":[]}`
}

// ── Acceptance bullet 1 — POST /dag/exec/start with valid DAG returns
//    202 Accepted + {executionId, wsUrl}. ─────────────────────────────

func TestDAGExecStart_AcceptedReturnsExecutionIdAndWsUrl(t *testing.T) {
	_, hs := startDAGTestServer(t)
	out := startDAGExecution(t, hs, validDAGBody())

	if out["executionId"] == "" {
		t.Fatal("missing executionId in 202 body")
	}
	if !strings.Contains(out["wsUrl"], "/dag/exec/"+out["executionId"]+"/stream") {
		t.Fatalf("wsUrl does not point at this execution's stream: %q", out["wsUrl"])
	}
	if !strings.HasPrefix(out["wsUrl"], "ws://") {
		t.Fatalf("expected ws:// scheme on wsUrl, got %q", out["wsUrl"])
	}
	if out["status"] != "pending" {
		t.Fatalf("expected initial status 'pending', got %q", out["status"])
	}
}

func TestDAGExecStart_InvalidJSONReturns400(t *testing.T) {
	_, hs := startDAGTestServer(t)
	resp, err := http.Post(hs.URL+"/dag/exec/start", "application/json", strings.NewReader("not-json"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestDAGExecStart_EmptyNodesReturns400(t *testing.T) {
	_, hs := startDAGTestServer(t)
	resp, err := http.Post(hs.URL+"/dag/exec/start", "application/json",
		strings.NewReader(`{"schemaVersion":"1.0","nodes":[],"edges":[]}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 on empty nodes, got %d", resp.StatusCode)
	}
}

func TestDAGExecStart_MissingNodesKeyReturns400(t *testing.T) {
	_, hs := startDAGTestServer(t)
	resp, err := http.Post(hs.URL+"/dag/exec/start", "application/json",
		strings.NewReader(`{"schemaVersion":"1.0"}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestDAGExecStart_GETReturns405(t *testing.T) {
	_, hs := startDAGTestServer(t)
	resp, err := http.Get(hs.URL + "/dag/exec/start")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

func TestDAGExecStart_RegistersExecutionInRegistry(t *testing.T) {
	srv, hs := startDAGTestServer(t)
	out := startDAGExecution(t, hs, validDAGBody())
	if _, ok := srv.dagExecutions.Load(out["executionId"]); !ok {
		t.Fatalf("execution %q not present in registry after start", out["executionId"])
	}
}

// ── Acceptance bullet 2 — WebSocket connect to wsUrl succeeds; receives
//    {type:'connected'} frame. ────────────────────────────────────────

func TestDAGExecStream_FirstFrameIsConnected(t *testing.T) {
	_, hs := startDAGTestServer(t)
	out := startDAGExecution(t, hs, validDAGBody())

	wsURL := httpToWS(out["wsUrl"], hs)
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("ws dial %s: %v", wsURL, err)
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var frame map[string]interface{}
	if err := conn.ReadJSON(&frame); err != nil {
		t.Fatalf("read first frame: %v", err)
	}
	if frame["type"] != "connected" {
		t.Fatalf("expected first frame type 'connected', got %v (%v)", frame["type"], frame)
	}
	data, ok := frame["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected 'data' object on connected frame, got %T", frame["data"])
	}
	if data["executionId"] != out["executionId"] {
		t.Fatalf("connected frame executionId mismatch: got %v, want %s", data["executionId"], out["executionId"])
	}
}

func TestDAGExecStream_UnknownIDReturns404(t *testing.T) {
	_, hs := startDAGTestServer(t)
	wsURL := strings.Replace(hs.URL, "http://", "ws://", 1) + "/dag/exec/does-not-exist/stream"
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err == nil {
		t.Fatalf("expected dial error for unknown id")
	}
	if resp == nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got resp=%v err=%v", resp, err)
	}
}

// ── Acceptance bullet 3 — GET /dag/exec/{id}/cancel cancels the
//    execution context; status flips to cancelled. ───────────────────

func TestDAGExecCancel_FlipsStatusAndCancelsContext(t *testing.T) {
	srv, hs := startDAGTestServer(t)
	out := startDAGExecution(t, hs, validDAGBody())

	v, _ := srv.dagExecutions.Load(out["executionId"])
	exec := v.(*dagExecution)

	// Wait until the executor goroutine has flipped to running so the
	// cancel-from-running transition is what we exercise.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s, _ := exec.snapshot(); s == dagStatusRunning {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	resp, err := http.Get(hs.URL + "/dag/exec/" + out["executionId"] + "/cancel")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 on cancel, got %d", resp.StatusCode)
	}
	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode cancel body: %v", err)
	}
	if body["status"] != "cancelled" {
		t.Fatalf("expected status 'cancelled', got %q", body["status"])
	}

	select {
	case <-exec.ctx.Done():
		// good — context observably cancelled
	case <-time.After(500 * time.Millisecond):
		t.Fatal("execution context not cancelled within 500ms of /cancel call")
	}
	if status, _ := exec.snapshot(); status != dagStatusCancelled {
		t.Fatalf("expected dagStatusCancelled in registry, got %q", status)
	}
}

func TestDAGExecCancel_UnknownIDReturns404(t *testing.T) {
	_, hs := startDAGTestServer(t)
	resp, err := http.Get(hs.URL + "/dag/exec/no-such-id/cancel")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestDAGExecCancel_IsIdempotent(t *testing.T) {
	_, hs := startDAGTestServer(t)
	out := startDAGExecution(t, hs, validDAGBody())
	for i := 0; i < 3; i++ {
		resp, err := http.Get(hs.URL + "/dag/exec/" + out["executionId"] + "/cancel")
		if err != nil {
			t.Fatalf("cancel #%d: %v", i, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("cancel #%d expected 200, got %d", i, resp.StatusCode)
		}
	}
}

// ── Acceptance bullet 4 — In-flight registry cleans up entries 60s
//    after completion. ────────────────────────────────────────────────

func TestDAGRegistry_ReapsCompletedEntries(t *testing.T) {
	// Tighten TTL so the test is fast. Restored after the test.
	origTTL := dagCompletedTTL
	dagCompletedTTL = 50 * time.Millisecond
	t.Cleanup(func() { dagCompletedTTL = origTTL })

	srv, hs := startDAGTestServer(t)
	out := startDAGExecution(t, hs, validDAGBody())

	// Cancel to drive the execution to a terminal state.
	resp, err := http.Get(hs.URL + "/dag/exec/" + out["executionId"] + "/cancel")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	resp.Body.Close()

	// Wait until the entry is observably terminal AND its completedAt
	// is at least dagCompletedTTL old, then drive a single sweep
	// instead of waiting for the (longer) reaper tick.
	v, _ := srv.dagExecutions.Load(out["executionId"])
	exec := v.(*dagExecution)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		status, completedAt := exec.snapshot()
		if (status == dagStatusCancelled || status == dagStatusCompleted || status == dagStatusFailed) &&
			!completedAt.IsZero() && time.Since(completedAt) >= dagCompletedTTL {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	srv.reapCompletedDAGExecutions()

	if _, present := srv.dagExecutions.Load(out["executionId"]); present {
		t.Fatal("expected reaper to remove terminal execution from registry")
	}
}

func TestDAGRegistry_DoesNotReapInFlightEntries(t *testing.T) {
	srv, hs := startDAGTestServer(t)
	out := startDAGExecution(t, hs, validDAGBody())

	srv.reapCompletedDAGExecutions()

	if _, present := srv.dagExecutions.Load(out["executionId"]); !present {
		t.Fatal("reaper removed an in-flight execution; should only sweep terminal entries")
	}
}

// ── Acceptance bullet 5 — Existing routes (/health, /admin/*, /mcp/*,
//    /sql, /k8s/*, etc.) still respond. Proxy regression coverage. ───

func TestDAGGateway_DoesNotBreakHealthRoute(t *testing.T) {
	_, hs := startDAGTestServer(t)
	resp, err := http.Get(hs.URL + "/health")
	if err != nil {
		t.Fatalf("get /health: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected /health 200, got %d", resp.StatusCode)
	}
	var body map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode /health: %v", err)
	}
	if body["ok"] != true {
		t.Fatalf("expected /health ok=true, got %v", body)
	}
}

func TestDAGGateway_DoesNotShadowMCPContext(t *testing.T) {
	_, hs := startDAGTestServer(t)
	// /mcp/context POST stores an arbitrary blob → returns a UUID.
	body := bytes.NewBufferString(`{"data":"hello","kind":"text"}`)
	resp, err := http.Post(hs.URL+"/mcp/context", "application/json", body)
	if err != nil {
		t.Fatalf("post /mcp/context: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected /mcp/context 200, got %d: %s", resp.StatusCode, raw)
	}
}

func TestDAGGateway_DoesNotShadowGatewayHeaderRouting(t *testing.T) {
	// Sanity check: a request carrying the gateway header but no /dag
	// path must still reach the gateway dispatch (which will fail with
	// a protocol-specific error since no real DB exists). What matters
	// for 16A is that the new /dag prefix did not eat the request.
	_, hs := startDAGTestServer(t)
	req, _ := http.NewRequest(http.MethodPost, hs.URL+"/", strings.NewReader(`{}`))
	req.Header.Set("X-DevStudio-Gateway-Route", "true")
	req.Header.Set("X-DevStudio-Gateway-Protocol", "fs")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	defer resp.Body.Close()
	// We do not care what status the fs handler returns, only that the
	// response isn't a /dag 404.
	raw, _ := io.ReadAll(resp.Body)
	if strings.Contains(string(raw), "execution not found") {
		t.Fatalf("/dag handler accidentally caught a gateway-header request: %s", raw)
	}
}

func TestDAGGateway_UnknownPathReturns404(t *testing.T) {
	_, hs := startDAGTestServer(t)
	resp, err := http.Get(hs.URL + "/dag/exec/")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

// ── helpers ──────────────────────────────────────────────────────────

// httpToWS rewrites the absolute wsUrl returned by /dag/exec/start so
// the dialer uses the test server's bound port. The proxy fills in the
// request Host header, which for httptest is 127.0.0.1:<port> already,
// but we re-derive defensively.
func httpToWS(wsURL string, hs *httptest.Server) string {
	u, err := url.Parse(wsURL)
	if err != nil || u.Host == "" {
		// Fallback: build from httptest server URL.
		base, _ := url.Parse(hs.URL)
		return "ws://" + base.Host + "/dag/exec/stream"
	}
	hsBase, _ := url.Parse(hs.URL)
	u.Host = hsBase.Host
	u.Scheme = "ws"
	return u.String()
}
