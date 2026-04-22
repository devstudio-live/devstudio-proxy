// Package proxycore — DAG gateway acceptance tests (Phase 16A).
//
// Plan: devstudio/plans/workflow-wizard-plan.md §G16 / 16A acceptance.
// Each top-level test maps 1:1 to one acceptance bullet so that a
// failing run pinpoints which contract regressed.
package proxycore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
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

	// Register manually and hold status in "running" so the cancel-
	// from-running transition is what we exercise. With 16B shipping a
	// real executor, a noop DAG finishes in microseconds — that would
	// make the /start + /cancel race non-deterministic. Registering
	// without spawning the executor goroutine keeps the /cancel HTTP
	// contract isolated from executor liveness.
	exec := srv.registerDAGExecution(map[string]interface{}{
		"schemaVersion": "1.0",
		"nodes":         []interface{}{map[string]interface{}{"id": "n1", "kind": "noop"}},
		"edges":         []interface{}{},
	})
	exec.setStatus(dagStatusRunning)

	resp, err := http.Get(hs.URL + "/dag/exec/" + exec.id + "/cancel")
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

// readDAGFramesUntilTerminal drains the WebSocket until a terminal frame
// ("run_completed" or "run_failed") is received, a read error occurs, or
// the deadline elapses. Returns all frames in order.
func readDAGFramesUntilTerminal(t *testing.T, conn *websocket.Conn, timeout time.Duration) []map[string]interface{} {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(timeout))
	var frames []map[string]interface{}
	for {
		var frame map[string]interface{}
		if err := conn.ReadJSON(&frame); err != nil {
			return frames
		}
		frames = append(frames, frame)
		if ty, _ := frame["type"].(string); ty == "run_completed" || ty == "run_failed" {
			return frames
		}
	}
}

// dialDAGStream connects the WS for an execution id.
func dialDAGStream(t *testing.T, hs *httptest.Server, startResp map[string]string) *websocket.Conn {
	t.Helper()
	wsURL := httpToWS(startResp["wsUrl"], hs)
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("ws dial %s: %v", wsURL, err)
	}
	return conn
}

// ── Phase 16B acceptance ─────────────────────────────────────────────

// Acceptance bullet 1 — 3-node linear DAG runs end-to-end via WebSocket;
// frames received in order. The plan calls out (sql → transform → fs);
// we substitute (fs.stat → transform → fs.list) because sql needs a live
// DB while fs is deterministic and already a G16 target gateway. The
// contract under test — topo-sorted per-node delegation with ordered
// node_start/node_result/node_skipped frames — is identical.
func TestDAGExecutor_LinearThreeNodeDAGRunsInOrder(t *testing.T) {
	tmp := t.TempDir()
	_, hs := startDAGTestServer(t)

	dag := fmt.Sprintf(`{
		"schemaVersion":"1.0",
		"nodes":[
			{"id":"a","kind":"input","params":{"gateway":{"protocol":"fs","route":"stat","body":{"path":%q}}}},
			{"id":"b","kind":"transform","params":{}},
			{"id":"c","kind":"output","params":{"gateway":{"protocol":"fs","route":"list","body":{"path":%q}}}}
		],
		"edges":[
			{"id":"e1","source":"a","target":"b"},
			{"id":"e2","source":"b","target":"c"}
		]
	}`, tmp, tmp)

	startResp := startDAGExecution(t, hs, dag)
	conn := dialDAGStream(t, hs, startResp)
	defer conn.Close()

	frames := readDAGFramesUntilTerminal(t, conn, 5*time.Second)

	// The executor must emit, in this exact order:
	//   connected, node_start(a), node_result(a),
	//   node_start(b), node_result(b),
	//   node_start(c), node_result(c),
	//   run_completed.
	want := []struct {
		typ    string
		nodeID string
	}{
		{"connected", ""},
		{"node_start", "a"},
		{"node_result", "a"},
		{"node_start", "b"},
		{"node_result", "b"},
		{"node_start", "c"},
		{"node_result", "c"},
		{"run_completed", ""},
	}
	if len(frames) != len(want) {
		t.Fatalf("expected %d frames, got %d: %+v", len(want), len(frames), frames)
	}
	for i, w := range want {
		got := frames[i]
		if got["type"] != w.typ {
			t.Fatalf("frame[%d]: expected type %q, got %q (%+v)", i, w.typ, got["type"], got)
		}
		if w.nodeID != "" {
			if got["nodeId"] != w.nodeID {
				t.Fatalf("frame[%d]: expected nodeId %q, got %v", i, w.nodeID, got["nodeId"])
			}
		}
	}
	// run_completed carries totalDurationMs + per-node summary in topo order.
	last := frames[len(frames)-1]
	data, _ := last["data"].(map[string]interface{})
	if data == nil {
		t.Fatalf("run_completed missing data: %+v", last)
	}
	if _, ok := data["totalDurationMs"]; !ok {
		t.Fatalf("run_completed missing totalDurationMs: %+v", data)
	}
	nodesSummary, _ := data["nodes"].([]interface{})
	if len(nodesSummary) != 3 {
		t.Fatalf("expected 3-entry nodes summary, got %d: %+v", len(nodesSummary), nodesSummary)
	}
	ids := []string{}
	for _, entry := range nodesSummary {
		m := entry.(map[string]interface{})
		ids = append(ids, m["nodeId"].(string))
	}
	if strings.Join(ids, ",") != "a,b,c" {
		t.Fatalf("expected summary ordered a,b,c; got %v", ids)
	}
}

// Acceptance bullet 2 — Failure in middle node marks downstream as
// "skipped-due-to-upstream-failure".
func TestDAGExecutor_FailureInMiddleSkipsDownstream(t *testing.T) {
	_, hs := startDAGTestServer(t)

	// Middle node 'b' fails via an unknown fs route (fs_gateway returns
	// 404 for any route not in its switch). Node 'a' is a passthrough so
	// the failure-propagation path is driven by 'b' only.
	dag := `{
		"schemaVersion":"1.0",
		"nodes":[
			{"id":"a","kind":"noop","params":{}},
			{"id":"b","kind":"noop","params":{"gateway":{"protocol":"fs","route":"definitely-not-a-real-route","body":{"path":"/tmp"}}}},
			{"id":"c","kind":"noop","params":{}}
		],
		"edges":[
			{"id":"e1","source":"a","target":"b"},
			{"id":"e2","source":"b","target":"c"}
		]
	}`

	startResp := startDAGExecution(t, hs, dag)
	conn := dialDAGStream(t, hs, startResp)
	defer conn.Close()

	frames := readDAGFramesUntilTerminal(t, conn, 5*time.Second)

	sawAResult, sawBFailed, sawCSkipped, sawRunFailed := false, false, false, false
	for _, f := range frames {
		typ, _ := f["type"].(string)
		nodeID, _ := f["nodeId"].(string)
		switch {
		case typ == "node_result" && nodeID == "a":
			sawAResult = true
		case typ == "node_failed" && nodeID == "b":
			sawBFailed = true
			data, _ := f["data"].(map[string]interface{})
			errStr, _ := data["error"].(string)
			if errStr == "" {
				t.Fatalf("node_failed for b missing error: %+v", f)
			}
		case typ == "node_skipped" && nodeID == "c":
			data, _ := f["data"].(map[string]interface{})
			if data == nil {
				t.Fatalf("node_skipped for c missing data: %+v", f)
			}
			if data["reason"] != "skipped-due-to-upstream-failure" {
				t.Fatalf("c skip reason must be 'skipped-due-to-upstream-failure', got %v", data["reason"])
			}
			if data["status"] != "skipped-due-to-upstream-failure" {
				t.Fatalf("c skip status must be 'skipped-due-to-upstream-failure', got %v", data["status"])
			}
			sawCSkipped = true
		case typ == "run_failed":
			sawRunFailed = true
		}
	}
	if !sawAResult {
		t.Fatalf("missing node_result for a: %+v", frames)
	}
	if !sawBFailed {
		t.Fatalf("missing node_failed for b: %+v", frames)
	}
	if !sawCSkipped {
		t.Fatalf("missing node_skipped for c: %+v", frames)
	}
	if !sawRunFailed {
		t.Fatalf("missing run_failed terminal frame: %+v", frames)
	}

	// run_failed summary must show b=failed, c=skipped-due-to-upstream-failure.
	last := frames[len(frames)-1]
	data, _ := last["data"].(map[string]interface{})
	nodesSummary, _ := data["nodes"].([]interface{})
	statuses := map[string]string{}
	for _, e := range nodesSummary {
		m := e.(map[string]interface{})
		statuses[m["nodeId"].(string)] = m["status"].(string)
	}
	if statuses["a"] != "completed" {
		t.Fatalf("a summary status want 'completed', got %q", statuses["a"])
	}
	if statuses["b"] != "failed" {
		t.Fatalf("b summary status want 'failed', got %q", statuses["b"])
	}
	if statuses["c"] != "skipped-due-to-upstream-failure" {
		t.Fatalf("c summary status want 'skipped-due-to-upstream-failure', got %q", statuses["c"])
	}
}

// Acceptance bullet 3 — Per-node duration captured; total run duration
// recorded. Asserts both node_result.durationMs and
// run_completed.totalDurationMs are present, numeric, and non-negative.
func TestDAGExecutor_CapturesPerNodeAndTotalDuration(t *testing.T) {
	tmp := t.TempDir()
	_, hs := startDAGTestServer(t)

	dag := fmt.Sprintf(`{
		"schemaVersion":"1.0",
		"nodes":[
			{"id":"n1","kind":"input","params":{"gateway":{"protocol":"fs","route":"stat","body":{"path":%q}}}},
			{"id":"n2","kind":"transform","params":{}}
		],
		"edges":[{"id":"e1","source":"n1","target":"n2"}]
	}`, tmp)

	startResp := startDAGExecution(t, hs, dag)
	conn := dialDAGStream(t, hs, startResp)
	defer conn.Close()

	frames := readDAGFramesUntilTerminal(t, conn, 5*time.Second)

	gotPerNode := 0
	for _, f := range frames {
		if f["type"] != "node_result" {
			continue
		}
		data, _ := f["data"].(map[string]interface{})
		if data == nil {
			t.Fatalf("node_result missing data: %+v", f)
		}
		dur, ok := data["durationMs"].(float64)
		if !ok {
			t.Fatalf("node_result.durationMs not numeric: %+v", data)
		}
		if dur < 0 {
			t.Fatalf("node_result.durationMs negative: %v", dur)
		}
		gotPerNode++
	}
	if gotPerNode != 2 {
		t.Fatalf("expected 2 node_result frames, got %d", gotPerNode)
	}

	last := frames[len(frames)-1]
	if last["type"] != "run_completed" {
		t.Fatalf("expected terminal run_completed, got %v", last["type"])
	}
	data := last["data"].(map[string]interface{})
	total, ok := data["totalDurationMs"].(float64)
	if !ok {
		t.Fatalf("run_completed.totalDurationMs not numeric: %+v", data)
	}
	if total < 0 {
		t.Fatalf("run_completed.totalDurationMs negative: %v", total)
	}
	// Per-node summary present.
	nodesSummary, _ := data["nodes"].([]interface{})
	if len(nodesSummary) != 2 {
		t.Fatalf("expected 2 summary entries, got %d", len(nodesSummary))
	}
	for _, e := range nodesSummary {
		m := e.(map[string]interface{})
		if _, ok := m["durationMs"].(float64); !ok {
			t.Fatalf("summary entry missing durationMs: %+v", m)
		}
	}
}

// Acceptance bullet 4 — Concurrent executions don't cross-contaminate
// state. Runs three executions in parallel; verifies each stream carries
// only its own frames and no executionId is duplicated.
func TestDAGExecutor_ConcurrentExecutionsDoNotCrossContaminate(t *testing.T) {
	tmp := t.TempDir()
	_, hs := startDAGTestServer(t)

	body := func(nodeID string) string {
		return fmt.Sprintf(`{
			"schemaVersion":"1.0",
			"nodes":[{"id":%q,"kind":"input","params":{"gateway":{"protocol":"fs","route":"stat","body":{"path":%q}}}}],
			"edges":[]
		}`, nodeID, tmp)
	}

	type result struct {
		execID   string
		nodeID   string
		frames   []map[string]interface{}
		dialErr  error
	}

	const N = 4
	var wg sync.WaitGroup
	results := make([]result, N)
	for i := 0; i < N; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			nodeID := fmt.Sprintf("n%d", i)
			startResp := startDAGExecution(t, hs, body(nodeID))
			wsURL := httpToWS(startResp["wsUrl"], hs)
			conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
			if err != nil {
				results[i] = result{dialErr: err}
				return
			}
			defer conn.Close()
			frames := readDAGFramesUntilTerminal(t, conn, 5*time.Second)
			results[i] = result{
				execID: startResp["executionId"],
				nodeID: nodeID,
				frames: frames,
			}
		}()
	}
	wg.Wait()

	seen := map[string]bool{}
	for i, r := range results {
		if r.dialErr != nil {
			t.Fatalf("run #%d dial: %v", i, r.dialErr)
		}
		if r.execID == "" {
			t.Fatalf("run #%d missing executionId", i)
		}
		if seen[r.execID] {
			t.Fatalf("duplicate executionId across concurrent runs: %s", r.execID)
		}
		seen[r.execID] = true

		// Frames on this stream must only reference this execution's
		// own node (nX). Any cross-talk would show a foreign nodeId.
		startCount, resultCount := 0, 0
		for _, f := range r.frames {
			typ, _ := f["type"].(string)
			nodeID, _ := f["nodeId"].(string)
			if typ == "node_start" || typ == "node_result" {
				if nodeID != r.nodeID {
					t.Fatalf("run %s saw foreign nodeId %q (own: %q): %+v",
						r.execID, nodeID, r.nodeID, f)
				}
			}
			if typ == "node_start" {
				startCount++
			}
			if typ == "node_result" {
				resultCount++
			}
			if typ == "connected" {
				data, _ := f["data"].(map[string]interface{})
				if data["executionId"] != r.execID {
					t.Fatalf("run %s connected frame carried wrong executionId %v",
						r.execID, data["executionId"])
				}
			}
		}
		if startCount != 1 || resultCount != 1 {
			t.Fatalf("run %s expected 1 start / 1 result, got %d / %d",
				r.execID, startCount, resultCount)
		}
		if len(r.frames) == 0 || r.frames[len(r.frames)-1]["type"] != "run_completed" {
			t.Fatalf("run %s did not terminate with run_completed: %+v", r.execID, r.frames)
		}
	}
}

// Additional 16B coverage — gateway non-2xx response surfaces as
// node_failed (not node_result). The executor is the only code path
// that turns HTTP status codes from the gateway handlers into frame
// types, so a regression here would silently treat errors as successes.
func TestDAGExecutor_GatewayNon2xxSurfacesAsNodeFailed(t *testing.T) {
	_, hs := startDAGTestServer(t)
	dag := `{
		"schemaVersion":"1.0",
		"nodes":[{"id":"n1","kind":"noop","params":{"gateway":{"protocol":"fs","route":"not-a-real-fs-route","body":{"path":"/tmp"}}}}],
		"edges":[]
	}`
	startResp := startDAGExecution(t, hs, dag)
	conn := dialDAGStream(t, hs, startResp)
	defer conn.Close()
	frames := readDAGFramesUntilTerminal(t, conn, 5*time.Second)
	sawNodeFailed, sawRunFailed := false, false
	for _, f := range frames {
		if f["type"] == "node_failed" && f["nodeId"] == "n1" {
			sawNodeFailed = true
		}
		if f["type"] == "run_failed" {
			sawRunFailed = true
		}
		if f["type"] == "node_result" {
			t.Fatalf("non-2xx gateway should not surface as node_result: %+v", f)
		}
	}
	if !sawNodeFailed {
		t.Fatalf("expected node_failed for non-2xx gateway response: %+v", frames)
	}
	if !sawRunFailed {
		t.Fatalf("expected run_failed terminal frame: %+v", frames)
	}
}

// Additional 16B coverage — unsupported protocol fails the node. Guards
// the protocol switch in dispatchGatewayInternal against a future
// refactor accidentally dropping the default clause.
func TestDAGExecutor_UnsupportedProtocolIsNodeFailed(t *testing.T) {
	_, hs := startDAGTestServer(t)
	dag := `{
		"schemaVersion":"1.0",
		"nodes":[{"id":"n1","kind":"noop","params":{"gateway":{"protocol":"not-a-protocol","route":"nowhere","body":{}}}}],
		"edges":[]
	}`
	startResp := startDAGExecution(t, hs, dag)
	conn := dialDAGStream(t, hs, startResp)
	defer conn.Close()
	frames := readDAGFramesUntilTerminal(t, conn, 5*time.Second)
	sawFailed := false
	for _, f := range frames {
		if f["type"] == "node_failed" && f["nodeId"] == "n1" {
			sawFailed = true
			data, _ := f["data"].(map[string]interface{})
			errStr, _ := data["error"].(string)
			if !strings.Contains(errStr, "not-a-protocol") {
				t.Fatalf("error should name the bad protocol; got %q", errStr)
			}
		}
	}
	if !sawFailed {
		t.Fatalf("expected node_failed for unsupported protocol: %+v", frames)
	}
}

// Additional 16B coverage — a cycle fails the run with a descriptive
// error. Kahn's algorithm is the only cycle detector on the proxy side
// (the devstudio-side dag-schema also catches cycles on import but
// the executor must still refuse to run one rather than hang).
func TestDAGExecutor_CycleFailsRun(t *testing.T) {
	_, hs := startDAGTestServer(t)
	dag := `{
		"schemaVersion":"1.0",
		"nodes":[
			{"id":"a","kind":"noop","params":{}},
			{"id":"b","kind":"noop","params":{}}
		],
		"edges":[
			{"id":"e1","source":"a","target":"b"},
			{"id":"e2","source":"b","target":"a"}
		]
	}`
	startResp := startDAGExecution(t, hs, dag)
	conn := dialDAGStream(t, hs, startResp)
	defer conn.Close()
	frames := readDAGFramesUntilTerminal(t, conn, 5*time.Second)
	for _, f := range frames {
		if f["type"] == "run_failed" {
			data, _ := f["data"].(map[string]interface{})
			errMsg, _ := data["error"].(string)
			if !strings.Contains(errMsg, "cycle") {
				t.Fatalf("expected cycle error, got %q", errMsg)
			}
			// No node_start should have been emitted before run_failed.
			for _, g := range frames {
				if g["type"] == "node_start" {
					t.Fatalf("cycle run must not start any node: %+v", frames)
				}
			}
			return
		}
	}
	t.Fatalf("expected run_failed for cycle: %+v", frames)
}

// Additional 16B coverage — passthrough node (no params.gateway) emits
// node_result with {passthrough:true}. 16B intentionally does not
// implement transform semantics; this locks the contract so 14A (plugin
// sandbox) can overlay real transform evaluation later without silently
// changing the wire shape.
func TestDAGExecutor_PassthroughNodeCompletes(t *testing.T) {
	_, hs := startDAGTestServer(t)
	dag := `{
		"schemaVersion":"1.0",
		"nodes":[{"id":"n1","kind":"transform","params":{}}],
		"edges":[]
	}`
	startResp := startDAGExecution(t, hs, dag)
	conn := dialDAGStream(t, hs, startResp)
	defer conn.Close()
	frames := readDAGFramesUntilTerminal(t, conn, 5*time.Second)
	for _, f := range frames {
		if f["type"] == "node_result" && f["nodeId"] == "n1" {
			data, _ := f["data"].(map[string]interface{})
			result, _ := data["result"].(map[string]interface{})
			pt, _ := result["passthrough"].(bool)
			if !pt {
				t.Fatalf("expected passthrough:true for gateway-less node: %+v", result)
			}
			if result["kind"] != "transform" {
				t.Fatalf("expected kind echo, got %v", result["kind"])
			}
			return
		}
	}
	t.Fatalf("no node_result for n1: %+v", frames)
}

// Additional 16B coverage — a gateway node that returns valid JSON has
// its response surfaced as the node_result.data.result object. fs.stat
// on a temp file exercises the happy decode path.
func TestDAGExecutor_GatewayResponseDecodedAsJSON(t *testing.T) {
	tmp := t.TempDir()
	_, hs := startDAGTestServer(t)
	dag := fmt.Sprintf(`{
		"schemaVersion":"1.0",
		"nodes":[{"id":"n1","kind":"input","params":{"gateway":{"protocol":"fs","route":"stat","body":{"path":%q}}}}],
		"edges":[]
	}`, tmp)
	startResp := startDAGExecution(t, hs, dag)
	conn := dialDAGStream(t, hs, startResp)
	defer conn.Close()
	frames := readDAGFramesUntilTerminal(t, conn, 5*time.Second)
	for _, f := range frames {
		if f["type"] == "node_result" && f["nodeId"] == "n1" {
			data, _ := f["data"].(map[string]interface{})
			result, _ := data["result"].(map[string]interface{})
			if result == nil {
				t.Fatalf("expected structured result map: %+v", data)
			}
			// fs.stat returns {"entry":{...}}.
			if _, ok := result["entry"]; !ok {
				t.Fatalf("expected fs.stat entry field: %+v", result)
			}
			return
		}
	}
	t.Fatalf("no node_result for n1: %+v", frames)
}
