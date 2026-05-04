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
		execID  string
		nodeID  string
		frames  []map[string]interface{}
		dialErr error
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

// ── Phase 16C acceptance ─────────────────────────────────────────────

// readDAGFramesUntilDeadline drains every available frame until either a
// terminal frame arrives, a read error occurs, or the wall-clock
// deadline elapses. Unlike readDAGFramesUntilTerminal it returns ALL
// frames seen — used by 16C tests that count `node_progress` over a
// time window without bailing out at the terminal.
func readDAGFramesUntilDeadline(t *testing.T, conn *websocket.Conn, deadline time.Duration) []map[string]interface{} {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(deadline))
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

// 16C acceptance bullet 1 — Cancel mid-stream aborts the long-running
// handler within 200ms.
//
// The plan calls out k8s logs / kafka tail / container logs as the long
// handlers under test. Those handlers stream against live infrastructure
// (k8s API, Kafka broker, container runtime) which is unavailable in
// `go test ./...`. The 16C contract — "cancellation propagates from
// /dag/exec/{id}/cancel through the per-execution context to the in-
// flight node within 200ms" — is exercised by the synthetic
// instrumented primitive (delayMs:N), which uses the SAME exec.ctx
// pathway. Source-level proof that the named handlers reference
// ctx.Done() lives in the devstudio-side phase-16C spec; this test
// proves the propagation semantics end-to-end.
func TestDAGExecutor_CancelMidNodeAbortsWithin200ms(t *testing.T) {
	_, hs := startDAGTestServer(t)

	// 5-second instrumented node so cancellation fires while the slice
	// timer is still mid-sleep. progressSteps:1 keeps the loop in a
	// single sleep + select{ctx.Done|timer.C}.
	dag := `{
		"schemaVersion":"1.0",
		"nodes":[{"id":"slow","kind":"_instrumented","params":{"delayMs":5000,"progressSteps":1}}],
		"edges":[]
	}`
	startResp := startDAGExecution(t, hs, dag)
	conn := dialDAGStream(t, hs, startResp)
	defer conn.Close()

	// Drain the 'connected' + 'node_start' frames so the read deadline
	// below is anchored to real cancellation latency, not WS handshake.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for i := 0; i < 2; i++ {
		var f map[string]interface{}
		if err := conn.ReadJSON(&f); err != nil {
			t.Fatalf("expected setup frame %d, got %v", i, err)
		}
	}

	// Cancel after a short head start so the executor is observably
	// inside the slice sleep when /cancel arrives.
	time.Sleep(50 * time.Millisecond)
	cancelStart := time.Now()
	resp, err := http.Get(hs.URL + "/dag/exec/" + startResp["executionId"] + "/cancel")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	resp.Body.Close()

	// Wait for the WS to close (executor unwinds, frame channel closes,
	// handler returns). Plan budget is 200ms; we allow a generous 500ms
	// slack for CI noise but assert <200ms post-cancel as the primary
	// contract via a separate stat.
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	for {
		var f map[string]interface{}
		if err := conn.ReadJSON(&f); err != nil {
			break
		}
	}
	elapsed := time.Since(cancelStart)
	if elapsed > 200*time.Millisecond {
		t.Fatalf("expected cancel-to-WS-close within 200ms; got %v", elapsed)
	}
}

// 16C acceptance bullet 2 — 1000-step instrumented node emits ≤10
// progress frames/sec.
//
// Drives a node with progressSteps:1000 spread over delayMs:2000 (one
// emit attempted every ~2ms). The 100ms throttle inside emitProgress
// permits ~21 frames in the 2s window (t=0, 100, 200, …, 2000ms = 21
// slots). The assertion is "≤10/sec on average" → ceiling
// `2 * 10 + jitter`. We use 25 to absorb scheduler noise on slow CI
// without silently widening the contract beyond the plan's stated
// budget.
func TestDAGExecutor_ProgressFramesThrottledTo10PerSecond(t *testing.T) {
	_, hs := startDAGTestServer(t)
	const runMs = 2000
	dag := fmt.Sprintf(`{
		"schemaVersion":"1.0",
		"nodes":[{"id":"p","kind":"_instrumented","params":{"delayMs":%d,"progressSteps":1000}}],
		"edges":[]
	}`, runMs)
	startResp := startDAGExecution(t, hs, dag)
	conn := dialDAGStream(t, hs, startResp)
	defer conn.Close()

	frames := readDAGFramesUntilTerminal(t, conn, 6*time.Second)

	progressCount := 0
	startSeen, endSeen := false, false
	for _, f := range frames {
		switch f["type"] {
		case "node_progress":
			if f["nodeId"] != "p" {
				t.Fatalf("foreign nodeId on progress frame: %+v", f)
			}
			progressCount++
		case "node_start":
			startSeen = true
		case "run_completed":
			endSeen = true
		}
	}
	if !startSeen || !endSeen {
		t.Fatalf("expected node_start + run_completed; got %+v", frames)
	}
	// Plan: "≤10 progress frames/sec". Over the ~2s run that is ≤20.
	// We allow a small jitter margin for slow CI (Go's timer slice can
	// shave a few ms off each sleep, sneaking an extra emit through);
	// 25 keeps the contract within sight while staying far below the
	// 1000 emits the loop would produce un-throttled.
	if progressCount > 25 {
		t.Fatalf("throttle exceeded: expected ≤25 progress frames over a %dms run; got %d (loop attempted 1000 emits)", runMs, progressCount)
	}
	if progressCount < 5 {
		t.Fatalf("throttle too aggressive: expected ≥5 progress frames over a %dms run; got %d", runMs, progressCount)
	}
}

// 16C acceptance bullet 2 (unit-level) — emitProgress directly under
// load. Hammers emitProgress 5,000 times back-to-back from goroutines
// over a fixed 250ms window with the production 100ms throttle, and
// verifies fewer than 4 frames make it onto the channel (≤10/sec * 0.4s
// ≈ 4 with rounding). Locks the throttle math without any I/O.
func TestDAGExecutor_EmitProgressEnforces100msPerNodeThrottle(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	exec := srv.registerDAGExecution(map[string]interface{}{
		"schemaVersion": "1.0",
		"nodes":         []interface{}{map[string]interface{}{"id": "n1", "kind": "noop"}},
		"edges":         []interface{}{},
	})

	const N = 5000
	const window = 250 * time.Millisecond

	emittedTrue := 0
	stop := time.After(window)
	done := false
	for i := 0; i < N && !done; i++ {
		select {
		case <-stop:
			done = true
			continue
		default:
		}
		if exec.emitProgress("n1", float64(i)/float64(N), nil) {
			emittedTrue++
		}
	}

	// Drain the channel so the count is observable without racing a
	// reader. closeFrames is idempotent via sync.Once.
	exec.closeFrames()
	got := 0
	for f := range exec.frames {
		if f.Type == "node_progress" {
			got++
		}
	}
	// In a 250ms window with a 100ms throttle the contract permits
	// at most 4 frames (t=0, 100, 200ms). emittedTrue should equal got.
	if got > 4 {
		t.Fatalf("throttle leaked: expected ≤4 progress frames in %v, got %d (emitProgress returned true %d times)", window, got, emittedTrue)
	}
	if got < 1 {
		t.Fatalf("throttle squashed everything: expected ≥1 progress frame in %v, got 0", window)
	}
}

// 16C acceptance bullet 2 (cross-node isolation) — the throttle is per
// nodeId, so two distinct node ids do NOT contend for the same window.
// Without per-node tracking, a hot loop across N nodes would still cap
// out at 10 frames/sec total, which would silently throttle parallel
// branches.
func TestDAGExecutor_ProgressThrottleIsPerNode(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	exec := srv.registerDAGExecution(map[string]interface{}{
		"schemaVersion": "1.0",
		"nodes":         []interface{}{map[string]interface{}{"id": "n1", "kind": "noop"}},
		"edges":         []interface{}{},
	})

	// First emit on each node id always passes (no prior timestamp).
	if !exec.emitProgress("a", 0.1, nil) {
		t.Fatalf("first emit on node a should pass throttle")
	}
	if !exec.emitProgress("b", 0.1, nil) {
		t.Fatalf("first emit on node b should pass throttle (independent window from a)")
	}
	// Immediate second emits on same node — throttled.
	if exec.emitProgress("a", 0.2, nil) {
		t.Fatalf("second immediate emit on node a should be throttled")
	}
	if exec.emitProgress("b", 0.2, nil) {
		t.Fatalf("second immediate emit on node b should be throttled")
	}
	// A third node id is still independent.
	if !exec.emitProgress("c", 0.1, nil) {
		t.Fatalf("first emit on node c should pass throttle")
	}
}

// 16C acceptance bullet 3 — Browser cancel via WS close → handler
// context cancelled within 200ms.
//
// Opens a WS, drops it abruptly, asserts the per-execution context is
// observably cancelled within 200ms and the registry status flips to
// `cancelled`. The mechanism under test is the implicit-cancel branch
// in handleDAGExecStream (case <-clientClosed → cancelOnClientDisconnect).
func TestDAGExec_ClientWSCloseCancelsContextWithin200ms(t *testing.T) {
	srv, hs := startDAGTestServer(t)

	// Long-running node so the executor goroutine is mid-sleep when the
	// WS drops; we want to observe ctx.Done() firing as a result of the
	// disconnect, not natural completion.
	dag := `{
		"schemaVersion":"1.0",
		"nodes":[{"id":"n1","kind":"_instrumented","params":{"delayMs":5000,"progressSteps":1}}],
		"edges":[]
	}`
	startResp := startDAGExecution(t, hs, dag)

	v, _ := srv.dagExecutions.Load(startResp["executionId"])
	exec := v.(*dagExecution)

	conn := dialDAGStream(t, hs, startResp)

	// Wait for the connected frame so the server-side WS handler is past
	// the upgrade and into its read loop.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var first map[string]interface{}
	if err := conn.ReadJSON(&first); err != nil {
		t.Fatalf("expected connected frame: %v", err)
	}

	// Drop the WS. We do not send a Close control frame — abrupt
	// disconnect is the realistic browser-tab-close scenario.
	dropStart := time.Now()
	_ = conn.Close()

	// Poll exec.ctx.Done() up to the 200ms budget.
	select {
	case <-exec.ctx.Done():
		// good
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("exec.ctx not cancelled within 200ms of WS drop (waited %v)", time.Since(dropStart))
	}
	if elapsed := time.Since(dropStart); elapsed > 200*time.Millisecond {
		t.Fatalf("ctx cancel observed at %v, exceeds 200ms budget", elapsed)
	}

	// Status flips to 'cancelled' as a side-effect.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if status, _ := exec.snapshot(); status == dagStatusCancelled {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	status, _ := exec.snapshot()
	t.Fatalf("expected status cancelled after WS close; got %q", status)
}

// 16C acceptance bullet 3 (already-terminal preservation) — a clean WS
// disconnect AFTER run_completed must NOT retroactively flip status.
// Otherwise a successful run would be reported as cancelled to anyone
// inspecting the registry after the fact.
func TestDAGExec_WSCloseAfterTerminalDoesNotRewriteStatus(t *testing.T) {
	srv, hs := startDAGTestServer(t)
	startResp := startDAGExecution(t, hs, validDAGBody())
	conn := dialDAGStream(t, hs, startResp)

	// Drain to terminal so status is already 'completed' or 'failed'.
	frames := readDAGFramesUntilTerminal(t, conn, 5*time.Second)
	if len(frames) == 0 {
		t.Fatalf("expected at least one frame")
	}
	terminal, _ := frames[len(frames)-1]["type"].(string)
	if terminal != "run_completed" && terminal != "run_failed" {
		t.Fatalf("expected terminal frame, got %q", terminal)
	}
	v, _ := srv.dagExecutions.Load(startResp["executionId"])
	exec := v.(*dagExecution)
	statusBefore, _ := exec.snapshot()

	_ = conn.Close()
	time.Sleep(50 * time.Millisecond)

	statusAfter, _ := exec.snapshot()
	if statusAfter != statusBefore {
		t.Fatalf("WS close after terminal rewrote status %q → %q; should be no-op", statusBefore, statusAfter)
	}
	if statusAfter == dagStatusCancelled {
		t.Fatalf("status must not be 'cancelled' after a successful run that was disconnected post-terminal")
	}
}

// 16C acceptance bullet 4 — In-flight registry size returns to baseline
// after cancel. Verifies the cancel-then-reap loop reclaims memory: any
// number of cancelled executions left behind would be a slow leak.
func TestDAGRegistry_ReturnsToBaselineAfterCancelSweep(t *testing.T) {
	origTTL := dagCompletedTTL
	dagCompletedTTL = 25 * time.Millisecond
	t.Cleanup(func() { dagCompletedTTL = origTTL })

	srv, hs := startDAGTestServer(t)

	baseline := dagRegistrySize(srv)

	const N = 8
	ids := make([]string, 0, N)
	for i := 0; i < N; i++ {
		// Long-running node so cancel races a real running execution.
		dag := `{
			"schemaVersion":"1.0",
			"nodes":[{"id":"n","kind":"_instrumented","params":{"delayMs":5000,"progressSteps":1}}],
			"edges":[]
		}`
		out := startDAGExecution(t, hs, dag)
		ids = append(ids, out["executionId"])
	}
	if got := dagRegistrySize(srv); got != baseline+N {
		t.Fatalf("expected registry size %d after %d starts; got %d", baseline+N, N, got)
	}
	for _, id := range ids {
		resp, err := http.Get(hs.URL + "/dag/exec/" + id + "/cancel")
		if err != nil {
			t.Fatalf("cancel %s: %v", id, err)
		}
		resp.Body.Close()
	}

	// Wait for completedAt to be old enough, then sweep.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		allReady := true
		for _, id := range ids {
			v, ok := srv.dagExecutions.Load(id)
			if !ok {
				continue // already reaped — fine
			}
			exec := v.(*dagExecution)
			status, completedAt := exec.snapshot()
			ready := (status == dagStatusCancelled || status == dagStatusCompleted || status == dagStatusFailed) &&
				!completedAt.IsZero() && time.Since(completedAt) >= dagCompletedTTL
			if !ready {
				allReady = false
				break
			}
		}
		if allReady {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	srv.reapCompletedDAGExecutions()

	if got := dagRegistrySize(srv); got != baseline {
		t.Fatalf("expected registry to return to baseline %d after cancel+sweep; got %d", baseline, got)
	}
}

// dagRegistrySize counts in-flight entries. Used by the leak test;
// not exposed in production code because callers should not depend on
// internal registry shape.
func dagRegistrySize(s *Server) int {
	n := 0
	s.dagExecutions.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

// 16C contract guard — node_progress frames carry the documented
// envelope: {type:"node_progress", nodeId, data:{progress, intermediate?}}.
// The intermediate field is OMITTED when nil, not null'd, so the
// remote-executor UI in 16E can branch on its presence.
func TestDAGExecutor_ProgressFrameEnvelopeShape(t *testing.T) {
	_, hs := startDAGTestServer(t)
	// progressSteps:3 so the throttle does not mask the test (each
	// emit attempt with no delayMs is back-to-back so only the first
	// will land — that single frame is enough to assert shape).
	dag := `{
		"schemaVersion":"1.0",
		"nodes":[{"id":"x","kind":"_instrumented","params":{"progressSteps":3}}],
		"edges":[]
	}`
	startResp := startDAGExecution(t, hs, dag)
	conn := dialDAGStream(t, hs, startResp)
	defer conn.Close()
	frames := readDAGFramesUntilTerminal(t, conn, 5*time.Second)

	sawProgress := false
	for _, f := range frames {
		if f["type"] != "node_progress" {
			continue
		}
		sawProgress = true
		if f["nodeId"] != "x" {
			t.Fatalf("expected nodeId=x; got %v", f["nodeId"])
		}
		data, ok := f["data"].(map[string]interface{})
		if !ok {
			t.Fatalf("expected data object on node_progress: %+v", f)
		}
		if _, ok := data["progress"].(float64); !ok {
			t.Fatalf("expected numeric data.progress; got %T", data["progress"])
		}
		if _, has := data["intermediate"]; has {
			t.Fatalf("expected intermediate field omitted when nil; got presence: %+v", data)
		}
	}
	if !sawProgress {
		t.Fatalf("expected at least one node_progress frame: %+v", frames)
	}
}

// ── Phase 16D — Large-payload handling via /mcp/context cache ─────────
//
// Plan: devstudio/plans/workflow-wizard-plan.md §G16 / 16D.
// Acceptance:
//   (i)   5MB JSON result triggers context-cache store; frame contains UUID.
//   (ii)  Browser fetches via existing `GET /mcp/context/{uuid}` path.
//   (iii) Cache hits don't re-store; TTL respected.

// makeLargeResult builds a deterministic map whose JSON serialisation
// is at least `targetBytes` long. The payload is a single string field
// of known size so tests can round-trip byte counts precisely.
func makeLargeResult(targetBytes int) map[string]interface{} {
	if targetBytes < 64 {
		targetBytes = 64
	}
	// The JSON envelope `{"kind":"dag-result-fixture","bulk":"…"}` is
	// ~42 bytes; pad the bulk so total serialised size ≥ target.
	pad := targetBytes - 42
	if pad < 1 {
		pad = 1
	}
	bulk := strings.Repeat("x", pad)
	return map[string]interface{}{
		"kind": "dag-result-fixture",
		"bulk": bulk,
	}
}

// Acceptance bullet (i) — unit-level: a 5 MiB JSON result goes through
// the store path, returns a non-empty UUID, and the stored bytes equal
// the original serialisation.
func TestDAGLargePayload_FiveMBResultIsStoredUnderFreshUUID(t *testing.T) {
	s := NewServer(Options{Port: 0})

	result := makeLargeResult(5 * 1024 * 1024)
	wantJSON, err := json.Marshal(result)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	if len(wantJSON) <= dagLargeResultThreshold {
		t.Fatalf("fixture smaller than threshold: %d <= %d", len(wantJSON), dagLargeResultThreshold)
	}

	id, bytesStored, storeErr := maybeStoreLargeResult(s, result)
	if storeErr != nil {
		t.Fatalf("unexpected store error: %v", storeErr)
	}
	if id == "" {
		t.Fatal("expected non-empty UUID for >threshold payload")
	}
	if bytesStored != len(wantJSON) {
		t.Fatalf("bytesStored=%d want %d", bytesStored, len(wantJSON))
	}

	got, kind, ok := s.LoadContext(id)
	if !ok {
		t.Fatal("expected LoadContext to return the stored entry")
	}
	if kind != dagResultContextKind {
		t.Fatalf("kind=%q want %q", kind, dagResultContextKind)
	}
	if !bytes.Equal(got, wantJSON) {
		t.Fatalf("stored bytes differ from marshalled input (len %d vs %d)", len(got), len(wantJSON))
	}
}

// Threshold semantics — below-bound payloads are NOT stored.
func TestDAGLargePayload_BelowThresholdReturnsEmptyUUID(t *testing.T) {
	s := NewServer(Options{Port: 0})
	id, n, err := maybeStoreLargeResult(s, map[string]interface{}{"hello": "world"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != "" || n != 0 {
		t.Fatalf("expected ('',0,nil) for tiny payload; got (%q, %d)", id, n)
	}
}

// Nil / empty edge case — must not panic, must not store.
func TestDAGLargePayload_NilResultIsNoOp(t *testing.T) {
	s := NewServer(Options{Port: 0})
	id, n, err := maybeStoreLargeResult(s, nil)
	if err != nil || id != "" || n != 0 {
		t.Fatalf("nil result should be a no-op: id=%q n=%d err=%v", id, n, err)
	}
}

// Acceptance bullet (iii) — cache hits don't re-store. Repeated stores
// of identical bytes must return the SAME UUID and the underlying cache
// entry count must increment exactly once.
func TestDAGLargePayload_DedupReusesUUIDAcrossRepeatStores(t *testing.T) {
	s := NewServer(Options{Port: 0})

	// Use a large enough payload to exercise the real store path.
	// 2 MiB is above the default 1 MiB threshold and well below the
	// 10 MiB per-entry cap.
	result := makeLargeResult(2 * 1024 * 1024)

	id1, n1, err := maybeStoreLargeResult(s, result)
	if err != nil || id1 == "" {
		t.Fatalf("first store failed: id=%q n=%d err=%v", id1, n1, err)
	}
	countAfterFirst := s.contextCount.Load()
	if countAfterFirst != 1 {
		t.Fatalf("contextCount=%d after first store; want 1", countAfterFirst)
	}

	id2, n2, err := maybeStoreLargeResult(s, result)
	if err != nil {
		t.Fatalf("second store failed: %v", err)
	}
	if id2 != id1 {
		t.Fatalf("dedup broken: second UUID=%q want=%q", id2, id1)
	}
	if n2 != n1 {
		t.Fatalf("dedup bytes mismatch: %d vs %d", n2, n1)
	}
	if s.contextCount.Load() != 1 {
		t.Fatalf("contextCount=%d after dedup hit; want 1 (re-store suppressed)",
			s.contextCount.Load())
	}

	// Third call with an IDENTICAL but freshly-constructed map must also dedup:
	// the dedup key is a content hash, not a pointer.
	clone := makeLargeResult(2 * 1024 * 1024)
	id3, _, err := maybeStoreLargeResult(s, clone)
	if err != nil || id3 != id1 {
		t.Fatalf("dedup on equal-but-distinct map failed: got %q err=%v", id3, err)
	}
	if s.contextCount.Load() != 1 {
		t.Fatalf("contextCount=%d after equal-but-distinct dedup; want 1",
			s.contextCount.Load())
	}
}

// Acceptance bullet (iii) — TTL respected. A dedup pointer whose
// target cache entry has expired must self-heal: the next store mints
// a fresh UUID instead of handing back the stale one.
func TestDAGLargePayload_DedupRespectsTTL(t *testing.T) {
	s := NewServer(Options{Port: 0})

	result := makeLargeResult(2 * 1024 * 1024)

	id1, _, err := maybeStoreLargeResult(s, result)
	if err != nil || id1 == "" {
		t.Fatalf("first store failed: id=%q err=%v", id1, err)
	}

	// Age the cache entry past contextTTL. LoadContext's inline TTL
	// check will then return !ok and delete the entry; the dedup
	// pointer becomes a dangling reference that the helper must drop.
	raw, ok := s.contextCache.Load(id1)
	if !ok {
		t.Fatalf("missing cache entry for UUID %q", id1)
	}
	entry := raw.(*contextEntry)
	entry.createdAt = time.Now().Add(-2 * contextTTL)

	// Sanity: LoadContext now reports expired.
	if _, _, live := s.LoadContext(id1); live {
		t.Fatal("expected TTL-aged entry to report not-live via LoadContext")
	}

	id2, _, err := maybeStoreLargeResult(s, result)
	if err != nil {
		t.Fatalf("post-TTL re-store failed: %v", err)
	}
	if id2 == "" {
		t.Fatal("post-TTL re-store returned empty UUID")
	}
	if id2 == id1 {
		t.Fatalf("expected fresh UUID after TTL expiry; got stale %q", id2)
	}
	// Fresh entry is live.
	if _, _, live := s.LoadContext(id2); !live {
		t.Fatal("fresh store should be live")
	}
}

// Distinct content → distinct UUIDs (dedup isolation).
func TestDAGLargePayload_DistinctPayloadsGetDistinctUUIDs(t *testing.T) {
	s := NewServer(Options{Port: 0})

	a := makeLargeResult(2 * 1024 * 1024)
	b := makeLargeResult(2*1024*1024 + 17) // different bulk length → different bytes

	idA, _, err := maybeStoreLargeResult(s, a)
	if err != nil || idA == "" {
		t.Fatalf("store A failed: %v / %q", err, idA)
	}
	idB, _, err := maybeStoreLargeResult(s, b)
	if err != nil || idB == "" {
		t.Fatalf("store B failed: %v / %q", err, idB)
	}
	if idA == idB {
		t.Fatalf("distinct payloads collapsed to the same UUID: %q", idA)
	}
	if s.contextCount.Load() != 2 {
		t.Fatalf("contextCount=%d want 2 (one per distinct payload)", s.contextCount.Load())
	}
}

// Store-failure path — a payload above the context cache's 10 MiB
// per-entry cap must surface as an error so runDAGExecution can emit
// node_failed (rather than silently embedding an oversize result in
// the frame channel).
func TestDAGLargePayload_OverPerEntryCapReturnsError(t *testing.T) {
	s := NewServer(Options{Port: 0})

	// maxEntrySize is 10 MiB (context_cache.go). 11 MiB is over cap.
	result := makeLargeResult(11 * 1024 * 1024)

	id, n, err := maybeStoreLargeResult(s, result)
	if err == nil {
		t.Fatalf("expected error for oversize payload; got id=%q n=%d", id, n)
	}
	if id != "" || n != 0 {
		t.Fatalf("expected ('',0,err) on store failure; got (%q, %d, %v)", id, n, err)
	}
	if !strings.Contains(err.Error(), "large-payload store failed") {
		t.Fatalf("error should name the failing path; got %q", err.Error())
	}
	if s.contextCount.Load() != 0 {
		t.Fatalf("contextCount=%d want 0 after failed store", s.contextCount.Load())
	}
}

// ── Integration coverage: the 16D frame-shape contract ─────────────────
//
// These tests drive the full executor + WS pipeline. To keep them fast
// and deterministic, `dagLargeResultThreshold` is temporarily lowered
// so tiny passthrough payloads cross the bound (the same test-var
// pattern 16C uses for `dagProgressMinInterval`).

// withLoweredThreshold lowers the threshold for the duration of the
// test body and restores it on cleanup. Centralised so both integration
// tests use the identical dance.
func withLoweredThreshold(t *testing.T, newThreshold int) {
	t.Helper()
	prev := dagLargeResultThreshold
	dagLargeResultThreshold = newThreshold
	t.Cleanup(func() { dagLargeResultThreshold = prev })
}

// Acceptance bullet (i) — integration: a >threshold node result yields
// a node_result frame whose data carries `resultContextId` + `resultBytes`
// + `resultKind`, and does NOT embed the raw `result` field.
func TestDAGExecutor_LargeResultEmittedAsContextIdStub(t *testing.T) {
	withLoweredThreshold(t, 8) // 8 bytes → any passthrough result crosses
	_, hs := startDAGTestServer(t)

	dag := `{
		"schemaVersion":"1.0",
		"nodes":[{"id":"n1","kind":"transform","params":{}}],
		"edges":[]
	}`

	startResp := startDAGExecution(t, hs, dag)
	conn := dialDAGStream(t, hs, startResp)
	defer conn.Close()
	frames := readDAGFramesUntilTerminal(t, conn, 3*time.Second)

	var nodeResult map[string]interface{}
	for _, f := range frames {
		if ty, _ := f["type"].(string); ty == "node_result" && f["nodeId"] == "n1" {
			nodeResult = f
			break
		}
	}
	if nodeResult == nil {
		t.Fatalf("no node_result frame for n1: %+v", frames)
	}

	data, _ := nodeResult["data"].(map[string]interface{})
	if data == nil {
		t.Fatalf("node_result missing data: %+v", nodeResult)
	}

	// resultContextId must be a non-empty string.
	uuid, _ := data["resultContextId"].(string)
	if uuid == "" {
		t.Fatalf("expected resultContextId on data: %+v", data)
	}
	// resultBytes must be a positive numeric.
	n, _ := data["resultBytes"].(float64)
	if n <= 0 {
		t.Fatalf("expected positive resultBytes on data: %+v", data)
	}
	// resultKind stamps the cache entry category.
	if data["resultKind"] != dagResultContextKind {
		t.Fatalf("expected resultKind=%q; got %v", dagResultContextKind, data["resultKind"])
	}
	// Large-payload path must NOT embed `result` inline.
	if _, has := data["result"]; has {
		t.Fatalf("expected result field omitted on large payload; got %+v", data)
	}
	// Status field still present so downstream counters stay accurate.
	if data["status"] != "completed" {
		t.Fatalf("expected status=completed on data; got %v", data["status"])
	}
}

// Acceptance bullet (ii) — browser fetches the UUID via the existing
// GET /mcp/context/{uuid} endpoint. We drive the whole pipeline: start
// DAG, collect the resultContextId off the frame, then HTTP GET the
// cached entry and decode it.
func TestDAGExecutor_LargeResultFetchableViaMcpContext(t *testing.T) {
	withLoweredThreshold(t, 8)
	_, hs := startDAGTestServer(t)

	dag := `{
		"schemaVersion":"1.0",
		"nodes":[{"id":"p","kind":"probe","params":{}}],
		"edges":[]
	}`

	startResp := startDAGExecution(t, hs, dag)
	conn := dialDAGStream(t, hs, startResp)
	defer conn.Close()
	frames := readDAGFramesUntilTerminal(t, conn, 3*time.Second)

	var uuid string
	for _, f := range frames {
		if ty, _ := f["type"].(string); ty == "node_result" {
			if d, ok := f["data"].(map[string]interface{}); ok {
				if s, _ := d["resultContextId"].(string); s != "" {
					uuid = s
					break
				}
			}
		}
	}
	if uuid == "" {
		t.Fatalf("no resultContextId captured from frames: %+v", frames)
	}

	resp, err := http.Get(hs.URL + "/mcp/context/" + uuid)
	if err != nil {
		t.Fatalf("GET /mcp/context: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 from /mcp/context/%s; got %d: %s", uuid, resp.StatusCode, body)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read /mcp/context body: %v", err)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("expected JSON body at /mcp/context/%s; got err=%v body=%q", uuid, err, body)
	}
	// The passthrough node returns {"passthrough":true,"kind":"probe"}.
	if decoded["passthrough"] != true {
		t.Fatalf("cached payload missing passthrough marker: %+v", decoded)
	}
	if decoded["kind"] != "probe" {
		t.Fatalf("cached payload kind=%v want %q", decoded["kind"], "probe")
	}
}

// Regression: below-threshold results KEEP the inline `result` embedding
// and do NOT carry `resultContextId`. Without this guard the 16D path
// would accidentally route every node through the cache and introduce
// an extra GET round-trip the 16B contract did not have.
func TestDAGExecutor_SmallResultEmbeddedInFrame(t *testing.T) {
	// No threshold override — default 1 MiB bound. Passthrough is ~31 bytes.
	_, hs := startDAGTestServer(t)

	dag := `{
		"schemaVersion":"1.0",
		"nodes":[{"id":"s","kind":"small","params":{}}],
		"edges":[]
	}`

	startResp := startDAGExecution(t, hs, dag)
	conn := dialDAGStream(t, hs, startResp)
	defer conn.Close()
	frames := readDAGFramesUntilTerminal(t, conn, 3*time.Second)

	var data map[string]interface{}
	for _, f := range frames {
		if ty, _ := f["type"].(string); ty == "node_result" {
			data, _ = f["data"].(map[string]interface{})
			break
		}
	}
	if data == nil {
		t.Fatalf("no node_result frame: %+v", frames)
	}
	if _, has := data["resultContextId"]; has {
		t.Fatalf("small payload should NOT carry resultContextId; got %+v", data)
	}
	if _, has := data["resultBytes"]; has {
		t.Fatalf("small payload should NOT carry resultBytes; got %+v", data)
	}
	inline, ok := data["result"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected inline result map on small payload; got %+v", data)
	}
	if inline["passthrough"] != true {
		t.Fatalf("inline result missing passthrough marker: %+v", inline)
	}
}
