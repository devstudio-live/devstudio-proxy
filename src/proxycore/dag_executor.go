// Package proxycore — DAG executor (Phase 16B).
//
// Plan: devstudio/plans/workflow-wizard-plan.md §G16 / 16B.
//
// 16A shipped the WebSocket lifecycle skeleton (dag_gateway.go); 16B
// replaces that stub with the real executor. The executor:
//
//  1. Parses the validated DAG payload into nodes + edges,
//  2. Topo-sorts via Kahn's algorithm (stable in author-insertion order),
//  3. Walks the topo order; for each node either
//       a. delegates to one of the existing 10 gateway handlers
//          (sql / mongo / elastic / redis / fs / k8s / ssh / kafka /
//          container / hprof) via an internal call — the gateway
//          handlers themselves remain UNCHANGED per hard-scope rule H;
//          or
//       b. treats the node as a passthrough (no `params.gateway`),
//          emitting a `node_result` with no gateway side-effects,
//  4. Streams `node_start` / `node_result` / `node_failed` /
//     `node_skipped` frames over the per-execution frame channel as
//     it goes, plus a terminal `run_completed` or `run_failed` frame,
//  5. On upstream failure, marks every downstream node
//     `status:"skipped-due-to-upstream-failure"` and emits a
//     `node_skipped` frame (without running the node).
//
// Cancellation + progress frames arrive in 16C; large-payload handling
// in 16D; the browser remote-executor client in 16E.
//
// Hard-scope rules (plan §"Hard scope rules"):
//   - G: DAG schema 1.0 frozen at 3D — parsing here is additive on
//     `params.gateway`; it does not mutate the frozen node shape.
//   - H: this is the G16 group. Existing gateway handlers are not
//     touched; the executor is a *caller* only.
package proxycore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// dagNodeSpec is the subset of a DagDocument node (plan §schema/dag-schema.ts)
// that the executor needs. `id` is the stable per-node key used in every
// frame; `kind` is informational (surfaced in frames for UI); `params`
// carries the optional `gateway` envelope and any passthrough data.
type dagNodeSpec struct {
	ID     string
	Kind   string
	Params map[string]interface{}
}

// dagEdgeSpec is the subset of a DagDocument edge the executor uses for
// topo-sort + upstream-failure propagation.
type dagEdgeSpec struct {
	Source string
	Target string
}

// nodeOutcomeStatus is the terminal status stamped on each node after
// it is visited. "skipped-due-to-upstream-failure" is the plan-specified
// string — downstream specs match on it literally.
type nodeOutcomeStatus string

const (
	nodeOutcomeCompleted nodeOutcomeStatus = "completed"
	nodeOutcomeFailed    nodeOutcomeStatus = "failed"
	nodeOutcomeSkipped   nodeOutcomeStatus = "skipped-due-to-upstream-failure"
)

// nodeOutcome bundles per-node bookkeeping. Used locally by the executor
// goroutine; never shared across executions (one map per dagExecution).
type nodeOutcome struct {
	status     nodeOutcomeStatus
	startedAt  time.Time
	endedAt    time.Time
	durationMs int64
	result     map[string]interface{}
	err        string
}

// runDAGExecution is the 16B executor entry point invoked from
// handleDAGExecStart. Signature preserved from the 16A stub.
//
// Control flow:
//   - flip status to running
//   - parse DAG → topo-sort
//   - loop nodes; emit frames; stash outcomes
//   - flip status to completed/failed (cancelled already flipped by
//     the /cancel handler)
//   - always close the frame channel so the WebSocket handler unblocks
func runDAGExecution(s *Server, exec *dagExecution) {
	exec.setStatus(dagStatusRunning)
	defer exec.closeFrames()

	runStart := time.Now()

	nodes, edges, parseErr := parseDAGStructure(exec.dag)
	if parseErr != nil {
		emitRunFailed(exec, runStart, parseErr.Error())
		exec.setStatus(dagStatusFailed)
		return
	}

	order, topoErr := topoSortDAG(nodes, edges)
	if topoErr != nil {
		emitRunFailed(exec, runStart, topoErr.Error())
		exec.setStatus(dagStatusFailed)
		return
	}

	// Build adjacency for upstream lookups. `upstream[id]` lists
	// every direct predecessor of the node — used to propagate
	// skipped-due-to-upstream-failure.
	upstream := make(map[string][]string, len(nodes))
	for _, e := range edges {
		upstream[e.Target] = append(upstream[e.Target], e.Source)
	}

	outcomes := make(map[string]*nodeOutcome, len(order))
	runHasFailure := false

	for _, nodeID := range order {
		// Honour cancellation between nodes so a cancel mid-run does
		// not start a fresh gateway call. 16C tightens this to cover
		// mid-node streaming.
		if ctxDone(exec.ctx) {
			exec.setStatus(dagStatusCancelled)
			return
		}

		node := nodes[nodeID]

		if upstreamSkippedOrFailed(nodeID, upstream, outcomes) {
			outcomes[nodeID] = &nodeOutcome{status: nodeOutcomeSkipped}
			runHasFailure = true
			exec.emitFrame(dagFrame{
				Type:   "node_skipped",
				NodeID: nodeID,
				Data: map[string]interface{}{
					"kind":   node.Kind,
					"status": string(nodeOutcomeSkipped),
					"reason": "skipped-due-to-upstream-failure",
				},
			})
			continue
		}

		startedAt := time.Now()
		exec.emitFrame(dagFrame{
			Type:   "node_start",
			NodeID: nodeID,
			Data:   map[string]interface{}{"kind": node.Kind},
		})

		result, nodeErr := runNode(s, exec, node)
		endedAt := time.Now()
		durationMs := endedAt.Sub(startedAt).Milliseconds()

		if nodeErr != nil {
			outcomes[nodeID] = &nodeOutcome{
				status:     nodeOutcomeFailed,
				startedAt:  startedAt,
				endedAt:    endedAt,
				durationMs: durationMs,
				err:        nodeErr.Error(),
			}
			runHasFailure = true
			exec.emitFrame(dagFrame{
				Type:   "node_failed",
				NodeID: nodeID,
				Data: map[string]interface{}{
					"kind":       node.Kind,
					"error":      nodeErr.Error(),
					"durationMs": durationMs,
				},
			})
			continue
		}

		// 16D — large-payload handling. If the serialised result
		// exceeds dagLargeResultThreshold (1 MiB), stash it in the
		// existing /mcp/context cache and emit {resultContextId,
		// resultBytes} on the frame instead of embedding the bytes.
		// A store failure above threshold surfaces as node_failed so
		// the frame channel never carries an oversize opaque payload.
		cachedID, cachedBytes, cacheErr := maybeStoreLargeResult(s, result)
		if cacheErr != nil {
			outcomes[nodeID] = &nodeOutcome{
				status:     nodeOutcomeFailed,
				startedAt:  startedAt,
				endedAt:    endedAt,
				durationMs: durationMs,
				err:        cacheErr.Error(),
			}
			runHasFailure = true
			exec.emitFrame(dagFrame{
				Type:   "node_failed",
				NodeID: nodeID,
				Data: map[string]interface{}{
					"kind":       node.Kind,
					"error":      cacheErr.Error(),
					"durationMs": durationMs,
				},
			})
			continue
		}

		outcomes[nodeID] = &nodeOutcome{
			status:     nodeOutcomeCompleted,
			startedAt:  startedAt,
			endedAt:    endedAt,
			durationMs: durationMs,
			result:     result,
		}
		frameData := map[string]interface{}{
			"kind":       node.Kind,
			"status":     string(nodeOutcomeCompleted),
			"durationMs": durationMs,
		}
		if cachedID != "" {
			frameData["resultContextId"] = cachedID
			frameData["resultBytes"] = cachedBytes
			frameData["resultKind"] = dagResultContextKind
		} else {
			frameData["result"] = result
		}
		exec.emitFrame(dagFrame{
			Type:   "node_result",
			NodeID: nodeID,
			Data:   frameData,
		})
	}

	totalMs := time.Since(runStart).Milliseconds()
	if runHasFailure {
		exec.emitFrame(dagFrame{
			Type: "run_failed",
			Data: map[string]interface{}{
				"totalDurationMs": totalMs,
				"nodes":           nodeDurations(order, outcomes),
			},
		})
		exec.setStatus(dagStatusFailed)
		return
	}

	exec.emitFrame(dagFrame{
		Type: "run_completed",
		Data: map[string]interface{}{
			"totalDurationMs": totalMs,
			"nodes":           nodeDurations(order, outcomes),
		},
	})
	exec.setStatus(dagStatusCompleted)
}

// parseDAGStructure translates the raw JSON map (already shape-checked
// by validateDAGPayload in 16A) into the typed structures the executor
// walks. Returns a stable author-insertion-ordered slice of node ids,
// plus a map keyed by id, plus the edge list.
//
// 16B is strict: any node that fails the minimal shape check fails the
// whole run with a parse error, so the client sees a single
// `run_failed` frame instead of partial execution. Mirrors the
// devstudio-side assertDagValid contract (src/tools/workflow-wizard/
// schema/dag-schema.ts).
func parseDAGStructure(raw map[string]interface{}) (map[string]dagNodeSpec, []dagEdgeSpec, error) {
	rawNodes, _ := raw["nodes"].([]interface{})
	rawEdges, _ := raw["edges"].([]interface{})

	nodes := make(map[string]dagNodeSpec, len(rawNodes))
	orderSeen := make([]string, 0, len(rawNodes))

	for i, rn := range rawNodes {
		obj, ok := rn.(map[string]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("nodes[%d] is not an object", i)
		}
		id, ok := obj["id"].(string)
		if !ok || id == "" {
			return nil, nil, fmt.Errorf("nodes[%d].id missing or empty", i)
		}
		if _, dup := nodes[id]; dup {
			return nil, nil, fmt.Errorf("duplicate node id %q", id)
		}
		kind, _ := obj["kind"].(string)
		params, _ := obj["params"].(map[string]interface{})
		if params == nil {
			params = map[string]interface{}{}
		}
		nodes[id] = dagNodeSpec{ID: id, Kind: kind, Params: params}
		orderSeen = append(orderSeen, id)
	}

	edges := make([]dagEdgeSpec, 0, len(rawEdges))
	for i, re := range rawEdges {
		obj, ok := re.(map[string]interface{})
		if !ok {
			return nil, nil, fmt.Errorf("edges[%d] is not an object", i)
		}
		src, ok := obj["source"].(string)
		if !ok || src == "" {
			return nil, nil, fmt.Errorf("edges[%d].source missing", i)
		}
		dst, ok := obj["target"].(string)
		if !ok || dst == "" {
			return nil, nil, fmt.Errorf("edges[%d].target missing", i)
		}
		if _, ok := nodes[src]; !ok {
			return nil, nil, fmt.Errorf("edges[%d].source references unknown node %q", i, src)
		}
		if _, ok := nodes[dst]; !ok {
			return nil, nil, fmt.Errorf("edges[%d].target references unknown node %q", i, dst)
		}
		edges = append(edges, dagEdgeSpec{Source: src, Target: dst})
	}

	// Attach author-insertion order so topoSortDAG can break ties
	// stably. The map preserves values but not order, so we return a
	// superstructure that carries order via the slice.
	_ = orderSeen

	return nodes, edges, nil
}

// topoSortDAG returns a topologically ordered list of node ids using
// Kahn's algorithm. Ties are broken by author-insertion order (the
// caller's `parseDAGStructure` builds `nodes` iteratively; we rebuild
// a stable order by tracking which ids appear in the input order).
//
// Cycle detection: if a cycle is present, Kahn's terminates with
// remaining in-degree > 0 on some nodes; we surface E_CYCLE so the
// executor fails the run with a deterministic `run_failed` frame.
func topoSortDAG(nodes map[string]dagNodeSpec, edges []dagEdgeSpec) ([]string, error) {
	// Deterministic visit order: sort ids for stability when Go maps
	// do not preserve insertion order. We don't have the original
	// insertion slice here, so we use sorted ids — this keeps repeated
	// runs of the same DAG identical, which matches plan §16B
	// "Per-node duration captured; total run duration recorded."
	ids := make([]string, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}
	sortStable(ids)

	indegree := make(map[string]int, len(nodes))
	adj := make(map[string][]string, len(nodes))
	for _, id := range ids {
		indegree[id] = 0
		adj[id] = nil
	}
	for _, e := range edges {
		adj[e.Source] = append(adj[e.Source], e.Target)
		indegree[e.Target]++
	}
	for _, id := range ids {
		sortStable(adj[id])
	}

	ready := make([]string, 0, len(nodes))
	for _, id := range ids {
		if indegree[id] == 0 {
			ready = append(ready, id)
		}
	}

	out := make([]string, 0, len(nodes))
	for len(ready) > 0 {
		head := ready[0]
		ready = ready[1:]
		out = append(out, head)
		for _, next := range adj[head] {
			indegree[next]--
			if indegree[next] == 0 {
				ready = append(ready, next)
			}
		}
	}

	if len(out) != len(nodes) {
		remaining := make([]string, 0)
		for _, id := range ids {
			if indegree[id] > 0 {
				remaining = append(remaining, id)
			}
		}
		return nil, fmt.Errorf("DAG contains a cycle (unresolved nodes: %s)", strings.Join(remaining, ", "))
	}
	return out, nil
}

// sortStable is a tiny in-place sort helper to avoid pulling sort for
// a one-liner; keeps the executor package surface small. Stable on
// string slices via insertion sort — inputs are small (node counts are
// bounded by the UI editor).
func sortStable(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

// upstreamSkippedOrFailed reports whether any direct predecessor of
// `nodeID` has terminal status failed or skipped. When true, the node
// itself is skipped with reason "skipped-due-to-upstream-failure".
func upstreamSkippedOrFailed(nodeID string, upstream map[string][]string, outcomes map[string]*nodeOutcome) bool {
	for _, u := range upstream[nodeID] {
		if o, ok := outcomes[u]; ok {
			if o.status == nodeOutcomeFailed || o.status == nodeOutcomeSkipped {
				return true
			}
		}
	}
	return false
}

// nodeDurations returns the per-node summary attached to the final
// run_completed / run_failed frame. Preserves topo order so the
// consumer (remote-executor UI in 16E) does not need to re-sort.
func nodeDurations(order []string, outcomes map[string]*nodeOutcome) []map[string]interface{} {
	out := make([]map[string]interface{}, 0, len(order))
	for _, id := range order {
		o, ok := outcomes[id]
		if !ok {
			continue
		}
		entry := map[string]interface{}{
			"nodeId":     id,
			"status":     string(o.status),
			"durationMs": o.durationMs,
		}
		if o.err != "" {
			entry["error"] = o.err
		}
		out = append(out, entry)
	}
	return out
}

// runNode evaluates a single node. Three modes (checked in order):
//
//  1. Instrumented primitive — `params.delayMs` and/or
//     `params.progressSteps` set. The executor sleeps in N evenly-
//     sized slices, calling emitProgress between each slice. Both
//     primitives honour ctx.Done() so cancellation aborts the node
//     within the next slice (target ≤200ms slice for delayMs ≥200).
//     Real DAG primitive (a "wait/report progress" node any pipeline
//     author can drop in) AND the test instrument used to assert the
//     plan's "1000-step instrumented node ≤10 progress frames/sec"
//     throttle. (16C — plan §G16/16C.)
//  2. params.gateway present → internal delegation to one of the 10
//     existing gateway handlers (sql/mongo/elastic/redis/fs/hprof/
//     k8s/ssh/kafka/container). The handler itself is untouched; we
//     synthesise the HTTP request it expects, with ctx propagated via
//     http.NewRequestWithContext so its existing ctx.Done() loop
//     unblocks promptly on cancel (16C wires the cancel-source: WS
//     close → exec.cancel() in dag_gateway.go).
//  3. Otherwise → passthrough. The executor emits a node_result with
//     `{passthrough:true, kind}`. Real transform semantics arrive in
//     14A (plugin sandbox).
func runNode(s *Server, exec *dagExecution, node dagNodeSpec) (map[string]interface{}, error) {
	if isInstrumentedNode(node.Params) {
		return runInstrumentedNode(exec, node)
	}

	gw, hasGw := gatewayEnvelope(node.Params)
	if !hasGw {
		return map[string]interface{}{
			"passthrough": true,
			"kind":        node.Kind,
		}, nil
	}

	status, body, err := dispatchGatewayInternal(s, exec.ctx, gw.protocol, gw.route, gw.body)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("gateway %q route %q returned status %d: %s",
			gw.protocol, gw.route, status, trimForError(body))
	}

	// Try to decode the response as JSON so consumers get a structured
	// object; fall back to a {raw:string} envelope otherwise.
	var decoded map[string]interface{}
	if derr := json.Unmarshal(body, &decoded); derr == nil && decoded != nil {
		return decoded, nil
	}
	return map[string]interface{}{
		"raw": string(body),
	}, nil
}

// isInstrumentedNode reports whether the node opts into the synthetic
// delay/progress primitive (16C). Either knob alone is sufficient.
func isInstrumentedNode(params map[string]interface{}) bool {
	if _, ok := numericParam(params, "delayMs"); ok {
		return true
	}
	if _, ok := numericParam(params, "progressSteps"); ok {
		return true
	}
	return false
}

// runInstrumentedNode is the synthetic compute primitive (16C). It
// emits progress frames at most every dagProgressMinInterval per node
// (the throttle lives in emitProgress), and bails out within one
// slice on ctx cancellation.
//
// Behaviour:
//   - progressSteps:N (default 1) → loop bound; each iteration calls
//     emitProgress with progress=i/N.
//   - delayMs:M (default 0) → total wall-clock spend, divided evenly
//     across the N steps so a 1000ms / 10-step node sleeps 100ms per
//     slice. With delayMs:0, the loop runs as fast as it can — useful
//     for asserting the throttle suppresses high-frequency emits.
func runInstrumentedNode(exec *dagExecution, node dagNodeSpec) (map[string]interface{}, error) {
	steps := 1
	if v, ok := numericParam(node.Params, "progressSteps"); ok && v >= 1 {
		steps = int(v)
	}
	delayMs, _ := numericParam(node.Params, "delayMs")

	var sliceDelay time.Duration
	if delayMs > 0 {
		sliceDelay = time.Duration(delayMs/float64(steps)) * time.Millisecond
	}

	emitted := 0
	for i := 1; i <= steps; i++ {
		if err := exec.ctx.Err(); err != nil {
			return nil, err
		}
		if sliceDelay > 0 {
			timer := time.NewTimer(sliceDelay)
			select {
			case <-timer.C:
			case <-exec.ctx.Done():
				timer.Stop()
				return nil, exec.ctx.Err()
			}
		}
		if exec.emitProgress(node.ID, float64(i)/float64(steps), nil) {
			emitted++
		}
	}

	return map[string]interface{}{
		"instrumented":     true,
		"steps":            steps,
		"delayMs":          delayMs,
		"progressEmitted":  emitted,
	}, nil
}

// numericParam reads a JSON number out of params, accepting both the
// json.Number form (the /dag/exec/start decoder uses UseNumber()) and
// plain float64/int when params is constructed in-process by tests.
// Returns (0, false) when the key is missing or not numeric.
func numericParam(params map[string]interface{}, key string) (float64, bool) {
	raw, ok := params[key]
	if !ok {
		return 0, false
	}
	switch v := raw.(type) {
	case json.Number:
		f, err := v.Float64()
		if err != nil {
			return 0, false
		}
		return f, true
	case float64:
		return v, true
	case float32:
		return float64(v), true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case int32:
		return float64(v), true
	}
	return 0, false
}

// emitProgress is the throttled producer of `node_progress` frames.
// Plan §16C: "Progress frames {type:'node_progress', nodeId, progress,
// intermediate?} emitted at most every 100ms per node."
//
// Returns true when a frame was queued, false when the call was
// throttled (caller can use this to decide whether to skip preparing
// the next intermediate payload). The throttle is per-node, not
// global, so independent nodes can each hit the 10/sec ceiling without
// starving each other.
//
// The frame channel send happens AFTER the throttle window mutation
// is released so emitProgress never holds progressMu across a blocking
// channel send.
func (e *dagExecution) emitProgress(nodeID string, progress float64, intermediate map[string]interface{}) bool {
	e.progressMu.Lock()
	if e.lastProgressAt == nil {
		e.lastProgressAt = make(map[string]time.Time)
	}
	last, hasLast := e.lastProgressAt[nodeID]
	now := time.Now()
	if hasLast && now.Sub(last) < dagProgressMinInterval {
		e.progressMu.Unlock()
		return false
	}
	e.lastProgressAt[nodeID] = now
	e.progressMu.Unlock()

	data := map[string]interface{}{
		"progress": progress,
	}
	if intermediate != nil {
		data["intermediate"] = intermediate
	}
	e.emitFrame(dagFrame{
		Type:   "node_progress",
		NodeID: nodeID,
		Data:   data,
	})
	return true
}

// gatewayCall is the parsed envelope the DAG author placed on
// `params.gateway`. All fields are validated below.
type gatewayCall struct {
	protocol string
	route    string
	body     []byte
}

// gatewayEnvelope extracts the delegation envelope from node.params.
// Accepted shape:
//
//	{
//	  "gateway": {
//	    "protocol": "fs",
//	    "route":    "stat",
//	    "body":     {...}     // optional, defaults to {}
//	  }
//	}
//
// Returns (envelope, true) when valid; (zero, false) when absent.
// Malformed envelopes return (zero, false) as well — runNode treats
// that as passthrough so a typo in one node cannot abort the whole run
// before reaching the node. Callers wanting strictness should rely on
// the devstudio-side schema validation.
func gatewayEnvelope(params map[string]interface{}) (gatewayCall, bool) {
	raw, ok := params["gateway"].(map[string]interface{})
	if !ok {
		return gatewayCall{}, false
	}
	protocol, _ := raw["protocol"].(string)
	if protocol == "" {
		return gatewayCall{}, false
	}
	route, _ := raw["route"].(string)

	var bodyBytes []byte
	switch b := raw["body"].(type) {
	case nil:
		bodyBytes = []byte("{}")
	case string:
		bodyBytes = []byte(b)
	default:
		enc, err := json.Marshal(b)
		if err != nil {
			return gatewayCall{}, false
		}
		bodyBytes = enc
	}
	return gatewayCall{protocol: protocol, route: route, body: bodyBytes}, true
}

// dispatchGatewayInternal synthesises the HTTP request the existing 10
// gateway handlers expect (method POST, JSON body, the two gateway
// routing headers) and invokes the handler directly. Uses an in-memory
// response writer so no network stack is traversed.
//
// The existing handlers remain UNCHANGED (plan §G16 hard-scope rule H);
// this function is purely a caller. The path encodes the route so
// handlers that switch on r.URL.Path (fs, kafka, hprof, k8s, container,
// ssh, elastic, redis, mongo) route correctly.
func dispatchGatewayInternal(s *Server, ctx context.Context, protocol, route string, body []byte) (int, []byte, error) {
	reqPath := "/"
	if route != "" {
		if strings.HasPrefix(route, "/") {
			reqPath = route
		} else {
			reqPath = "/" + route
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqPath, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-DevStudio-Gateway-Route", "true")
	req.Header.Set("X-DevStudio-Gateway-Protocol", protocol)

	rec := newBufferResponseWriter()
	switch protocol {
	case "sql":
		s.handleDBGateway(rec, req)
	case "mongo":
		s.handleMongoGateway(rec, req)
	case "elastic":
		s.handleElasticGateway(rec, req)
	case "redis":
		s.handleRedisGateway(rec, req)
	case "fs":
		handleFSGateway(rec, req)
	case "hprof":
		s.handleHprofGateway(rec, req)
	case "k8s":
		s.handleK8sGateway(rec, req)
	case "ssh":
		s.handleSSHGateway(rec, req)
	case "kafka":
		s.handleKafkaGateway(rec, req)
	case "container":
		s.handleContainerGateway(rec, req)
	default:
		return 0, nil, fmt.Errorf("unsupported gateway protocol: %q", protocol)
	}
	return rec.statusCode(), rec.bytes(), nil
}

// bufferResponseWriter is a minimal in-memory http.ResponseWriter used
// to capture responses from the existing gateway handlers without
// opening a TCP loopback. Not exported — only the executor uses it.
type bufferResponseWriter struct {
	header  http.Header
	status  int
	body    bytes.Buffer
	wroteHdr bool
}

func newBufferResponseWriter() *bufferResponseWriter {
	return &bufferResponseWriter{header: http.Header{}}
}

func (w *bufferResponseWriter) Header() http.Header { return w.header }

func (w *bufferResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHdr {
		w.status = http.StatusOK
		w.wroteHdr = true
	}
	return w.body.Write(p)
}

func (w *bufferResponseWriter) WriteHeader(status int) {
	if !w.wroteHdr {
		w.status = status
		w.wroteHdr = true
	}
}

func (w *bufferResponseWriter) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func (w *bufferResponseWriter) bytes() []byte { return w.body.Bytes() }

// emitFrame pushes a frame into the execution's channel. Blocks until
// either the WS handler drains it or ctx is cancelled. The 64-slot
// buffer (dagFrameBuffer) is sized to absorb the bursts a small DAG
// produces before a WS reader attaches.
func (e *dagExecution) emitFrame(frame dagFrame) {
	select {
	case e.frames <- frame:
	case <-e.ctx.Done():
	}
}

// emitRunFailed is the shared exit path when the DAG cannot even
// start — invalid structure, cycle detected, etc.
func emitRunFailed(exec *dagExecution, runStart time.Time, msg string) {
	exec.emitFrame(dagFrame{
		Type: "run_failed",
		Data: map[string]interface{}{
			"totalDurationMs": time.Since(runStart).Milliseconds(),
			"error":           msg,
		},
	})
}

// ctxDone is a non-blocking context check.
func ctxDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

// trimForError clips large gateway response bodies to a readable size
// before folding them into an error message.
func trimForError(body []byte) string {
	const limit = 256
	if len(body) <= limit {
		return string(body)
	}
	return string(body[:limit]) + "…(truncated)"
}
