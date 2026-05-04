// Package proxycore — DAG execution gateway (Phase 16A skeleton).
//
// Plan: devstudio/plans/workflow-wizard-plan.md §G16 / 16A.
//
// Exposes three HTTP routes used by the Workflow Wizard remote-executor
// (devstudio/src/tools/workflow-wizard/runtime/remote-executor.js, lands
// in 16E). 16A only ships the skeleton: route registration, the
// in-flight execution registry, per-execution context cancellation, and
// the WebSocket upgrade handshake. Topo-sort + per-node delegation lands
// in 16B (dag_executor.go, currently a stub that just emits a
// 'completed' frame so the WS lifecycle is exercisable end-to-end).
//
//	POST /dag/exec/start             → 202 + {executionId, wsUrl}
//	GET  /dag/exec/{id}/cancel       → 200, cancels per-execution ctx
//	GET  /dag/exec/{id}/stream       → WebSocket upgrade; first frame
//	                                    is always {"type":"connected"}.
//
// Hard-scope rule H (plan §"Hard scope rules"): G16 is the only group
// allowed to touch this repository; the existing 10 gateway handlers
// (sql/mongo/elastic/redis/fs/k8s/ssh/kafka/container/hprof) remain
// unchanged. The DAG executor delegates to them via internal call only.
package proxycore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// dagExecutionStatus is the lifecycle state of a registered execution.
// The transitions are: pending → running → (completed | failed | cancelled).
type dagExecutionStatus string

const (
	dagStatusPending   dagExecutionStatus = "pending"
	dagStatusRunning   dagExecutionStatus = "running"
	dagStatusCompleted dagExecutionStatus = "completed"
	dagStatusFailed    dagExecutionStatus = "failed"
	dagStatusCancelled dagExecutionStatus = "cancelled"
)

// dagFrame is the wire envelope sent over the WebSocket. Every frame has
// a `type`; node-level frames also carry `nodeId`. 16A only emits
// "connected" + the terminal frame; 16B/16C add node_start/node_result/
// node_failed/node_progress.
type dagFrame struct {
	Type   string                 `json:"type"`
	NodeID string                 `json:"nodeId,omitempty"`
	Data   map[string]interface{} `json:"data,omitempty"`
}

// dagExecution is one in-flight DAG run. Concurrency model: status +
// completedAt are guarded by mu; frames is a buffered channel that the
// executor goroutine writes to and the WebSocket handler reads from.
// 16C adds the per-node `lastProgressAt` map (guarded by progressMu) so
// `emitProgress` can throttle high-frequency progress callbacks down to
// the plan-mandated ≤10 frames/sec ceiling.
type dagExecution struct {
	id          string
	dag         map[string]interface{}
	ctx         context.Context
	cancel      context.CancelFunc
	frames      chan dagFrame
	createdAt   time.Time
	mu          sync.Mutex
	status      dagExecutionStatus
	completedAt time.Time
	// closeOnce guards frames-channel close so cancel + executor finish
	// can race without panicking on double-close.
	closeOnce sync.Once
	// progressMu guards lastProgressAt. Held only for the duration of
	// the throttle decision in emitProgress — never across the actual
	// frame send (which can block on the frame channel).
	progressMu     sync.Mutex
	lastProgressAt map[string]time.Time
}

// dagCompletedTTL is how long a finished execution stays in the
// in-flight registry before the reaper removes it. Plan §16A acceptance:
// "In-flight registry cleans up entries 60s after completion."
// Test code lowers this via the package-level var below.
var dagCompletedTTL = 60 * time.Second

// dagReapInterval is how often the background reaper sweeps the
// registry. Independent from TTL so tests can shorten both cleanly.
var dagReapInterval = 5 * time.Second

// dagFrameBuffer sizes the per-execution frame channel. Picked to
// absorb the burst of node_start/node_result frames a small DAG emits
// without blocking the executor goroutine when no WS is yet attached.
const dagFrameBuffer = 64

// dagMaxBodyBytes caps the /dag/exec/start request body. The DAG
// itself is bounded by client-side editor limits, but the proxy must
// refuse pathological bodies before they pin a goroutine.
const dagMaxBodyBytes = 1 * 1024 * 1024

// dagProgressMinInterval is the per-node minimum interval between two
// `node_progress` frames. Plan §16C acceptance: "Progress frames
// emitted at most every 100ms per node" → "≤10 progress frames/sec".
// Surfaced as a package-level var so the throttle test can lower it
// without altering production behaviour.
var dagProgressMinInterval = 100 * time.Millisecond

// setStatus updates the execution status atomically and stamps
// completedAt on terminal transitions so the reaper can age it out.
//
// 16C — cancellation is sticky: once status is `cancelled`, no later
// transition (failed/completed) can overwrite it. The executor races
// the cancel handler — it would otherwise observe ctx.Err() mid-node,
// emit a node_failed frame, then call setStatus(failed) AFTER the
// cancel handler already set cancelled. Pinning cancelled keeps the
// user-visible registry status honest.
func (e *dagExecution) setStatus(s dagExecutionStatus) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.status == dagStatusCancelled {
		return
	}
	e.status = s
	switch s {
	case dagStatusCompleted, dagStatusFailed, dagStatusCancelled:
		if e.completedAt.IsZero() {
			e.completedAt = time.Now()
		}
	}
}

// snapshot returns a stable copy of mutable fields under the mutex.
// Callers must NOT mutate the returned struct's pointer fields.
func (e *dagExecution) snapshot() (dagExecutionStatus, time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.status, e.completedAt
}

// closeFrames closes the frame channel exactly once; safe to call from
// either the cancel handler or the executor's finally path.
func (e *dagExecution) closeFrames() {
	e.closeOnce.Do(func() { close(e.frames) })
}

// handleDAGGateway is the single entry point for /dag/exec/* routed
// before the header-based gateway dispatch in proxy.go.
func (s *Server) handleDAGGateway(w http.ResponseWriter, r *http.Request) {
	setCORS(w, r)
	path := r.URL.Path

	if path == "/dag/exec/start" {
		if r.Method != http.MethodPost {
			writeDAGError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.handleDAGExecStart(w, r)
		return
	}

	id, suffix, ok := splitDAGExecPath(path)
	if !ok {
		writeDAGError(w, http.StatusNotFound, "not found")
		return
	}
	switch suffix {
	case "cancel":
		if r.Method != http.MethodGet {
			writeDAGError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		s.handleDAGExecCancel(w, r, id)
	case "stream":
		s.handleDAGExecStream(w, r, id)
	default:
		writeDAGError(w, http.StatusNotFound, "not found")
	}
}

// splitDAGExecPath extracts {id, suffix} from "/dag/exec/{id}/{suffix}".
// Returns ok=false on any other shape so the caller can 404.
func splitDAGExecPath(path string) (id, suffix string, ok bool) {
	const prefix = "/dag/exec/"
	if !strings.HasPrefix(path, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(path, prefix)
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

// handleDAGExecStart parses the DAG payload, registers a new execution,
// hands it off to the executor goroutine, and returns 202 Accepted with
// the execution id + WebSocket URL the client should connect to.
//
// Spec acceptance: "POST /dag/exec/start with valid DAG returns
// 202 Accepted + {executionId, wsUrl}".
func (s *Server) handleDAGExecStart(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, dagMaxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.UseNumber()

	var dag map[string]interface{}
	if err := dec.Decode(&dag); err != nil {
		writeDAGError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}
	if err := validateDAGPayload(dag); err != nil {
		writeDAGError(w, http.StatusBadRequest, err.Error())
		return
	}

	exec := s.registerDAGExecution(dag)
	go runDAGExecution(s, exec)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"executionId": exec.id,
		"wsUrl":       buildDAGWebSocketURL(r, exec.id),
		"status":      string(dagStatusPending),
	})
}

// validateDAGPayload performs the minimal 16A shape check. Topology
// validation (cycles, dangling edges, schemaVersion match) lands in 16B
// so the executor can fail-fast with rich diagnostics before submission.
func validateDAGPayload(dag map[string]interface{}) error {
	if len(dag) == 0 {
		return fmt.Errorf("dag payload is empty")
	}
	rawNodes, ok := dag["nodes"]
	if !ok {
		return fmt.Errorf("dag payload missing 'nodes' array")
	}
	nodes, ok := rawNodes.([]interface{})
	if !ok {
		return fmt.Errorf("'nodes' must be an array")
	}
	if len(nodes) == 0 {
		return fmt.Errorf("'nodes' must be non-empty")
	}
	if rawEdges, present := dag["edges"]; present {
		if _, ok := rawEdges.([]interface{}); !ok {
			return fmt.Errorf("'edges' must be an array when present")
		}
	}
	return nil
}

// registerDAGExecution mints an execution id, creates the cancellable
// context, stores the entry in the registry, and lazily starts the
// reaper. Returns the live *dagExecution.
func (s *Server) registerDAGExecution(dag map[string]interface{}) *dagExecution {
	ctx, cancel := context.WithCancel(context.Background())
	exec := &dagExecution{
		id:             uuid.New().String(),
		dag:            dag,
		ctx:            ctx,
		cancel:         cancel,
		frames:         make(chan dagFrame, dagFrameBuffer),
		createdAt:      time.Now(),
		status:         dagStatusPending,
		lastProgressAt: make(map[string]time.Time),
	}
	s.dagExecutions.Store(exec.id, exec)
	s.ensureDAGReaper()
	return exec
}

// ensureDAGReaper starts the cleanup goroutine on first use. Runs once
// per *Server lifetime — multiple registrations share the single
// reaper. Tests using fresh Server instances each get their own reaper.
func (s *Server) ensureDAGReaper() {
	s.dagReaperOnce.Do(func() { go s.startDAGReaper() })
}

// startDAGReaper is the registry janitor. Plan §16A acceptance:
// "In-flight registry cleans up entries 60s after completion."
func (s *Server) startDAGReaper() {
	ticker := time.NewTicker(dagReapInterval)
	defer ticker.Stop()
	for range ticker.C {
		s.reapCompletedDAGExecutions()
	}
}

// reapCompletedDAGExecutions sweeps once. Extracted from startDAGReaper
// so tests can drive a single sweep deterministically.
func (s *Server) reapCompletedDAGExecutions() {
	now := time.Now()
	s.dagExecutions.Range(func(k, v any) bool {
		exec := v.(*dagExecution)
		status, completedAt := exec.snapshot()
		switch status {
		case dagStatusCompleted, dagStatusFailed, dagStatusCancelled:
			if !completedAt.IsZero() && now.Sub(completedAt) >= dagCompletedTTL {
				s.dagExecutions.Delete(k)
			}
		}
		return true
	})
}

// handleDAGExecCancel cancels the per-execution context. Idempotent:
// calling cancel after the executor already finished still returns 200
// with the now-terminal status, which lets the client safely retry.
//
// Spec acceptance: "GET /dag/exec/{id}/cancel cancels the execution
// context; status flips to cancelled."
func (s *Server) handleDAGExecCancel(w http.ResponseWriter, _ *http.Request, id string) {
	v, ok := s.dagExecutions.Load(id)
	if !ok {
		writeDAGError(w, http.StatusNotFound, "execution not found")
		return
	}
	exec := v.(*dagExecution)
	status, _ := exec.snapshot()
	switch status {
	case dagStatusCompleted, dagStatusFailed, dagStatusCancelled:
		// Already terminal — return current status without re-flipping.
	default:
		exec.setStatus(dagStatusCancelled)
		exec.cancel()
	}
	finalStatus, _ := exec.snapshot()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"executionId": id,
		"status":      string(finalStatus),
	})
}

// handleDAGExecStream upgrades to WebSocket and bridges the execution's
// frame channel to the client. The very first frame is always
// {type:"connected"} per plan §16A acceptance.
func (s *Server) handleDAGExecStream(w http.ResponseWriter, r *http.Request, id string) {
	v, ok := s.dagExecutions.Load(id)
	if !ok {
		writeDAGError(w, http.StatusNotFound, "execution not found")
		return
	}
	exec := v.(*dagExecution)

	wsConn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		// Upgrader already wrote an HTTP error response.
		return
	}
	defer wsConn.Close()

	if err := wsConn.WriteJSON(dagFrame{Type: "connected", Data: map[string]interface{}{
		"executionId": exec.id,
	}}); err != nil {
		return
	}

	// Reader goroutine: drain client → server frames so the gorilla
	// transport processes pings/closes. Body content is reserved for a
	// future client-cancel channel; for 16A we just discard it.
	clientClosed := make(chan struct{})
	go func() {
		defer close(clientClosed)
		for {
			if _, _, err := wsConn.NextReader(); err != nil {
				return
			}
		}
	}()

	for {
		select {
		case frame, ok := <-exec.frames:
			if !ok {
				return
			}
			if err := wsConn.WriteJSON(frame); err != nil {
				return
			}
		case <-exec.ctx.Done():
			// Drain anything the executor wrote between the cancel and
			// the close, so a final 'cancelled' frame still reaches the
			// client. Non-blocking: stop on empty channel.
			for {
				select {
				case frame, ok := <-exec.frames:
					if !ok {
						return
					}
					if err := wsConn.WriteJSON(frame); err != nil {
						return
					}
				default:
					return
				}
			}
		case <-clientClosed:
			// 16C: an unsolicited WS drop is treated as an implicit
			// cancel so the executor (and the long handlers it dispatches
			// to via per-execution ctx) unwind promptly. Plan §16C
			// acceptance: "Browser cancel via WS close → handler context
			// cancelled within 200ms." Idempotent — already-terminal
			// executions are left alone so a clean disconnect after
			// `run_completed` does not retroactively flip status.
			cancelOnClientDisconnect(exec)
			return
		}
	}
}

// cancelOnClientDisconnect flips a still-running execution to cancelled
// and cancels its context. Mirrors the side-effects of the explicit
// /dag/exec/{id}/cancel handler — the only difference is the trigger.
func cancelOnClientDisconnect(exec *dagExecution) {
	status, _ := exec.snapshot()
	switch status {
	case dagStatusCompleted, dagStatusFailed, dagStatusCancelled:
		return
	}
	exec.setStatus(dagStatusCancelled)
	exec.cancel()
}

// buildDAGWebSocketURL constructs ws[s]://host/dag/exec/{id}/stream
// from the incoming request. We honour TLS based on r.TLS so the
// returned URL works whether the proxy is fronted by HTTP or HTTPS.
func buildDAGWebSocketURL(r *http.Request, id string) string {
	scheme := "ws"
	if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
		scheme = "wss"
	}
	host := r.Host
	if fwd := r.Header.Get("X-Forwarded-Host"); fwd != "" {
		host = fwd
	}
	return scheme + "://" + host + "/dag/exec/" + id + "/stream"
}

// writeDAGError centralises error JSON to keep handler bodies tidy.
func writeDAGError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
