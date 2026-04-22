// Package proxycore — DAG executor stub (Phase 16A).
//
// Plan: devstudio/plans/workflow-wizard-plan.md §G16 / 16B.
//
// 16A only ships the WebSocket lifecycle skeleton (dag_gateway.go); the
// actual topo-sort + per-node delegation arrives in 16B and richer
// progress + cancellation semantics in 16C. This stub keeps the
// gateway end-to-end exercisable: it marks the execution running,
// waits for either cancellation or its own no-op "completion", and
// closes the frame channel so the WebSocket handler unblocks.
//
// The function signature (s *Server, exec *dagExecution) is the
// extension point 16B replaces wholesale.
package proxycore

// runDAGExecution is the executor entry point invoked from
// handleDAGExecStart. 16A semantics:
//   - Flip status pending → running.
//   - Wait for the per-execution context to be cancelled. There is no
//     real work to do yet — 16B fills in topo-sort + delegation.
//   - On cancellation, leave the cancelled status in place (the cancel
//     handler already set it) and tear down the frame channel so the
//     WebSocket handler returns.
//
// This intentionally has NO timeout; 16B introduces per-node deadlines.
func runDAGExecution(_ *Server, exec *dagExecution) {
	exec.setStatus(dagStatusRunning)
	defer exec.closeFrames()

	<-exec.ctx.Done()
	// If the cancel handler hasn't already flipped the status (race
	// where ctx is cancelled by some other path), normalise here.
	status, _ := exec.snapshot()
	if status == dagStatusRunning {
		exec.setStatus(dagStatusCancelled)
	}
}
