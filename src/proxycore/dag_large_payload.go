// Package proxycore — DAG large-payload handling (Phase 16D).
//
// Plan: devstudio/plans/workflow-wizard-plan.md §G16 / 16D.
//
// 16B/16C shipped the executor + cancellation/progress frames. 16D
// prevents large node results from being embedded inline in the
// WebSocket frame stream: any result whose JSON-serialised size exceeds
// dagLargeResultThreshold (1 MiB) is instead stored under a UUID in the
// existing /mcp/context cache, and the node_result frame carries a
// {resultContextId:<uuid>, resultBytes:<n>} stub the client can fetch
// via the existing `GET /mcp/context/{uuid}` endpoint.
//
// Dedup: identical result bytes across repeat runs or across branches
// that emit the same payload reuse the same UUID instead of re-storing.
// Dedup pointers self-heal — if the target context entry has expired
// (TTL respected), the stale pointer is dropped on the next lookup and
// a fresh store proceeds.
//
// Hard-scope rules (plan §"Hard scope rules"):
//   - G: DAG schema 1.0 frozen — the cached envelope is the node's
//     `result` map bytes; the frame field keys (resultContextId /
//     resultBytes) are additive, not a schema mutation.
//   - H: this is the G16 group. The /mcp/context cache + its
//     `POST /mcp/context` + `GET /mcp/context/{uuid}` handlers existed
//     pre-16D. 16D is a new *caller* of s.StoreContext / s.LoadContext,
//     not a modifier; the context cache code is unchanged.
package proxycore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// dagLargeResultThreshold is the wire contract for "what counts as
// large". Plan §16D: "Results >1MB auto-stored in existing UUID cache".
// Any serialised result at or below this bound is embedded inline in
// the node_result frame; anything above is stashed in the cache.
// Surfaced as a package-level var so tests can exercise the store path
// without allocating 1 MiB per case (same pattern 16C uses for
// dagProgressMinInterval in dag_gateway.go).
var dagLargeResultThreshold = 1 * 1024 * 1024

// dagResultContextKind tags entries written by the DAG executor so a
// future audit pass can distinguish them from MCP snapshot entries
// (which `/mcp/context`'s handleContextStore stamps with "snapshot").
const dagResultContextKind = "dag-result"

// maybeStoreLargeResult inspects a node result and, when its JSON
// serialisation exceeds dagLargeResultThreshold, stores it in the
// /mcp/context UUID cache via the dedup-aware helper.
//
// Return contract:
//   - (uuid, N, nil) — stored; caller emits the {resultContextId, resultBytes}
//     stub in place of embedding `result` in the frame.
//   - ("",   0, nil) — below threshold OR nil input; caller embeds `result`.
//   - ("",   0, err) — a >threshold payload could not be stored (over the
//     10 MiB per-entry cap, total-cache-full, etc.); caller
//     surfaces as node_failed so large opaque payloads
//     never leak into the frame channel silently.
//
// A result that fails json.Marshal (e.g. contains a channel) is treated
// as below threshold so the executor's existing fallthrough behaviour
// is unchanged — we never want the large-payload path to break a node
// that would otherwise serialise fine over the WS (the gorilla
// WriteJSON would also refuse it, and that is the caller's problem to
// surface, not ours to mask).
func maybeStoreLargeResult(s *Server, result map[string]interface{}) (string, int, error) {
	if result == nil {
		return "", 0, nil
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return "", 0, nil
	}
	if len(encoded) <= dagLargeResultThreshold {
		return "", 0, nil
	}
	id, storeErr := storeLargeResultWithDedup(s, encoded)
	if storeErr != nil {
		return "", 0, fmt.Errorf("large-payload store failed (%d bytes): %w", len(encoded), storeErr)
	}
	return id, len(encoded), nil
}

// storeLargeResultWithDedup hashes `data` (SHA-256) and consults
// s.dagResultDedup for a prior UUID. When the prior UUID still resolves
// via s.LoadContext (i.e. the TTL has not elapsed), the existing UUID
// is returned and no second store is performed — plan §16D "Cache hits
// don't re-store; TTL respected".
//
// Otherwise the helper delegates to s.StoreContext (which enforces the
// 10 MiB per-entry cap + 100 MiB total cap + 200 entries cap), records
// the {hash → UUID} pointer, and returns the fresh UUID.
func storeLargeResultWithDedup(s *Server, data []byte) (string, error) {
	sum := sha256.Sum256(data)
	hashHex := hex.EncodeToString(sum[:])

	if prev, ok := s.dagResultDedup.Load(hashHex); ok {
		uuid, _ := prev.(string)
		if uuid != "" {
			if _, _, live := s.LoadContext(uuid); live {
				return uuid, nil
			}
			s.dagResultDedup.Delete(hashHex)
		}
	}

	id, err := s.StoreContext(data, dagResultContextKind)
	if err != nil {
		return "", err
	}
	s.dagResultDedup.Store(hashHex, id)
	return id, nil
}
