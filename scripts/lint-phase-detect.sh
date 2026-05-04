#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# lint-phase-detect.sh — pre-Claude detection helper (Go side)
#
# Runs the cheap, deterministic detection step for one phase of
# plans/lint-cleanup-plan.md WITHOUT invoking Claude. Emits a
# JSON worklist to tmp/lint-cleanup/<phase>-worklist.json that
# the orchestrator (run-lint-cleanup-phases.sh) hands to Claude
# in the per-phase prompt. If the worklist comes back with
# count=0, the orchestrator skips the Claude invocation entirely
# and auto-marks the phase done.
#
# Usage:
#   bash scripts/lint-phase-detect.sh --phase=P1
#   bash scripts/lint-phase-detect.sh --phase=P2 --quiet
#   bash scripts/lint-phase-detect.sh --phase=all
#   bash scripts/lint-phase-detect.sh --list
#
# Exit codes:
#   0  detection completed (worklist may be empty or non-empty)
#   2  bad arguments / unknown phase
#   3  underlying linter crashed (output preserved in worklist
#      for diagnosis; orchestrator proceeds with empty worklist)
#
# Notes:
#   - Tools required: gofmt, goimports, golangci-lint, govulncheck.
#     Missing-tool case writes a worklist with note="tool missing"
#     and count=0 so the orchestrator can move on.
#   - This script never invokes Claude.
# ──────────────────────────────────────────────────────────────

set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="$REPO_ROOT/tmp/lint-cleanup"
mkdir -p "$OUT_DIR"

QUIET=false
PHASE=""
LIST=false

usage() {
  cat <<USAGE
Usage: bash scripts/lint-phase-detect.sh --phase=<id> [--quiet]
       bash scripts/lint-phase-detect.sh --phase=all
       bash scripts/lint-phase-detect.sh --list

Phases:
  P1   gofmt + goimports
  P2   govet + ineffassign + errcheck
  P3   staticcheck + unused
  P4   govulncheck

Output: tmp/lint-cleanup/<phase>-worklist.json (gitignored).
USAGE
}

for arg in "$@"; do
  case "$arg" in
    --phase=*) PHASE="${arg#--phase=}" ;;
    --quiet)   QUIET=true ;;
    --list)    LIST=true ;;
    --help|-h) usage; exit 0 ;;
    *) echo "Unknown arg: $arg" >&2; usage; exit 2 ;;
  esac
done

if $LIST; then
  echo "P1   gofmt + goimports"
  echo "P2   govet + ineffassign + errcheck"
  echo "P3   staticcheck + unused"
  echo "P4   govulncheck"
  exit 0
fi

[[ -z "$PHASE" ]] && { usage; exit 2; }

# ── JSON helpers (no jq dependency) ──────────────────────────
# Writes a minimal {phase, detector, count, …} JSON. We avoid jq
# so the proxy repo doesn't acquire a new dev-dep just for this.

GIT_HEAD="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

emit_json() {
  # $1 = phase, $2 = detector, $3 = count, $4 = body (raw JSON snippet, optional)
  local phase="$1" detector="$2" count="$3" body="${4:-}"
  local out="$OUT_DIR/${phase}-worklist.json"
  {
    printf '{\n'
    printf '  "phase": "%s",\n' "$phase"
    printf '  "detector": %s,\n' "$(json_string "$detector")"
    printf '  "count": %s,\n' "$count"
    printf '  "detected_at": "%s",\n' "$TIMESTAMP"
    printf '  "repo_head": "%s"' "$GIT_HEAD"
    if [[ -n "$body" ]]; then
      printf ',\n  %s\n' "$body"
    else
      printf '\n'
    fi
    printf '}\n'
  } >"$out"
  if ! $QUIET; then
    echo "${phase}: ${count} issue(s) → $out"
  fi
}

json_string() {
  # Escape backslashes and double-quotes; keep it simple.
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  printf '"%s"' "$s"
}

json_array_of_strings() {
  # Read lines from stdin, emit as JSON array.
  local first=true
  printf '['
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    if $first; then first=false; else printf ', '; fi
    json_string "$line"
  done
  printf ']'
}

# ── Phase detectors ───────────────────────────────────────────

detect_p1() {
  local gofmt_out goimports_out
  if ! command -v gofmt >/dev/null 2>&1; then
    emit_json "P1" "gofmt + goimports" 0 '"note": "gofmt not on PATH"'
    return
  fi
  gofmt_out="$(cd "$REPO_ROOT" && gofmt -l . 2>/dev/null || true)"
  if command -v goimports >/dev/null 2>&1; then
    goimports_out="$(cd "$REPO_ROOT" && goimports -l . 2>/dev/null || true)"
  else
    goimports_out=""
  fi
  local combined
  combined="$(printf '%s\n%s\n' "$gofmt_out" "$goimports_out" | sed '/^$/d' | sort -u)"
  local count
  count="$(printf '%s\n' "$combined" | sed '/^$/d' | wc -l | tr -d ' ')"
  local files_json
  files_json="$(printf '%s\n' "$combined" | json_array_of_strings)"
  emit_json "P1" "gofmt -l + goimports -l" "$count" "\"files\": $files_json"
}

run_golangci() {
  # $1 = linters CSV, $2 = out file
  local linters="$1" out="$2"
  if ! command -v golangci-lint >/dev/null 2>&1; then
    printf '{"Issues":[],"_note":"golangci-lint not on PATH"}' >"$out"
    return
  fi
  # --issues-exit-code=0 so we can capture even when issues exist.
  (cd "$REPO_ROOT" && golangci-lint run \
    --linters="$linters" \
    --out-format=json \
    --issues-exit-code=0 \
    ./... >"$out" 2>/dev/null) || \
    printf '{"Issues":[],"_note":"golangci-lint crashed"}' >"$out"
}

count_issues() {
  # Count "Issues" array entries in golangci-lint JSON without jq.
  local file="$1"
  python3 - "$file" <<'PY' 2>/dev/null || echo 0
import json, sys
try:
    d = json.load(open(sys.argv[1]))
    print(len(d.get("Issues") or []))
except Exception:
    print(0)
PY
}

detect_p2() {
  local raw="$OUT_DIR/P2-raw.json"
  run_golangci "govet,ineffassign,errcheck" "$raw"
  local count
  count="$(count_issues "$raw")"
  emit_json "P2" "golangci-lint run --linters=govet,ineffassign,errcheck" "$count" \
    "\"raw_path\": $(json_string "$raw")"
}

detect_p3() {
  local raw="$OUT_DIR/P3-raw.json"
  run_golangci "staticcheck,unused" "$raw"
  local count
  count="$(count_issues "$raw")"
  emit_json "P3" "golangci-lint run --linters=staticcheck,unused" "$count" \
    "\"raw_path\": $(json_string "$raw")"
}

detect_p4() {
  local raw="$OUT_DIR/P4-raw.json"
  if ! command -v govulncheck >/dev/null 2>&1; then
    printf '{"_note":"govulncheck not on PATH"}' >"$raw"
    emit_json "P4" "govulncheck" 0 "\"note\": \"govulncheck not installed (go install golang.org/x/vuln/cmd/govulncheck@latest)\""
    return
  fi
  (cd "$REPO_ROOT" && govulncheck -json ./... >"$raw" 2>/dev/null) || true
  # govulncheck -json emits a stream of JSON objects; count "finding" entries.
  local count
  count="$(python3 - "$raw" <<'PY' 2>/dev/null || echo 0
import json, sys
n = 0
try:
    with open(sys.argv[1]) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            if "finding" in obj:
                n += 1
except Exception:
    pass
print(n)
PY
)"
  emit_json "P4" "govulncheck -json ./..." "$count" \
    "\"raw_path\": $(json_string "$raw")"
}

# ── Dispatch ──────────────────────────────────────────────────
if [[ "$PHASE" == "all" ]]; then
  detect_p1
  detect_p2
  detect_p3
  detect_p4
  exit 0
fi

case "$PHASE" in
  P1) detect_p1 ;;
  P2) detect_p2 ;;
  P3) detect_p3 ;;
  P4) detect_p4 ;;
  *)  echo "Unknown phase: $PHASE" >&2; exit 2 ;;
esac
