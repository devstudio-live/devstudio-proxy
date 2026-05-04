#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# Lint Cleanup — Multi-Phase Orchestrator (devstudio-proxy / Go)
#
# Runs each phase of plans/lint-cleanup-plan.md as an
# independent `claude -p` invocation with a fresh context window.
# Shows real-time progress by parsing Claude's stream-json
# output. Progress is tracked in LINT-CLEANUP-PROGRESS.md between
# invocations so the run is fully resumable: kill the process at
# any point (Ctrl-C, network drop, machine reboot, hit your
# Claude Max usage cap) and re-run the same command — it picks
# up at the first row whose Status column is not `done`.
#
# Plan: plans/lint-cleanup-plan.md
#   P1 — gofmt + goimports
#   P2 — govet + ineffassign + errcheck
#   P3 — staticcheck + unused
#   P4 — govulncheck
#
# Mirror of devstudio/scripts/run-lint-cleanup-phases.sh — same
# orchestrator shape, same prompt structure, Go-specific phases.
#
# Pre-Claude detection: scripts/lint-phase-detect.sh runs first
# for each phase and writes a worklist into
# tmp/lint-cleanup/<phase>-worklist.json. If the worklist comes
# back with count=0, the orchestrator skips the Claude invocation
# entirely and auto-marks the row done.
#
# Usage:
#   bash scripts/run-lint-cleanup-phases.sh                  # all pending
#   bash scripts/run-lint-cleanup-phases.sh P1               # single phase
#   bash scripts/run-lint-cleanup-phases.sh P1 P3            # range P1..P3
#   bash scripts/run-lint-cleanup-phases.sh --set P1,P3      # explicit list
#   bash scripts/run-lint-cleanup-phases.sh P1 --dry-run     # preview
#   bash scripts/run-lint-cleanup-phases.sh --list           # phases + status
#   bash scripts/run-lint-cleanup-phases.sh --next           # next pending ID
#   bash scripts/run-lint-cleanup-phases.sh --estimate P1 P4 # rough budget
#
# Environment:
#   LINT_OPEN_RANGE=1   single positional arg means "from here to end"
#
# ──────────────────────────────────────────────────────────────
# STOP/START SAFETY
# ──────────────────────────────────────────────────────────────
#
# - Resumability lives in LINT-CLEANUP-PROGRESS.md, not in any
#   in-memory state. The script greps `| <ID> .* | done |` on
#   every dispatch and skips done phases.
# - A row only flips from `pending` → `done` AFTER Claude commits
#   AND updates the row AND the per-phase regression checklist
#   passes. So a mid-phase abort always leaves the row `pending`.
# - When a phase is re-dispatched after an abort, the prompt
#   tells Claude to first inspect `git status` / `git log` for
#   any partial work and FINISH it rather than redo it.
# - error_max_turns continuation path (max 2 retries) is
#   in-window only; cross-window continuation is handled by the
#   resume-from-`pending` mechanism above.
# - Killing the script with Ctrl-C kills the live `claude -p`
#   subprocess; partial git work stays on disk and the next
#   dispatch handles it cleanly.
#
# ──────────────────────────────────────────────────────────────

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PLAN="$REPO_ROOT/plans/lint-cleanup-plan.md"
PROGRESS="$REPO_ROOT/LINT-CLEANUP-PROGRESS.md"
LOG_DIR="$REPO_ROOT/logs/lint-cleanup-phases"
WORKLIST_DIR="$REPO_ROOT/tmp/lint-cleanup"
DETECT_SCRIPT="$REPO_ROOT/scripts/lint-phase-detect.sh"
mkdir -p "$LOG_DIR" "$WORKLIST_DIR"

# ── Phase definitions (ID:Name) ──────────────────────────────
PHASES=(
  "P1:gofmt + goimports (mechanical, single PR)"
  "P2:govet + ineffassign + errcheck (manual fixes; errcheck surfaces real bugs)"
  "P3:staticcheck + unused (manual fixes; unused safe to delete, staticcheck judgment)"
  "P4:govulncheck (dependency bumps; document unfixable transitives in SECURITY.md)"
)

PHASE_IDS=()
for entry in "${PHASES[@]}"; do PHASE_IDS+=("${entry%%:*}"); done
TOTAL_PHASES=${#PHASES[@]}

# ── Helpers ───────────────────────────────────────────────────
is_phase_done() {
  local phase_id="$1"
  grep -Eq "^\| ${phase_id}[[:space:]]+\|.*\|[[:space:]]+done[[:space:]]+\|" "$PROGRESS" 2>/dev/null
}

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

count_done() {
  local n
  n=$(grep -Ec "\|[[:space:]]+done[[:space:]]+\|" "$PROGRESS" 2>/dev/null) || true
  echo "${n:-0}"
}

phase_name_for() {
  local id="$1"
  for entry in "${PHASES[@]}"; do
    if [[ "${entry%%:*}" == "$id" ]]; then
      echo "${entry#*:}"
      return 0
    fi
  done
  echo ""
  return 1
}

phase_status_for() {
  local id="$1"
  local line
  line=$(grep -E "^\| ${id} " "$PROGRESS" 2>/dev/null || true)
  if [[ -z "$line" ]]; then
    echo "unknown"
    return
  fi
  local status
  status=$(echo "$line" | awk -F'|' '{gsub(/^ +| +$/, "", $4); print $4}')
  echo "${status:-unknown}"
}

list_phases() {
  printf "%-6s  %-10s  %s\n" "ID" "Status" "Name"
  printf "%-6s  %-10s  %s\n" "------" "----------" "-----------------------------------"
  for entry in "${PHASES[@]}"; do
    local id="${entry%%:*}"
    local name="${entry#*:}"
    local status
    status=$(phase_status_for "$id")
    printf "%-6s  %-10s  %s\n" "$id" "$status" "$name"
  done
}

next_pending() {
  for entry in "${PHASES[@]}"; do
    local id="${entry%%:*}"
    local status
    status=$(phase_status_for "$id")
    if [[ "$status" != "done" ]]; then
      echo "$id"
      return 0
    fi
  done
  return 1
}

estimate() {
  local first="${1:-}" second="${2:-}"
  local -a ids
  if [[ -z "$first" ]]; then
    ids=("${PHASE_IDS[@]}")
  else
    local start_idx="" end_idx=""
    for i in "${!PHASE_IDS[@]}"; do
      [[ "${PHASE_IDS[$i]}" == "$first"  ]] && start_idx="$i"
      [[ "${PHASE_IDS[$i]}" == "$second" ]] && end_idx="$i"
    done
    [[ -z "$start_idx" ]] && { echo "Unknown start phase: $first"; exit 2; }
    [[ -z "$end_idx"   ]] && end_idx="$start_idx"
    ids=("${PHASE_IDS[@]:$start_idx:$((end_idx - start_idx + 1))}")
  fi

  local n=${#ids[@]}
  # Rough: P1 is mechanical and cheap; P2/P3 are the heavy ones
  # because errcheck/staticcheck fixes are judgment calls; P4 is
  # bounded by how many dep bumps are required.
  local low=$((  n * 60  ))
  local high=$(( n * 400 ))
  echo "Phases selected: $n (${ids[*]})"
  echo "Estimated tokens: ${low}k – ${high}k (rough: 60–400k per phase, ultrathink-heavy)"
  echo "Claude Max 100 (5h window) typically absorbs:"
  echo "  - All of P1 (mechanical formatter pass + go test -race)"
  echo "  - P2 alone — errcheck judgment calls, may exceed window for large surfaces"
  echo "  - P3 alone — staticcheck triage"
  echo "  - P4 alone — dep bumps + SECURITY.md updates"
}

# ── Pre-Claude detection ──────────────────────────────────────
run_detection() {
  local phase_id="$1"
  local out="$WORKLIST_DIR/${phase_id}-worklist.json"
  echo "  ▸ Detection — bash scripts/lint-phase-detect.sh --phase=$phase_id"
  if ! bash "$DETECT_SCRIPT" --phase="$phase_id" --quiet; then
    echo "  ⚠  Detection script failed — proceeding to Claude with empty worklist context"
    return 0
  fi
  if [[ ! -f "$out" ]]; then
    echo "  ⚠  Detection produced no worklist file — proceeding anyway"
    return 0
  fi
  local count
  count=$(python3 -c "
import json, sys
try:
    d = json.load(open('$out'))
    print(d.get('count', 0))
except Exception:
    print(-1)
" 2>/dev/null || echo "-1")
  echo "  ◆  Worklist: $count issue(s) → $out"
  if [[ "$count" == "0" ]]; then
    return 4  # signal: empty worklist
  fi
  return 0
}

# ── Progress display (parses stream-json) ─────────────────────
run_with_progress() {
  local phase_id="$1"
  local phase_name="$2"
  local log_file="$3"
  local claude_pid="${4:-0}"

  local tool_calls=0
  local files_read=0
  local files_written=0
  local files_edited=0
  local bash_cmds=0
  local agent_calls=0
  local current_tool=""
  local current_file=""
  local tokens_in=0
  local tokens_out=0
  local tokens_cache_read=0
  local cost_usd="0.0000"
  local turn=0
  local idle_secs=0
  local read_timeout=10
  # Lint phases include go test -race + make full in the
  # regression checklist; idle limit matched to legacy-removal.
  local idle_limit=2700  # 45 min stream silence → watchdog kills worker
  local start_time
  start_time=$(date +%s)

  local done_count
  done_count=$(count_done)
  local overall_pct=$(( (done_count * 100) / TOTAL_PHASES ))
  printf "\n"
  printf "  ┌─────────────────────────────────────────────────┐\n"
  printf "  │ Overall: %d/%d phases (%d%%)                       \n" "$done_count" "$TOTAL_PHASES" "$overall_pct"
  printf "  │ Current: Phase %s — %s\n" "$phase_id" "$phase_name"
  printf "  │ Watchdog: kills worker after %ds of stream silence\n" "$idle_limit"
  printf "  └─────────────────────────────────────────────────┘\n"
  printf "\n"

  render_status() {
    local elapsed=$(( $(date +%s) - start_time ))
    local mins=$((elapsed / 60))
    local secs=$((elapsed % 60))
    local display_file="$current_file"
    if [[ ${#display_file} -gt 38 ]]; then
      display_file="...${display_file: -35}"
    fi
    local idle_str=""
    if (( idle_secs > 0 )); then
      local imins=$((idle_secs / 60))
      local isecs=$((idle_secs % 60))
      idle_str=$(printf " ⏱%d:%02d" "$imins" "$isecs")
    fi
    printf "\r\033[K  ⚙  [%02d:%02d] %-9s %-38s (R:%d W:%d E:%d B:%d A:%d) │ %dk↓ %dk↑ │ \$%s │ t:%d%s" \
      "$mins" "$secs" "$current_tool" "$display_file" \
      "$files_read" "$files_written" "$files_edited" "$bash_cmds" "$agent_calls" \
      "$(( tokens_in / 1000 ))" "$(( tokens_out / 1000 ))" "$cost_usd" "$turn" "$idle_str"
  }

  while true; do
    if ! IFS= read -r -t "$read_timeout" line; then
      idle_secs=$((idle_secs + read_timeout))
      render_status
      if (( idle_secs >= idle_limit )); then
        printf "\n  ⚠  No stream events for %ds — killing worker PID %s\n" "$idle_secs" "$claude_pid"
        kill "$claude_pid" 2>/dev/null || true
        sleep 2
        kill -9 "$claude_pid" 2>/dev/null || true
        return 2
      fi
      continue
    fi

    idle_secs=0
    echo "$line" >> "$log_file"

    local evt_type
    evt_type=$(echo "$line" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('type',''))" 2>/dev/null || echo "")

    case "$evt_type" in
      assistant)
        turn=$((turn + 1))
        local parsed
        parsed=$(echo "$line" | python3 -c "
import sys, json
d = json.load(sys.stdin)
msg = d.get('message', {}) or {}
content = msg.get('content', []) or []
usage = msg.get('usage', {}) or {}
ti = int(usage.get('input_tokens') or 0)
to = int(usage.get('output_tokens') or 0)
tc = int(usage.get('cache_read_input_tokens') or 0)
tool_name = ''
tool_file = ''
text = ''
for c in content:
    if c.get('type') == 'tool_use' and not tool_name:
        tool_name = c.get('name', '')
        inp = c.get('input', {}) or {}
        tool_file = inp.get('file_path', inp.get('path', inp.get('pattern', (inp.get('command') or '')[:60])))
    elif c.get('type') == 'text' and not text:
        t = (c.get('text') or '').strip().split('\n')[0]
        text = t[:100]
print(f'{ti}|{to}|{tc}|{tool_name}|{tool_file}|{text}')
" 2>/dev/null || echo "0|0|0|||")

        local ti="${parsed%%|*}"; parsed="${parsed#*|}"
        local to="${parsed%%|*}"; parsed="${parsed#*|}"
        local tc="${parsed%%|*}"; parsed="${parsed#*|}"
        local tname="${parsed%%|*}"; parsed="${parsed#*|}"
        local tfile="${parsed%%|*}"; parsed="${parsed#*|}"
        local ttext="$parsed"

        tokens_in=$((tokens_in + ti))
        tokens_out=$((tokens_out + to))
        tokens_cache_read=$((tokens_cache_read + tc))

        if [[ -n "$ttext" ]]; then
          printf "\r\033[K  💬 %s\n" "$ttext"
        fi

        if [[ -n "$tname" ]]; then
          current_tool="$tname"
          current_file="$tfile"
          tool_calls=$((tool_calls + 1))
          case "$current_tool" in
            Read)         files_read=$((files_read + 1)) ;;
            Write)        files_written=$((files_written + 1)) ;;
            Edit)         files_edited=$((files_edited + 1)) ;;
            Bash)         bash_cmds=$((bash_cmds + 1)) ;;
            Agent|Task)   agent_calls=$((agent_calls + 1)) ;;
          esac
        fi
        render_status
        ;;

      user)
        local subagent
        subagent=$(echo "$line" | python3 -c "
import sys, json
d = json.load(sys.stdin)
r = d.get('tool_use_result') or {}
if isinstance(r, dict) and r.get('totalDurationMs') is not None:
    dur = int(r.get('totalDurationMs') or 0) // 1000
    tok = int(r.get('totalTokens') or 0)
    uses = int(r.get('totalToolUseCount') or 0)
    agent = r.get('agentType') or 'Agent'
    print(f'{agent}|{dur}|{tok}|{uses}')
" 2>/dev/null || echo "")

        if [[ -n "$subagent" ]]; then
          local sa="${subagent%%|*}"; subagent="${subagent#*|}"
          local sdur="${subagent%%|*}"; subagent="${subagent#*|}"
          local stok="${subagent%%|*}"; subagent="${subagent#*|}"
          local suses="$subagent"
          printf "\r\033[K  🤖 %s subagent finished — %ds, %dk tokens, %s tool uses\n" \
            "$sa" "$sdur" "$(( stok / 1000 ))" "$suses"
          render_status
        fi
        ;;

      result)
        local result_info
        result_info=$(echo "$line" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f\"{d.get('subtype','unknown')}|{d.get('duration_ms',0)}|{d.get('total_cost_usd',0):.4f}|{d.get('num_turns',0)}\")
" 2>/dev/null || echo "unknown|0|0.0000|0")

        local status="${result_info%%|*}"
        local rest="${result_info#*|}"
        local dur_ms="${rest%%|*}"
        rest="${rest#*|}"
        local cost="${rest%%|*}"
        local turns="${rest#*|}"
        local dur_secs=$((dur_ms / 1000))
        local dur_mins=$((dur_secs / 60))
        local dur_rem=$((dur_secs % 60))
        cost_usd="$cost"

        printf "\r\033[K\n"
        printf "  ┌─────────────────────────────────────────────────┐\n"
        printf "  │ Phase %s — %s\n" "$phase_id" "$status"
        printf "  │ Duration: %dm %ds │ Cost: \$%s │ Turns: %s\n" "$dur_mins" "$dur_rem" "$cost" "$turns"
        printf "  │ Tools: %d calls (R:%d W:%d E:%d B:%d Agent:%d)\n" \
          "$tool_calls" "$files_read" "$files_written" "$files_edited" "$bash_cmds" "$agent_calls"
        printf "  │ Tokens: %dk in / %dk out / %dk cache-read\n" \
          "$(( tokens_in / 1000 ))" "$(( tokens_out / 1000 ))" "$(( tokens_cache_read / 1000 ))"
        printf "  └─────────────────────────────────────────────────┘\n"

        case "$status" in
          success)          return 0 ;;
          error_max_turns)  return 3 ;;
          *)                return 1 ;;
        esac
        ;;
    esac
  done

  printf "\r\033[K  ⚠  Stream ended without a result event\n"
  return 1
}

# ── Build prompt for a phase ─────────────────────────────────
# Every prompt opens with `ultrathink` to push the per-turn
# extended-thinking budget to its maximum, then layers in the
# Go-side lint-cleanup scope, the no-regression contract, the
# warn-mode invariant, and the exit format.
build_prompt() {
  local phase_id="$1"
  local phase_name="$2"
  local worklist="$WORKLIST_DIR/${phase_id}-worklist.json"

  cat <<PROMPT_EOF
ultrathink

You are executing Phase ${phase_id} (${phase_name}) of the
devstudio-proxy lint-cleanup plan
(plans/lint-cleanup-plan.md).

This plan introduces a golangci-lint v2 + govulncheck +
Lefthook stack in WARN-MODE ONLY. The phased cleanup drives
the violation count down so a future, separate decision can
promote the gates to error mode. Your job in this phase is to
apply the documented fix strategy for ${phase_id}, verify zero
test regressions, and flip the row in
LINT-CLEANUP-PROGRESS.md from \`pending\` to \`done\`.

Use the deepest extended-thinking budget available before each
significant decision. Go errcheck and staticcheck fixes are
judgment calls, not mechanical rewrites — every "should this
error propagate, log, or be discarded?" call has to consider
the caller chain. Think carefully; act decisively.

## Critical Instructions
0. CHECK FOR PRIOR IN-PROGRESS WORK FIRST. This phase may have
   been started in a previous run that was aborted. Run
   \`git status\` and \`git log --oneline -10\` immediately. If
   you see:
   - uncommitted changes related to Phase ${phase_id}, OR
   - recent commits whose message starts with
     "lint(${phase_id})" but the progress row is still
     \`pending\`,
   then your task is to FINISH that work — do not redo it.
   Inventory what landed, do only what is missing, and create
   a NEW commit. If commits clearly belong to OTHER phases,
   leave them alone. If git is clean and no commits match this
   phase, proceed as a fresh start.
1. Read plans/lint-cleanup-plan.md — focus on the section
   covering Phase ${phase_id} (Scope, Detect, Fix, Accept,
   Risk).
2. Read the pre-computed worklist: ${worklist}
   This JSON is authoritative for what to fix. The orchestrator
   already ran scripts/lint-phase-detect.sh --phase=${phase_id}
   before invoking you. For P2/P3 it includes a "raw_path"
   pointing to the full golangci-lint JSON output.
3. Read LINT-CLEANUP-PROGRESS.md to confirm the current row
   for ${phase_id}.
4. Read ONLY the source files you will directly edit. Do NOT
   pre-read every file in the worklist — open them on demand.
5. Apply the documented fix strategy from
   plans/lint-cleanup-plan.md §"Phase ${phase_id}":
   - P1 → \`gofmt -w .\` and \`goimports -w .\`. Mechanical.
   - P2 → manual fixes for govet/ineffassign/errcheck. Each
     errcheck diagnostic is a judgment call: propagate the
     error, wrap-and-return, log-with-context, or explicitly
     \`_ =\` discard with a comment justifying the discard.
   - P3 → manual fixes for staticcheck/unused. \`unused\` is
     usually safe-delete; staticcheck SA-series often flags
     deprecated stdlib usage that needs the replacement API.
   - P4 → \`go get -u\` for vulnerable deps; for unfixable
     transitive vulns, write a SECURITY.md entry explaining
     the reachability rationale.
6. Run the per-phase regression checklist (below). EVERY
   command must exit 0 before flipping the row.
7. Commit with the message:
   "lint(${phase_id}): ${phase_name}"
   For multi-batch phases (large P2/P3), commit per ~10-file
   batch with stable messages: "lint(${phase_id}): <slug>".
8. Update LINT-CLEANUP-PROGRESS.md: pending → done, append
   commit SHA(s), files-touched count, diagnostic-count delta
   (e.g. "errcheck: 47 → 0"), regression-checklist result, any
   deviations.
9. If genuinely blocked by an unresolvable upstream, mark the
   row "blocked" with explanation and stop — do not invent
   workarounds.

## Hard scope rules (non-negotiable)
A. Do NOT regress any test. Run the per-phase regression
   checklist before declaring done.
B. Do NOT modify .golangci.yml severity to make a phase pass.
   If a rule is genuinely wrong for this codebase, propose
   disabling it under DEVIATIONS — do not silently downgrade.
C. P1 (auto-fix) — apply gofmt+goimports, run the regression
   checklist, commit. Do NOT hand-edit files the formatter
   touches; if the formatter is wrong for a file (e.g.
   generated code), exclude it via .golangci.yml or build tags.
D. P2/P3 (manual) — work file-by-file from the worklist.
   Errcheck especially: each unhandled error is a real
   judgment call. Do not bulk-discard with \`_ =\` to make
   diagnostics go away.
E. P4 (vuln) — bumps that change major versions need their
   own commit and a CHANGELOG note. Document any
   non-upgradeable vulns under SECURITY.md with reachability
   rationale (e.g. "only called by test scaffolding").
F. Tests are gating: every per-phase command in the regression
   checklist must exit 0 before flipping the progress row.
G. Wails IPC contract: do NOT rename, remove, or change the
   signature of any exported method on
   \`wails/{app,ssh_bridge,sftp_bridge}.go\` — those are part
   of the cross-repo contract documented in
   devstudio/CLAUDE.md (§"Surfaces & transports"). Lint
   cleanup may touch internals but must keep the exported
   surface identical.
H. Warn-mode rollout invariant: do NOT touch the lefthook.yml
   masks (\`|| true\` suffixes), the \`--issues-exit-code=0\`
   flag, or any \`continue-on-error: true\` flag in CI.
   Promotion to strict mode is a separate, post-Phase-P4
   decision.
I. Do NOT amend commits. Every retry creates a NEW commit.
J. Do NOT edit other phases' rows in LINT-CLEANUP-PROGRESS.md.

## Per-phase regression checklist (gating)
Run these in order. Stop on the first failure and DO NOT flip
the progress row to done.

  1. \`go build ./...\` — must compile cleanly.
  2. \`go test -race ./...\` — full suite passes.
  3. \`make full\` — release build path passes (gofumpt
     rewrites can break struct tags or build tags).
  4. \`gofmt -l .\` and (if installed) \`goimports -l .\`
     return zero files.
  5. If this phase touched \`wails/\`: report under
     DEVIATIONS that a manual macOS Wails smoke
     (\`wails build && open\`) is required before merging the
     PR. Do NOT attempt wails build from this prompt — it is
     long-running, requires an interactive macOS environment,
     and is a release-tier check.

If steps 1–4 fail because of a rare flake, record it under
DEVIATIONS and re-run ONCE; if still failing, mark the phase
\`blocked\` and stop.

## Token-conservation policy
1. NO mid-task summaries. No plan recap, no "here's what I'm
   about to do".
2. NO final recap of the diff.
3. NO re-reading files you just edited.
4. NO exploratory subagents unless a genuinely new unknown
   surfaces. Record under DEVIATIONS if you spawn one.
5. Use TodoWrite for in-phase task lists.
6. Tool calls > prose.

## End-of-phase output (exact format)
  (a) "PHASE ${phase_id} COMPLETE: ${phase_name}"
  (b) commit SHA(s), one per line
  (c) regression-checklist results, one per line:
      "✓ go build", "✓ go test -race", "✓ make full", etc.
  (d) diagnostic-count delta: "errcheck: 47 → 0" or
      "govulncheck reachable: 3 → 0"
  Optional "DEVIATIONS:" block with ≤3 bullets ONLY if deviated.

## Scope guard
Implement ONLY Phase ${phase_id}. Do not touch other phases'
worklists. Do not modify .golangci.yml unless the phase
explicitly authorises it (none currently do; rule disablement
is a separate post-rollout decision).
PROMPT_EOF
}

# ── Continuation prompt (used after error_max_turns) ─────────
build_continuation_prompt() {
  local phase_id="$1"
  local phase_name="$2"
  local attempt="$3"
  local worklist="$WORKLIST_DIR/${phase_id}-worklist.json"

  cat <<CONT_EOF
ultrathink

You are RESUMING Phase ${phase_id} (${phase_name}) of the
devstudio-proxy lint-cleanup plan. A previous invocation hit
the turn limit mid-flight. This is continuation attempt
${attempt}.

Use deep extended thinking to identify the minimum remaining
work and execute it without re-doing finished work.

Files written by the previous attempt are still on disk. Your
job is to finish ONLY what is left — do not redo completed
work. Do NOT re-explain prior progress. Do NOT re-summarise
the plan.

## Start here (minimal re-orientation)
1. Run \`git status\` and \`git diff --stat HEAD\` to see what
   the previous attempt left uncommitted.
2. Read LINT-CLEANUP-PROGRESS.md to confirm the current phase
   row and recent finished phases.
3. Re-read ${worklist} — find entries NOT yet addressed.
4. Re-read ONLY the plans/lint-cleanup-plan.md section for
   Phase ${phase_id}.
5. Identify the remaining work.
6. Do ONLY that remaining work.
7. Run the per-phase regression checklist (gating).
8. Commit:
   "lint(${phase_id}): ${phase_name} (continuation)"
   (NEW commit; do not amend a prior partial commit).
9. Update LINT-CLEANUP-PROGRESS.md row to done.

## Hard rules (unchanged)
- No regression of previous phases. \`go test -race ./...\`
  and \`make full\` must pass.
- No modification of .golangci.yml severity to make the phase
  pass.
- No changes to exported Wails methods on
  wails/{app,ssh_bridge,sftp_bridge}.go.
- No touching warn-mode masks in lefthook.yml or CI workflows.

## Efficiency guidance
You are short on turns. Be decisive: no speculative reads, no
preamble, no reviewing unrelated files. Use Grep before Read.
Batch edits when possible. End with the standard PHASE
COMPLETE block.
CONT_EOF
}

# ── Argument parsing ─────────────────────────────────────────
SELECTED=()
DRY_RUN=false

parse_selection() {
  local positional=()
  for a in "$@"; do
    case "$a" in
      --dry-run) DRY_RUN=true ;;
      *)         positional+=("$a") ;;
    esac
  done

  local first="${positional[0]:-}"
  local second="${positional[1]:-}"

  if [[ "$first" == "--list" ]]; then
    list_phases
    exit 0
  fi

  if [[ "$first" == "--next" ]]; then
    if next_pending; then
      exit 0
    else
      echo "All phases complete." >&2
      exit 1
    fi
  fi

  if [[ "$first" == "--estimate" ]]; then
    estimate "$second" "${positional[2]:-}"
    exit 0
  fi

  if [[ "$first" == "--set" ]]; then
    [[ -z "$second" ]] && { echo "Usage: --set ID1,ID2,..."; exit 2; }
    IFS=',' read -r -a SELECTED <<< "$second"
    for id in "${SELECTED[@]}"; do
      local found=false
      for known in "${PHASE_IDS[@]}"; do
        [[ "$known" == "$id" ]] && { found=true; break; }
      done
      $found || { echo "Unknown phase id: $id"; exit 2; }
    done
    return
  fi

  if [[ -z "$first" ]]; then
    SELECTED=("${PHASE_IDS[@]}")
    return
  fi

  local start_idx="" end_idx=""
  for i in "${!PHASE_IDS[@]}"; do
    [[ "${PHASE_IDS[$i]}" == "$first"  ]] && start_idx="$i"
    [[ "${PHASE_IDS[$i]}" == "$second" ]] && end_idx="$i"
  done
  [[ -z "$start_idx" ]] && { echo "Unknown start phase: $first"; exit 2; }

  if [[ -n "$second" ]]; then
    [[ -z "$end_idx" ]] && { echo "Unknown end phase: $second"; exit 2; }
    (( start_idx <= end_idx )) || { echo "START ($first) is after END ($second)"; exit 2; }
  else
    end_idx="$start_idx"
    if [[ "${LINT_OPEN_RANGE:-0}" == "1" ]]; then
      end_idx=$(( ${#PHASE_IDS[@]} - 1 ))
    fi
  fi
  SELECTED=("${PHASE_IDS[@]:$start_idx:$((end_idx - start_idx + 1))}")
}

parse_selection "$@"

# ── Preflight ────────────────────────────────────────────────
preflight_setup_check() {
  if [[ ! -f "$PLAN" ]]; then
    echo "  ✗ plan missing: $PLAN"
    return 1
  fi
  if [[ ! -f "$PROGRESS" ]]; then
    echo "  ✗ progress tracker missing: $PROGRESS"
    echo "    Bootstrap it with one row per phase ID, columns:"
    echo "    | Phase | Name | Status | Notes |  (status defaults to 'pending')"
    return 1
  fi
  if [[ ! -f "$DETECT_SCRIPT" ]]; then
    echo "  ✗ detection helper missing: $DETECT_SCRIPT"
    return 1
  fi
  if ! command -v go >/dev/null 2>&1; then
    echo "  ✗ go not on PATH"
    return 1
  fi
  if ! command -v claude >/dev/null 2>&1; then
    echo "  ✗ claude CLI not on PATH"
    return 1
  fi
  if ! command -v python3 >/dev/null 2>&1; then
    echo "  ✗ python3 not on PATH (required for stream-json parsing)"
    return 1
  fi
  return 0
}

# ── Header ───────────────────────────────────────────────────
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  Lint Cleanup — Phase Orchestrator (devstudio-proxy)     ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Plan:     $PLAN"
echo "║  Progress: $PROGRESS"
echo "║  Worklist: $WORKLIST_DIR"
echo "║  Logs:     $LOG_DIR"
echo "║  Selected: ${SELECTED[*]}"
echo "║  Model:    opus (with ultrathink directive in every prompt)"
echo "║  Dry run:  $DRY_RUN"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""
echo "  Legend: R=Read W=Write E=Edit B=Bash A=Agent"

if ! $DRY_RUN; then
  echo ""
  echo "  ▸ Preflight — repo setup"
  if ! preflight_setup_check; then
    echo "  Aborting."
    exit 1
  fi
fi

COMPLETED=0
FAILED=0
SKIPPED_EMPTY=0

for PHASE_ID in "${SELECTED[@]}"; do
  PHASE_NAME="$(phase_name_for "$PHASE_ID")"
  [[ -z "$PHASE_NAME" ]] && { echo "Skipping unknown phase: $PHASE_ID"; continue; }

  if is_phase_done "$PHASE_ID"; then
    echo "  ✓ Phase $PHASE_ID — already done, skipping"
    COMPLETED=$((COMPLETED + 1))
    continue
  fi

  if $DRY_RUN; then
    echo "  ▷ Phase $PHASE_ID — $PHASE_NAME (would run)"
    continue
  fi

  echo ""
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
  echo "▶ Phase $PHASE_ID: $PHASE_NAME"
  echo "  Started: $(timestamp)"
  echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

  # ── Pre-Claude detection — skip Claude if worklist is empty ──
  set +e
  run_detection "$PHASE_ID"
  DETECT_RC=$?
  set -e
  if (( DETECT_RC == 4 )); then
    echo "  ◆  No violations detected — auto-marking phase done (no Claude invocation)"
    LOG_FILE="$LOG_DIR/phase-${PHASE_ID}-$(date +%Y%m%d-%H%M%S)-empty.log"
    NOOP_PROMPT="ultrathink

Phase ${PHASE_ID} (${PHASE_NAME}) detection produced an EMPTY worklist
(${WORKLIST_DIR}/${PHASE_ID}-worklist.json shows count=0).

Your only task: update LINT-CLEANUP-PROGRESS.md, flip the row
for ${PHASE_ID} from 'pending' to 'done', append a Notes entry:
'no violations detected at <git rev-parse HEAD>'. Then commit
with message 'lint(${PHASE_ID}): no violations detected — closing row'.

Do not modify any other file. Do not run tests. Do not amend.
End with the standard PHASE COMPLETE block."

    FIFO="$(mktemp -u -t lint-cleanup-fifo.XXXXXX)"
    mkfifo "$FIFO"
    claude -p "$NOOP_PROMPT" \
      --dangerously-skip-permissions \
      --model opus \
      --max-turns 30 \
      --output-format stream-json \
      --verbose \
      2>"$LOG_FILE.stderr" \
      >"$FIFO" &
    CLAUDE_PID=$!
    set +e
    run_with_progress "$PHASE_ID" "$PHASE_NAME (empty)" "$LOG_FILE" "$CLAUDE_PID" <"$FIFO"
    NOOP_RC=$?
    set -e
    kill "$CLAUDE_PID" 2>/dev/null || true
    wait "$CLAUDE_PID" 2>/dev/null || true
    rm -f "$FIFO"
    if (( NOOP_RC == 0 )) && is_phase_done "$PHASE_ID"; then
      echo "  ✓ Phase $PHASE_ID — empty worklist, row flipped"
      COMPLETED=$((COMPLETED + 1))
      SKIPPED_EMPTY=$((SKIPPED_EMPTY + 1))
    else
      echo "  ⚠  Phase $PHASE_ID — empty worklist but row update failed"
      FAILED=$((FAILED + 1))
      exit 1
    fi
    continue
  fi

  MAX_RETRIES=2
  RUN_RC=1
  for attempt in $(seq 0 $MAX_RETRIES); do
    LOG_SUFFIX=""
    if (( attempt > 0 )); then
      LOG_SUFFIX="-retry${attempt}"
    fi
    LOG_FILE="$LOG_DIR/phase-${PHASE_ID}-$(date +%Y%m%d-%H%M%S)${LOG_SUFFIX}.log"
    if (( attempt == 0 )); then
      PROMPT="$(build_prompt "$PHASE_ID" "$PHASE_NAME")"
    else
      echo ""
      echo "  🔄 Phase $PHASE_ID hit turn limit — continuation attempt $attempt/$MAX_RETRIES"
      PROMPT="$(build_continuation_prompt "$PHASE_ID" "$PHASE_NAME" "$attempt")"
    fi

    FIFO="$(mktemp -u -t lint-cleanup-fifo.XXXXXX)"
    mkfifo "$FIFO"

    claude -p "$PROMPT" \
      --dangerously-skip-permissions \
      --model opus \
      --max-turns 400 \
      --output-format stream-json \
      --verbose \
      2>"$LOG_FILE.stderr" \
      >"$FIFO" &
    CLAUDE_PID=$!

    set +e
    run_with_progress "$PHASE_ID" "$PHASE_NAME" "$LOG_FILE" "$CLAUDE_PID" <"$FIFO"
    RUN_RC=$?
    set -e
    kill "$CLAUDE_PID" 2>/dev/null || true
    wait "$CLAUDE_PID" 2>/dev/null || true
    rm -f "$FIFO"

    if (( RUN_RC != 3 )); then
      break
    fi
  done

  if (( RUN_RC == 0 )); then
    if is_phase_done "$PHASE_ID"; then
      echo "  ✓ Phase $PHASE_ID completed — $(timestamp)"
      COMPLETED=$((COMPLETED + 1))
    else
      echo "  ⚠  Phase $PHASE_ID returned success but progress row is not 'done'."
      echo "     Inspect LINT-CLEANUP-PROGRESS.md and the log:"
      echo "       $LOG_FILE"
      echo "     Re-run the same phase to retry, or hand-edit the row if"
      echo "     work landed but the row update was skipped."
      FAILED=$((FAILED + 1))
      exit 1
    fi
  else
    case "$RUN_RC" in
      2) echo "  ✗ Phase $PHASE_ID KILLED by watchdog — $(timestamp)" ;;
      3) echo "  ✗ Phase $PHASE_ID EXHAUSTED $MAX_RETRIES retries on turn limit — $(timestamp)" ;;
      *) echo "  ✗ Phase $PHASE_ID FAILED (rc=$RUN_RC) — $(timestamp)" ;;
    esac
    echo "  Log: $LOG_FILE"
    echo "  Stderr: $LOG_FILE.stderr"
    echo ""
    echo "  To resume: bash scripts/run-lint-cleanup-phases.sh $PHASE_ID"
    FAILED=$((FAILED + 1))
    exit 1
  fi
done

# ── Final Summary ─────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  Orchestration Complete                                  ║"
echo "╠══════════════════════════════════════════════════════════╣"
printf "║  Completed:    %-3d phases                               ║\n" "$COMPLETED"
printf "║  No-op (empty worklist): %-3d                            ║\n" "$SKIPPED_EMPTY"
printf "║  Failed:       %-3d phases                               ║\n" "$FAILED"
echo "║  Logs:         $LOG_DIR"
echo "╚══════════════════════════════════════════════════════════╝"

if [[ $FAILED -eq 0 ]] && ! $DRY_RUN; then
  REMAINING=$(grep -Ec "\|[[:space:]]+pending[[:space:]]+\|" "$PROGRESS" 2>/dev/null || echo 0)
  if [[ "$REMAINING" -gt 0 ]]; then
    echo ""
    echo "  $REMAINING phases still pending. Next:"
    next_pending || true
  else
    echo ""
    echo "Generating completion report..."
    claude -p "ultrathink. Read LINT-CLEANUP-PROGRESS.md and produce a completion report strictly capped at <=200 words in table form only: | ID | Name | Commit | Diagnostics before→after | Tests | Deviations |. No prose, no preamble, no summary paragraphs." \
      --dangerously-skip-permissions \
      --model opus \
      2>&1 | tee "$LOG_DIR/completion-report.txt"
  fi
fi
