# Plan: Warn-mode linting + phased baseline cleanup for devstudio-proxy

## Context

devstudio-proxy currently runs only `go test -race` in CI. There is no
`golangci-lint`, no `go vet` gate, no `govulncheck`, no `gofmt` enforcement,
and no commit-time gate of any kind. Unhandled errors, shadowed variables,
deprecated stdlib calls, and reachable CVEs all reach `main` until the next
release tag (or never).

This plan introduces:
1. The **fastest** open-source Go linting stack (golangci-lint v2 +
   govulncheck), brought in **warn-mode only** (never blocks commits in the
   rollout phase).
2. A **phased baseline-cleanup** structured so a Claude-CLI-driven runner
   (`scripts/run-lint-cleanup-phases.sh`) can walk the violations and burn
   them down systematically, mirroring the
   `scripts/run-legacy-removal-phases.sh` idiom from the devstudio repo.

Companion: this is the proxy-side counterpart to
`devstudio/plans/lint-cleanup-plan.md`. The two plans share the same
runner shape, the same warn-mode invariants, and the same JS-side stack
choice (Biome). This file covers only the Go-specific phases (P1–P4) and
the proxy-side runner.

---

## Codebase signals (ground truth)

| Signal | Impact |
|---|---|
| Single Go module, ~Wails desktop bundle (`wails/`) + helper sources (`src/`) | golangci-lint v2 with one config file covers the whole tree. |
| Release-driven CI (`.github/workflows/release.yml` calls `make full`) | Add a separate `lint-warn.yml` workflow for PR-time linting; do not touch the release pipeline. |
| No prior `.golangci.yml`, no `lefthook.yml`, no pre-commit infra | Clean slate. |
| Wails-generated bindings under `wails/` (CGO, frontend bindings) | golangci-lint must exclude generated files (`*_gen.go`, Wails binding stubs) — surfaced by `staticcheck` / `unused` if not. |

---

## Final stack (Go side)

| Layer | Tool |
|---|---|
| Go lint | **golangci-lint v2** (GA Mar 2025) |
| Go vuln scan | **govulncheck** (separate gate) |
| Pre-commit runner | **Lefthook** (single Go binary, language-agnostic) |
| Format | **gofmt + goimports** (bundled via golangci-lint v2) |

`golangci-lint v2` bundles `govet`, `staticcheck`, `errcheck`,
`ineffassign`, `unused`, `gofumpt`. `linters.default: standard` gives the
right baseline. `--new-from-rev` provides incremental scanning for the
commit gate.

---

## Warn-mode rollout (non-negotiable)

Same posture as the devstudio side:

1. **Lefthook hooks** install but every command is suffixed with `|| true`
   so commits never fail.
2. **golangci-lint** runs with `--issues-exit-code=0` everywhere except
   the opt-in strict mode.
3. **CI parity**: a new `lint-warn.yml` workflow runs the same commands
   with `continue-on-error: true`.
4. **No ratchet during warn mode.** Phased cleanup (below) drives the
   diagnostic count down; promotion to error mode is a separate
   post-Phase-P4 decision.
5. Env override `DEVSTUDIO_LINT_STRICT=1` flips both tools to error mode
   for opt-in dev testing.

Promotion criteria (deferred): zero violations on `main` for 14
consecutive days, sign-off from the user, then flip the `|| true` masks
and `continue-on-error` flags in one commit.

---

## Phased baseline cleanup — Phases P1–P4

### P1. `gofmt` + `goimports`
- **Scope**: every `.go` file in the module, except generated files (`*_gen.go`, Wails binding stubs).
- **Detect**: `gofmt -l .` and `goimports -l .` (paths only).
- **Fix**: `gofmt -w .`, `goimports -w .`. Pure mechanical — whitespace, import ordering, no semantic risk.
- **Accept**: both commands report zero files.
- **Risk**: trivial. Single PR, large diff acceptable.

### P2. `go vet` + ineffassign + errcheck
- **Scope**: all packages.
- **Detect**: `golangci-lint run --linters=govet,ineffassign,errcheck --out-format=json`.
- **Fix**: manual. Most diagnostics are unhandled errors (errcheck), shadowed variables (govet), or assignments that never get read (ineffassign). errcheck especially surfaces real bugs — every fix is a judgment call between propagating, logging, or explicitly discarding (`_ =`).
- **Accept**: chosen linter set returns clean.
- **Risk**: medium — errcheck commonly surfaces real bugs that need behavioural fixes, not just suppressions.

### P3. `staticcheck` + `unused`
- **Scope**: all packages, generated files excluded.
- **Detect**: `golangci-lint run --linters=staticcheck,unused --out-format=json`.
- **Fix**: manual. `unused` is usually a safe delete. `staticcheck` often flags deprecated stdlib usage, redundant type conversions, or simplification opportunities.
- **Accept**: clean.
- **Risk**: medium — `staticcheck` SA-series checks can be load-bearing; review each before applying.

### P4. `govulncheck`
- **Scope**: every reachable dependency call site.
- **Detect**: `govulncheck -json ./...`.
- **Fix**: bump dependencies; for unfixable transitive vulns, document in a `SECURITY.md` with reachability rationale.
- **Accept**: zero reachable vulnerabilities; suppressions documented.
- **Risk**: low — Go module bumps are usually quick and well-isolated.

---

## Phase-script contract — Claude-CLI-driven runner

Mirrors `devstudio/scripts/run-legacy-removal-phases.sh`:

- `scripts/run-lint-cleanup-phases.sh` — bash orchestrator. Same shape
  as the devstudio counterpart: positional CLI, FIFO+watchdog,
  `MAX_RETRIES=2` continuation, `--list` / `--next` / `--dry-run` /
  `--set` / `--estimate`.
- `scripts/lint-phase-detect.sh` — pre-Claude detection helper. Pure
  bash (no Node dependency on the proxy side). Emits a JSON worklist
  into `tmp/lint-cleanup/<phase>-worklist.json`. Empty worklist =
  orchestrator skips Claude and auto-marks the row done.
- `LINT-CLEANUP-PROGRESS.md` — markdown progress table.

### Claude CLI invocation

```bash
claude -p "$PROMPT" \
  --dangerously-skip-permissions \
  --model opus \
  --max-turns 400 \
  --output-format stream-json \
  --verbose \
  2>"$LOG_FILE.stderr" \
  >"$FIFO" &
```

Same flags as the devstudio runner. `ultrathink` prefix in every prompt
drives extended thinking (codebase convention; no `--thinking-budget`
flag is used anywhere).

### CLI shape

```bash
bash scripts/run-lint-cleanup-phases.sh                       # all pending
bash scripts/run-lint-cleanup-phases.sh P1                    # single phase
bash scripts/run-lint-cleanup-phases.sh P1 P3                 # range
bash scripts/run-lint-cleanup-phases.sh --set P1,P3           # explicit list
bash scripts/run-lint-cleanup-phases.sh P2 --dry-run
bash scripts/run-lint-cleanup-phases.sh --list
bash scripts/run-lint-cleanup-phases.sh --next
bash scripts/run-lint-cleanup-phases.sh --estimate P1 P4
```

### Detection helper

`scripts/lint-phase-detect.sh --phase=<id>` — pure bash, runs:

- P1 → `gofmt -l .` + `goimports -l .`
- P2 → `golangci-lint run --linters=govet,ineffassign,errcheck --out-format=json`
- P3 → `golangci-lint run --linters=staticcheck,unused --out-format=json`
- P4 → `govulncheck -json ./...`

Output: `tmp/lint-cleanup/${phase}-worklist.json` (gitignored).

### What the runner does NOT do

- Does **not** flip warn-mode to strict mode.
- Does **not** touch `.github/workflows/release.yml` or `Makefile` release
  targets.
- Does **not** edit other phases' progress rows.
- Does **not** amend commits — every retry creates a NEW commit.

---

## Files to create (rollout, separate from this plan)

### Configs (separate PRs from runner authoring)
- `.golangci.yml` — `version: "2"`, `linters.default: standard`, opt-in
  `gofumpt`, `goimports`. Generated-file excludes (Wails bindings,
  `*_gen.go`).
- `lefthook.yml` — `pre-commit` parallel block, every command suffixed
  with `|| true`. `golangci-lint run --new-from-rev=HEAD~1 --fast-only`
  for staged `.go` files; `gofmt -l` check.
- `Makefile` — targets: `lint`, `lint-fix`, `vuln`, `lint-cleanup`.
- `.github/workflows/lint-warn.yml` — golangci-lint-action +
  govulncheck-action with `continue-on-error: true`.
- `.gitignore` — add `tmp/lint-cleanup/`, `logs/lint-cleanup-phases/`.

### Runner infrastructure (this PR)
- `scripts/run-lint-cleanup-phases.sh` — bash orchestrator.
- `scripts/lint-phase-detect.sh` — bash detection helper.
- `LINT-CLEANUP-PROGRESS.md` — progress table (P1–P4).
- `plans/lint-cleanup-plan.md` (this file).

---

## Regression safety — non-negotiable per phase

The orchestrator's per-phase acceptance step refuses to mark the row
`done` if any of the following fail:

1. `go build ./...` — compiles cleanly.
2. `go test -race ./...` — full suite passes (matches `make test`).
3. `make full` — release build path passes (gofumpt rewrites can break
   struct tags or build tags if mishandled).
4. `gofmt -l .` and `goimports -l .` return zero files (post-fix).
5. **macOS Wails smoke** (release tier): if the phase touched
   `wails/`, run `wails build && open` to confirm the binary launches
   and the SSH/SFTP gateways respond — golangci-lint won't catch CGO
   or Wails-binding regressions.

If steps 1–4 fail because of a known flake (rare in Go race tests but
possible), record under DEVIATIONS and re-run the suite ONCE; if still
failing, mark the phase `blocked` and stop.

### Rollout-level regression gate

Before promoting from warn-mode to error-mode (separate decision after
P4 — out of this rollout's scope):
- Full release build (`make full`) passes.
- macOS Wails smoke passes manually.
- Cross-repo wails-injection round-trip test (lives in devstudio) passes
  if any P-phase touched `wails/app.go`, `wails/ssh_bridge.go`, or
  `wails/sftp_bridge.go`.

---

## Verification

1. **Warn-mode is genuinely warn-only**:
   - Intentionally introduce an unhandled error in a `.go` file,
     `git add`, `git commit -m "test"`. Expect: commit succeeds with
     diagnostic printed.

2. **Strict-mode opt-in works**:
   - `DEVSTUDIO_LINT_STRICT=1 git commit ...` — expect block.

3. **CI doesn't block PRs**:
   - Push the same intentional violation. `lint` job shows red X but the
     PR is mergeable.

4. **Phase runner walks correctly**:
   ```
   bash scripts/run-lint-cleanup-phases.sh --list
   bash scripts/run-lint-cleanup-phases.sh P1 --dry-run
   bash scripts/run-lint-cleanup-phases.sh P1
   ```

5. **Test suite still passes after each phase**:
   - `go test -race ./...` exits 0.
   - `make full` builds the release binary.

---

## Critical files referenced

- `wails/app.go`, `wails/ssh_bridge.go`, `wails/sftp_bridge.go` — first
  golangci-lint pass surfaces the baseline; expect a P2/P3 cleanup PR.
- `Makefile` — release path; new lint targets land here.
- `.github/workflows/release.yml` — unchanged; new `lint-warn.yml` lands
  alongside.
- Companion plan: `devstudio/plans/lint-cleanup-plan.md`.
- Companion runner: `devstudio/scripts/run-legacy-removal-phases.sh` (the
  canonical orchestrator pattern this script mirrors).
