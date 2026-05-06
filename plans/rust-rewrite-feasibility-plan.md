# devstudio-proxy → Rust Rewrite Feasibility & Implementation Plan

## Verdict: **Feasible.** Every dependency has a production-grade Rust equivalent; no library is missing, only a few are different in shape.

The codebase is ~38k LOC of Go (~30k non-test, ~600 LOC for the Wails layer). Direct external dependencies number 17, plus heavy use of Go stdlib (`net/http`, `crypto/tls`, `database/sql`, `encoding/binary`). Every protocol surface and every binding has a Rust crate that covers it.

The strategic case is also strong — devstudio already has a `tauri` build surface in `vite.config.tauri.js`. A Rust core unifies the desktop story (Wails + Tauri → just Tauri).

---

## 1. Dependency-by-dependency mapping

| Subsystem | Go library | Rust replacement | Risk |
| --- | --- | --- | --- |
| HTTP server / forward proxy / CONNECT tunnel | `net/http`, `httputil` | `axum` + `hyper` + `tokio` | None — strictly better |
| WebSocket (SSH terminal, K8s exec, SSE) | `gorilla/websocket` | `tokio-tungstenite` (or `axum::extract::ws`) | None |
| TLS (server) | `crypto/tls` | `rustls` + `tokio-rustls` | None |
| Local CA + leaf cert generation | `crypto/x509`, `crypto/ecdsa` | `rcgen` | None — direct map |
| TLS dual-listener (byte-sniff 0x16 → TLS, else plain) | custom (`tls_listen.go`) | direct port over `tokio::net::TcpListener` + `BufReader::peek` | None |
| Trust store install (macOS Keychain / Windows certutil / Firefox NSS) | shells out to `security`/`certutil`/`osascript` | `security-framework` crate (native APIs) or shell out identically | None — already process-shell based |
| PostgreSQL | `lib/pq` | `tokio-postgres` or `sqlx` | None |
| MySQL | `go-sql-driver/mysql` | `mysql_async` or `sqlx` | None |
| SQLite | `modernc.org/sqlite` (pure-Go transpile) | `rusqlite` with `bundled` feature (statically links libsqlite3.c) or `sqlx` | Low — statically links C, but cross-compiles cleanly to all platforms |
| ClickHouse | `ClickHouse/clickhouse-go/v2` | `clickhouse.rs` (official) | None |
| MongoDB | `go.mongodb.org/mongo-driver` | `mongodb` (official driver) | None |
| Redis | `redis/go-redis/v9` | `redis-rs` (or `fred` for cluster/sentinel) | None |
| Elasticsearch | `elastic/go-elasticsearch/v8` | `elasticsearch` (official) | Low — supports v8 |
| Kafka (consumer/producer/admin/SASL/SCRAM) | `segmentio/kafka-go` | `rdkafka` (wraps librdkafka via `cmake`) | **Medium** — rdkafka is the only mature option; pure-Rust `rskafka` lacks admin API parity. Cross-compile builds fine with vendored librdkafka |
| SSH client (auth methods, jump hosts, KI, agent forwarding, PTY, sessions) | `golang.org/x/crypto/ssh` | `russh` + `russh-keys` | Low — feature-complete, async, actively maintained |
| SFTP (streaming up/download, segmented) | `pkg/sftp` | `russh-sftp` | None |
| Kubernetes client (dynamic client, exec, port-forward, watch, kubeconfig) | `k8s.io/client-go` | `kube-rs` | **Medium** — feature-complete, API-shape-different. WebSocket exec supported; SPDY port-forward supported (kube-rs ≥0.95) |
| Kubernetes YAML | `sigs.k8s.io/yaml` | `serde_yml` (+ `serde_json` round-trip when needed) | None |
| Embedded JS (MCP routines runner with `__storeContext`/`__storeObject` host bindings) | `dop251/goja` (ES5+) | `rquickjs` (QuickJS bindings, ES2020, ~600 KB) **or** `boa_engine` (pure-Rust, ES2015+) | **Medium** — semantics differ subtly; `mcp_routines.js` may need surface-level edits |
| Self-update via GitHub releases (checksum-verified) | `creativeprojects/go-selfupdate` | `self_update` crate | Low — same model: release lookup, asset download, SHA verify, in-place swap |
| Wails desktop wrapper (webview + Go↔JS bindings + EventsEmit/EventsOn) | `wailsapp/wails/v2` | **Tauri 2** (`tauri`, `tauri-build`, `wry`) | None — same architecture, stronger ecosystem |
| UUID, uniseg, csv, html templating | `google/uuid`, etc. | `uuid`, `csv`, `askama`/`tera` | None |
| Memory-mapped HPROF files | `syscall.Mmap` (custom unix/windows) | `memmap2` | None |
| HPROF binary parser (~5k LOC: dominator tree, OQL, threads, classloader) | custom Go | direct port | None — pure logic, faster in Rust |
| Container runtimes (Docker/Podman over Unix socket; nerdctl/buildah/crictl/compose via subprocess) | `os/exec`, `net/http` over UnixStream | `tokio::net::UnixStream` + `hyper`, `tokio::process::Command`. Optional: `bollard` for Docker | None |
| Docker/Podman API HTTP-over-socket | custom (`container_docker.go`) | `hyperlocal` or hand-rolled (Go also hand-rolls) | None |
| URL inspection (cert chain, DNS/CDN, URLhaus phishing) | `crypto/tls`, `net`, `net/http` | direct port | None |

**Bottom line: zero dependencies are *missing* from the Rust ecosystem.** Two require care (rdkafka/Kafka admin, kube-rs/SPDY port-forward); one requires semantic verification (Goja → rquickjs).

---

## 2. Portability analysis

The proxy ships as a single static binary on darwin/linux/windows (amd64+arm64). Rust toolchains support every target, and the dependency graph above produces a **statically linked, dependency-free binary** modulo two C libraries that both vendor cleanly:

- `librdkafka` (via `rdkafka` with `cmake-build`/`vendored` features) — needs `cmake` at build time, embeds at link time.
- `libsqlite3` (via `rusqlite`'s `bundled` feature) — `cc`-built from vendored C source, no system dependency.

Cross-compilation: `cargo zigbuild` or `cross` handle every platform pair (including Windows from macOS). The only platform-specific code paths today are `mmap_unix.go` / `mmap_windows.go` (handled by `memmap2` cross-platform), `update_restart_*` (handled by `self_update`), and `installCATrust*` (already shells out to OS commands; identical strategy in Rust).

---

## 3. The non-trivial subsystems

### a. Goja (embedded JavaScript) — `mcp_gateway.go`, ~300 LOC
The MCP gateway loads `mcp_routines.js` from `https://www.devstudio.live/resources/mcp_routines.js` and calls `mcp.handleRequest(body)` with two host-injected globals: `__storeContext`, `__storeObject`. Both **rquickjs** and **boa_engine** support exactly this pattern. Recommendation: **rquickjs** — small footprint (~600 KB), ES2020, ~5–10× faster than boa for typical scripts. Risk: subtle ES-semantics differences (e.g., regex flags, JSON.stringify edge cases). Mitigation: replay the existing MCP test corpus against both runtimes during the spike.

### b. Kubernetes client — `k8s_*.go`, ~3.5k LOC
`kube-rs` is the mature choice (used by Linkerd, Talos, dozens of operators). Differences from `client-go`:
- Dynamic client uses `kube::Api<DynamicObject>`; idiom-different but feature-equivalent.
- `kubectl exec` over WebSocket: supported.
- Port-forward: supported, both SPDY (legacy) and WebSocket (K8s 1.30+).
- The current code's "SSH-mode kubectl" path (`k8s_ssh.go`, ~900 LOC) shells out to `kubectl` over SSH — this stays **identical** in Rust (just executes commands over `russh::Channel`).

### c. Kafka — `kafka_*.go`, ~4.3k LOC
`rdkafka` covers consumer/producer/admin/SASL+SCRAM/TLS/connection modes. The current code uses `kafka-go`'s pure-Go TCP path; `rdkafka` is C-backed but feature-richer (better admin API coverage). The "connection-mode pool" (Kafka via SSH tunnel / K8s port-forward) plugs into `rdkafka` cleanly through its socket factory hook.

### d. HPROF parser — `hprof_*.go`, ~5k LOC
Pure binary parsing + algorithms (dominator tree, OQL, classloader hierarchy, thread analysis). Direct port to Rust with `memmap2` and zero unsafe code. Likely **faster** than the Go original due to better SIMD, no GC, and `nom`/manual byte-slice parsing.

### e. Wails → Tauri shim
The Wails layer (`wails/`, ~600 LOC) is the thinnest part. Tauri 2's command/event system maps 1:1:
- `runtime.EventsEmit(ctx, name, data)` → `app.emit(name, data)`
- `EventsOn` / `EventsOff` (frontend side, in `src/tools/*.transport.{ts,js}`) → `listen(name, handler)` from `@tauri-apps/api/event`
- Method bindings (e.g., `App.SSHOpenTerminal`) → `#[tauri::command]` functions
- The HTML rewriter (`wails/proxy.go`) that injects `window.__WAILS__=true` becomes a Tauri response interceptor (or the React side detects `window.__TAURI__` instead — already a defined surface in `vite.config.tauri.js`)

The IPC method *names* listed in devstudio's CLAUDE.md (`SSHOpenTerminal`, `MCPGateway`, `SFTPDownloadStart`, etc.) all stay identical — the React `*.transport.{ts,js}` files (the only consumers) get one new branch.

### f. macOS keychain trust install — `tls_trust.go`, ~170 LOC
Currently shells out to `security add-trusted-cert` and falls back to `osascript`+sudo. Direct port: same `Command::new("security")` pattern, or use `security-framework` crate for native API. The macOS-15-trust-not-set bug detection (`verifyLoginKeychainTrust`) ports verbatim.

---

## 4. Risk-ranked summary

| Risk | Severity | Mitigation |
| --- | --- | --- |
| Goja → rquickjs subtle semantic differences | Medium | Spike: load `mcp_routines.js`, replay request corpus, fix discrepancies in JS source |
| kube-rs API ergonomics differ from client-go | Low-Medium | Wrap in a small adapter trait; port handler-by-handler |
| rdkafka build complexity (cmake + librdkafka) | Low-Medium | Use `vendored` feature; pin librdkafka version; CI-build per target on cargo build |
| HPROF mmap edge cases | Low | `memmap2` is well-tested; existing test fixtures port directly |
| Tauri's IPC ergonomics in 2.0 (some breaking changes from Tauri 1.x) | Low | Tauri 2 is GA; well-documented |
| Effort estimation (38k LOC Go) | High | See plan below — phased rewrite, gateway-by-gateway, in parallel with Go |

---

## 5. Implementation plan (proposed)

A 4–6 engineer-month, low-risk staged migration. The HTTP/SSE contract on `:7700` is the natural seam; the Rust binary can be a drop-in for the Go binary at any phase boundary.

### Phase 0 — Spike (1–2 weeks)
- Stand up `axum` skeleton with `/health`, `/admin/config`, dual-listener (TLS sniff), local CA generation via `rcgen`.
- Verify rdkafka + kube-rs + russh + rquickjs build statically across darwin/linux/windows.
- Run `mcp_routines.js` corpus through rquickjs; document any compatibility gaps.
- **Exit criterion**: a Rust binary that passes `/health` and serves the forward proxy + CONNECT tunnel.

### Phase 1 — Forward proxy + admin + MCP cache (1 week)
- `proxy.go` (forward + CONNECT) → `axum`/`hyper` proxy router.
- `admin.go`, `event_ring.go`, `traffic_ring.go`, `logger.go` → `tokio::sync::broadcast` ring buffers, SSE via `axum::Sse`.
- `context_cache.go` → `dashmap` + atomic counters.
- `mcp_gateway.go` → `rquickjs` runtime with hot-swap.

### Phase 2 — Database gateways (1 week)
SQL (postgres/mysql/sqlite/clickhouse), Mongo, Redis, Elastic — each via its mature Rust client + a connection pool. These are stateless POST handlers; trivial port.

### Phase 3 — FS + URL inspector + container gateway (1 week)
`fs_gateway.go`, `urlopener_inspect.go`, container detect/Docker/Podman/nerdctl/buildah/crictl/compose. Pure logic + subprocess shelling out. Direct ports.

### Phase 4 — SSH (terminal + SFTP + tunnel) (1.5 weeks)
`russh` for client, `russh-sftp` for SFTP. PTY + KI auth + jump hosts + agent forwarding all map. WebSocket terminal endpoint via `axum`'s WS support.

### Phase 5 — Kubernetes (2 weeks)
`kube-rs` — most code paths (`k8s_phase3.go`, `k8s_write.go`, `k8s_logs.go`, `k8s_portforward.go`, `k8s_exec.go`). The SSH-mode handlers (`k8s_ssh*.go`) port unchanged (kubectl over russh).

### Phase 6 — Kafka (1.5 weeks)
`rdkafka` — connection modes (direct/SSH-tunnel/K8s-port-forward), admin API (topics, brokers, ACLs, configs, offsets, consumer groups). The "innovative" features in `kafka_innovative.go` (1.2k LOC of admin/lag/throughput tooling) need careful API-shape mapping but no missing primitives.

### Phase 7 — HPROF (1.5 weeks)
Direct port. `memmap2` + `nom` for the binary parser; the dominator-tree, OQL, classloader, threads, and report code is pure-logic and ports verbatim.

### Phase 8 — DAG executor (1 week)
`dag_executor.go`, `dag_gateway.go`, `dag_large_payload.go` — internal HTTP calls to the 10 gateways stay internal `axum::Router` calls. Topo-sort + frame streaming via `tokio::sync::mpsc`.

### Phase 9 — Self-update + restart loop (3 days)
`self_update` crate; restart loop and platform-specific binary swap (`update_restart_{darwin,unix,windows}.go`) port to the corresponding `self_update` features.

### Phase 10 — Tauri shell (1.5 weeks)
Replace `wails/` with a `src-tauri/` directory:
- `app.go` IPC bindings → `#[tauri::command]` functions calling internal `axum::Router::oneshot()` (mirrors current Wails pattern of using `httptest.NewRecorder`).
- `ssh_bridge.go`, `sftp_bridge.go` → Tauri command + event emitter.
- `proxy.go` HTML rewriter → Tauri response interceptor (or React detects `__TAURI__`).
- Frontend `*.transport.{ts,js}` files gain a `tauri` branch; `wails-in-react` lint gate stays as-is.

### Phase 11 — CI + parity gate (1 week)
- Cross-compile matrix: 6 targets × cargo-zigbuild.
- Parity test harness: drive both Go and Rust binaries on the same port, replay a fixture corpus, diff JSON responses byte-for-byte.
- Cut a feature-flagged release: ship Rust binary alongside Go for a release; switch over once parity tests are green for two consecutive releases.

### Total: ~14–18 engineer-weeks
Bracketed by phase 0 spike risk and phase 5 (k8s) + phase 6 (Kafka) + phase 7 (HPROF) being the heaviest single ports.

---

## 6. Recommendation

**Proceed, but spike phase 0 first.** The two libraries that warrant verification before committing are **rquickjs** (against the actual `mcp_routines.js`) and **kube-rs** (against the SPDY port-forward + dynamic-client paths). If both clear the spike, the rest is mechanical translation across well-trodden Rust APIs. The unification with the existing Tauri build surface is a bonus that simplifies devstudio's desktop story long-term.

The single biggest *non-feasibility* risk would be Goja semantic divergence in the MCP runtime — and even that is solvable by editing the JS source, not the Rust runtime.
