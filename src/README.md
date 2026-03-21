# devproxy

A lightweight HTTP/HTTPS forward proxy with a multi-protocol database gateway, written in Go. Cross-compiled for Windows, macOS, and Linux across all common CPU architectures.

---

## Features

- **HTTP forwarding** — proxies plain HTTP requests to upstream servers
- **HTTPS tunneling** — handles `CONNECT` method for transparent TLS passthrough (the proxy never sees encrypted content)
- **Database gateway** — header-routed gateway for SQL, MongoDB, Elasticsearch, and Redis from browser-based clients
- **Connection pooling** — per-protocol connection pools with automatic background reaping
- **Request logging** — optional per-request log output with method, URL, status, bytes, and duration
- **CORS support** — allows browser clients to send any method/headers
- **Health check** — `GET /health` returns `{"ok":true,"mode":"forward-proxy"}`
- **Hop-by-hop header stripping** — RFC 7230 §6.1 compliant; removes `Connection`, `Transfer-Encoding`, and related headers
- **Slowloris mitigation** — `ReadHeaderTimeout: 5s` on the server
- **Sensible upstream timeouts** — configurable via the transport (see [Architecture](#architecture))

---

## Installation

### Download a prebuilt binary

After running `make all`, binaries are placed in `dist/`:

```
dist/
├── devproxy-darwin-amd64
├── devproxy-darwin-arm64
├── devproxy-linux-amd64
├── devproxy-linux-arm64
├── devproxy-linux-386
├── devproxy-linux-arm
├── devproxy-windows-amd64.exe
└── devproxy-windows-arm64.exe
```

### Build from source

Requires Go 1.21+.

```sh
# Build for your current platform
go build -o devproxy .

# Build all platforms
make all

# Run tests
make test
```

---

## Usage

```sh
# Start on default port 7700
./devproxy

# Start on a custom port
./devproxy -port 8080

# Start with request logging enabled
./devproxy -log

# Both flags
./devproxy -port 9090 -log
```

### Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-port` | int | `7700` | Port to listen on |
| `-log` | bool | `false` | Enable per-request logging to stderr |

### Startup output

```
Proxy listening on :7700
```

---

## Configuring clients to use the proxy

### curl

```sh
curl -x http://localhost:7700 https://example.com
```

### Go HTTP client

```go
proxyURL, _ := url.Parse("http://localhost:7700")
client := &http.Client{
    Transport: &http.Transport{
        Proxy: http.ProxyURL(proxyURL),
    },
}
resp, err := client.Get("https://example.com")
```

### Environment variables (most HTTP clients respect these)

```sh
export http_proxy=http://localhost:7700
export https_proxy=http://localhost:7700
```

### Browser / system proxy

Set HTTP and HTTPS proxy to `127.0.0.1:7700` in your OS or browser network settings.

---

## Logging output

When `-log` is enabled, each completed request is written to stderr:

```
2026/03/12 06:53:33 127.0.0.1:52104 GET http://example.com/api -> 200 (1234 bytes) in 87.3ms
2026/03/12 06:53:34 127.0.0.1:52105 CONNECT example.com:443 -> 200 (0 bytes) in 5.1ms
```

Format: `<remote addr> <method> <url> -> <status> (<bytes> bytes) in <duration>`

Note: for CONNECT tunnels, `bytes` reflects only the bytes in the HTTP handshake phase (not the tunneled TLS payload, which is opaque to the proxy).

---

## Database Gateway

devproxy embeds a database gateway that allows browser-based clients (which cannot open raw TCP sockets) to connect to SQL, MongoDB, Elasticsearch, and Redis servers. Routing is done via HTTP headers rather than path prefixes to avoid collisions with upstream URLs.

### Routing headers

| Header | Value |
|--------|-------|
| `X-DevStudio-Gateway-Route` | any non-empty value (enables gateway mode) |
| `X-DevStudio-Gateway-Protocol` | `sql`, `mongo`, `elastic`, or `redis` |

### Endpoints

All protocols expose the same REST-style endpoints (POST only):

| Path | Description |
|------|-------------|
| `/test` | Ping the server; returns a row with `"PONG"` or equivalent |
| `/query` | Execute a query string and return tabular results |
| `/tables` | List tables / collections / indices / keys |
| `/describe` | Describe the schema of a specific table / collection / index |
| `/databases` | List databases / namespaces |

### Request body

```json
{
  "connection": "protocol://user:pass@host:port/dbname",
  "query": "SELECT * FROM users",
  "limit": 100
}
```

- `connection` — standard DSN / URL for the target database
- `query` — query string (SQL, MQL JSON, Lucene/DSL, Redis command)
- `limit` — max rows/documents returned (default 100, max 1000)

### Response body

```json
{
  "columns": [{"name": "id"}, {"name": "email"}],
  "rows": [[1, "alice@example.com"]],
  "rowCount": 1,
  "duration": 12.4
}
```

### Connection pooling

Each protocol maintains its own idle connection pool. Background goroutines reap stale connections so the process does not leak resources over time.

### Protocol notes

| Protocol | `query` format | `tables` returns | `databases` returns |
|----------|---------------|-----------------|---------------------|
| SQL | SQL string | table names | database names |
| MongoDB | JSON filter `{"field":"value"}` | collection names | database names |
| Elasticsearch | Lucene query string or `{"query":{...}}` JSON | index names | index namespaces |
| Redis | Redis command e.g. `KEYS *`, `GET foo` | key list (SCAN) | DB index list |

---

## Architecture

```
Client
  │
  │  HTTP GET http://example.com/      ← plain HTTP request
  │  HTTP CONNECT example.com:443      ← HTTPS tunnel request
  ▼
┌─────────────────────────────────────┐
│           devproxy                  │
│                                     │
│  ┌──────────────────────────────┐   │
│  │   LoggingMiddleware (opt.)   │   │
│  └──────────────┬───────────────┘   │
│                 │                   │
│  ┌──────────────▼───────────────┐   │
│  │         Handler{}            │   │
│  │   r.Method == CONNECT?       │   │
│  └──────┬──────────────┬────────┘   │
│         │ yes          │ no         │
│  ┌──────▼──────┐ ┌─────▼────────┐  │
│  │ handleTunnel│ │handleForward │  │
│  │  tunnel.go  │ │  forward.go  │  │
│  └──────┬──────┘ └─────┬────────┘  │
└─────────│──────────────│───────────┘
          │              │
     TCP tunnel     http.Transport
     (raw copy)     RoundTrip
          │              │
          ▼              ▼
       Upstream       Upstream
       (TLS)          (HTTP)
```

### Plain HTTP forwarding (`forward.go`)

1. Clone the incoming request
2. Clear `RequestURI` (required by `http.Transport.RoundTrip`)
3. Strip hop-by-hop headers from the request (RFC 7230 §6.1)
4. Append client IP to `X-Forwarded-For`
5. Execute `transport.RoundTrip(outReq)` to the upstream
6. Strip hop-by-hop headers from the response (especially `Transfer-Encoding`, which `net/http` manages automatically)
7. Stream the response body back to the client

### HTTPS CONNECT tunneling (`tunnel.go`)

The proxy never terminates TLS — it creates an opaque TCP pipe:

1. Dial the target `host:port` (TCP only, no TLS at this layer)
2. Hijack the client connection via `http.Hijacker` to get raw `net.Conn` access
3. Write `HTTP/1.1 200 Connection Established\r\n\r\n` to the raw conn
4. Flush any bytes buffered by the HTTP parser to the upstream
5. Launch two goroutines for bidirectional `io.Copy` (client→upstream, upstream→client)
6. Wait for either direction to finish; deferred `Close()` on both conns cleans up the other

### Logging middleware (`logger.go`)

Wraps `http.ResponseWriter` to capture status code and byte count, then logs after the handler returns. Critically, it also delegates `http.Hijacker` to the underlying writer — without this, CONNECT tunneling would fail when logging is enabled because the proxy could not hijack through the middleware.

### Upstream transport timeouts

| Timeout | Value |
|---------|-------|
| Dial | 10s |
| TLS handshake | 10s |
| Response header | 30s |
| Idle connection | 90s |
| Server read header | 5s |

---

## Project structure

```
src/
├── main.go              # Entry point: flag parsing, server setup, pool reapers
├── proxy.go             # Core handler: gateway routing, CONNECT vs plain HTTP
├── forward.go           # Plain HTTP forwarding and hop-by-hop stripping
├── tunnel.go            # HTTPS CONNECT TCP tunnel via connection hijacking
├── logger.go            # Optional request logging middleware
├── db_gateway.go        # SQL gateway (Postgres, MySQL, SQLite, etc.)
├── db_pool.go           # SQL connection pool
├── mongo_gateway.go     # MongoDB gateway
├── mongo_pool.go        # MongoDB connection pool
├── elastic_gateway.go   # Elasticsearch gateway
├── elastic_pool.go      # Elasticsearch connection pool
├── redis_gateway.go     # Redis gateway
├── redis_pool.go        # Redis connection pool
├── proxy_test.go        # HTTP forwarding tests
├── tunnel_test.go       # CONNECT tunnel tests
├── go.mod               # Module dependencies
└── Makefile             # Cross-compilation and test targets
```

---

## Building for all platforms

```sh
make all
```

Produces binaries in `dist/` for:

| OS | amd64 | arm64 | 386 | arm |
|----|-------|-------|-----|-----|
| Windows | ✓ | ✓ | | |
| macOS | ✓ | ✓ | | |
| Linux | ✓ | ✓ | ✓ | ✓ |

Binaries are stripped of debug symbols (`-s -w`) to minimize size (~5–7 MB per binary).

```sh
make clean   # remove dist/
make test    # go test -v -race ./...
```

---

## Tests

```sh
make test
# or
go test -v -race ./...
```

### HTTP forwarding (`proxy_test.go`)

| Test | What it verifies |
|------|-----------------|
| `TestHTTPForward_200` | Basic GET passthrough, response body intact |
| `TestHTTPForward_POST` | Request body forwarded and echoed correctly |
| `TestHTTPForward_404` | Upstream 4xx status codes pass through |
| `TestHTTPForward_HopByHop` | `Connection`/`Transfer-Encoding` stripped before forwarding |
| `TestHTTPForward_InvalidTarget` | Returns 502 when upstream is unreachable |

### CONNECT tunneling (`tunnel_test.go`)

| Test | What it verifies |
|------|-----------------|
| `TestCONNECTTunnel_Basic` | Full CONNECT flow to a real TLS upstream |
| `TestCONNECTTunnel_InvalidHost` | Returns 502 when target is unreachable |
| `TestCONNECTTunnel_LargePayload` | 1 MB response integrity through the tunnel |
| `TestLoggingMiddleware_HijackDelegate` | CONNECT still works with logging middleware active |

All tests use `httptest.NewServer` / `httptest.NewTLSServer` — no mocks, no network dependencies, no test flakiness.

---

## Implementation notes for developers

### Why not `httputil.ReverseProxy`?

`httputil.ReverseProxy` is designed for backend load balancing. It rewrites `Host`, manages `X-Forwarded-For` differently, and has no understanding of the `CONNECT` method. A forward proxy needs full control over header manipulation and must handle CONNECT via raw TCP hijacking, which ReverseProxy cannot do.

### The `RequestURI = ""` requirement

Go's `http.Transport.RoundTrip` rejects any request where `RequestURI != ""`. Incoming requests from `http.Server` always have `RequestURI` populated. It must be explicitly cleared before forwarding.

### Hijacker delegation in middleware

Any `http.ResponseWriter` wrapper that sits in front of a CONNECT handler **must** implement `http.Hijacker` by delegating to the underlying writer. If it does not, `w.(http.Hijacker)` type-asserts to `false` and the tunnel returns 500. This is the single most common bug in proxy+middleware combinations.

### Buffered reader flush after hijack

After `Hijack()`, the HTTP server's `bufio.Reader` may contain bytes the client sent after the CONNECT line. These must be written to the upstream connection before starting `io.Copy`, or the first bytes of the TLS handshake are silently dropped.
