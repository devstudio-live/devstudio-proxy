# devstudio-proxy

A lightweight HTTP/HTTPS forward proxy with transparent TLS passthrough and a multi-protocol database gateway (SQL, MongoDB, Elasticsearch, Redis). See [src/README.md](src/README.md) for full documentation.

## Install (standalone)

**macOS / Linux:**

```sh
curl -fsSL https://raw.githubusercontent.com/devstudio-live/devstudio-proxy/main/scripts/install/install.sh | bash
```

**Windows (PowerShell):**

```powershell
irm https://raw.githubusercontent.com/devstudio-live/devstudio-proxy/main/scripts/install/install.ps1 | iex
```

The installer downloads the latest release binary, verifies its SHA256 checksum, and places it in `~/.local/bin` (Unix) or `%LOCALAPPDATA%\devstudio-proxy` (Windows). Pass `--help` (Unix) or `-?` (Windows) for options including custom install directories and auto-start service setup.

To install with auto-start (macOS launchd / Linux systemd):

```sh
curl -fsSL https://raw.githubusercontent.com/devstudio-live/devstudio-proxy/main/scripts/install/install.sh | bash -s -- --service
```

## Install via Homebrew

```sh
brew tap devstudio-live/devstudio-proxy https://github.com/devstudio-live/devstudio-proxy
brew install devstudio-proxy
```

### Upgrade to the latest version

```sh
brew update
brew upgrade devstudio-proxy
```

If the upgrade does not pick up the latest tag (e.g. the formula is cached), force-reinstall:

```sh
brew reinstall devstudio-proxy
```

To force Homebrew to re-fetch the tap and pull the latest formula:

```sh
brew update --force
brew upgrade devstudio-proxy
```

### Uninstall and reinstall to get the latest

If you're having trouble getting the latest version, uninstall completely and reinstall:

```sh
brew uninstall devstudio-proxy
brew untap devstudio-live/devstudio-proxy
brew tap devstudio-live/devstudio-proxy https://github.com/devstudio-live/devstudio-proxy
brew install devstudio-proxy
```

### Usage

```sh
devproxy                     # listens on :7700
devproxy -port 8080          # custom port
devproxy -port 7700 -log     # with request logging
devproxy -port 7700 -verbose # log request headers (useful for debugging gateway routing)
devproxy -version            # print version
```

### Configuration file

When installed via Homebrew, settings can be persisted in the config file so they survive service restarts. Edit the file for your platform:

- **Apple Silicon:** `/opt/homebrew/etc/devstudio-proxy.conf`
- **Intel Mac:** `/usr/local/etc/devstudio-proxy.conf`

```
PORT=7700
LOG=false
VERBOSE=false
MCP_REFRESH=30m
MCP_FALLBACK=false
```

After editing, restart the service to apply changes:

```sh
brew services restart devstudio-proxy
```

Settings can also be set via environment variables (`DEVPROXY_PORT`, `DEVPROXY_LOG`, `DEVPROXY_VERBOSE`, `DEVPROXY_MCP_REFRESH`, `DEVPROXY_MCP_FALLBACK`). Priority order: config file < env vars < CLI flags.

### DevStudio Gateway (database protocols)

devproxy also acts as a local gateway for database connections from browser-based clients. Route requests using these headers:

| Header | Value |
|--------|-------|
| `X-DevStudio-Gateway-Route` | any non-empty value |
| `X-DevStudio-Gateway-Protocol` | `sql`, `mongo`, `elastic`, or `redis` |

Each protocol exposes the same set of endpoints (via `r.URL.Path`):

| Endpoint | Description |
|----------|-------------|
| `/test` | Ping / connectivity check |
| `/query` | Run a query and return rows |
| `/tables` | List tables / collections / indices / keys |
| `/describe` | Describe schema of a table / collection |
| `/databases` | List databases / namespaces |

The request body is a JSON object with a `connection` object, a `sql` string, and an optional `table` and `limit`:

```json
{
  "connection": {
    "driver": "postgres",
    "host": "localhost",
    "port": 5432,
    "database": "mydb",
    "user": "alice",
    "password": "secret",
    "ssl": "disable"
  },
  "sql": "SELECT * FROM users LIMIT 10"
}
```

MongoDB Atlas example using a full SRV URI:

```json
{
  "connection": {
    "connectionString": "mongodb+srv://user:p%40ssword@cluster0.abc123.mongodb.net/?retryWrites=true&w=majority",
    "database": "mydb"
  },
  "table": "orders",
  "sql": "{}"
}
```

See [src/README.md](src/README.md) for connection field reference and examples for all protocols (MySQL, SQLite, MongoDB, Elasticsearch, Redis).

Connections are pooled and reaped automatically per protocol.

### Run as a background service (auto-starts on login)

```sh
brew services start devstudio-proxy
```

### MCP Server

devproxy exposes a local MCP endpoint at `http://localhost:7700/mcp` that proxies to `https://devstudio.live/mcp`.

**Claude Code**

> **Note:** The proxy must be running before adding the MCP server. If `devproxy` is not running, the MCP server will not be registered. Start it first:
> ```sh
> brew services start devstudio-proxy
> ```

```sh
claude mcp add --transport http devstudio http://localhost:7700/mcp
```

**OpenAI Codex / Codex CLI**

```sh
codex mcp add devstudio --url http://localhost:7700/mcp
```

Or add it manually to your `~/.codex/config.json`:

```json
{
  "mcpServers": {
    "devstudio": {
      "url": "http://localhost:7700/mcp"
    }
  }
}
```

### Point your client at the proxy

```sh
# curl
curl -x http://localhost:7700 https://example.com

# environment variables (applies to most CLI tools)
export http_proxy=http://localhost:7700
export https_proxy=http://localhost:7700
```

## Run locally

```sh
cd src
go run .                          # listens on :7700
go run . -port 8080               # custom port
go run . -port 7700 -log          # with request logging
go run . -port 7700 -verbose      # log request headers
```

Verify it's running:

```sh
curl -x http://localhost:7700 https://example.com
```

## Build from source

```sh
cd src
make all        # builds all platforms into dist/
make test       # run tests
```

## Release

Use `scripts/release.sh` to safely create and push a version tag. The script validates that you're on `main`, synced with the remote, and the tag doesn't already exist.

```sh
./scripts/release.sh v0.13.0
```

The GitHub Actions workflow will then automatically:
1. Build all platform binaries (8 targets)
2. Generate checksums
3. Update the Homebrew formula with new SHA256 values
4. Commit the formula changes back to `main`
5. Create a GitHub Release with release notes and binary assets

### Cleanup old releases

To free up storage, delete all releases except the latest:

```sh
./scripts/cleanup-releases.sh --dry-run  # preview
./scripts/cleanup-releases.sh             # execute (keeps 1 latest)
```

Optionally keep more releases:

```sh
./scripts/cleanup-releases.sh --keep 3   # keep 3 most recent
```
