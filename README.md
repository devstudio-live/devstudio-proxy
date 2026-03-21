# devstudio-proxy

A lightweight HTTP/HTTPS forward proxy with transparent TLS passthrough and a multi-protocol database gateway (SQL, MongoDB, Elasticsearch, Redis). See [src/README.md](src/README.md) for full documentation.

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
devproxy                  # listens on :7700
devproxy -port 8080       # custom port
devproxy -port 7700 -log  # with request logging
devproxy -version         # print version
```

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

Connections are pooled and reaped automatically per protocol.

### Run as a background service (auto-starts on login)

```sh
brew services start devstudio-proxy
```

### Point your client at the proxy

```sh
# curl
curl -x http://localhost:7700 https://example.com

# environment variables (applies to most CLI tools)
export http_proxy=http://localhost:7700
export https_proxy=http://localhost:7700
```

## Build from source

```sh
cd src
make all        # builds all platforms into dist/
make test       # run tests
```

## Release

Tag a commit to trigger a GitHub Actions release build. The workflow will automatically build all platform binaries, update the formula SHA256 values, commit them back to `main`, and publish the GitHub Release.

```sh
git tag v0.1.0
git push origin v0.1.0
```

To release a newer version, increment the tag:

```sh
git tag v0.8.0
git push origin v0.8.0
```
