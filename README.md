# devstudio-proxy

A lightweight HTTP/HTTPS forward proxy with transparent TLS passthrough. See [src/README.md](src/README.md) for full documentation.

## Install via Homebrew

```sh
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

### Run as a background service

```sh
brew services start devproxy
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
git tag v0.2.0
git push origin v0.2.0
```
