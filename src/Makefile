BINARY  = devproxy
DIST    = dist
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS = -ldflags="-s -w -X main.Version=$(VERSION)"

PLATFORMS_DARWIN = \
	darwin/amd64  \
	darwin/arm64

PLATFORMS_ALL = \
	windows/amd64 \
	windows/arm64 \
	darwin/amd64  \
	darwin/arm64  \
	linux/amd64   \
	linux/arm64   \
	linux/386     \
	linux/arm

PLATFORMS = $(PLATFORMS_DARWIN)

WAILS_DIR = ../wails
WAILS_BIN = $(WAILS_DIR)/build/bin
GOBIN     = $(shell go env GOPATH)/bin
WAILS     = $(GOBIN)/wails

.PHONY: all full clean test checksums wails-setup wails-mac wails-dev wails-full $(PLATFORMS_ALL)

all: $(PLATFORMS_DARWIN) checksums

full: $(PLATFORMS_ALL) checksums

$(PLATFORMS_ALL):
	$(eval OS   := $(firstword $(subst /, ,$@)))
	$(eval ARCH := $(lastword $(subst /, ,$@)))
	$(eval EXT  := $(if $(filter windows,$(OS)),.exe,))
	mkdir -p $(DIST)
	CGO_ENABLED=0 GOOS=$(OS) GOARCH=$(ARCH) go build $(LDFLAGS) -o $(DIST)/$(BINARY)-$(OS)-$(ARCH)$(EXT) .

checksums:
	cd $(DIST) && sha256sum $(BINARY)-* > checksums.txt

test:
	go test -v -race ./...

clean:
	rm -rf $(DIST)/

# Install wails CLI if missing; re-run go mod tidy only when go.mod changes
$(WAILS_DIR)/go.sum: $(WAILS_DIR)/go.mod
	cd $(WAILS_DIR) && go mod tidy

wails-setup: $(WAILS_DIR)/go.sum
	@test -f $(WAILS) || go install github.com/wailsapp/wails/v2/cmd/wails@latest

# macOS only: universal .app bundle + arch-split CLI binaries extracted via lipo
wails-mac: wails-setup
	cd $(WAILS_DIR) && $(WAILS) build -platform darwin/universal -clean
	lipo $(WAILS_BIN)/devproxy.app/Contents/MacOS/devproxy \
		-extract arm64  -output $(WAILS_BIN)/devproxy-darwin-arm64
	lipo $(WAILS_BIN)/devproxy.app/Contents/MacOS/devproxy \
		-extract x86_64 -output $(WAILS_BIN)/devproxy-darwin-amd64

wails-dev: wails-mac
	DEVSTUDIO_TARGET_URL=http://localhost:5173 \
		$(WAILS_BIN)/devproxy.app/Contents/MacOS/devproxy

# All platforms: macOS (as above) + Windows + Linux
wails-full: wails-mac
	cd $(WAILS_DIR) && $(WAILS) build -platform windows/amd64
	cp $(WAILS_BIN)/devproxy.exe $(WAILS_BIN)/devproxy-windows-amd64.exe
	cd $(WAILS_DIR) && $(WAILS) build -platform windows/arm64
	cp $(WAILS_BIN)/devproxy.exe $(WAILS_BIN)/devproxy-windows-arm64.exe
	cd $(WAILS_DIR) && $(WAILS) build -platform linux/amd64
	cp $(WAILS_BIN)/devproxy     $(WAILS_BIN)/devproxy-linux-amd64
	cd $(WAILS_DIR) && $(WAILS) build -platform linux/arm64
	cp $(WAILS_BIN)/devproxy     $(WAILS_BIN)/devproxy-linux-arm64
