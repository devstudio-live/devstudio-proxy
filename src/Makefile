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

.PHONY: all full clean test checksums $(PLATFORMS_ALL)

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
