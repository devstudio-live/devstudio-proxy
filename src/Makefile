BINARY  = devproxy
DIST    = dist
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS = -ldflags="-s -w -X main.Version=$(VERSION)"

PLATFORMS = \
	windows/amd64 \
	windows/arm64 \
	darwin/amd64  \
	darwin/arm64  \
	linux/amd64   \
	linux/arm64   \
	linux/386     \
	linux/arm

.PHONY: all clean test checksums $(PLATFORMS)

all: $(PLATFORMS) checksums

$(PLATFORMS):
	$(eval OS   := $(firstword $(subst /, ,$@)))
	$(eval ARCH := $(lastword $(subst /, ,$@)))
	$(eval EXT  := $(if $(filter windows,$(OS)),.exe,))
	mkdir -p $(DIST)
	GOOS=$(OS) GOARCH=$(ARCH) go build $(LDFLAGS) -o $(DIST)/$(BINARY)-$(OS)-$(ARCH)$(EXT) .

checksums:
	cd $(DIST) && sha256sum $(BINARY)-* > checksums.txt

test:
	go test -v -race ./...

clean:
	rm -rf $(DIST)/
