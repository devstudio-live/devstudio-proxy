BINARY  = devproxy
LDFLAGS = -ldflags="-s -w"
DIST    = dist

PLATFORMS = \
	windows/amd64 \
	windows/arm64 \
	darwin/amd64  \
	darwin/arm64  \
	linux/amd64   \
	linux/arm64   \
	linux/386     \
	linux/arm

.PHONY: all clean test $(PLATFORMS)

all: $(PLATFORMS)

$(PLATFORMS):
	$(eval OS   := $(firstword $(subst /, ,$@)))
	$(eval ARCH := $(lastword $(subst /, ,$@)))
	$(eval EXT  := $(if $(filter windows,$(OS)),.exe,))
	mkdir -p $(DIST)
	GOOS=$(OS) GOARCH=$(ARCH) go build $(LDFLAGS) -o $(DIST)/$(BINARY)-$(OS)-$(ARCH)$(EXT) .

test:
	go test -v -race ./...

clean:
	rm -rf $(DIST)/
