# devstudio-proxy Architecture & Integration

## Overview

devstudio-proxy is the backend gateway and integration layer for devstudio. It provides:
- HTTP/SSE proxying for the web surface
- Wails desktop IPC contract (see CLAUDE.md in devstudio)
- Gateway, SSH, SFTP, and streaming endpoints
- Configuration, event, and context cache management

## Integration with devstudio
- Serves the same `dist-web/` artefacts for desktop and web
- HTML rewriter injects `window.__WAILS__=true` for desktop detection
- All host communication (IPC, HTTP, SSE) is routed through well-defined contracts
- For local development, see devstudio-proxy/README.md for setup, and devstudio/docs/ for integration test patterns

## Design Details
- Modular Go codebase: proxycore/ for core logic, wails/ for desktop integration
- IPC and HTTP/SSE contracts are additive-only (see CLAUDE.md)
- Event channels and method surfaces are documented in devstudio/CLAUDE.md
- Configuration and context cache are managed via Go modules

## References
- See devstudio/CLAUDE.md for architectural rules and integration contracts
- See devstudio-proxy/docs/sdlc.md for SDLC and workflow
- See devstudio-proxy/docs/testing.md for CI/CD and test conventions
- See devstudio-proxy/docs/contributing.md for review and ownership
