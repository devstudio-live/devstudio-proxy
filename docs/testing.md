# devstudio-proxy Testing & CI/CD

## CI/CD Gates
- Linting, unit tests, and integration tests required for all PRs
- Use scripts in devstudio-proxy/scripts/ for lint, test, and release
- See devstudio/CLAUDE.md for test tier definitions and conventions

## Test Conventions
- Go unit tests in proxycore/ and wails/
- Integration tests for IPC, HTTP/SSE, and gateway endpoints
- Test plans and coverage tracked in devstudio/plans/ and referenced here

## Running Tests
- `make test` for unit tests
- Integration tests: see devstudio-proxy/README.md
