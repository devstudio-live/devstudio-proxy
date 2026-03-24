# Avoiding Xcode Dependency on macOS

The devstudio-proxy project previously required Xcode tools on macOS because `modernc.org/sqlite` can pull in C library dependencies.

## Solution

Two complementary approaches have been implemented:

### 1. **CGO_ENABLED=0 in Makefile (Immediate Fix)**

The Makefile now builds with `CGO_ENABLED=0`, forcing pure Go compilation even if CGO-using packages are imported.

**Build the project normally:**
```bash
cd src
make all
```

This will now work on macOS without Xcode installed.

### 2. **Optional SQLite Support (Advanced)**

SQLite support is now optional via Go build tags:

**Build without SQLite (pure Go, no Xcode needed):**
```bash
cd src
CGO_ENABLED=0 go build -o devproxy .
```

**Build with SQLite support:**
```bash
cd src
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -tags sqlite -o devproxy-sqlite .
# or use the Makefile which now includes CGO_ENABLED=0
make all
```

## Files Changed

- **Makefile**: Added `CGO_ENABLED=0` to all build commands
- **db_drivers.go**: Removed unconditional SQLite import, added runtime check
- **db_drivers_sqlite.go** (new): SQLite driver imported only when `-tags sqlite` is used
- **db_drivers_nosqlite.go** (new): Stub implementation for when SQLite is not available

## Error Handling

If a user tries to use SQLite without building with the sqlite tag, they'll get a clear error:
```
sqlite driver not available — rebuild with -tags sqlite or use CGO_ENABLED=1
```

## Notes

- **PostgreSQL and MySQL drivers** remain unconditionally available and are pure Go (no Xcode needed)
- The default `make all` command continues to work as before
- The Makefile now sets `CGO_ENABLED=0` globally, ensuring all platforms get pure Go builds
