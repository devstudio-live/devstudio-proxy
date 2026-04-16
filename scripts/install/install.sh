#!/usr/bin/env bash
# scripts/install/install.sh — standalone installer for devstudio-proxy
# Downloads the latest release binary from GitHub and installs it to
# $HOME/.local/bin/devproxy (or a user-specified directory).
#
# Usage:
#   curl -fsSL https://raw.githubusercontent.com/devstudio-live/devstudio-proxy/main/scripts/install/install.sh | bash
#   curl -fsSL ... | bash -s -- --dir /usr/local/bin --service
#
# Options:
#   --dir DIR       Install directory (default: $HOME/.local/bin)
#   --version VER   Install a specific version (default: latest)
#   --service       Install a user-level launchd (macOS) or systemd (Linux) service
#   --no-verify     Skip SHA256 checksum verification
#   --help          Show this help message

set -euo pipefail

REPO="devstudio-live/devstudio-proxy"
BINARY_NAME="devproxy"
INSTALL_DIR="${HOME}/.local/bin"
VERSION=""
INSTALL_SERVICE=false
VERIFY_CHECKSUM=true

# ── Helpers ──────────────────────────────────────────────────────────────

die()  { printf '\033[1;31merror:\033[0m %s\n' "$1" >&2; exit 1; }
info() { printf '\033[1;34m==>\033[0m %s\n' "$1"; }
ok()   { printf '\033[1;32m==>\033[0m %s\n' "$1"; }
warn() { printf '\033[1;33mwarning:\033[0m %s\n' "$1" >&2; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

# ── Parse arguments ─────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dir)       INSTALL_DIR="$2"; shift 2 ;;
    --version)   VERSION="$2"; shift 2 ;;
    --service)   INSTALL_SERVICE=true; shift ;;
    --no-verify) VERIFY_CHECKSUM=false; shift ;;
    --help|-h)
      sed -n '2,/^$/{ s/^# //; s/^#$//; p }' "$0"
      exit 0
      ;;
    *) die "unknown option: $1" ;;
  esac
done

# ── Detect platform ─────────────────────────────────────────────────────

detect_os() {
  local os
  os="$(uname -s | tr '[:upper:]' '[:lower:]')"
  case "${os}" in
    darwin) echo "darwin" ;;
    linux)  echo "linux" ;;
    *)      die "unsupported OS: ${os} — use install.ps1 for Windows" ;;
  esac
}

detect_arch() {
  local arch
  arch="$(uname -m)"
  case "${arch}" in
    x86_64|amd64)       echo "amd64" ;;
    aarch64|arm64)      echo "arm64" ;;
    armv7l|armv6l|arm*) echo "arm" ;;
    i386|i686)          echo "386" ;;
    *)                  die "unsupported architecture: ${arch}" ;;
  esac
}

OS="$(detect_os)"
ARCH="$(detect_arch)"
ASSET_NAME="${BINARY_NAME}-${OS}-${ARCH}"

info "Detected platform: ${OS}/${ARCH}"

# ── Resolve version ─────────────────────────────────────────────────────

need_cmd curl

if [[ -z "${VERSION}" ]]; then
  info "Fetching latest release version..."
  VERSION="$(curl -fsSL "https://api.github.com/repos/${REPO}/releases/latest" \
    | grep '"tag_name"' | head -1 | sed 's/.*"tag_name": *"//;s/".*//')" \
    || die "failed to fetch latest release — check your network or set --version"
  [[ -n "${VERSION}" ]] || die "could not determine latest version"
fi

info "Installing devstudio-proxy ${VERSION}"

# ── Download ─────────────────────────────────────────────────────────────

DOWNLOAD_URL="https://github.com/${REPO}/releases/download/${VERSION}/${ASSET_NAME}"
CHECKSUMS_URL="https://github.com/${REPO}/releases/download/${VERSION}/checksums.txt"

TMPDIR_INSTALL="$(mktemp -d)"
trap 'rm -rf "${TMPDIR_INSTALL}"' EXIT

info "Downloading ${ASSET_NAME}..."
curl -fSL --progress-bar -o "${TMPDIR_INSTALL}/${ASSET_NAME}" "${DOWNLOAD_URL}" \
  || die "download failed — check that ${VERSION} exists and includes ${ASSET_NAME}"

# ── Verify checksum ──────────────────────────────────────────────────────

if ${VERIFY_CHECKSUM}; then
  info "Verifying SHA256 checksum..."
  curl -fsSL -o "${TMPDIR_INSTALL}/checksums.txt" "${CHECKSUMS_URL}" \
    || die "failed to download checksums.txt"

  EXPECTED="$(grep "  ${ASSET_NAME}$" "${TMPDIR_INSTALL}/checksums.txt" | awk '{print $1}')"
  [[ -n "${EXPECTED}" ]] || die "asset ${ASSET_NAME} not found in checksums.txt"

  if command -v sha256sum >/dev/null 2>&1; then
    ACTUAL="$(sha256sum "${TMPDIR_INSTALL}/${ASSET_NAME}" | awk '{print $1}')"
  elif command -v shasum >/dev/null 2>&1; then
    ACTUAL="$(shasum -a 256 "${TMPDIR_INSTALL}/${ASSET_NAME}" | awk '{print $1}')"
  else
    warn "neither sha256sum nor shasum found — skipping verification"
    ACTUAL="${EXPECTED}"
  fi

  if [[ "${ACTUAL}" != "${EXPECTED}" ]]; then
    die "checksum mismatch!\n  expected: ${EXPECTED}\n  actual:   ${ACTUAL}"
  fi
  ok "Checksum verified"
else
  warn "Skipping checksum verification (--no-verify)"
fi

# ── Install binary ───────────────────────────────────────────────────────

mkdir -p "${INSTALL_DIR}"
install -m 755 "${TMPDIR_INSTALL}/${ASSET_NAME}" "${INSTALL_DIR}/${BINARY_NAME}"
ok "Installed ${BINARY_NAME} to ${INSTALL_DIR}/${BINARY_NAME}"

# ── Write default config ────────────────────────────────────────────────

if [[ "${OS}" == "darwin" ]]; then
  CONFIG_DIR="${HOME}/Library/Application Support/devstudio-proxy"
else
  CONFIG_DIR="${XDG_CONFIG_HOME:-${HOME}/.config}/devstudio-proxy"
fi

CONFIG_FILE="${CONFIG_DIR}/config.conf"

if [[ ! -f "${CONFIG_FILE}" ]]; then
  mkdir -p "${CONFIG_DIR}"
  cat > "${CONFIG_FILE}" <<'CONF'
# devstudio-proxy configuration
# CLI flags override these settings. See: devproxy -help
#
# Priority: compiled defaults < this file < DEVPROXY_* env vars < CLI flags

# Port to listen on (default: 7700)
PORT=7700

# Enable per-request logging to stderr (default: false)
LOG=false

# Also log all request headers (default: false)
VERBOSE=false

# MCP script refresh interval, e.g. 30m, 1h, 0 to disable (default: 30m)
MCP_REFRESH=30m

# Enable remote MCP forward fallback (default: false)
MCP_FALLBACK=false
CONF
  info "Default config written to ${CONFIG_FILE}"
fi

# ── Install service (optional) ───────────────────────────────────────────

if ${INSTALL_SERVICE}; then
  DEVPROXY_BIN="${INSTALL_DIR}/${BINARY_NAME}"

  if [[ "${OS}" == "darwin" ]]; then
    # launchd user agent
    PLIST_DIR="${HOME}/Library/LaunchAgents"
    PLIST_FILE="${PLIST_DIR}/live.devstudio.proxy.plist"
    LOG_DIR="${HOME}/Library/Logs"

    mkdir -p "${PLIST_DIR}" "${LOG_DIR}"

    cat > "${PLIST_FILE}" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>live.devstudio.proxy</string>
    <key>ProgramArguments</key>
    <array>
        <string>${DEVPROXY_BIN}</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>${LOG_DIR}/devproxy.log</string>
    <key>StandardErrorPath</key>
    <string>${LOG_DIR}/devproxy.log</string>
</dict>
</plist>
PLIST

    launchctl bootout "gui/$(id -u)/live.devstudio.proxy" 2>/dev/null || true
    launchctl bootstrap "gui/$(id -u)" "${PLIST_FILE}"
    ok "launchd service installed and started (${PLIST_FILE})"
    info "Manage with:"
    info "  launchctl kickstart -k gui/$(id -u)/live.devstudio.proxy   # restart"
    info "  launchctl bootout gui/$(id -u)/live.devstudio.proxy        # stop & unload"

  elif [[ "${OS}" == "linux" ]]; then
    # systemd user unit
    UNIT_DIR="${HOME}/.config/systemd/user"
    UNIT_FILE="${UNIT_DIR}/devstudio-proxy.service"

    mkdir -p "${UNIT_DIR}"

    cat > "${UNIT_FILE}" <<UNIT
[Unit]
Description=DevStudio Proxy
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${DEVPROXY_BIN}
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
UNIT

    systemctl --user daemon-reload
    systemctl --user enable --now devstudio-proxy.service
    ok "systemd user service installed and started (${UNIT_FILE})"
    info "Manage with:"
    info "  systemctl --user status devstudio-proxy"
    info "  systemctl --user restart devstudio-proxy"
    info "  systemctl --user stop devstudio-proxy"
    info "  journalctl --user -u devstudio-proxy -f"
  fi
fi

# ── PATH check ───────────────────────────────────────────────────────────

if ! echo "${PATH}" | tr ':' '\n' | grep -qx "${INSTALL_DIR}"; then
  warn "${INSTALL_DIR} is not in your PATH"
  info "Add it to your shell profile:"
  SHELL_NAME="$(basename "${SHELL:-bash}")"
  case "${SHELL_NAME}" in
    zsh)  info "  echo 'export PATH=\"${INSTALL_DIR}:\$PATH\"' >> ~/.zshrc && source ~/.zshrc" ;;
    fish) info "  fish_add_path ${INSTALL_DIR}" ;;
    *)    info "  echo 'export PATH=\"${INSTALL_DIR}:\$PATH\"' >> ~/.bashrc && source ~/.bashrc" ;;
  esac
fi

# ── Done ─────────────────────────────────────────────────────────────────

ok "devstudio-proxy ${VERSION} installed successfully!"
info "Run:  ${BINARY_NAME} -version"
info "Docs: https://github.com/${REPO}#readme"
