class DevstudioProxy < Formula
  desc "Lightweight HTTP/HTTPS forward proxy with transparent TLS passthrough"
  homepage "https://github.com/devstudio-live/devstudio-proxy"
  version "0.38.0"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-darwin-arm64"
      sha256 "661218277715df21a9e9a9065a577015ba60f7381073563dc3618c1fdc0fb158"
    end
    on_intel do
      url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-darwin-amd64"
      sha256 "162ee9dc274885e90467501661e9ae0d30483ade5e16b9bdca4b43fee8054d5a"
    end
  end

  on_linux do
    on_arm do
      if Hardware::CPU.is_64_bit?
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-arm64"
        sha256 "4dc2bee225c6b33ee2a6fe7de436a99985be8be1ea97376fcee4613452eb647f"
      else
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-arm"
        sha256 "4dc2bee225c6b33ee2a6fe7de436a99985be8be1ea97376fcee4613452eb647f"
      end
    end
    on_intel do
      if Hardware::CPU.is_64_bit?
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-amd64"
        sha256 "1b5cf6e7367e8c2d7d9645c13a70a2bef3523dd566f4435a0e879034604ce256"
      else
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-386"
        sha256 "2890aae96ed5b5bf74c1f957df257e184e78e5253174332d44833fb857bceb93"
      end
    end
  end

  def install
    bin.install Dir["devproxy-*"].first => "devproxy"
  end

  def post_install
    # Write a default config file only if one does not already exist.
    # The `etc` helper resolves to <brew_prefix>/etc/, so this works on both
    # Apple Silicon (/opt/homebrew/etc/) and Intel (/usr/local/etc/) without
    # any conditional logic. The `unless` guard prevents clobbering user edits
    # during upgrades — same pattern used by the Homebrew nginx formula.
    conf_file = etc/"devstudio-proxy.conf"
    unless conf_file.exist?
      conf_file.write <<~EOS
        # devstudio-proxy configuration
        # CLI flags override these settings. See: devproxy -help
        #
        # Priority: compiled defaults < this file < user config file
        #   (~/.config/devstudio-proxy/config.conf or
        #    ~/Library/Application Support/devstudio-proxy/config.conf)
        # < DEVPROXY_* env vars < CLI flags

        # Port to listen on (default: 7700)
        PORT=7700

        # Enable per-request logging to stderr (default: false)
        LOG=false

        # Also log all request headers — useful for debugging gateway routing (default: false)
        # Implies LOG=true when enabled.
        VERBOSE=false

        # MCP script refresh interval, e.g. 30m, 1h, 0 to disable (default: 30m)
        MCP_REFRESH=30m

        # Enable remote MCP forward fallback (default: false)
        MCP_FALLBACK=false
      EOS
    end
  end

  service do
    # Port and other settings are read from /opt/homebrew/etc/devstudio-proxy.conf
    # (written by post_install). Edit that file to change the port without
    # modifying the formula or using `brew services edit`.
    run [opt_bin/"devproxy"]
    keep_alive true
    log_path var/"log/devproxy.log"
    error_log_path var/"log/devproxy.log"
  end

  test do
    port = free_port
    pid = fork { exec bin/"devproxy", "-port", port.to_s }
    sleep 1
    assert_match "ok", shell_output("curl -s http://localhost:#{port}/health")
  ensure
    Process.kill("TERM", pid)
  end
end
