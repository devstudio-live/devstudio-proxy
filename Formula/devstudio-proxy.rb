class DevstudioProxy < Formula
  desc "Lightweight HTTP/HTTPS forward proxy with transparent TLS passthrough"
  homepage "https://github.com/devstudio-live/devstudio-proxy"
  version "0.36.0"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-darwin-arm64"
      sha256 "5c1ff7a357279ad1103485718f398d6b51e4bb931e8e96f651fd4a79ed46583c"
    end
    on_intel do
      url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-darwin-amd64"
      sha256 "e338143a1d46edb645299941013096a97eed463a13ce1fdaacc11a7708435555"
    end
  end

  on_linux do
    on_arm do
      if Hardware::CPU.is_64_bit?
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-arm64"
        sha256 "3fa069b48f6fe43330cd871a1e0ea0c0d64612375f153242234e30fe069d71e2"
      else
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-arm"
        sha256 "3fa069b48f6fe43330cd871a1e0ea0c0d64612375f153242234e30fe069d71e2"
      end
    end
    on_intel do
      if Hardware::CPU.is_64_bit?
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-amd64"
        sha256 "9705440e5fbea0a89d9ce875db92df8657bae2b683338b78da38b0d1c3509742"
      else
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-386"
        sha256 "cead426500ea7947068cc13564e7a1f998562e590dbee573c638524227f7ca04"
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
