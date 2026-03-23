class DevstudioProxy < Formula
  desc "Lightweight HTTP/HTTPS forward proxy with transparent TLS passthrough"
  homepage "https://github.com/devstudio-live/devstudio-proxy"
  version "0.13.0"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-darwin-arm64"
      sha256 "db4e73bb84a01f40dd931f0d1e2fef3571511f3b56a8a7ea7edd10361db7e5a0"
    end
    on_intel do
      url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-darwin-amd64"
      sha256 "8fb27eecee160884e6599899cf726fe8a87fb579cdb1b613a8425a289ed42813"
    end
  end

  on_linux do
    on_arm do
      if Hardware::CPU.is_64_bit?
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-arm64"
        sha256 "8e642f78345027d4e4b3559604a16bcc279ca3833915a66b79f5fb01a5a77087"
      else
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-arm"
        sha256 "8e642f78345027d4e4b3559604a16bcc279ca3833915a66b79f5fb01a5a77087"
      end
    end
    on_intel do
      if Hardware::CPU.is_64_bit?
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-amd64"
        sha256 "8b0bf1bb90f906c917835bac7cd9dd35766b25043d19f4847cb76e98191f0a7c"
      else
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-386"
        sha256 "47c9051cab092c2b1224e3b829093fac207e2e8c18264925d9ddc15d221e7401"
      end
    end
  end

  def install
    bin.install Dir["devproxy-*"].first => "devproxy"
  end

  service do
    run [opt_bin/"devproxy", "-port", "7700"]
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
