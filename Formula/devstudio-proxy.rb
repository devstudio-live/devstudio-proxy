class DevstudioProxy < Formula
  desc "Lightweight HTTP/HTTPS forward proxy with transparent TLS passthrough"
  homepage "https://github.com/devstudio-live/devstudio-proxy"
  version "0.11.0"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-darwin-arm64"
      sha256 "24149c36df410fa050dffdec58a97755086ca9813e2d57bfdc8afab4016d7361"
    end
    on_intel do
      url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-darwin-amd64"
      sha256 "197ab412705045cff043b48c58e6dc8fb53c7a9c51eba719b425b6b69981d2b0"
    end
  end

  on_linux do
    on_arm do
      if Hardware::CPU.is_64_bit?
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-arm64"
        sha256 "b2a978c6a5a9c71a4e94b945472ad27b5c7a64ec327ffd34e3601aac72726282"
      else
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-arm"
        sha256 "b2a978c6a5a9c71a4e94b945472ad27b5c7a64ec327ffd34e3601aac72726282"
      end
    end
    on_intel do
      if Hardware::CPU.is_64_bit?
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-amd64"
        sha256 "28859906ad79813facabb5d6fdf65a3083109f149f923570fcc4ad82870e29e9"
      else
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-386"
        sha256 "964ad4f135f64857ccd1548af7835731dd1d2ba31483686677066c7dbee06bb7"
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
