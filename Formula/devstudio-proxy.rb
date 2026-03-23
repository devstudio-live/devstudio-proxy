class DevstudioProxy < Formula
  desc "Lightweight HTTP/HTTPS forward proxy with transparent TLS passthrough"
  homepage "https://github.com/devstudio-live/devstudio-proxy"
  version "0.12.0"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-darwin-arm64"
      sha256 "3bf0da3d082dcadd80cb42c36a9a653b1957c67b7de9327ac33041cf3f3f05fe"
    end
    on_intel do
      url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-darwin-amd64"
      sha256 "cacd1e1752b4741dc9bc23381109be39b83af6ef22a3d4b211a8ea0ad272f808"
    end
  end

  on_linux do
    on_arm do
      if Hardware::CPU.is_64_bit?
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-arm64"
        sha256 "9c7f5f4895352d9ddf70b2cc269aa9937a2c21e99deee62282d8c999e44e46e6"
      else
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-arm"
        sha256 "9c7f5f4895352d9ddf70b2cc269aa9937a2c21e99deee62282d8c999e44e46e6"
      end
    end
    on_intel do
      if Hardware::CPU.is_64_bit?
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-amd64"
        sha256 "844d617b289ece34d256b7ba80fa11f1ebd308c4f135ab32cfac21bf7973342f"
      else
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-386"
        sha256 "2cfa480b369e9944b6918d55a7f3da9a2a8c5cd3a854d8b21efc2fc04c23f9e3"
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
