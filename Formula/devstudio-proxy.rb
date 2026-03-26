class DevstudioProxy < Formula
  desc "Lightweight HTTP/HTTPS forward proxy with transparent TLS passthrough"
  homepage "https://github.com/devstudio-live/devstudio-proxy"
  version "0.18.0"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-darwin-arm64"
      sha256 "cd37ad023860e0606aa57c757cbb127007f5a5c04ed3c3840d8d133b734c927e"
    end
    on_intel do
      url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-darwin-amd64"
      sha256 "8e6cf8a4dd7347bb520816cb8bfeb11e5bf0282c9d604e9f5434691f4e240099"
    end
  end

  on_linux do
    on_arm do
      if Hardware::CPU.is_64_bit?
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-arm64"
        sha256 "eeb105ed9acf6867e048357bcf2dd6c440b81a1d3f19a3a42bca17719a6a319f"
      else
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-arm"
        sha256 "eeb105ed9acf6867e048357bcf2dd6c440b81a1d3f19a3a42bca17719a6a319f"
      end
    end
    on_intel do
      if Hardware::CPU.is_64_bit?
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-amd64"
        sha256 "ab7af1cc67dd9a0f9530b8cafa08cfa060bb5169de7451c4aa500e5b2e62dd2a"
      else
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-386"
        sha256 "696787f1c23a0e8d40f840d5fb92295f47167fd6db7f1c2ccb78b2986a5a9971"
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
