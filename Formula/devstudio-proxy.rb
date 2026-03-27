class DevstudioProxy < Formula
  desc "Lightweight HTTP/HTTPS forward proxy with transparent TLS passthrough"
  homepage "https://github.com/devstudio-live/devstudio-proxy"
  version "0.20.0"
  license "MIT"

  on_macos do
    on_arm do
      url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-darwin-arm64"
      sha256 "44b93ef089430d365681ae4766f59b6318827846c1d9a5ee9c20c988f450210a"
    end
    on_intel do
      url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-darwin-amd64"
      sha256 "2b2f2b09cbe7789cfcb41d06d1422b7300ec00bda41023dd24a2e71918a48fdf"
    end
  end

  on_linux do
    on_arm do
      if Hardware::CPU.is_64_bit?
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-arm64"
        sha256 "28d4047e31dd516aa0fadf0b50a9a08203faea8f28a435203bf8ed046e425f71"
      else
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-arm"
        sha256 "28d4047e31dd516aa0fadf0b50a9a08203faea8f28a435203bf8ed046e425f71"
      end
    end
    on_intel do
      if Hardware::CPU.is_64_bit?
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-amd64"
        sha256 "92298e4e2c8e204a1f50a1d0e766a5d8da1750348d935a9013adc1ada0db3e06"
      else
        url "https://github.com/devstudio-live/devstudio-proxy/releases/download/v#{version}/devproxy-linux-386"
        sha256 "11162333173eef858f419a911eae22a928243b0f033438da43475140dec66c11"
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
