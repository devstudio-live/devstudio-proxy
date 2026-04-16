package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"devstudio/proxy/proxycore"
)

// isFlagSet returns true if the named flag was explicitly provided on the CLI.
func isFlagSet(name string) bool {
	found := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

func main() {
	cfg, err := proxycore.LoadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	port := flag.Int("port", cfg.Port, "port to listen on")
	enableLog := flag.Bool("log", cfg.Log, "enable request logging")
	verbose := flag.Bool("verbose", cfg.Verbose, "log request headers (implies -log)")
	showVersion := flag.Bool("version", false, "print version and exit")
	mcpRefresh := flag.Duration("mcp-refresh", cfg.MCPRefresh, "MCP script refresh interval (0 to disable)")
	mcpFallback := flag.Bool("mcp-fallback", cfg.MCPFallback, "enable remote MCP forward fallback")
	httpsFlag := flag.Bool("https", cfg.HTTPS, "enable HTTPS/TLS (default: off)")
	flag.Parse()

	if *showVersion {
		fmt.Println(proxycore.Version)
		os.Exit(0)
	}

	srv := proxycore.NewServer(proxycore.Options{
		Port:        *port,
		MCPRefresh:  *mcpRefresh,
		MCPFallback: *mcpFallback,
	})

	srv.LogEnabled.Store(*enableLog || *verbose)
	srv.VerboseEnabled.Store(*verbose)

	// If --https was explicitly passed on the CLI, persist it.
	if isFlagSet("https") {
		_ = proxycore.PersistConfigKey("HTTPS", fmt.Sprintf("%t", *httpsFlag))
	}

	srv.Start(*mcpRefresh)

	// Non-blocking version check on startup — populates the update cache.
	go proxycore.BackgroundUpdateCheck()

	// ── Restart loop ──────────────────────────────────────────────────────────
	for {
		loopCfg, _ := proxycore.LoadConfig()
		httpsWanted := loopCfg.HTTPS

		var tlsCfg *tls.Config
		srv.TLSAvailable = false
		srv.TLSCAPath = ""

		if httpsWanted {
			certPEM, keyPEM, caPath, fresh, certErr := proxycore.EnsureLocalCert()
			if certErr != nil {
				log.Printf("proxy: TLS cert unavailable (%v) — HTTPS disabled", certErr)
			} else {
				srv.TLSCAPath = caPath
				if fresh {
					go func() { _ = proxycore.InstallCATrust(caPath) }()
				}
				tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
				if err != nil {
					log.Printf("proxy: TLS key pair error (%v) — HTTPS disabled", err)
				} else {
					tlsCfg = &tls.Config{Certificates: []tls.Certificate{tlsCert}, MinVersion: tls.VersionTLS12}
					srv.TLSAvailable = true
				}
			}
		}

		log.Printf("proxy: listening on :%d (log=%t verbose=%t tls=%t)", srv.AdminPort, srv.LogEnabled.Load(), srv.VerboseEnabled.Load(), srv.TLSAvailable)
		srv.AdminServer = &http.Server{
			Addr:              fmt.Sprintf(":%d", srv.AdminPort),
			Handler:           srv.Handler,
			ReadHeaderTimeout: 5 * time.Second,
		}
		ln, err := proxycore.NewDualListener(fmt.Sprintf(":%d", srv.AdminPort), tlsCfg)
		if err != nil {
			log.Fatalf("listener error: %v", err)
		}
		if err := srv.AdminServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}
}
