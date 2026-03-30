package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

// adminPort and adminServer are package-level so admin.go can read the port
// and trigger a restart via server.Close().
var adminPort int
var adminServer *http.Server
var tlsAvailable bool
var tlsCAPath string // path to ca.crt; set once before the restart loop

func main() {
	// Load layered config: defaults → system config file → user config file → env vars.
	// The resolved values become flag defaults, so CLI flags still override everything.
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	port := flag.Int("port", cfg.Port, "port to listen on")
	enableLog := flag.Bool("log", cfg.Log, "enable request logging")
	verbose := flag.Bool("verbose", cfg.Verbose, "log request headers (implies -log)")
	showVersion := flag.Bool("version", false, "print version and exit")
	mcpRefresh := flag.Duration("mcp-refresh", cfg.MCPRefresh, "MCP script refresh interval (0 to disable)")
	mcpFallback := flag.Bool("mcp-fallback", cfg.MCPFallback, "enable remote MCP forward fallback")
	flag.Parse()

	if *showVersion {
		fmt.Println(Version)
		os.Exit(0)
	}

	// Wire atomic bools from CLI flags so admin endpoints can toggle them at runtime.
	logEnabled.Store(*enableLog || *verbose)
	verboseEnabled.Store(*verbose)

	// Wire log output through the ring buffer so /admin/logs can stream it.
	initLogOutput()

	adminPort = *port
	mcpFallbackEnabled = *mcpFallback
	initMCPRuntime(*mcpRefresh)

	go startPoolReaper()
	go startMongoPoolReaper()
	go startElasticPoolReaper()
	go startRedisPoolReaper()
	go startContextReaper()
	go startHealthTicker()

	handler := NewLoggingMiddleware(Handler{})

	// ── One-time TLS setup (before loop) ─────────────────────────────────────
	var tlsCfg *tls.Config
	certPEM, keyPEM, caPath, fresh, certErr := ensureLocalCert()
	if certErr != nil {
		log.Printf("proxy: TLS cert unavailable (%v) — HTTPS disabled", certErr)
	} else {
		tlsCAPath = caPath
		if fresh {
			go func() { _ = installCATrust(caPath) }()
		}
		tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			log.Printf("proxy: TLS key pair error (%v) — HTTPS disabled", err)
		} else {
			tlsCfg = &tls.Config{Certificates: []tls.Certificate{tlsCert}, MinVersion: tls.VersionTLS12}
			tlsAvailable = true
		}
	}

	// ── Restart loop ──────────────────────────────────────────────────────────
	// Gotcha #1: newDualListener is called inside the loop — fresh listener on
	// every iteration; a closed net.Listener cannot be reused.
	for {
		log.Printf("proxy: listening on :%d (log=%t verbose=%t tls=%t)", adminPort, logEnabled.Load(), verboseEnabled.Load(), tlsAvailable)
		adminServer = &http.Server{
			Addr:              fmt.Sprintf(":%d", adminPort),
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
		}
		ln, err := newDualListener(fmt.Sprintf(":%d", adminPort), tlsCfg)
		if err != nil {
			log.Fatalf("listener error: %v", err)
		}
		if err := adminServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}
}
