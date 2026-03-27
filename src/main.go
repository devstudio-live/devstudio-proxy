package main

import (
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

	handler := NewLoggingMiddleware(Handler{})

	fmt.Fprintf(os.Stderr, "Proxy listening on :%d\n", *port)

	// Restart loop: /admin/restart closes adminServer; we re-listen until a hard error.
	for {
		adminServer = &http.Server{
			Addr:              fmt.Sprintf(":%d", adminPort),
			Handler:           handler,
			ReadHeaderTimeout: 5 * time.Second,
		}
		if err := adminServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
		fmt.Fprintf(os.Stderr, "Proxy restarted on :%d\n", adminPort)
	}
}
