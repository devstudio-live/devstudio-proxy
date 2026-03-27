package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"
)

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

	var handler http.Handler = Handler{}
	if *enableLog || *verbose {
		handler = NewLoggingMiddleware(handler, *verbose)
	}

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", *port),
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	mcpFallbackEnabled = *mcpFallback
	initMCPRuntime(*mcpRefresh)

	go startPoolReaper()
	go startMongoPoolReaper()
	go startElasticPoolReaper()
	go startRedisPoolReaper()
	go startContextReaper()

	fmt.Fprintf(os.Stderr, "Proxy listening on :%d\n", *port)
	log.Fatal(server.ListenAndServe())
}
