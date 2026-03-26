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
	port := flag.Int("port", 7700, "port to listen on")
	enableLog := flag.Bool("log", false, "enable request logging")
	showVersion := flag.Bool("version", false, "print version and exit")
	mcpRefresh := flag.Duration("mcp-refresh", 30*time.Minute, "MCP script refresh interval (0 to disable)")
	mcpFallback := flag.Bool("mcp-fallback", false, "enable remote MCP forward fallback")
	flag.Parse()

	if *showVersion {
		fmt.Println(Version)
		os.Exit(0)
	}

	var handler http.Handler = Handler{}
	if *enableLog {
		handler = NewLoggingMiddleware(handler)
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
