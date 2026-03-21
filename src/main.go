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

	go startPoolReaper()

	fmt.Fprintf(os.Stderr, "Proxy listening on :%d\n", *port)
	log.Fatal(server.ListenAndServe())
}
