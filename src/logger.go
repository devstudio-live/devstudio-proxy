package main

import (
	"bufio"
	"log"
	"net"
	"net/http"
	"time"
)

// loggingResponseWriter wraps http.ResponseWriter to capture the status code
// and number of bytes written for logging purposes.
type loggingResponseWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

// WriteHeader captures the status code and delegates to the underlying writer.
func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.status = code
	lrw.ResponseWriter.WriteHeader(code)
}

// Write counts the bytes written and delegates to the underlying writer.
func (lrw *loggingResponseWriter) Write(b []byte) (int, error) {
	n, err := lrw.ResponseWriter.Write(b)
	lrw.bytes += int64(n)
	return n, err
}

// Hijack delegates to the underlying ResponseWriter's Hijack method.
// This is CRITICAL: without this, CONNECT tunneling through the logging
// middleware would fail because the loggingResponseWriter would not satisfy
// http.Hijacker, causing handleTunnel to return an error.
func (lrw *loggingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return lrw.ResponseWriter.(http.Hijacker).Hijack()
}

// loggingMiddleware wraps an http.Handler to log each request.
type loggingMiddleware struct {
	next    http.Handler
	verbose bool
}

// NewLoggingMiddleware returns an http.Handler that logs each request after
// it completes, including method, URL, status, bytes, and elapsed time.
// When verbose is true, request headers are also logged.
func NewLoggingMiddleware(next http.Handler, verbose bool) http.Handler {
	return &loggingMiddleware{next: next, verbose: verbose}
}

// ServeHTTP logs request details after the underlying handler completes.
func (m *loggingMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if m.verbose {
		log.Printf(">> %s %s", r.Method, r.URL)
		for name, values := range r.Header {
			for _, v := range values {
				log.Printf("   %s: %s", name, v)
			}
		}
	}
	start := time.Now()
	lrw := &loggingResponseWriter{ResponseWriter: w, status: 200}
	m.next.ServeHTTP(lrw, r)
	log.Printf("%s %s %s -> %d (%d bytes) in %s",
		r.RemoteAddr, r.Method, r.URL, lrw.status, lrw.bytes, time.Since(start))
}
