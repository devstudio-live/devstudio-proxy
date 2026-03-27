package main

import (
	"bufio"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// logEnabled and verboseEnabled are toggled at runtime by /admin/config.
var logEnabled atomic.Bool
var verboseEnabled atomic.Bool

// ── Log ring buffer with SSE subscriptions ────────────────────────────────────

const logBufCap = 200

type logRing struct {
	mu          sync.Mutex
	lines       []string
	pos         int
	subscribers map[chan string]struct{}
}

var logBuf = &logRing{
	lines:       make([]string, 0, logBufCap),
	subscribers: make(map[chan string]struct{}),
}

func (r *logRing) push(line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.lines) < logBufCap {
		r.lines = append(r.lines, line)
	} else {
		r.lines[r.pos] = line
		r.pos = (r.pos + 1) % logBufCap
	}
	for ch := range r.subscribers {
		select {
		case ch <- line:
		default:
		}
	}
}

func (r *logRing) snapshot() []string {
	out := make([]string, len(r.lines))
	if len(r.lines) < logBufCap {
		copy(out, r.lines)
		return out
	}
	// Wrap-around: pos is the oldest entry
	copy(out, r.lines[r.pos:])
	copy(out[logBufCap-r.pos:], r.lines[:r.pos])
	return out
}

func (r *logRing) subscribe(ch chan string) {
	r.mu.Lock()
	r.subscribers[ch] = struct{}{}
	r.mu.Unlock()
}

func (r *logRing) unsubscribe(ch chan string) {
	r.mu.Lock()
	delete(r.subscribers, ch)
	r.mu.Unlock()
}

// ── Log writer that tees to stderr + ring buffer ──────────────────────────────

type teeWriter struct {
	w io.Writer
}

func (tw teeWriter) Write(b []byte) (int, error) {
	n, err := tw.w.Write(b)
	if n > 0 {
		logBuf.push(string(b[:n]))
	}
	return n, err
}

// initLogOutput wires the log package to tee through our ring buffer.
func initLogOutput() {
	log.SetOutput(teeWriter{w: log.Writer()})
}

// ── HTTP logging middleware ───────────────────────────────────────────────────

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

// loggingMiddleware wraps an http.Handler to log each request using the
// atomic logEnabled / verboseEnabled flags so they can be toggled at runtime.
type loggingMiddleware struct {
	next http.Handler
}

// NewLoggingMiddleware returns an http.Handler that logs each request after
// it completes, including method, URL, status, bytes, and elapsed time.
func NewLoggingMiddleware(next http.Handler) http.Handler {
	return &loggingMiddleware{next: next}
}

// ServeHTTP logs request details after the underlying handler completes.
func (m *loggingMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	verbose := verboseEnabled.Load()
	enabled := logEnabled.Load() || verbose

	if verbose {
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

	if enabled {
		log.Printf("%s %s %s -> %d (%d bytes) in %s",
			r.RemoteAddr, r.Method, r.URL, lrw.status, lrw.bytes, time.Since(start))
	}
}
