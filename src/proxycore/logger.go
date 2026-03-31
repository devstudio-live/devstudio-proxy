package proxycore

import (
	"bufio"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

// ── Log ring buffer with SSE subscriptions ────────────────────────────────────

// LogEntry represents a single log line with an auto-incrementing ID.
type LogEntry struct {
	ID   int64  `json:"id"`
	Line string `json:"line"`
}

// LogRing is a fixed-capacity ring buffer for log lines with pub/sub support.
type LogRing struct {
	mu          sync.Mutex
	lines       []LogEntry
	pos         int
	nextID      int64
	cap         int
	subscribers map[chan string]struct{}
}

// NewLogRing creates a LogRing with the given capacity.
func NewLogRing(cap int) *LogRing {
	return &LogRing{
		lines:       make([]LogEntry, 0, cap),
		cap:         cap,
		subscribers: make(map[chan string]struct{}),
	}
}

// Push adds a line, broadcasts to subscribers.
func (r *LogRing) Push(line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := LogEntry{
		ID:   r.nextID + 1,
		Line: line,
	}
	r.nextID = entry.ID
	if len(r.lines) < r.cap {
		r.lines = append(r.lines, entry)
	} else {
		r.lines[r.pos] = entry
		r.pos = (r.pos + 1) % r.cap
	}
	for ch := range r.subscribers {
		select {
		case ch <- entry.Line:
		default:
		}
	}
}

// Snapshot returns a copy of the buffer in chronological order.
func (r *LogRing) Snapshot() []LogEntry {
	out := make([]LogEntry, len(r.lines))
	if len(r.lines) < r.cap {
		copy(out, r.lines)
		return out
	}
	copy(out, r.lines[r.pos:])
	copy(out[r.cap-r.pos:], r.lines[:r.pos])
	return out
}

// SnapshotSince returns entries with ID > since.
func (r *LogRing) SnapshotSince(since int64) []LogEntry {
	all := r.Snapshot()
	if since <= 0 {
		return all
	}
	out := make([]LogEntry, 0, len(all))
	for _, entry := range all {
		if entry.ID > since {
			out = append(out, entry)
		}
	}
	return out
}

// Subscribe registers a channel for live log lines.
func (r *LogRing) Subscribe(ch chan string) {
	r.mu.Lock()
	r.subscribers[ch] = struct{}{}
	r.mu.Unlock()
}

// Unsubscribe removes a subscriber channel.
func (r *LogRing) Unsubscribe(ch chan string) {
	r.mu.Lock()
	delete(r.subscribers, ch)
	r.mu.Unlock()
}

// Lock exposes the mutex for snapshot-under-lock patterns.
func (r *LogRing) Lock()   { r.mu.Lock() }
func (r *LogRing) Unlock() { r.mu.Unlock() }

// ── Log writer that tees to stderr + ring buffer ──────────────────────────────

type teeWriter struct {
	w   io.Writer
	buf *LogRing
}

func (tw teeWriter) Write(b []byte) (int, error) {
	n, err := tw.w.Write(b)
	if n > 0 {
		tw.buf.Push(string(b[:n]))
	}
	return n, err
}

// initLogOutput wires the log package to tee through the ring buffer.
func (s *Server) initLogOutput() {
	log.SetOutput(teeWriter{w: log.Writer(), buf: s.LogBuf})
}

// ── HTTP logging middleware ───────────────────────────────────────────────────

// LoggingResponseWriter wraps http.ResponseWriter to capture the status code
// and number of bytes written for logging purposes.
type LoggingResponseWriter struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (lrw *LoggingResponseWriter) WriteHeader(code int) {
	lrw.status = code
	lrw.ResponseWriter.WriteHeader(code)
}

func (lrw *LoggingResponseWriter) Write(b []byte) (int, error) {
	n, err := lrw.ResponseWriter.Write(b)
	lrw.bytes += int64(n)
	return n, err
}

func (lrw *LoggingResponseWriter) Flush() {
	lrw.ResponseWriter.(http.Flusher).Flush()
}

// Hijack delegates to the underlying ResponseWriter's Hijack method.
// CRITICAL: without this, CONNECT tunneling through the logging
// middleware would fail.
func (lrw *LoggingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return lrw.ResponseWriter.(http.Hijacker).Hijack()
}

type loggingMiddleware struct {
	s    *Server
	next http.Handler
}

// NewLoggingMiddleware returns an http.Handler that logs each request.
func (s *Server) NewLoggingMiddleware(next http.Handler) http.Handler {
	return &loggingMiddleware{s: s, next: next}
}

func (m *loggingMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	verbose := m.s.VerboseEnabled.Load()
	enabled := m.s.LogEnabled.Load() || verbose

	if verbose {
		log.Printf(">> %s %s", r.Method, r.URL)
		for name, values := range r.Header {
			for _, v := range values {
				log.Printf("   %s: %s", name, v)
			}
		}
	}

	start := time.Now()
	lrw := &LoggingResponseWriter{ResponseWriter: w, status: 200}
	m.next.ServeHTTP(lrw, r)

	if enabled {
		log.Printf("%s %s %s -> %d (%d bytes) in %s",
			r.RemoteAddr, r.Method, r.URL, lrw.status, lrw.bytes, time.Since(start))
	}
}
