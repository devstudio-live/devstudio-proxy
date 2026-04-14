package proxycore

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const trafficBodyCap = 2048 // bytes captured per side when verbose

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

// Len returns the current number of entries in the buffer.
func (r *LogRing) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.lines)
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
	status    int
	bytes     int64
	captureOn bool
	capBuf    bytes.Buffer
	capTrunc  bool
}

func (lrw *LoggingResponseWriter) WriteHeader(code int) {
	lrw.status = code
	lrw.ResponseWriter.WriteHeader(code)
}

func (lrw *LoggingResponseWriter) Write(b []byte) (int, error) {
	n, err := lrw.ResponseWriter.Write(b)
	lrw.bytes += int64(n)
	if lrw.captureOn && n > 0 {
		remaining := trafficBodyCap - lrw.capBuf.Len()
		if remaining > 0 {
			if n <= remaining {
				lrw.capBuf.Write(b[:n])
			} else {
				lrw.capBuf.Write(b[:remaining])
				lrw.capTrunc = true
			}
		} else {
			lrw.capTrunc = true
		}
	}
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

// bodyCaptureReader tees up to trafficBodyCap bytes into cap while passing
// through to the underlying reader unchanged.
type bodyCaptureReader struct {
	r      io.ReadCloser
	cap    *bytes.Buffer
	trunc  *bool
	remain int
}

func (b *bodyCaptureReader) Read(p []byte) (int, error) {
	n, err := b.r.Read(p)
	if n > 0 && b.remain > 0 {
		take := n
		if take > b.remain {
			take = b.remain
			*b.trunc = true
		}
		b.cap.Write(p[:take])
		b.remain -= take
		if b.remain == 0 && n > take {
			*b.trunc = true
		}
	} else if n > 0 && b.remain == 0 {
		*b.trunc = true
	}
	return n, err
}

func (b *bodyCaptureReader) Close() error { return b.r.Close() }

// isWebSocketUpgrade returns true when the request is a WebSocket upgrade.
func isWebSocketUpgrade(r *http.Request) bool {
	if !strings.EqualFold(r.Header.Get("Connection"), "upgrade") &&
		!strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") {
		return false
	}
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

// sanitizeBodyPreview returns a UTF-8-safe preview, replacing control bytes
// so the JSON frame stays valid and the UI can display it.
func sanitizeBodyPreview(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	// Replace NUL and other invalid control bytes with a placeholder char.
	out := make([]byte, 0, len(b))
	for _, c := range b {
		if c == '\t' || c == '\n' || c == '\r' || (c >= 0x20 && c < 0x7f) || c >= 0x80 {
			out = append(out, c)
		} else {
			out = append(out, '.')
		}
	}
	return string(out)
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

	// Detect gateway metadata early — gateway handlers strip these headers
	// for defense-in-depth before the middleware would otherwise read them.
	operation := r.Header.Get("X-DevStudio-Gateway-Operation")
	target := r.Header.Get("X-DevStudio-Gateway-Target")
	protocol := detectProtocol(r)
	wsUpgrade := isWebSocketUpgrade(r)

	// Request body capture (verbose only, never for WS which hijacks).
	var reqCap *bytes.Buffer
	var reqTrunc bool
	if verbose && !wsUpgrade && r.Body != nil && r.ContentLength != 0 {
		reqCap = &bytes.Buffer{}
		r.Body = &bodyCaptureReader{
			r:      r.Body,
			cap:    reqCap,
			trunc:  &reqTrunc,
			remain: trafficBodyCap,
		}
	}

	start := time.Now()
	lrw := &LoggingResponseWriter{ResponseWriter: w, status: 200}
	if verbose && !wsUpgrade {
		lrw.captureOn = true
	}
	m.next.ServeHTTP(lrw, r)
	duration := time.Since(start)

	if enabled {
		log.Printf("%s %s %s -> %d (%d bytes) in %s",
			r.RemoteAddr, r.Method, r.URL, lrw.status, lrw.bytes, duration)
	}

	// Always capture traffic (independent of log/verbose toggle).
	if m.s.TrafficBuf != nil && !isAdminPath(r.URL.Path) && r.URL.Path != "/health" {
		rec := TrafficRecord{
			Timestamp:  start.UnixMilli(),
			Method:     r.Method,
			Path:       r.URL.Path,
			Protocol:   protocol,
			Operation:  operation,
			Target:     target,
			RemoteAddr: r.RemoteAddr,
			Status:     lrw.status,
			DurationUS: duration.Microseconds(),
			ReqSize:    r.ContentLength,
			RespSize:   lrw.bytes,
			WSUpgrade:  wsUpgrade,
		}
		if verbose {
			rec.ReqHeaders = flattenHeaders(r.Header)
			rec.RespHeaders = flattenHeaders(lrw.Header())
			if reqCap != nil && reqCap.Len() > 0 {
				rec.ReqBody = sanitizeBodyPreview(reqCap.Bytes())
				rec.ReqBodyTrunc = reqTrunc
			}
			if lrw.capBuf.Len() > 0 {
				rec.RespBody = sanitizeBodyPreview(lrw.capBuf.Bytes())
				rec.RespBodyTrunc = lrw.capTrunc
			}
		}
		m.s.TrafficBuf.Push(rec)
	}
}
