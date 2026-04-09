package proxycore

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newAdminTestServer() *Server {
	s := &Server{
		AdminPort:       17700,
		ServerStartTime: time.Now(),
		LogBuf:          NewLogRing(200),
		EventBuf:        NewEventRing(100),
		TrafficBuf:      NewTrafficRing(500),
		hprofSessions:   NewHprofSessionStore(),
	}
	s.Handler = s.NewLoggingMiddleware(&handler{s: s})
	return s
}

func TestAdminTraffic_JSON(t *testing.T) {
	s := newAdminTestServer()

	s.TrafficBuf.Push(TrafficRecord{
		Timestamp: time.Now().UnixMilli(),
		Method:    "POST",
		Path:      "/list",
		Protocol:  "fs",
		Status:    200,
	})

	req := httptest.NewRequest("GET", "/admin/traffic?format=json&since=0", nil)
	rw := httptest.NewRecorder()
	s.handleAdmin(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	var body struct {
		Records []TrafficRecord `json:"records"`
	}
	if err := json.NewDecoder(rw.Body).Decode(&body); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(body.Records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(body.Records))
	}
	if body.Records[0].Protocol != "fs" {
		t.Errorf("expected protocol fs, got %q", body.Records[0].Protocol)
	}
}

func TestAdminTraffic_SSE(t *testing.T) {
	s := newAdminTestServer()

	s.TrafficBuf.Push(TrafficRecord{
		Timestamp: time.Now().UnixMilli(),
		Method:    "POST",
		Path:      "/list",
		Protocol:  "fs",
		Status:    200,
	})

	pr, pw := newSSEPipe()
	defer pr.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		req, _ := http.NewRequest("GET", "/admin/traffic", nil)
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		req = req.WithContext(ctx)
		s.handleAdmin(pw, req)
	}()

	scanner := bufio.NewScanner(pr)
	var eventLine, dataLine string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "event:") {
			eventLine = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		}
		if strings.HasPrefix(line, "data:") {
			dataLine = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			break
		}
	}

	<-done

	if eventLine != "traffic" {
		t.Errorf("expected event type 'traffic', got %q", eventLine)
	}
	var rec TrafficRecord
	if err := json.Unmarshal([]byte(dataLine), &rec); err != nil {
		t.Fatalf("unmarshal SSE data: %v", err)
	}
	if rec.Protocol != "fs" {
		t.Errorf("expected protocol fs, got %q", rec.Protocol)
	}
}

func TestAdminInternals(t *testing.T) {
	s := newAdminTestServer()
	s.initLogOutput()

	req := httptest.NewRequest("GET", "/admin/internals", nil)
	rw := httptest.NewRecorder()
	s.handleAdmin(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}

	var body map[string]any
	if err := json.NewDecoder(rw.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	for _, key := range []string{"uptime_seconds", "goroutines", "memory", "pools", "context_cache", "hprof", "mcp", "buffers"} {
		if _, ok := body[key]; !ok {
			t.Errorf("missing key %q in internals response", key)
		}
	}

	pools, ok := body["pools"].(map[string]any)
	if !ok {
		t.Fatal("pools is not an object")
	}
	for _, proto := range []string{"sql", "mongo", "redis", "elastic", "k8s"} {
		if _, ok := pools[proto]; !ok {
			t.Errorf("missing pool %q", proto)
		}
	}
}

func TestAdminConfig_VerboseTTL(t *testing.T) {
	s := newAdminTestServer()

	body := `{"verbose":true,"verbose_ttl_seconds":1}`
	req := httptest.NewRequest("POST", "/admin/config", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rw := httptest.NewRecorder()
	s.handleAdmin(rw, req)

	if !s.VerboseEnabled.Load() {
		t.Fatal("expected verbose to be enabled immediately")
	}

	time.Sleep(1500 * time.Millisecond)

	if s.VerboseEnabled.Load() {
		t.Error("expected verbose to auto-revert after TTL")
	}
}

// ── SSE pipe helpers ─────────────────────────────────────────────────────────

type sseResponseWriter struct {
	pw     *io.PipeWriter
	header http.Header
	code   int
}

func newSSEPipe() (*io.PipeReader, *sseResponseWriter) {
	pr, pw := io.Pipe()
	return pr, &sseResponseWriter{pw: pw, header: make(http.Header), code: 200}
}

func (s *sseResponseWriter) Header() http.Header         { return s.header }
func (s *sseResponseWriter) WriteHeader(code int)        { s.code = code }
func (s *sseResponseWriter) Write(b []byte) (int, error) { return s.pw.Write(b) }
func (s *sseResponseWriter) Flush()                      {}
func (s *sseResponseWriter) Close()                      { s.pw.Close() }
