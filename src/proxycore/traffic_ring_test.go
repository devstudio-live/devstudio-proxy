package proxycore

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTrafficRing_PushAndSnapshot(t *testing.T) {
	r := NewTrafficRing(10)
	for i := 0; i < 5; i++ {
		r.Push(TrafficRecord{Method: "GET", Path: "/foo", Status: 200})
	}
	snap := r.Snapshot()
	if len(snap) != 5 {
		t.Fatalf("expected 5 records, got %d", len(snap))
	}
	for i, rec := range snap {
		if rec.ID != int64(i+1) {
			t.Errorf("record %d: expected ID %d, got %d", i, i+1, rec.ID)
		}
	}
}

func TestTrafficRing_Wraparound(t *testing.T) {
	cap := 10
	r := NewTrafficRing(cap)
	total := cap + 5
	for i := 0; i < total; i++ {
		r.Push(TrafficRecord{Method: "GET", Path: "/foo", Status: 200})
	}
	snap := r.Snapshot()
	if len(snap) != cap {
		t.Fatalf("expected %d records after wraparound, got %d", cap, len(snap))
	}
	// Oldest should be evicted; first remaining ID should be total-cap+1.
	expectedFirstID := int64(total - cap + 1)
	if snap[0].ID != expectedFirstID {
		t.Errorf("expected first ID %d after wraparound, got %d", expectedFirstID, snap[0].ID)
	}
}

func TestTrafficRing_SnapshotSince(t *testing.T) {
	r := NewTrafficRing(20)
	for i := 0; i < 10; i++ {
		r.Push(TrafficRecord{Method: "GET", Path: "/foo", Status: 200})
	}
	snap := r.SnapshotSince(5)
	if len(snap) != 5 {
		t.Fatalf("expected 5 records since ID 5, got %d", len(snap))
	}
	if snap[0].ID != 6 {
		t.Errorf("expected first ID 6, got %d", snap[0].ID)
	}
}

func TestTrafficRing_Subscribe(t *testing.T) {
	r := NewTrafficRing(10)
	ch := make(chan TrafficRecord, 1)
	r.Subscribe(ch)
	defer r.Unsubscribe(ch)

	r.Push(TrafficRecord{Method: "POST", Path: "/bar", Status: 201})

	select {
	case rec := <-ch:
		if rec.Method != "POST" || rec.Path != "/bar" || rec.Status != 201 {
			t.Errorf("unexpected record: %+v", rec)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for record on subscriber channel")
	}
}

func TestTrafficRing_NonBlockingSend(t *testing.T) {
	r := NewTrafficRing(10)
	// Unbuffered channel — subscriber is not reading.
	ch := make(chan TrafficRecord)
	r.Subscribe(ch)
	defer r.Unsubscribe(ch)

	// Must not deadlock.
	done := make(chan struct{})
	go func() {
		r.Push(TrafficRecord{Method: "GET", Path: "/nb", Status: 200})
		close(done)
	}()

	select {
	case <-done:
		// success
	case <-time.After(time.Second):
		t.Fatal("Push blocked on non-reading subscriber (deadlock)")
	}
}

func TestDetectProtocol(t *testing.T) {
	tests := []struct {
		name     string
		method   string
		path     string
		header   string
		expected string
	}{
		{"header sql", "POST", "/query", "sql", "sql"},
		{"header mongo", "POST", "/query", "mongo", "mongo"},
		{"header k8s", "POST", "/query", "k8s", "k8s"},
		{"header redis", "POST", "/query", "redis", "redis"},
		{"header elastic", "POST", "/query", "elastic", "elastic"},
		{"header fs", "POST", "/list", "fs", "fs"},
		{"header hprof", "POST", "/hprof", "hprof", "hprof"},
		{"header mcp", "POST", "/mcp", "mcp", "mcp"},
		{"path health", "GET", "/health", "", "health"},
		{"path admin", "GET", "/admin/config", "", "admin"},
		{"path mcp no header", "POST", "/mcp/tools", "", "mcp"},
		{"path k8s exec", "GET", "/k8s/exec/shell", "", "k8s"},
		{"method CONNECT", "CONNECT", "/example.com:443", "", "tunnel"},
		{"default forward", "GET", "/some/path", "", "forward"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			if tt.header != "" {
				req.Header.Set("X-DevStudio-Gateway-Protocol", tt.header)
			}
			got := detectProtocol(req)
			if got != tt.expected {
				t.Errorf("detectProtocol(%q %q header=%q) = %q, want %q",
					tt.method, tt.path, tt.header, got, tt.expected)
			}
		})
	}
}

// ── Logger middleware traffic capture tests ──────────────────────────────────

func newBareServer() *Server {
	return &Server{
		AdminPort:       17700,
		ServerStartTime: time.Now(),
		LogBuf:          NewLogRing(200),
		EventBuf:        NewEventRing(100),
		TrafficBuf:      NewTrafficRing(500),
		hprofSessions:   NewHprofSessionStore(),
	}
}

func TestLoggingMiddleware_PushesTrafficRecord(t *testing.T) {
	s := newBareServer()
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})
	h := s.NewLoggingMiddleware(inner)

	req := httptest.NewRequest("GET", "/some/path", nil)
	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	snap := s.TrafficBuf.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 traffic record, got %d", len(snap))
	}
	rec := snap[0]
	if rec.Method != "GET" {
		t.Errorf("method: got %q, want GET", rec.Method)
	}
	if rec.Path != "/some/path" {
		t.Errorf("path: got %q, want /some/path", rec.Path)
	}
	if rec.Status != 200 {
		t.Errorf("status: got %d, want 200", rec.Status)
	}
}

func TestLoggingMiddleware_SkipsAdminPaths(t *testing.T) {
	s := newBareServer()
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	h := s.NewLoggingMiddleware(inner)

	req := httptest.NewRequest("GET", "/admin/config", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	if s.TrafficBuf.Len() != 0 {
		t.Errorf("expected TrafficBuf empty for /admin/ path, got %d records", s.TrafficBuf.Len())
	}
}

func TestLoggingMiddleware_SkipsHealthPath(t *testing.T) {
	s := newBareServer()
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	h := s.NewLoggingMiddleware(inner)

	req := httptest.NewRequest("GET", "/health", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	if s.TrafficBuf.Len() != 0 {
		t.Errorf("expected TrafficBuf empty for /health path, got %d records", s.TrafficBuf.Len())
	}
}

func TestLoggingMiddleware_CapturesHeadersInVerbose(t *testing.T) {
	s := newBareServer()
	s.VerboseEnabled.Store(true)
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	h := s.NewLoggingMiddleware(inner)

	req := httptest.NewRequest("GET", "/some/path", nil)
	req.Header.Set("X-Custom", "value")
	h.ServeHTTP(httptest.NewRecorder(), req)

	snap := s.TrafficBuf.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 record, got %d", len(snap))
	}
	if snap[0].ReqHeaders == nil {
		t.Error("expected non-nil ReqHeaders in verbose mode")
	}
}

func TestLoggingMiddleware_OmitsHeadersWhenNotVerbose(t *testing.T) {
	s := newBareServer()
	s.VerboseEnabled.Store(false)
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	h := s.NewLoggingMiddleware(inner)

	req := httptest.NewRequest("GET", "/some/path", nil)
	req.Header.Set("X-Custom", "value")
	h.ServeHTTP(httptest.NewRecorder(), req)

	snap := s.TrafficBuf.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 record, got %d", len(snap))
	}
	if snap[0].ReqHeaders != nil {
		t.Error("expected nil ReqHeaders when verbose is disabled")
	}
}

func TestLoggingMiddleware_CapturesRemoteAddrAndOperation(t *testing.T) {
	s := newBareServer()
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	h := s.NewLoggingMiddleware(inner)

	req := httptest.NewRequest("POST", "/query", nil)
	req.Header.Set("X-DevStudio-Gateway-Operation", "select")
	req.Header.Set("X-DevStudio-Gateway-Target", "sqlite-dev")
	req.RemoteAddr = "10.0.0.5:54321"
	h.ServeHTTP(httptest.NewRecorder(), req)

	snap := s.TrafficBuf.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 record, got %d", len(snap))
	}
	rec := snap[0]
	if rec.Operation != "select" {
		t.Errorf("operation: got %q, want select", rec.Operation)
	}
	if rec.Target != "sqlite-dev" {
		t.Errorf("target: got %q, want sqlite-dev", rec.Target)
	}
	if rec.RemoteAddr != "10.0.0.5:54321" {
		t.Errorf("remote_addr: got %q, want 10.0.0.5:54321", rec.RemoteAddr)
	}
}

func TestLoggingMiddleware_CapturesBodyInVerbose(t *testing.T) {
	s := newBareServer()
	s.VerboseEnabled.Store(true)
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // drain request
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"result":"ok"}`))
	})
	h := s.NewLoggingMiddleware(inner)

	body := strings.NewReader(`{"sql":"SELECT 1"}`)
	req := httptest.NewRequest("POST", "/query", body)
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(httptest.NewRecorder(), req)

	snap := s.TrafficBuf.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("expected 1 record, got %d", len(snap))
	}
	if !strings.Contains(snap[0].ReqBody, "SELECT 1") {
		t.Errorf("req_body: got %q, expected to contain SELECT 1", snap[0].ReqBody)
	}
	if !strings.Contains(snap[0].RespBody, "ok") {
		t.Errorf("resp_body: got %q, expected to contain ok", snap[0].RespBody)
	}
}

func TestLoggingMiddleware_TruncatesLargeBody(t *testing.T) {
	s := newBareServer()
	s.VerboseEnabled.Store(true)
	big := strings.Repeat("a", trafficBodyCap*2)
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(big))
	})
	h := s.NewLoggingMiddleware(inner)
	req := httptest.NewRequest("POST", "/q", strings.NewReader(big))
	h.ServeHTTP(httptest.NewRecorder(), req)

	rec := s.TrafficBuf.Snapshot()[0]
	if len(rec.RespBody) > trafficBodyCap {
		t.Errorf("resp body preview should cap at %d bytes, got %d", trafficBodyCap, len(rec.RespBody))
	}
	if !rec.RespBodyTrunc {
		t.Error("expected resp_body_trunc=true for oversize body")
	}
	if !rec.ReqBodyTrunc {
		t.Error("expected req_body_trunc=true for oversize body")
	}
}

func TestTrafficRing_Clear(t *testing.T) {
	r := NewTrafficRing(10)
	for i := 0; i < 5; i++ {
		r.Push(TrafficRecord{Method: "GET", Status: 200})
	}
	if r.Len() != 5 {
		t.Fatalf("expected 5 before clear, got %d", r.Len())
	}
	r.Clear()
	if r.Len() != 0 {
		t.Fatalf("expected 0 after clear, got %d", r.Len())
	}
	// IDs continue increasing after clear.
	r.Push(TrafficRecord{Method: "GET"})
	if r.Snapshot()[0].ID != 6 {
		t.Errorf("expected ID to keep monotonic counter after Clear: got %d, want 6", r.Snapshot()[0].ID)
	}
}

func TestTrafficRing_Stats(t *testing.T) {
	r := NewTrafficRing(100)
	now := time.Now().UnixMilli()
	r.Push(TrafficRecord{Timestamp: now - 1000, Protocol: "sql", Status: 200, DurationUS: 1000, ReqSize: 100, RespSize: 200})
	r.Push(TrafficRecord{Timestamp: now - 800, Protocol: "sql", Status: 500, DurationUS: 5000, ReqSize: 100, RespSize: 50})
	r.Push(TrafficRecord{Timestamp: now - 500, Protocol: "mongo", Status: 200, DurationUS: 2000, ReqSize: 0, RespSize: 300})
	r.Push(TrafficRecord{Timestamp: now - 200, Protocol: "k8s", Status: 404, DurationUS: 800, ReqSize: 0, RespSize: 0})

	st := r.Stats()
	if st.Count != 4 {
		t.Errorf("count: got %d, want 4", st.Count)
	}
	if st.ErrorCount != 2 {
		t.Errorf("error_count: got %d, want 2 (one 4xx + one 5xx)", st.ErrorCount)
	}
	if st.ByProtocol["sql"] != 2 || st.ByProtocol["mongo"] != 1 || st.ByProtocol["k8s"] != 1 {
		t.Errorf("by_protocol wrong: %+v", st.ByProtocol)
	}
	if st.ByStatusClass["2xx"] != 2 || st.ByStatusClass["4xx"] != 1 || st.ByStatusClass["5xx"] != 1 {
		t.Errorf("by_status_class wrong: %+v", st.ByStatusClass)
	}
	if st.AvgDurationUS != (1000+5000+2000+800)/4 {
		t.Errorf("avg_duration_us wrong: %d", st.AvgDurationUS)
	}
	if st.P95DurationUS < 2000 {
		t.Errorf("p95 should be >= 2000, got %d", st.P95DurationUS)
	}
	if st.RPS1m <= 0 {
		t.Errorf("rps_1m should be > 0, got %f", st.RPS1m)
	}
}

func TestAdminTrafficStats_ReturnsShape(t *testing.T) {
	s := newAdminTestServer()
	s.TrafficBuf.Push(TrafficRecord{
		Timestamp: time.Now().UnixMilli(),
		Protocol:  "sql",
		Status:    200,
		DurationUS: 1500,
	})
	req := httptest.NewRequest("GET", "/admin/traffic/stats", nil)
	rw := httptest.NewRecorder()
	s.handleAdmin(rw, req)
	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(rw.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, k := range []string{"count", "error_count", "avg_duration_us", "p50_duration_us", "p95_duration_us", "by_protocol", "by_status_class"} {
		if _, ok := body[k]; !ok {
			t.Errorf("missing key %q in stats response", k)
		}
	}
}

func TestAdminTrafficDelete_Clears(t *testing.T) {
	s := newAdminTestServer()
	s.TrafficBuf.Push(TrafficRecord{Protocol: "sql", Status: 200})
	s.TrafficBuf.Push(TrafficRecord{Protocol: "mongo", Status: 200})
	if s.TrafficBuf.Len() != 2 {
		t.Fatalf("expected 2 before delete, got %d", s.TrafficBuf.Len())
	}

	req := httptest.NewRequest("DELETE", "/admin/traffic", nil)
	rw := httptest.NewRecorder()
	s.handleAdmin(rw, req)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rw.Code)
	}
	if s.TrafficBuf.Len() != 0 {
		t.Errorf("expected TrafficBuf empty after DELETE, got %d", s.TrafficBuf.Len())
	}
}
