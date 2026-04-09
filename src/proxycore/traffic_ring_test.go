package proxycore

import (
	"net/http"
	"net/http/httptest"
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
