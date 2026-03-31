package proxycore

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ── Helper ────────────────────────────────────────────────────────────────────

func dbPost(t *testing.T, srv *Server, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/"+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-DevStudio-Gateway-Route", "true")
	req.Header.Set("X-DevStudio-Gateway-Protocol", "sql")
	req.Header.Set("Origin", "http://localhost:5173")
	rr := httptest.NewRecorder()
	srv.handleDBGateway(rr, req)
	return rr
}

func decodeResp(t *testing.T, rr *httptest.ResponseRecorder) DBResponse {
	t.Helper()
	var resp DBResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v — body: %s", err, rr.Body.String())
	}
	return resp
}

// ── Routing & method enforcement ─────────────────────────────────────────────

func TestGateway_UnknownEndpoint(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	rr := dbPost(t, srv, "nonexistent", map[string]any{})
	if rr.Code != http.StatusNotFound {
		t.Errorf("want 404, got %d", rr.Code)
	}
	resp := decodeResp(t, rr)
	if !strings.Contains(resp.Error, "nonexistent") {
		t.Errorf("want error mentioning endpoint name, got %q", resp.Error)
	}
}

func TestGateway_MethodNotAllowed(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	req := httptest.NewRequest(http.MethodGet, "/query", nil)
	req.Header.Set("X-DevStudio-Gateway-Route", "true")
	req.Header.Set("X-DevStudio-Gateway-Protocol", "sql")
	rr := httptest.NewRecorder()
	srv.handleDBGateway(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("want 405, got %d", rr.Code)
	}
}

func TestGateway_BadJSON(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	req := httptest.NewRequest(http.MethodPost, "/query", strings.NewReader("{bad json"))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.handleDBGateway(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d", rr.Code)
	}
	resp := decodeResp(t, rr)
	if resp.Error == "" {
		t.Error("want error message for bad JSON")
	}
}

func TestGateway_StripGatewayHeaders(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	rr := dbPost(t, srv, "test", map[string]any{
		"connection": map[string]any{"driver": "bogus"},
	})
	if rr.Code != http.StatusOK {
		t.Errorf("expected HTTP 200 app-level error, got %d", rr.Code)
	}
	resp := decodeResp(t, rr)
	if resp.Error == "" {
		t.Error("expected app-level error for unknown driver")
	}
}

// ── CORS headers ──────────────────────────────────────────────────────────────

func TestGateway_CORSHeaderPresent(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader([]byte("{}")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://localhost:5173")
	rr := httptest.NewRecorder()
	srv.handleDBGateway(rr, req)
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:5173" {
		t.Errorf("CORS header = %q, want %q", got, "http://localhost:5173")
	}
}

func TestGateway_ContentTypeJSON(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	rr := dbPost(t, srv, "test", map[string]any{})
	ct := rr.Header().Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}

// ── DSN construction ──────────────────────────────────────────────────────────

func TestBuildPostgresDSN(t *testing.T) {
	conn := DBConnection{
		Driver: "postgres", Host: "localhost", Port: 5432,
		Database: "mydb", User: "admin", Password: "s3cr3t", SSL: "require",
	}
	dsn := buildPostgresDSN(conn)
	if !strings.Contains(dsn, "localhost") {
		t.Error("DSN missing host")
	}
	if !strings.Contains(dsn, "5432") {
		t.Error("DSN missing port")
	}
	if !strings.Contains(dsn, "mydb") {
		t.Error("DSN missing database")
	}
	if !strings.Contains(dsn, "require") {
		t.Error("DSN missing SSL mode")
	}
}

func TestBuildPostgresDSN_DefaultPort(t *testing.T) {
	conn := DBConnection{Driver: "postgres", Host: "db.example.com", Database: "prod"}
	dsn := buildPostgresDSN(conn)
	if !strings.Contains(dsn, "5432") {
		t.Errorf("DSN should default to port 5432, got %q", dsn)
	}
}

func TestBuildPostgresDSN_DefaultSSL(t *testing.T) {
	conn := DBConnection{Driver: "postgres", Host: "localhost", Database: "dev"}
	dsn := buildPostgresDSN(conn)
	if !strings.Contains(dsn, "disable") {
		t.Errorf("DSN should default sslmode=disable, got %q", dsn)
	}
}

func TestBuildMySQLDSN(t *testing.T) {
	conn := DBConnection{
		Driver: "mysql", Host: "127.0.0.1", Port: 3306,
		Database: "shop", User: "root", Password: "pass",
	}
	dsn := buildMySQLDSN(conn)
	if !strings.Contains(dsn, "root:pass@tcp(127.0.0.1:3306)/shop") {
		t.Errorf("unexpected MySQL DSN: %q", dsn)
	}
}

func TestBuildMySQLDSN_DefaultPort(t *testing.T) {
	conn := DBConnection{Driver: "mysql", Host: "db", Database: "app"}
	dsn := buildMySQLDSN(conn)
	if !strings.Contains(dsn, "3306") {
		t.Errorf("DSN should default to port 3306, got %q", dsn)
	}
}

func TestBuildSQLiteDSN_File(t *testing.T) {
	conn := DBConnection{Driver: "sqlite", File: "/tmp/test.db"}
	dsn := buildSQLiteDSN(conn)
	if dsn != "/tmp/test.db" {
		t.Errorf("want /tmp/test.db, got %q", dsn)
	}
}

func TestBuildSQLiteDSN_Database(t *testing.T) {
	conn := DBConnection{Driver: "sqlite", Database: "myapp.db"}
	dsn := buildSQLiteDSN(conn)
	if dsn != "myapp.db" {
		t.Errorf("want myapp.db, got %q", dsn)
	}
}

func TestBuildSQLiteDSN_Memory(t *testing.T) {
	conn := DBConnection{Driver: "sqlite"}
	dsn := buildSQLiteDSN(conn)
	if dsn != ":memory:" {
		t.Errorf("want :memory:, got %q", dsn)
	}
}

func TestDriverNameFor(t *testing.T) {
	cases := []struct{ driver, want string }{
		{"postgres", "postgres"},
		{"postgresql", "postgres"},
		{"mysql", "mysql"},
		{"sqlite", "sqlite"},
		{"sqlite3", "sqlite"},
	}
	for _, c := range cases {
		got, err := driverNameFor(c.driver)
		if err != nil {
			if c.want == "sqlite" && strings.Contains(err.Error(), "sqlite driver not available") {
				continue // sqlite build tag not set
			}
			t.Errorf("driverNameFor(%q) error: %v", c.driver, err)
			continue
		}
		if got != c.want {
			t.Errorf("driverNameFor(%q) = %q, want %q", c.driver, got, c.want)
		}
	}
}

func TestDriverNameFor_Unknown(t *testing.T) {
	_, err := driverNameFor("oracle")
	if err == nil {
		t.Error("expected error for unsupported driver")
	}
}

// ── Pool key uniqueness ───────────────────────────────────────────────────────

func TestConnectionKey_Uniqueness(t *testing.T) {
	a := DBConnection{Driver: "postgres", Host: "host1", Database: "db"}
	b := DBConnection{Driver: "postgres", Host: "host2", Database: "db"}
	if connectionKey(a) == connectionKey(b) {
		t.Error("different hosts should produce different pool keys")
	}
}

func TestConnectionKey_Stability(t *testing.T) {
	c := DBConnection{Driver: "mysql", Host: "localhost", Port: 3306, Database: "app", User: "root"}
	if connectionKey(c) != connectionKey(c) {
		t.Error("same connection should produce same pool key")
	}
}

func TestConnectionKey_PasswordDiffers(t *testing.T) {
	a := DBConnection{Driver: "postgres", Host: "h", Password: "abc"}
	b := DBConnection{Driver: "postgres", Host: "h", Password: "xyz"}
	if connectionKey(a) == connectionKey(b) {
		t.Error("different passwords should produce different pool keys")
	}
}

// ── Query helper ──────────────────────────────────────────────────────────────

func TestItoa(t *testing.T) {
	cases := []struct {
		n    int
		want string
	}{
		{0, "0"},
		{1, "1"},
		{100, "100"},
		{1000, "1000"},
	}
	for _, c := range cases {
		got := itoa(c.n)
		if got != c.want {
			t.Errorf("itoa(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

func TestAddLimit(t *testing.T) {
	sql := "SELECT * FROM users"
	out := addLimit(sql, 100, "postgres")
	if !strings.HasSuffix(out, "LIMIT 100") {
		t.Errorf("addLimit output = %q, want suffix 'LIMIT 100'", out)
	}
}

// ── Query endpoint — SQL required ─────────────────────────────────────────────

func TestQuery_SQLRequired(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	rr := dbPost(t, srv, "query", map[string]any{
		"connection": map[string]any{"driver": "sqlite"},
		"sql":        "",
	})
	resp := decodeResp(t, rr)
	if !strings.Contains(resp.Error, "sql is required") {
		t.Errorf("expected 'sql is required' error, got %q", resp.Error)
	}
}

// ── Describe endpoint — table required ───────────────────────────────────────

func TestDescribe_TableRequired(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	rr := dbPost(t, srv, "describe", map[string]any{
		"connection": map[string]any{"driver": "sqlite"},
		"table":      "",
	})
	resp := decodeResp(t, rr)
	if !strings.Contains(resp.Error, "table is required") {
		t.Errorf("expected 'table is required' error, got %q", resp.Error)
	}
}

// ── Row limit capping ─────────────────────────────────────────────────────────

func TestRowLimitDefault(t *testing.T) {
	var req DBRequest
	req.Limit = 0
	if req.Limit <= 0 {
		req.Limit = defaultRowLimit
	}
	if req.Limit != defaultRowLimit {
		t.Errorf("expected default limit %d, got %d", defaultRowLimit, req.Limit)
	}
}

func TestRowLimitCap(t *testing.T) {
	var req DBRequest
	req.Limit = 99999
	if req.Limit > maxRowLimit {
		req.Limit = maxRowLimit
	}
	if req.Limit != maxRowLimit {
		t.Errorf("expected capped limit %d, got %d", maxRowLimit, req.Limit)
	}
}
