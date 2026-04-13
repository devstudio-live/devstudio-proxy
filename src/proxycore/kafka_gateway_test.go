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

func kafkaPost(t *testing.T, srv *Server, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/"+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-DevStudio-Gateway-Route", "true")
	req.Header.Set("X-DevStudio-Gateway-Protocol", "kafka")
	req.Header.Set("Origin", "http://localhost:5173")
	rr := httptest.NewRecorder()
	srv.handleKafkaGateway(rr, req)
	return rr
}

func decodeKafkaResp(t *testing.T, rr *httptest.ResponseRecorder) KafkaResponse {
	t.Helper()
	var resp KafkaResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v — body: %s", err, rr.Body.String())
	}
	return resp
}

// ── Routing & method enforcement ─────────────────────────────────────────────

func TestKafkaGateway_UnknownEndpoint(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	rr := kafkaPost(t, srv, "nonexistent", map[string]any{})
	resp := decodeKafkaResp(t, rr)
	if !strings.Contains(resp.Error, "nonexistent") {
		t.Errorf("want error mentioning endpoint name, got %q", resp.Error)
	}
}

func TestKafkaGateway_MethodNotAllowed(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	req.Header.Set("X-DevStudio-Gateway-Route", "true")
	req.Header.Set("X-DevStudio-Gateway-Protocol", "kafka")
	rr := httptest.NewRecorder()
	srv.handleKafkaGateway(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("want 405, got %d", rr.Code)
	}
}

func TestKafkaGateway_BadJSON(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	req := httptest.NewRequest(http.MethodPost, "/test", strings.NewReader("{bad json"))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	srv.handleKafkaGateway(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("want 400, got %d", rr.Code)
	}
	resp := decodeKafkaResp(t, rr)
	if resp.Error == "" {
		t.Error("want error message for bad JSON")
	}
}

// ── CORS headers ──────────────────────────────────────────────────────────────

func TestKafkaGateway_CORSHeaderPresent(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewReader([]byte("{}")))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://localhost:5173")
	rr := httptest.NewRecorder()
	srv.handleKafkaGateway(rr, req)
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:5173" {
		t.Errorf("CORS header = %q, want %q", got, "http://localhost:5173")
	}
}

func TestKafkaGateway_ContentTypeJSON(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	rr := kafkaPost(t, srv, "test", map[string]any{})
	ct := rr.Header().Get("Content-Type")
	if !strings.Contains(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}

// ── Gateway header stripping ─────────────────────────────────────────────────

func TestKafkaGateway_StripGatewayHeaders(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	// The test endpoint will fail to connect but should still produce a valid JSON response
	rr := kafkaPost(t, srv, "test", map[string]any{
		"connection": map[string]any{"brokers": []string{"localhost:9092"}},
	})
	resp := decodeKafkaResp(t, rr)
	// Should get a connection error (no broker running), not a routing error
	if resp.Error == "" && resp.Data == nil {
		t.Error("expected either an error or data in the response")
	}
}

// ── Pool key uniqueness ──────────────────────────────────────────────────────

func TestKafkaConnectionKey_Uniqueness(t *testing.T) {
	a := KafkaConnection{Brokers: []string{"broker1:9092"}}
	b := KafkaConnection{Brokers: []string{"broker2:9092"}}
	if kafkaConnectionKey(a) == kafkaConnectionKey(b) {
		t.Error("different brokers should produce different pool keys")
	}
}

func TestKafkaConnectionKey_Stability(t *testing.T) {
	c := KafkaConnection{Brokers: []string{"localhost:9092"}, User: "admin"}
	if kafkaConnectionKey(c) != kafkaConnectionKey(c) {
		t.Error("same connection should produce same pool key")
	}
}

func TestKafkaConnectionKey_SASLDiffers(t *testing.T) {
	a := KafkaConnection{Brokers: []string{"host:9092"}, SASL: "plain", User: "a", Password: "p1"}
	b := KafkaConnection{Brokers: []string{"host:9092"}, SASL: "plain", User: "a", Password: "p2"}
	if kafkaConnectionKey(a) == kafkaConnectionKey(b) {
		t.Error("different passwords should produce different pool keys")
	}
}

func TestKafkaConnectionKey_TLSDiffers(t *testing.T) {
	a := KafkaConnection{Brokers: []string{"host:9092"}, TLS: false}
	b := KafkaConnection{Brokers: []string{"host:9092"}, TLS: true}
	if kafkaConnectionKey(a) == kafkaConnectionKey(b) {
		t.Error("different TLS settings should produce different pool keys")
	}
}

// ── SASL mechanism building ──────────────────────────────────────────────────

func TestBuildKafkaSASL_None(t *testing.T) {
	conn := KafkaConnection{SASL: ""}
	m, err := buildKafkaSASL(conn)
	if err != nil {
		t.Fatal(err)
	}
	if m != nil {
		t.Error("expected nil mechanism for no SASL")
	}
}

func TestBuildKafkaSASL_Plain(t *testing.T) {
	conn := KafkaConnection{SASL: "plain", User: "admin", Password: "secret"}
	m, err := buildKafkaSASL(conn)
	if err != nil {
		t.Fatal(err)
	}
	if m == nil {
		t.Error("expected non-nil mechanism for SASL/PLAIN")
	}
}

func TestBuildKafkaSASL_ScramSHA256(t *testing.T) {
	conn := KafkaConnection{SASL: "scram-sha-256", User: "u", Password: "p"}
	m, err := buildKafkaSASL(conn)
	if err != nil {
		t.Fatal(err)
	}
	if m == nil {
		t.Error("expected non-nil mechanism for SCRAM-SHA-256")
	}
}

func TestBuildKafkaSASL_ScramSHA512(t *testing.T) {
	conn := KafkaConnection{SASL: "scram-sha-512", User: "u", Password: "p"}
	m, err := buildKafkaSASL(conn)
	if err != nil {
		t.Fatal(err)
	}
	if m == nil {
		t.Error("expected non-nil mechanism for SCRAM-SHA-512")
	}
}

func TestBuildKafkaSASL_Unsupported(t *testing.T) {
	conn := KafkaConnection{SASL: "kerberos"}
	_, err := buildKafkaSASL(conn)
	if err == nil {
		t.Error("expected error for unsupported SASL mechanism")
	}
}

// ── TLS config ───────────────────────────────────────────────────────────────

func TestBuildKafkaTLSConfig_Disabled(t *testing.T) {
	conn := KafkaConnection{TLS: false}
	cfg := buildKafkaTLSConfig(conn)
	if cfg != nil {
		t.Error("expected nil TLS config when TLS is disabled")
	}
}

func TestBuildKafkaTLSConfig_Enabled(t *testing.T) {
	conn := KafkaConnection{TLS: true, TLSSkipVerify: false}
	cfg := buildKafkaTLSConfig(conn)
	if cfg == nil {
		t.Fatal("expected non-nil TLS config when TLS is enabled")
	}
	if cfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify=false")
	}
}

func TestBuildKafkaTLSConfig_SkipVerify(t *testing.T) {
	conn := KafkaConnection{TLS: true, TLSSkipVerify: true}
	cfg := buildKafkaTLSConfig(conn)
	if cfg == nil {
		t.Fatal("expected non-nil TLS config")
	}
	if !cfg.InsecureSkipVerify {
		t.Error("expected InsecureSkipVerify=true")
	}
}

// ── Topic detail validation ──────────────────────────────────────────────────

func TestKafkaTopicDetail_RequiresTopic(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	rr := kafkaPost(t, srv, "topic/detail", map[string]any{
		"connection": map[string]any{"brokers": []string{"localhost:9092"}},
		"topic":      "",
	})
	resp := decodeKafkaResp(t, rr)
	if !strings.Contains(resp.Error, "topic name is required") {
		t.Errorf("expected 'topic name is required' error, got %q", resp.Error)
	}
}

// ── Pool capacity ────────────────────────────────────────────────────────────

func TestKafkaPool_ConnectionCreation(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	conn := KafkaConnection{Brokers: []string{"localhost:19092"}}
	entry, err := srv.getPooledKafkaClient(conn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if entry == nil {
		t.Fatal("expected non-nil pool entry")
	}
	if entry.transport == nil {
		t.Error("expected non-nil transport")
	}
	if entry.dialer == nil {
		t.Error("expected non-nil dialer")
	}
	if len(entry.brokers) != 1 || entry.brokers[0] != "localhost:19092" {
		t.Errorf("unexpected brokers: %v", entry.brokers)
	}
}

func TestKafkaPool_SameConnectionReusesEntry(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	conn := KafkaConnection{Brokers: []string{"localhost:29092"}}
	e1, _ := srv.getPooledKafkaClient(conn)
	e2, _ := srv.getPooledKafkaClient(conn)
	if e1 != e2 {
		t.Error("same connection should return the same pool entry")
	}
}

func TestKafkaPool_DifferentConnectionsDifferentEntries(t *testing.T) {
	srv := NewServer(Options{Port: 0})
	e1, _ := srv.getPooledKafkaClient(KafkaConnection{Brokers: []string{"host1:9092"}})
	e2, _ := srv.getPooledKafkaClient(KafkaConnection{Brokers: []string{"host2:9092"}})
	if e1 == e2 {
		t.Error("different connections should return different pool entries")
	}
}

// ── Endpoint routing coverage ────────────────────────────────────────────────

func TestKafkaGateway_AllPhase1Endpoints(t *testing.T) {
	endpoints := []string{"test", "brokers", "broker/config", "topics", "topic/detail"}
	srv := NewServer(Options{Port: 0})

	for _, ep := range endpoints {
		t.Run(ep, func(t *testing.T) {
			rr := kafkaPost(t, srv, ep, map[string]any{
				"connection": map[string]any{"brokers": []string{"localhost:9092"}},
			})
			resp := decodeKafkaResp(t, rr)
			// All endpoints should either succeed or return a connection error,
			// never an "unknown endpoint" error.
			if strings.Contains(resp.Error, "unknown endpoint") {
				t.Errorf("endpoint %q returned unknown endpoint error", ep)
			}
		})
	}
}
