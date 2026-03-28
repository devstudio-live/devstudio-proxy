package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// newTestClient creates an http.Client configured to use the given proxy URL.
func newTestClient(proxyURL string) *http.Client {
	u, _ := url.Parse(proxyURL)
	return &http.Client{
		Transport: &http.Transport{Proxy: http.ProxyURL(u)},
	}
}

// TestHTTPForward_200 verifies that a basic GET request is forwarded and the
// response body and status are relayed correctly.
func TestHTTPForward_200(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("hello"))
	}))
	defer upstream.Close()

	proxyServer := httptest.NewServer(Handler{})
	defer proxyServer.Close()

	client := newTestClient(proxyServer.URL)
	resp, err := client.Get(upstream.URL + "/test")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "hello" {
		t.Fatalf("expected 'hello', got %q", body)
	}
}

// TestHTTPForward_POST verifies that POST requests with bodies are forwarded
// and the upstream-echoed body is correctly relayed back.
func TestHTTPForward_POST(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "body read error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(200)
		w.Write(body)
	}))
	defer upstream.Close()

	proxyServer := httptest.NewServer(Handler{})
	defer proxyServer.Close()

	client := newTestClient(proxyServer.URL)
	payload := "request body data"
	resp, err := client.Post(upstream.URL+"/echo", "text/plain", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != payload {
		t.Fatalf("expected %q, got %q", payload, string(body))
	}
}

// TestHTTPForward_404 verifies that a 404 from the upstream is passed through
// unchanged by the proxy.
func TestHTTPForward_404(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer upstream.Close()

	proxyServer := httptest.NewServer(Handler{})
	defer proxyServer.Close()

	client := newTestClient(proxyServer.URL)
	resp, err := client.Get(upstream.URL + "/missing")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 404 {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

// TestHTTPForward_HopByHop verifies that hop-by-hop headers (Connection,
// Transfer-Encoding) are stripped from requests before forwarding.
func TestHTTPForward_HopByHop(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// These hop-by-hop headers must not reach the upstream
		if r.Header.Get("Connection") != "" {
			http.Error(w, "Connection header leaked", http.StatusBadRequest)
			return
		}
		if r.Header.Get("Transfer-Encoding") != "" {
			http.Error(w, "Transfer-Encoding header leaked", http.StatusBadRequest)
			return
		}
		if r.Header.Get("Keep-Alive") != "" {
			http.Error(w, "Keep-Alive header leaked", http.StatusBadRequest)
			return
		}
		w.WriteHeader(200)
	}))
	defer upstream.Close()

	proxyServer := httptest.NewServer(Handler{})
	defer proxyServer.Close()

	// Build a request with hop-by-hop headers manually
	req, err := http.NewRequest(http.MethodGet, upstream.URL+"/check", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Connection", "Keep-Alive")
	req.Header.Set("Keep-Alive", "timeout=5")
	req.Header.Set("Transfer-Encoding", "chunked")

	// Route through proxy by setting the request URL to the proxy and
	// setting the Host header to the upstream
	proxyURL, _ := url.Parse(proxyServer.URL)
	client := &http.Client{
		Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)},
	}
	// Re-target the request to go through proxy to upstream
	req.URL, _ = url.Parse(upstream.URL + "/check")

	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200 (hop headers stripped), got %d: %s", resp.StatusCode, body)
	}
}

// TestHTTPForward_InvalidTarget verifies that the proxy returns 502 when it
// cannot connect to the upstream.
func TestHTTPForward_InvalidTarget(t *testing.T) {
	proxyServer := httptest.NewServer(Handler{})
	defer proxyServer.Close()

	client := newTestClient(proxyServer.URL)
	// This host is guaranteed to be unreachable
	resp, err := client.Get("http://127.0.0.1:1/unreachable")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 502 {
		t.Fatalf("expected 502, got %d", resp.StatusCode)
	}
}

func TestOptionsPreflight_AllowsPrivateNetwork(t *testing.T) {
	req := httptest.NewRequest(http.MethodOptions, "/admin/logs", nil)
	req.Header.Set("Origin", "https://www.devstudio.live")
	req.Header.Set("Access-Control-Request-Method", "GET")
	req.Header.Set("Access-Control-Request-Headers", "content-type")
	req.Header.Set("Access-Control-Request-Private-Network", "true")

	rr := httptest.NewRecorder()
	Handler{}.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rr.Code)
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "https://www.devstudio.live" {
		t.Fatalf("allow-origin = %q", got)
	}
	if got := rr.Header().Get("Access-Control-Allow-Private-Network"); got != "true" {
		t.Fatalf("allow-private-network = %q, want true", got)
	}
}

func TestHealth_IncludesPrivateNetworkHeader(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	req.Header.Set("Origin", "https://www.devstudio.live")

	rr := httptest.NewRecorder()
	Handler{}.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if got := rr.Header().Get("Access-Control-Allow-Origin"); got != "https://www.devstudio.live" {
		t.Fatalf("allow-origin = %q", got)
	}
	if got := rr.Header().Get("Access-Control-Allow-Private-Network"); got != "true" {
		t.Fatalf("allow-private-network = %q, want true", got)
	}
}
