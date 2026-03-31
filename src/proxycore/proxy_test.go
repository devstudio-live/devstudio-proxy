package proxycore

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// newTestServer creates a test proxy server using a fresh Server instance.
func newTestServer() (*Server, *httptest.Server) {
	srv := NewServer(Options{Port: 0})
	return srv, httptest.NewServer(srv.Handler)
}

func newTestClient(proxyURL string) *http.Client {
	u, _ := url.Parse(proxyURL)
	return &http.Client{
		Transport: &http.Transport{Proxy: http.ProxyURL(u)},
	}
}

func TestHTTPForward_200(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("hello"))
	}))
	defer upstream.Close()

	_, proxyServer := newTestServer()
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

	_, proxyServer := newTestServer()
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

func TestHTTPForward_404(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer upstream.Close()

	_, proxyServer := newTestServer()
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

func TestHTTPForward_HopByHop(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	_, proxyServer := newTestServer()
	defer proxyServer.Close()

	req, err := http.NewRequest(http.MethodGet, upstream.URL+"/check", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Connection", "Keep-Alive")
	req.Header.Set("Keep-Alive", "timeout=5")
	req.Header.Set("Transfer-Encoding", "chunked")

	proxyURL, _ := url.Parse(proxyServer.URL)
	client := &http.Client{
		Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)},
	}
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

func TestHTTPForward_InvalidTarget(t *testing.T) {
	_, proxyServer := newTestServer()
	defer proxyServer.Close()

	client := newTestClient(proxyServer.URL)
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

	srv := NewServer(Options{Port: 0})
	rr := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rr, req)

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

	srv := NewServer(Options{Port: 0})
	rr := httptest.NewRecorder()
	srv.Handler.ServeHTTP(rr, req)

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
