package proxycore

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func newTLSTestClient(proxyURL string, tlsCfg *tls.Config) *http.Client {
	u, _ := url.Parse(proxyURL)
	return &http.Client{
		Transport: &http.Transport{
			Proxy:           http.ProxyURL(u),
			TLSClientConfig: tlsCfg,
		},
	}
}

func TestCONNECTTunnel_Basic(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("tls-hello"))
	}))
	defer upstream.Close()

	_, proxyServer := newTestServer()
	defer proxyServer.Close()

	client := newTLSTestClient(proxyServer.URL, upstream.Client().Transport.(*http.Transport).TLSClientConfig)

	resp, err := client.Get(upstream.URL + "/tls-test")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "tls-hello" {
		t.Fatalf("expected 'tls-hello', got %q", body)
	}
}

func TestCONNECTTunnel_InvalidHost(t *testing.T) {
	_, proxyServer := newTestServer()
	defer proxyServer.Close()

	proxyURL, _ := url.Parse(proxyServer.URL)
	client := &http.Client{
		Transport: &http.Transport{
			Proxy:           http.ProxyURL(proxyURL),
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
		},
	}

	_, err := client.Get("https://127.0.0.1:1/unreachable")
	if err == nil {
		t.Fatal("expected error for unreachable host, got nil")
	}
}

func TestCONNECTTunnel_LargePayload(t *testing.T) {
	const size = 1 << 20 // 1 MB

	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
		w.WriteHeader(200)
		buf := make([]byte, 4096)
		for i := range buf {
			buf[i] = 'x'
		}
		written := 0
		for written < size {
			n := size - written
			if n > len(buf) {
				n = len(buf)
			}
			w.Write(buf[:n])
			written += n
		}
	}))
	defer upstream.Close()

	_, proxyServer := newTestServer()
	defer proxyServer.Close()

	client := newTLSTestClient(proxyServer.URL, upstream.Client().Transport.(*http.Transport).TLSClientConfig)

	resp, err := client.Get(upstream.URL + "/large")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("error reading large body: %v", err)
	}
	if len(body) != size {
		t.Fatalf("expected %d bytes, got %d", size, len(body))
	}
}

func TestLoggingMiddleware_HijackDelegate(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("logged-tls"))
	}))
	defer upstream.Close()

	srv := NewServer(Options{Port: 0})
	proxyServer := httptest.NewServer(srv.Handler) // Handler already has logging middleware
	defer proxyServer.Close()

	client := newTLSTestClient(proxyServer.URL, upstream.Client().Transport.(*http.Transport).TLSClientConfig)

	resp, err := client.Get(upstream.URL + "/logged")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		t.Fatalf("expected 200 through logging middleware, got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != "logged-tls" {
		t.Fatalf("expected 'logged-tls', got %q", body)
	}
}
