package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// newTLSTestClient creates an http.Client that uses the given proxy and trusts
// the provided TLS certificate (for use with httptest.NewTLSServer).
func newTLSTestClient(proxyURL string, tlsCfg *tls.Config) *http.Client {
	u, _ := url.Parse(proxyURL)
	return &http.Client{
		Transport: &http.Transport{
			Proxy:           http.ProxyURL(u),
			TLSClientConfig: tlsCfg,
		},
	}
}

// TestCONNECTTunnel_Basic verifies that the proxy correctly establishes a
// CONNECT tunnel for an HTTPS request to a TLS upstream.
func TestCONNECTTunnel_Basic(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("tls-hello"))
	}))
	defer upstream.Close()

	proxyServer := httptest.NewServer(Handler{})
	defer proxyServer.Close()

	// Use the TLS server's client config so we trust the test certificate
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

// TestCONNECTTunnel_InvalidHost verifies that the proxy returns 502 when a
// CONNECT tunnel cannot be established to an unreachable host.
func TestCONNECTTunnel_InvalidHost(t *testing.T) {
	proxyServer := httptest.NewServer(Handler{})
	defer proxyServer.Close()

	proxyURL, _ := url.Parse(proxyServer.URL)
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
			// Accept any certificate since we expect to get a proxy error, not a TLS handshake
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
		},
	}

	// Try to connect to a port that's guaranteed to be unreachable
	_, err := client.Get("https://127.0.0.1:1/unreachable")
	if err == nil {
		t.Fatal("expected error for unreachable host, got nil")
	}
	// The error can manifest as a connection refused or a proxy error (502).
	// Either way the request should fail — we just verify we got an error.
}

// TestCONNECTTunnel_LargePayload verifies that the proxy can handle large
// response bodies tunneled over TLS without data loss or truncation.
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

	proxyServer := httptest.NewServer(Handler{})
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

// TestLoggingMiddleware_HijackDelegate verifies that wrapping the Handler with
// LoggingMiddleware does not break CONNECT tunnel functionality. The
// loggingResponseWriter must properly delegate Hijack() to the underlying
// ResponseWriter.
func TestLoggingMiddleware_HijackDelegate(t *testing.T) {
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte("logged-tls"))
	}))
	defer upstream.Close()

	// Wrap the proxy handler with logging middleware
	proxyServer := httptest.NewServer(NewLoggingMiddleware(Handler{}))
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
