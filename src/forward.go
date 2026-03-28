package main

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// hopHeaders are headers that should not be forwarded between proxies.
// These are "hop-by-hop" headers defined in RFC 2616 §13.5.1.
var hopHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"TE",
	"Trailers",
	"Transfer-Encoding",
	"Upgrade",
}

// transport is a package-level HTTP transport with sensible timeouts for
// proxying requests to upstream servers. TLS certificate verification is
// skipped to allow self-signed and non-standard certificates common in
// development environments.
var transport = &http.Transport{
	DialContext: (&net.Dialer{
		Timeout: 10 * time.Second,
	}).DialContext,
	TLSHandshakeTimeout:   10 * time.Second,
	ResponseHeaderTimeout: 30 * time.Second,
	IdleConnTimeout:       90 * time.Second,
	TLSClientConfig:       &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
}

// setCORS adds CORS headers when the request has an Origin header,
// allowing browser-based clients (rest-client) to read the response.
func setCORS(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); origin != "" {
		w.Header().Add("Vary", "Origin")
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Expose-Headers", "*")
		// Chrome's Private Network Access checks require this for public HTTPS
		// pages reaching loopback services such as localhost.
		w.Header().Set("Access-Control-Allow-Private-Network", "true")
	}
}

// handleForward handles plain HTTP proxy requests by forwarding the request
// to the upstream server and relaying the response back to the client.
func handleForward(w http.ResponseWriter, r *http.Request) {
	// Clone the request so we can safely modify it
	outReq := r.Clone(r.Context())

	// CRITICAL: Transport.RoundTrip rejects requests with non-empty RequestURI
	outReq.RequestURI = ""

	// Ensure the URL scheme is set; default to http for plain proxy requests
	if outReq.URL.Scheme == "" {
		// Check X-Forwarded-Proto (browser clients can't configure proxy directly)
		if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
			outReq.URL.Scheme = proto
		} else {
			outReq.URL.Scheme = "http"
		}
	}
	if outReq.URL.Host == "" {
		// Check X-Forwarded-Host (browsers can't set the Host header directly,
		// so browser-based clients set X-Forwarded-Host to the real target)
		if fwdHost := r.Header.Get("X-Forwarded-Host"); fwdHost != "" {
			outReq.URL.Host = fwdHost
			outReq.Host = fwdHost
		} else {
			outReq.URL.Host = r.Host
		}
	}

	// Strip proxy-specific headers before forwarding
	outReq.Header.Del("X-Forwarded-Host")
	outReq.Header.Del("X-Forwarded-Proto")

	// Strip headers listed in the Connection header value (per RFC 7230 §6.1)
	for _, connHeader := range strings.Split(r.Header.Get("Connection"), ",") {
		outReq.Header.Del(strings.TrimSpace(connHeader))
	}

	// Strip hop-by-hop headers from the outgoing request
	for _, h := range hopHeaders {
		outReq.Header.Del(h)
	}

	// Add/append X-Forwarded-For header
	clientIP, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		if prior := outReq.Header.Get("X-Forwarded-For"); prior != "" {
			outReq.Header.Set("X-Forwarded-For", prior+", "+clientIP)
		} else {
			outReq.Header.Set("X-Forwarded-For", clientIP)
		}
	}

	// Forward the request to the upstream server
	resp, err := transport.RoundTrip(outReq)
	if err != nil {
		setCORS(w, r)
		http.Error(w, "upstream error: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Strip hop-by-hop headers from response (especially Transfer-Encoding,
	// which net/http handles automatically when writing the response)
	for _, h := range hopHeaders {
		resp.Header.Del(h)
	}

	// Also strip headers listed in the response Connection header
	for _, connHeader := range strings.Split(resp.Header.Get("Connection"), ",") {
		resp.Header.Del(strings.TrimSpace(connHeader))
	}

	// Copy remaining response headers to the client
	for key, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(key, v)
		}
	}

	// Add CORS headers
	setCORS(w, r)

	// Write the status code
	w.WriteHeader(resp.StatusCode)

	// Stream the response body to the client
	io.Copy(w, resp.Body) //nolint:errcheck
}
