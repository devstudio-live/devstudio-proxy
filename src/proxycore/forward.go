package proxycore

import (
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// hopHeaders are headers that should not be forwarded between proxies.
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

// Transport is a package-level HTTP transport with sensible timeouts for
// proxying requests to upstream servers.
var Transport = &http.Transport{
	DialContext: (&net.Dialer{
		Timeout: 10 * time.Second,
	}).DialContext,
	TLSHandshakeTimeout:   10 * time.Second,
	ResponseHeaderTimeout: 30 * time.Second,
	IdleConnTimeout:       90 * time.Second,
	TLSClientConfig:       &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
}

// setCORS adds CORS headers when the request has an Origin header.
func setCORS(w http.ResponseWriter, r *http.Request) {
	if origin := r.Header.Get("Origin"); origin != "" {
		w.Header().Add("Vary", "Origin")
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Expose-Headers", "*")
		w.Header().Set("Access-Control-Allow-Private-Network", "true")
	}
}

// handleForward handles plain HTTP proxy requests by forwarding the request
// to the upstream server and relaying the response back to the client.
func handleForward(w http.ResponseWriter, r *http.Request) {
	outReq := r.Clone(r.Context())
	outReq.RequestURI = ""

	if outReq.URL.Scheme == "" {
		if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
			outReq.URL.Scheme = proto
		} else {
			outReq.URL.Scheme = "http"
		}
	}
	if outReq.URL.Host == "" {
		if fwdHost := r.Header.Get("X-Forwarded-Host"); fwdHost != "" {
			outReq.URL.Host = fwdHost
			outReq.Host = fwdHost
		} else {
			outReq.URL.Host = r.Host
		}
	}

	outReq.Header.Del("X-Forwarded-Host")
	outReq.Header.Del("X-Forwarded-Proto")

	for _, connHeader := range strings.Split(r.Header.Get("Connection"), ",") {
		outReq.Header.Del(strings.TrimSpace(connHeader))
	}
	for _, h := range hopHeaders {
		outReq.Header.Del(h)
	}

	clientIP, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		if prior := outReq.Header.Get("X-Forwarded-For"); prior != "" {
			outReq.Header.Set("X-Forwarded-For", prior+", "+clientIP)
		} else {
			outReq.Header.Set("X-Forwarded-For", clientIP)
		}
	}

	resp, err := Transport.RoundTrip(outReq)
	if err != nil {
		setCORS(w, r)
		http.Error(w, "upstream error: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for _, h := range hopHeaders {
		resp.Header.Del(h)
	}
	for _, connHeader := range strings.Split(resp.Header.Get("Connection"), ",") {
		resp.Header.Del(strings.TrimSpace(connHeader))
	}

	for key, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(key, v)
		}
	}

	setCORS(w, r)
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body) //nolint:errcheck
}
