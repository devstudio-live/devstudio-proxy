package main

import (
	"io"
	"net"
	"net/http"
	"time"
)

// handleTunnel handles HTTP CONNECT requests by establishing a TCP tunnel
// between the client and the upstream server. This is used for HTTPS proxying.
func handleTunnel(w http.ResponseWriter, r *http.Request) {
	// Extract target host from r.Host (for CONNECT requests the target is in Host)
	target := r.Host
	if target == "" {
		http.Error(w, "missing host", http.StatusBadRequest)
		return
	}

	// If no port is specified, default to 443
	if _, _, err := net.SplitHostPort(target); err != nil {
		target = net.JoinHostPort(target, "443")
	}

	// Dial the upstream target
	upstreamConn, err := net.DialTimeout("tcp", target, 10*time.Second)
	if err != nil {
		http.Error(w, "could not connect to upstream: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer upstreamConn.Close()

	// Hijack the client connection to get raw TCP access
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "hijacking not supported", http.StatusInternalServerError)
		return
	}

	conn, bufrw, err := hijacker.Hijack()
	if err != nil {
		http.Error(w, "hijack failed: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	// Send 200 Connection Established directly on the raw connection
	_, err = conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	if err != nil {
		return
	}

	// If there are bytes buffered from the client that arrived before we hijacked,
	// flush them upstream immediately
	if bufrw.Reader.Buffered() > 0 {
		buffered := make([]byte, bufrw.Reader.Buffered())
		_, err = bufrw.Read(buffered)
		if err != nil {
			return
		}
		_, err = upstreamConn.Write(buffered)
		if err != nil {
			return
		}
	}

	// Bidirectional copy: client <-> upstream
	errc := make(chan error, 2)
	go func() {
		_, err := io.Copy(upstreamConn, conn)
		errc <- err
	}()
	go func() {
		_, err := io.Copy(conn, upstreamConn)
		errc <- err
	}()

	// Wait for either direction to finish (the other will close on next read/write)
	<-errc
}
