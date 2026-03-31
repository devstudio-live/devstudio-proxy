package proxycore

import (
	"io"
	"net"
	"net/http"
	"time"
)

// handleTunnel handles HTTP CONNECT requests by establishing a TCP tunnel.
func handleTunnel(w http.ResponseWriter, r *http.Request) {
	target := r.Host
	if target == "" {
		http.Error(w, "missing host", http.StatusBadRequest)
		return
	}

	if _, _, err := net.SplitHostPort(target); err != nil {
		target = net.JoinHostPort(target, "443")
	}

	upstreamConn, err := net.DialTimeout("tcp", target, 10*time.Second)
	if err != nil {
		http.Error(w, "could not connect to upstream: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer upstreamConn.Close()

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

	_, err = conn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	if err != nil {
		return
	}

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

	errc := make(chan error, 2)
	go func() {
		_, err := io.Copy(upstreamConn, conn)
		errc <- err
	}()
	go func() {
		_, err := io.Copy(conn, upstreamConn)
		errc <- err
	}()

	<-errc
}
