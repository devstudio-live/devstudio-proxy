package main

import (
	"bytes"
	"crypto/tls"
	"io"
	"net"
	"time"
)

// multiConn is a net.Conn whose Read method returns peeked bytes before
// delegating to the underlying connection reader.
type multiConn struct {
	net.Conn
	reader io.Reader
}

func (c *multiConn) Read(b []byte) (int, error) {
	return c.reader.Read(b)
}

// dualListener wraps net.Listener and sniffs the first byte of each
// accepted connection to decide whether to upgrade to TLS.
//
// 0x16 = TLS ClientHello (record type: handshake) → wrap in tls.Server.
// Any other byte = plain HTTP → pass through unchanged.
//
// If tlsConfig is nil, all connections are treated as plain HTTP.
type dualListener struct {
	net.Listener
	tlsConfig *tls.Config
}

func (l *dualListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	// Gotcha #5: SetReadDeadline prevents hung idle connections from
	// blocking the Accept loop. Clear the deadline after the peek.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 1)
	n, err := conn.Read(buf)
	_ = conn.SetReadDeadline(time.Time{}) // clear deadline

	if err != nil || n == 0 {
		_ = conn.Close()
		return l.Accept()
	}

	// Reconstitute the full stream by prepending the peeked byte.
	combined := io.MultiReader(bytes.NewReader(buf[:n]), conn)
	mc := &multiConn{Conn: conn, reader: combined}

	if l.tlsConfig != nil && buf[0] == 0x16 {
		return tls.Server(mc, l.tlsConfig), nil
	}
	return mc, nil
}

// newDualListener creates a fresh dual-protocol TCP listener on addr.
//
// Gotcha #1: MUST be called inside the restart loop, NOT before it.
// A closed net.Listener cannot be reused; each loop iteration needs a
// new one.
func newDualListener(addr string, tlsCfg *tls.Config) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &dualListener{Listener: ln, tlsConfig: tlsCfg}, nil
}
