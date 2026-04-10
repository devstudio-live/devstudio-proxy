package proxycore

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/ssh"
)

const sshTunnelReaperInterval = 60 * time.Second

// sshTunnel represents an active SSH port-forward or SOCKS5 proxy tunnel.
type sshTunnel struct {
	ID         string    `json:"id"`
	ConnKey    string    `json:"-"`
	Type       string    `json:"type"`               // local | remote | dynamic
	BindAddr   string    `json:"bindAddr"`
	RemoteAddr string    `json:"remoteAddr,omitempty"`
	StartedAt  time.Time `json:"startedAt"`
	stopCh     chan struct{}
	listener   net.Listener // may be nil for remote forward
}

// ── Gateway handlers ──────────────────────────────────────────────────────────

func (s *Server) handleSSHTunnelStart(w http.ResponseWriter, req SSHRequest, start time.Time) {
	if req.TunnelType == "" {
		json.NewEncoder(w).Encode(SSHResponse{Error: "tunnelType is required (local, remote, dynamic)", DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	if req.BindAddr == "" {
		json.NewEncoder(w).Encode(SSHResponse{Error: "bindAddr is required", DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	if req.TunnelType != "dynamic" && req.RemoteAddr == "" {
		json.NewEncoder(w).Encode(SSHResponse{Error: "remoteAddr is required for " + req.TunnelType + " tunnels", DurationMs: ms(start)}) //nolint:errcheck
		return
	}

	sshClient, err := s.getPooledSSHClient(req.Connection)
	if err != nil {
		json.NewEncoder(w).Encode(SSHResponse{Error: "SSH: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}

	id := uuid.New().String()[:8]
	stopCh := make(chan struct{})

	var ln net.Listener
	switch req.TunnelType {
	case "local":
		ln, err = startLocalForward(sshClient, req.BindAddr, req.RemoteAddr, stopCh)
	case "remote":
		ln, err = startRemoteForward(sshClient, req.RemoteAddr, req.BindAddr, stopCh)
	case "dynamic":
		ln, err = startDynamicForward(sshClient, req.BindAddr, stopCh)
	default:
		close(stopCh)
		json.NewEncoder(w).Encode(SSHResponse{Error: "unknown tunnelType: " + req.TunnelType, DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	if err != nil {
		close(stopCh)
		json.NewEncoder(w).Encode(SSHResponse{Error: "start tunnel: " + err.Error(), DurationMs: ms(start)}) //nolint:errcheck
		return
	}

	tunnel := &sshTunnel{
		ID:         id,
		ConnKey:    sshConnectionKey(req.Connection),
		Type:       req.TunnelType,
		BindAddr:   req.BindAddr,
		RemoteAddr: req.RemoteAddr,
		StartedAt:  time.Now(),
		stopCh:     stopCh,
		listener:   ln,
	}
	s.sshTunnels.Store(id, tunnel)

	// Remove from map when the tunnel stops naturally
	go func() {
		<-stopCh
		s.sshTunnels.Delete(id)
	}()

	json.NewEncoder(w).Encode(SSHResponse{ //nolint:errcheck
		Success:    true,
		Message:    fmt.Sprintf("%s tunnel active on %s", req.TunnelType, req.BindAddr),
		Item:       map[string]any{"id": id, "type": req.TunnelType, "bindAddr": req.BindAddr, "remoteAddr": req.RemoteAddr},
		DurationMs: ms(start),
	})
}

func (s *Server) handleSSHTunnelStop(w http.ResponseWriter, req SSHRequest, start time.Time) {
	if req.TunnelID == "" {
		json.NewEncoder(w).Encode(SSHResponse{Error: "tunnelId is required", DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	v, ok := s.sshTunnels.Load(req.TunnelID)
	if !ok {
		json.NewEncoder(w).Encode(SSHResponse{Error: "tunnel not found: " + req.TunnelID, DurationMs: ms(start)}) //nolint:errcheck
		return
	}
	t := v.(*sshTunnel)
	s.sshTunnels.Delete(req.TunnelID)
	close(t.stopCh) // triggers goroutines to exit
	if t.listener != nil {
		t.listener.Close() //nolint:errcheck
	}
	json.NewEncoder(w).Encode(SSHResponse{Success: true, Message: "tunnel stopped", DurationMs: ms(start)}) //nolint:errcheck
}

func (s *Server) handleSSHTunnelList(w http.ResponseWriter, start time.Time) {
	var items []map[string]any
	s.sshTunnels.Range(func(_, v any) bool {
		t := v.(*sshTunnel)
		items = append(items, map[string]any{
			"id":         t.ID,
			"type":       t.Type,
			"bindAddr":   t.BindAddr,
			"remoteAddr": t.RemoteAddr,
			"startedAt":  t.StartedAt,
		})
		return true
	})
	if items == nil {
		items = []map[string]any{}
	}
	json.NewEncoder(w).Encode(SSHResponse{Items: &items, DurationMs: ms(start)}) //nolint:errcheck
}

// ── Tunnel implementations ────────────────────────────────────────────────────

// startLocalForward listens on localAddr and forwards each accepted connection
// to remoteAddr through the SSH client (-L forwarding).
func startLocalForward(client *ssh.Client, localAddr, remoteAddr string, stopCh <-chan struct{}) (net.Listener, error) {
	ln, err := net.Listen("tcp", localAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", localAddr, err)
	}
	go func() {
		defer ln.Close()
		go func() { <-stopCh; ln.Close() }() //nolint:errcheck
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(local net.Conn) {
				defer local.Close()
				remote, err := client.Dial("tcp", remoteAddr)
				if err != nil {
					return
				}
				defer remote.Close()
				biCopy(local, remote)
			}(conn)
		}
	}()
	return ln, nil
}

// startRemoteForward requests the SSH server to listen on remoteAddr and
// forwards each accepted connection to localAddr (-R forwarding).
func startRemoteForward(client *ssh.Client, remoteAddr, localAddr string, stopCh <-chan struct{}) (net.Listener, error) {
	ln, err := client.Listen("tcp", remoteAddr)
	if err != nil {
		return nil, fmt.Errorf("remote listen %s: %w", remoteAddr, err)
	}
	go func() {
		defer ln.Close()
		go func() { <-stopCh; ln.Close() }() //nolint:errcheck
		for {
			remote, err := ln.Accept()
			if err != nil {
				return
			}
			go func(remote net.Conn) {
				defer remote.Close()
				local, err := net.Dial("tcp", localAddr)
				if err != nil {
					return
				}
				defer local.Close()
				biCopy(remote, local)
			}(remote)
		}
	}()
	return ln, nil
}

// startDynamicForward starts a SOCKS5 proxy on bindAddr that routes all
// connections through the SSH client (-D dynamic forwarding).
func startDynamicForward(client *ssh.Client, bindAddr string, stopCh <-chan struct{}) (net.Listener, error) {
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", bindAddr, err)
	}
	go func() {
		defer ln.Close()
		go func() { <-stopCh; ln.Close() }() //nolint:errcheck
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleSOCKS5(conn, client)
		}
	}()
	return ln, nil
}

// handleSOCKS5 handles a single SOCKS5 CONNECT request and proxies through SSH.
// Only the CONNECT command (TCP) is supported; UDP ASSOCIATE is not.
func handleSOCKS5(conn net.Conn, client *ssh.Client) {
	defer conn.Close()

	// Greeting: [VER=5][NMETHODS][METHOD...]
	hdr := make([]byte, 2)
	if _, err := io.ReadFull(conn, hdr); err != nil || hdr[0] != 5 {
		return
	}
	methods := make([]byte, int(hdr[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return
	}
	// Accept: no auth required
	if _, err := conn.Write([]byte{5, 0}); err != nil {
		return
	}

	// Request: [VER=5][CMD][RSV=0][ATYP][DST.ADDR][DST.PORT]
	req := make([]byte, 4)
	if _, err := io.ReadFull(conn, req); err != nil {
		return
	}
	if req[0] != 5 {
		return
	}
	if req[1] != 1 { // only CONNECT is supported
		conn.Write([]byte{5, 7, 0, 1, 0, 0, 0, 0, 0, 0}) //nolint:errcheck
		return
	}

	var host string
	switch req[3] { // ATYP
	case 1: // IPv4
		addr := make([]byte, 4)
		if _, err := io.ReadFull(conn, addr); err != nil {
			return
		}
		host = net.IP(addr).String()
	case 3: // Domain name
		lb := make([]byte, 1)
		if _, err := io.ReadFull(conn, lb); err != nil {
			return
		}
		domain := make([]byte, int(lb[0]))
		if _, err := io.ReadFull(conn, domain); err != nil {
			return
		}
		host = string(domain)
	case 4: // IPv6
		addr := make([]byte, 16)
		if _, err := io.ReadFull(conn, addr); err != nil {
			return
		}
		host = "[" + net.IP(addr).String() + "]"
	default:
		conn.Write([]byte{5, 8, 0, 1, 0, 0, 0, 0, 0, 0}) //nolint:errcheck
		return
	}

	portBuf := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBuf); err != nil {
		return
	}
	target := fmt.Sprintf("%s:%d", host, binary.BigEndian.Uint16(portBuf))

	remote, err := client.Dial("tcp", target)
	if err != nil {
		conn.Write([]byte{5, 4, 0, 1, 0, 0, 0, 0, 0, 0}) //nolint:errcheck
		return
	}
	defer remote.Close()

	// Success: [VER=5][REP=0][RSV=0][ATYP=1][BND.ADDR=0.0.0.0][BND.PORT=0]
	conn.Write([]byte{5, 0, 0, 1, 0, 0, 0, 0, 0, 0}) //nolint:errcheck
	biCopy(conn, remote)
}

// biCopy performs bidirectional copy between two connections.
// When either direction finishes (EOF or error), both connections are closed
// so the other goroutine unblocks. Returns after both goroutines exit.
func biCopy(a, b net.Conn) {
	done := make(chan struct{}, 2)
	go func() { io.Copy(a, b); done <- struct{}{} }() //nolint:errcheck
	go func() { io.Copy(b, a); done <- struct{}{} }() //nolint:errcheck
	<-done        // wait for first direction to finish
	a.Close()     //nolint:errcheck  — unblocks the other goroutine
	b.Close()     //nolint:errcheck
	<-done        // wait for second direction to finish
}

// ── Reaper ────────────────────────────────────────────────────────────────────

func (s *Server) startSSHTunnelReaper() {
	ticker := time.NewTicker(sshTunnelReaperInterval)
	for range ticker.C {
		s.reapDeadTunnels()
	}
}

// reapDeadTunnels removes tunnels whose stopCh was already closed externally.
func (s *Server) reapDeadTunnels() {
	s.sshTunnels.Range(func(k, v any) bool {
		t := v.(*sshTunnel)
		select {
		case <-t.stopCh:
			// Already stopped — ensure listener is closed and remove from map
			if t.listener != nil {
				t.listener.Close() //nolint:errcheck
			}
			s.sshTunnels.Delete(k)
		default:
			// Still active
		}
		return true
	})
}
