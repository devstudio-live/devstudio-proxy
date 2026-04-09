package proxycore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"golang.org/x/crypto/ssh"
)

// SSHConnection holds the parameters needed to open an SSH connection.
type SSHConnection struct {
	Host       string `json:"host"`
	Port       int    `json:"port,omitempty"`
	User       string `json:"user"`
	AuthMethod string `json:"authMethod"` // password | key
	Password   string `json:"password,omitempty"`
	PrivateKey string `json:"privateKey,omitempty"` // PEM content
	Passphrase string `json:"passphrase,omitempty"`
	Label      string `json:"label,omitempty"`
	Group      string `json:"group,omitempty"`
}

const (
	maxSSHPoolSize    = 20
	sshIdleExpiry     = 10 * time.Minute
	sshReaperInterval = 30 * time.Second
)

type sshPoolEntry struct {
	client   *ssh.Client
	lastUsed time.Time
}

func sshConnectionKey(conn SSHConnection) string {
	port := conn.Port
	if port == 0 {
		port = 22
	}
	raw := fmt.Sprintf("%s:%d:%s:%s", conn.Host, port, conn.User, conn.AuthMethod)
	switch conn.AuthMethod {
	case "password":
		raw += ":" + conn.Password
	case "key":
		sum := sha256.Sum256([]byte(conn.PrivateKey))
		raw += ":" + hex.EncodeToString(sum[:])
	}
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

func buildSSHConfig(conn SSHConnection) (*ssh.ClientConfig, error) {
	var authMethods []ssh.AuthMethod

	switch conn.AuthMethod {
	case "password":
		authMethods = append(authMethods, ssh.Password(conn.Password))
	case "key":
		if conn.PrivateKey == "" {
			return nil, fmt.Errorf("private key content is required for key auth")
		}
		var (
			signer ssh.Signer
			err    error
		)
		if conn.Passphrase != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase([]byte(conn.PrivateKey), []byte(conn.Passphrase))
		} else {
			signer, err = ssh.ParsePrivateKey([]byte(conn.PrivateKey))
		}
		if err != nil {
			return nil, fmt.Errorf("parse private key: %w", err)
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	default:
		return nil, fmt.Errorf("unsupported auth method: %q (use password or key)", conn.AuthMethod)
	}

	return &ssh.ClientConfig{
		User:            conn.User,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), //nolint:gosec // user-managed connections
		Timeout:         15 * time.Second,
	}, nil
}

func (s *Server) getPooledSSHClient(conn SSHConnection) (*ssh.Client, error) {
	key := sshConnectionKey(conn)

	// Fast path
	if v, ok := s.sshPool.Load(key); ok {
		entry := v.(*sshPoolEntry)
		entry.lastUsed = time.Now()
		return entry.client, nil
	}

	s.sshPoolMu.Lock()
	defer s.sshPoolMu.Unlock()

	// Double-check after lock
	if v, ok := s.sshPool.Load(key); ok {
		entry := v.(*sshPoolEntry)
		entry.lastUsed = time.Now()
		return entry.client, nil
	}

	if s.sshPoolSize >= maxSSHPoolSize {
		return nil, fmt.Errorf("SSH connection pool full (%d/%d): disconnect unused connections first", s.sshPoolSize, maxSSHPoolSize)
	}

	cfg, err := buildSSHConfig(conn)
	if err != nil {
		return nil, err
	}

	port := conn.Port
	if port == 0 {
		port = 22
	}
	addr := fmt.Sprintf("%s:%d", conn.Host, port)

	client, err := ssh.Dial("tcp", addr, cfg)
	if err != nil {
		return nil, fmt.Errorf("ssh dial %s: %w", addr, err)
	}

	s.sshPool.Store(key, &sshPoolEntry{client: client, lastUsed: time.Now()})
	s.sshPoolSize++

	return client, nil
}

func (s *Server) closeSSHConnection(key string) {
	s.sshPoolMu.Lock()
	defer s.sshPoolMu.Unlock()
	if v, ok := s.sshPool.Load(key); ok {
		v.(*sshPoolEntry).client.Close()
		s.sshPool.Delete(key)
		s.sshPoolSize--
	}
}

func (s *Server) startSSHPoolReaper() {
	ticker := time.NewTicker(sshReaperInterval)
	for range ticker.C {
		s.reapIdleSSHConnections()
	}
}

func (s *Server) reapIdleSSHConnections() {
	now := time.Now()
	var toEvict []string

	s.sshPool.Range(func(k, v any) bool {
		if now.Sub(v.(*sshPoolEntry).lastUsed) > sshIdleExpiry {
			toEvict = append(toEvict, k.(string))
		}
		return true
	})

	if len(toEvict) == 0 {
		return
	}

	s.sshPoolMu.Lock()
	defer s.sshPoolMu.Unlock()

	for _, key := range toEvict {
		if v, ok := s.sshPool.Load(key); ok {
			entry := v.(*sshPoolEntry)
			if now.Sub(entry.lastUsed) > sshIdleExpiry {
				entry.client.Close()
				s.sshPool.Delete(key)
				s.sshPoolSize--
			}
		}
	}
}
