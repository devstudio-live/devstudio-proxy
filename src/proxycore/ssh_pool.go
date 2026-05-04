package proxycore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"time"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

// SSHConnection holds the parameters needed to open an SSH connection.
type SSHConnection struct {
	Host       string `json:"host"`
	Port       int    `json:"port,omitempty"`
	User       string `json:"user"`
	AuthMethod string `json:"authMethod"` // password | key | agent | keyboard-interactive
	Password   string `json:"password,omitempty"`
	PrivateKey string `json:"privateKey,omitempty"` // PEM content
	Passphrase string `json:"passphrase,omitempty"`
	Label      string `json:"label,omitempty"`
	Group      string `json:"group,omitempty"`
	// Phase 5 — jump hosts & advanced auth
	JumpHosts []SSHConnection `json:"jumpHosts,omitempty"` // ordered chain of proxy hops
	KIAnswers []string        `json:"kiAnswers,omitempty"` // pre-provided keyboard-interactive answers
}

const (
	maxSSHPoolSize    = 20
	sshIdleExpiry     = 10 * time.Minute
	sshReaperInterval = 30 * time.Second
)

type sshPoolEntry struct {
	client   *ssh.Client
	jumps    []*ssh.Client // intermediate jump clients, closed when evicted
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
		// agent, keyboard-interactive: no stable secret to include; identity is from env/agent
	}
	// Include jump host chain so different routes get different pool entries
	for _, jh := range conn.JumpHosts {
		jp := jh.Port
		if jp == 0 {
			jp = 22
		}
		raw += fmt.Sprintf("|%s:%d:%s:%s", jh.Host, jp, jh.User, jh.AuthMethod)
	}
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// buildSSHConfig constructs an ssh.ClientConfig for conn.
// challengeFn is used for keyboard-interactive auth when not nil;
// otherwise conn.KIAnswers provides pre-supplied answers.
func buildSSHConfig(conn SSHConnection, challengeFn ssh.KeyboardInteractiveChallenge) (*ssh.ClientConfig, error) {
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

	case "agent":
		socket := os.Getenv("SSH_AUTH_SOCK")
		if socket == "" {
			return nil, fmt.Errorf("SSH_AUTH_SOCK not set: no SSH agent available on the proxy host")
		}
		agentConn, err := net.Dial("unix", socket)
		if err != nil {
			return nil, fmt.Errorf("connect to SSH agent: %w", err)
		}
		// PublicKeysCallback keeps agentConn alive for signature operations during auth.
		authMethods = append(authMethods, ssh.PublicKeysCallback(agent.NewClient(agentConn).Signers))

	case "keyboard-interactive":
		fn := challengeFn
		if fn == nil {
			// Use pre-supplied answers in order; fill remaining questions with empty strings.
			answers := conn.KIAnswers
			fn = func(_, _ string, questions []string, _ []bool) ([]string, error) {
				result := make([]string, len(questions))
				for i := range questions {
					if i < len(answers) {
						result[i] = answers[i]
					}
				}
				return result, nil
			}
		}
		authMethods = append(authMethods, ssh.KeyboardInteractive(fn))

	default:
		return nil, fmt.Errorf("unsupported auth method: %q (use password, key, agent, or keyboard-interactive)", conn.AuthMethod)
	}

	timeout := 15 * time.Second
	if conn.AuthMethod == "keyboard-interactive" {
		timeout = 60 * time.Second // allow time for challenge/response UI
	}

	return &ssh.ClientConfig{
		User:            conn.User,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), //nolint:gosec // user-managed connections
		Timeout:         timeout,
	}, nil
}

func sshPort(conn SSHConnection) int {
	if conn.Port == 0 {
		return 22
	}
	return conn.Port
}

func closeSSHChain(clients []*ssh.Client) {
	for i := len(clients) - 1; i >= 0; i-- {
		clients[i].Close()
	}
}

// dialSSHWithJumps dials conn, optionally tunnelling through one or more jump hosts.
// Returns the final client plus a slice of intermediate jump clients (must be closed when done).
func dialSSHWithJumps(conn SSHConnection, challengeFn ssh.KeyboardInteractiveChallenge) (*ssh.Client, []*ssh.Client, error) {
	if len(conn.JumpHosts) == 0 {
		cfg, err := buildSSHConfig(conn, challengeFn)
		if err != nil {
			return nil, nil, err
		}
		addr := fmt.Sprintf("%s:%d", conn.Host, sshPort(conn))
		client, err := ssh.Dial("tcp", addr, cfg)
		if err != nil {
			return nil, nil, fmt.Errorf("ssh dial %s: %w", addr, err)
		}
		return client, nil, nil
	}

	// Dial first jump host directly
	jumps := make([]*ssh.Client, 0, len(conn.JumpHosts))

	first := conn.JumpHosts[0]
	firstCfg, err := buildSSHConfig(first, nil)
	if err != nil {
		return nil, nil, err
	}
	firstAddr := fmt.Sprintf("%s:%d", first.Host, sshPort(first))
	current, err := ssh.Dial("tcp", firstAddr, firstCfg)
	if err != nil {
		return nil, nil, fmt.Errorf("jump host 1 (%s): %w", firstAddr, err)
	}
	jumps = append(jumps, current)

	// Dial intermediate jump hosts through the previous hop
	for i, hop := range conn.JumpHosts[1:] {
		hopCfg, err := buildSSHConfig(hop, nil)
		if err != nil {
			closeSSHChain(jumps)
			return nil, nil, err
		}
		hopAddr := fmt.Sprintf("%s:%d", hop.Host, sshPort(hop))

		netConn, err := current.Dial("tcp", hopAddr)
		if err != nil {
			closeSSHChain(jumps)
			return nil, nil, fmt.Errorf("jump host %d (%s): %w", i+2, hopAddr, err)
		}

		c, chans, reqs, err := ssh.NewClientConn(netConn, hopAddr, hopCfg)
		if err != nil {
			netConn.Close()
			closeSSHChain(jumps)
			return nil, nil, fmt.Errorf("ssh handshake jump %d (%s): %w", i+2, hopAddr, err)
		}

		current = ssh.NewClient(c, chans, reqs)
		jumps = append(jumps, current)
	}

	// Dial the final target through the last jump
	finalCfg, err := buildSSHConfig(conn, challengeFn)
	if err != nil {
		closeSSHChain(jumps)
		return nil, nil, err
	}
	finalAddr := fmt.Sprintf("%s:%d", conn.Host, sshPort(conn))

	netConn, err := current.Dial("tcp", finalAddr)
	if err != nil {
		closeSSHChain(jumps)
		return nil, nil, fmt.Errorf("dial target %s through jump: %w", finalAddr, err)
	}

	c, chans, reqs, err := ssh.NewClientConn(netConn, finalAddr, finalCfg)
	if err != nil {
		netConn.Close()
		closeSSHChain(jumps)
		return nil, nil, fmt.Errorf("ssh handshake target %s: %w", finalAddr, err)
	}

	return ssh.NewClient(c, chans, reqs), jumps, nil
}

// isSSHClientAlive sends a lightweight keepalive global request.
// Returns true if the connection is live, false if it is dead.
func isSSHClientAlive(client *ssh.Client) bool {
	// SendRequest on a dead transport returns an error immediately.
	_, _, err := client.SendRequest("keepalive@devstudio", true, nil)
	return err == nil
}

// getOrDialSSHClient returns a pooled client for conn or dials a new one.
// challengeFn is forwarded to buildSSHConfig for keyboard-interactive auth.
// Stale/dead pool entries are evicted and re-dialed transparently.
func (s *Server) getOrDialSSHClient(conn SSHConnection, challengeFn ssh.KeyboardInteractiveChallenge) (*ssh.Client, error) {
	key := sshConnectionKey(conn)

	// Fast path: return pooled client only if the connection is still alive.
	if v, ok := s.sshPool.Load(key); ok {
		entry := v.(*sshPoolEntry)
		if isSSHClientAlive(entry.client) {
			entry.lastUsed = time.Now()
			return entry.client, nil
		}
		// Dead — evict under the pool lock below.
	}

	s.sshPoolMu.Lock()
	defer s.sshPoolMu.Unlock()

	// After acquiring the lock, evict any dead entry and re-check for a live one.
	if v, ok := s.sshPool.Load(key); ok {
		entry := v.(*sshPoolEntry)
		if isSSHClientAlive(entry.client) {
			entry.lastUsed = time.Now()
			return entry.client, nil
		}
		// Still dead — evict it so we can dial a fresh connection.
		entry.client.Close()
		closeSSHChain(entry.jumps)
		s.sshPool.Delete(key)
		s.sshPoolSize--
	}

	if s.sshPoolSize >= maxSSHPoolSize {
		return nil, fmt.Errorf("SSH connection pool full (%d/%d): disconnect unused connections first", s.sshPoolSize, maxSSHPoolSize)
	}

	client, jumps, err := dialSSHWithJumps(conn, challengeFn)
	if err != nil {
		return nil, err
	}

	s.sshPool.Store(key, &sshPoolEntry{client: client, jumps: jumps, lastUsed: time.Now()})
	s.sshPoolSize++
	return client, nil
}

// getPooledSSHClient is the standard entry point (no challenge fn, uses KIAnswers).
func (s *Server) getPooledSSHClient(conn SSHConnection) (*ssh.Client, error) {
	return s.getOrDialSSHClient(conn, nil)
}

func (s *Server) closeSSHConnection(key string) {
	s.sshPoolMu.Lock()
	defer s.sshPoolMu.Unlock()
	if v, ok := s.sshPool.Load(key); ok {
		entry := v.(*sshPoolEntry)
		entry.client.Close()
		closeSSHChain(entry.jumps)
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
				closeSSHChain(entry.jumps)
				s.sshPool.Delete(key)
				s.sshPoolSize--
			}
		}
	}
}
