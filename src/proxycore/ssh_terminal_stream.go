package proxycore

import (
	"fmt"
	"io"
	"sync"

	"golang.org/x/crypto/ssh"
)

// TerminalCallbacks is the transport-agnostic fan-out for a terminal stream.
// OnData receives stdout+stderr bytes (the slice is safe to retain).
// OnExit fires exactly once after the shell terminates and the session is closed.
// OnKIChallenge, if set, is called synchronously during auth for each
// keyboard-interactive round and must return one answer per question.
type TerminalCallbacks struct {
	OnData        func(data []byte)
	OnExit        func()
	OnKIChallenge func(name, instruction string, questions []string, echos []bool) []string
}

// TerminalStream is a handle for an active PTY streaming session opened by
// OpenTerminalStream. The SSH client itself is owned by the pool; only the
// session layer is closed on Close.
type TerminalStream struct {
	session  *ssh.Session
	stdin    io.WriteCloser
	closed   bool
	closedMu sync.Mutex
}

// Write forwards bytes to the PTY stdin.
func (t *TerminalStream) Write(data []byte) error {
	if _, err := t.stdin.Write(data); err != nil {
		return err
	}
	return nil
}

// Resize sends a PTY window-change request.
func (t *TerminalStream) Resize(cols, rows int) error {
	return t.session.WindowChange(rows, cols)
}

// Close terminates the shell session. OnExit will fire once the wait goroutine
// observes the close.
func (t *TerminalStream) Close() error {
	t.closedMu.Lock()
	if t.closed {
		t.closedMu.Unlock()
		return nil
	}
	t.closed = true
	t.closedMu.Unlock()
	return t.session.Close()
}

// OpenTerminalStream dials (via the pool) and starts a PTY shell session for
// conn, wiring stdout/stderr to cb.OnData and session exit to cb.OnExit. The
// returned handle is used for stdin, resize, and close. On failure no handle
// is returned and cb.OnExit is NOT invoked — the caller is responsible for
// surfacing the error to the consumer.
func (s *Server) OpenTerminalStream(conn SSHConnection, cols, rows int, cb TerminalCallbacks) (*TerminalStream, error) {
	if cols <= 0 {
		cols = 80
	}
	if rows <= 0 {
		rows = 24
	}

	var challengeFn ssh.KeyboardInteractiveChallenge
	if conn.AuthMethod == "keyboard-interactive" && cb.OnKIChallenge != nil {
		challengeFn = func(name, instruction string, questions []string, echos []bool) ([]string, error) {
			return cb.OnKIChallenge(name, instruction, questions, echos), nil
		}
	}

	client, err := s.getOrDialSSHClient(conn, challengeFn)
	if err != nil {
		return nil, err
	}

	sess, err := client.NewSession()
	if err != nil {
		return nil, fmt.Errorf("create session: %w", err)
	}

	stdin, err := sess.StdinPipe()
	if err != nil {
		sess.Close()
		return nil, fmt.Errorf("stdin pipe: %w", err)
	}
	stdout, err := sess.StdoutPipe()
	if err != nil {
		sess.Close()
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	stderr, err := sess.StderrPipe()
	if err != nil {
		sess.Close()
		return nil, fmt.Errorf("stderr pipe: %w", err)
	}

	modes := ssh.TerminalModes{
		ssh.ECHO:          1,
		ssh.TTY_OP_ISPEED: 14400,
		ssh.TTY_OP_OSPEED: 14400,
	}
	if err := sess.RequestPty("xterm-256color", rows, cols, modes); err != nil {
		sess.Close()
		return nil, fmt.Errorf("request pty: %w", err)
	}
	if err := sess.Shell(); err != nil {
		sess.Close()
		return nil, fmt.Errorf("start shell: %w", err)
	}

	stream := &TerminalStream{session: sess, stdin: stdin}

	go pumpTerminalPipe(stdout, cb.OnData)
	go pumpTerminalPipe(stderr, cb.OnData)

	go func() {
		_ = sess.Wait()
		sess.Close()
		if cb.OnExit != nil {
			cb.OnExit()
		}
	}()

	return stream, nil
}

func pumpTerminalPipe(r io.Reader, onData func([]byte)) {
	buf := make([]byte, 32*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 && onData != nil {
			dup := make([]byte, n)
			copy(dup, buf[:n])
			onData(dup)
		}
		if err != nil {
			return
		}
	}
}
