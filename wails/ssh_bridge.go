package main

import (
	"encoding/base64"
	"encoding/json"
	"sync"

	"devstudio/proxy/proxycore"

	"github.com/google/uuid"
	"github.com/wailsapp/wails/v2/pkg/runtime"
)

// sshBridgeSession holds the per-terminal state owned by the Wails bridge.
// stream is nil until OpenTerminalStream returns successfully; ready is
// closed once stream is set (or once the open attempt fails) so that
// subsequent Stdin/Resize calls don't block indefinitely.
type sshBridgeSession struct {
	stream   *proxycore.TerminalStream
	kiCh     chan []string
	ready    chan struct{}
	readyMu  sync.Mutex
	readyDn  bool
	mu       sync.Mutex
	disposed bool
}

func (s *sshBridgeSession) markReady() {
	s.readyMu.Lock()
	defer s.readyMu.Unlock()
	if !s.readyDn {
		s.readyDn = true
		close(s.ready)
	}
}

// SSHOpenTerminal starts a new PTY streaming session. Returns immediately
// with {"ok":true,"sessionId":...} — the underlying SSH auth runs in a
// goroutine and the frontend is notified via ssh-ready-{id} / ssh-error-{id}
// events. Errors during open surface as ssh-error-{id} followed by ssh-exit-{id}.
func (a *App) SSHOpenTerminal(connJSON string, cols, rows int) string {
	var conn proxycore.SSHConnection
	if err := json.Unmarshal([]byte(connJSON), &conn); err != nil {
		return sshErrJSON(err.Error())
	}
	if cols <= 0 {
		cols = 80
	}
	if rows <= 0 {
		rows = 24
	}

	sessionID := uuid.New().String()
	session := &sshBridgeSession{
		kiCh:  make(chan []string, 1),
		ready: make(chan struct{}),
	}
	a.sshSessions.Store(sessionID, session)

	go func() {
		cb := proxycore.TerminalCallbacks{
			OnData: func(data []byte) {
				runtime.EventsEmit(a.ctx, "ssh-data-"+sessionID, base64.StdEncoding.EncodeToString(data))
			},
			OnExit: func() {
				runtime.EventsEmit(a.ctx, "ssh-exit-"+sessionID, "")
				a.sshSessions.Delete(sessionID)
			},
			OnKIChallenge: func(name, instruction string, questions []string, echos []bool) []string {
				payload, _ := json.Marshal(map[string]any{
					"type":        "ki-challenge",
					"name":        name,
					"instruction": instruction,
					"questions":   questions,
					"echos":       echos,
				})
				runtime.EventsEmit(a.ctx, "ssh-ki-challenge-"+sessionID, string(payload))
				answers := <-session.kiCh
				if answers == nil {
					answers = make([]string, len(questions))
				}
				return answers
			},
		}

		stream, err := a.proxy.OpenTerminalStream(conn, cols, rows, cb)
		if err != nil {
			runtime.EventsEmit(a.ctx, "ssh-error-"+sessionID, err.Error())
			runtime.EventsEmit(a.ctx, "ssh-exit-"+sessionID, "")
			a.sshSessions.Delete(sessionID)
			session.markReady()
			return
		}

		session.mu.Lock()
		if session.disposed {
			// Close was called before auth finished — tear the stream down.
			session.mu.Unlock()
			stream.Close()
			session.markReady()
			return
		}
		session.stream = stream
		session.mu.Unlock()
		session.markReady()
		runtime.EventsEmit(a.ctx, "ssh-ready-"+sessionID, "")
	}()

	resp, _ := json.Marshal(map[string]any{"ok": true, "sessionId": sessionID})
	return string(resp)
}

// SSHStdin forwards base64-encoded bytes to the PTY stdin of an open session.
func (a *App) SSHStdin(sessionID string, b64 string) string {
	sess := a.lookupSSHSession(sessionID)
	if sess == nil {
		return sshErrJSON("unknown session")
	}
	<-sess.ready
	sess.mu.Lock()
	stream := sess.stream
	sess.mu.Unlock()
	if stream == nil {
		return sshErrJSON("session not open")
	}
	data, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return sshErrJSON(err.Error())
	}
	if err := stream.Write(data); err != nil {
		return sshErrJSON(err.Error())
	}
	return `{"ok":true}`
}

// SSHResize sends a PTY window-change.
func (a *App) SSHResize(sessionID string, cols, rows int) string {
	sess := a.lookupSSHSession(sessionID)
	if sess == nil {
		return sshErrJSON("unknown session")
	}
	<-sess.ready
	sess.mu.Lock()
	stream := sess.stream
	sess.mu.Unlock()
	if stream == nil {
		return sshErrJSON("session not open")
	}
	if err := stream.Resize(cols, rows); err != nil {
		return sshErrJSON(err.Error())
	}
	return `{"ok":true}`
}

// SSHCloseTerminal tears down a streaming session. Safe to call if the
// session has already exited.
func (a *App) SSHCloseTerminal(sessionID string) string {
	v, ok := a.sshSessions.LoadAndDelete(sessionID)
	if !ok {
		return `{"ok":true}`
	}
	sess := v.(*sshBridgeSession)
	sess.mu.Lock()
	sess.disposed = true
	stream := sess.stream
	sess.mu.Unlock()
	if stream != nil {
		stream.Close()
	}
	// Unblock any pending KI challenge waiter so the goroutine can exit.
	select {
	case sess.kiCh <- nil:
	default:
	}
	return `{"ok":true}`
}

// SSHSubmitKIResponse delivers keyboard-interactive answers for the pending
// challenge on a session.
func (a *App) SSHSubmitKIResponse(sessionID string, answersJSON string) string {
	sess := a.lookupSSHSession(sessionID)
	if sess == nil {
		return sshErrJSON("unknown session")
	}
	var answers []string
	if err := json.Unmarshal([]byte(answersJSON), &answers); err != nil {
		return sshErrJSON(err.Error())
	}
	select {
	case sess.kiCh <- answers:
		return `{"ok":true}`
	default:
		return sshErrJSON("no pending challenge")
	}
}

func (a *App) lookupSSHSession(sessionID string) *sshBridgeSession {
	v, ok := a.sshSessions.Load(sessionID)
	if !ok {
		return nil
	}
	return v.(*sshBridgeSession)
}

func sshErrJSON(msg string) string {
	b, _ := json.Marshal(map[string]any{"ok": false, "error": msg})
	return string(b)
}
