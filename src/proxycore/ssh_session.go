package proxycore

import "time"

const (
	sshSessionExpiry       = 1 * time.Hour
	sshSessionReapInterval = 5 * time.Minute
)

// startSSHSessionReaper periodically closes orphaned SSH terminal sessions.
func (s *Server) startSSHSessionReaper() {
	ticker := time.NewTicker(sshSessionReapInterval)
	for range ticker.C {
		s.reapOrphanedSSHSessions()
	}
}

func (s *Server) reapOrphanedSSHSessions() {
	now := time.Now()
	s.sshSessions.Range(func(k, v any) bool {
		sess := v.(*sshTerminalSession)
		if now.Sub(sess.CreatedAt) > sshSessionExpiry {
			sess.Session.Close()
			s.sshSessions.Delete(k)
		}
		return true
	})
}
