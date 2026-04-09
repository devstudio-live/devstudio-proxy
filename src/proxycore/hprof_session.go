package proxycore

import (
	"os"
	"sync"
	"time"
)

// ── Session Cache (Phase 3) ────────────────────────────────────────────────
// Keyed by (path, mtime, size). 1-hr TTL, max 2 resident sessions.

const (
	sessionTTL          = 1 * time.Hour
	sessionMaxResident  = 2
	sessionReapInterval = 5 * time.Minute
)

type sessionKey struct {
	Path  string
	Mtime int64 // UnixNano
	Size  int64
}

type hprofSession struct {
	Key       sessionKey
	Result    *HprofResult
	JobID     string
	CreatedAt time.Time
}

// HprofSessionStore caches parsed HPROF results for instant re-open.
type HprofSessionStore struct {
	mu       sync.Mutex
	sessions []*hprofSession
}

// NewHprofSessionStore creates an empty session cache.
func NewHprofSessionStore() *HprofSessionStore {
	return &HprofSessionStore{}
}

// Lookup returns a cached session if (path, mtime, size) matches and TTL has not expired.
func (ss *HprofSessionStore) Lookup(path string) (*hprofSession, bool) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, false
	}
	key := sessionKey{
		Path:  path,
		Mtime: info.ModTime().UnixNano(),
		Size:  info.Size(),
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()

	for _, s := range ss.sessions {
		if s.Key == key && time.Since(s.CreatedAt) < sessionTTL {
			return s, true
		}
	}
	return nil, false
}

// Store caches a parsed result. Evicts the oldest session if at capacity.
func (ss *HprofSessionStore) Store(path string, result *HprofResult, jobID string) {
	info, err := os.Stat(path)
	if err != nil {
		return
	}
	key := sessionKey{
		Path:  path,
		Mtime: info.ModTime().UnixNano(),
		Size:  info.Size(),
	}

	ss.mu.Lock()
	defer ss.mu.Unlock()

	// Update if already cached
	for i, s := range ss.sessions {
		if s.Key == key {
			ss.sessions[i].Result = result
			ss.sessions[i].JobID = jobID
			ss.sessions[i].CreatedAt = time.Now()
			return
		}
	}

	// Evict oldest if at capacity
	if len(ss.sessions) >= sessionMaxResident {
		oldest := 0
		for i := 1; i < len(ss.sessions); i++ {
			if ss.sessions[i].CreatedAt.Before(ss.sessions[oldest].CreatedAt) {
				oldest = i
			}
		}
		if ss.sessions[oldest].Result != nil {
			ss.sessions[oldest].Result.Close()
		}
		ss.sessions = append(ss.sessions[:oldest], ss.sessions[oldest+1:]...)
	}

	ss.sessions = append(ss.sessions, &hprofSession{
		Key:       key,
		Result:    result,
		JobID:     jobID,
		CreatedAt: time.Now(),
	})
}

// Reap removes sessions that have exceeded their TTL.
func (ss *HprofSessionStore) Reap() {
	ss.mu.Lock()
	defer ss.mu.Unlock()

	now := time.Now()
	kept := make([]*hprofSession, 0, len(ss.sessions))
	for _, s := range ss.sessions {
		if now.Sub(s.CreatedAt) < sessionTTL {
			kept = append(kept, s)
		} else if s.Result != nil {
			s.Result.Close()
		}
	}
	ss.sessions = kept
}

// Len returns the number of cached sessions.
func (ss *HprofSessionStore) Len() int {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return len(ss.sessions)
}

// startSessionReaper runs the reaper on a ticker.
func (s *Server) startSessionReaper() {
	ticker := time.NewTicker(sessionReapInterval)
	for range ticker.C {
		s.hprofSessions.Reap()
	}
}
