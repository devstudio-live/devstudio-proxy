package proxycore

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	contextTTL          = 15 * time.Minute
	contextReapInterval = 30 * time.Second
	maxContextEntries   = 200
	maxEntrySize        = 10 * 1024 * 1024  // 10MB
	maxTotalSize        = 100 * 1024 * 1024 // 100MB
)

type contextEntry struct {
	data      []byte
	kind      string
	createdAt time.Time
}

// StoreContext stores data in the context cache under a new UUID key.
func (s *Server) StoreContext(data []byte, kind string) (string, error) {
	if len(data) > maxEntrySize {
		return "", fmt.Errorf("payload too large: %d bytes (max %d)", len(data), maxEntrySize)
	}
	if int(s.contextTotalSize.Load())+len(data) > maxTotalSize {
		return "", fmt.Errorf("context cache full: total size limit (%d bytes) would be exceeded", maxTotalSize)
	}
	if int(s.contextCount.Load()) >= maxContextEntries {
		return "", fmt.Errorf("context cache full: entry limit (%d) reached", maxContextEntries)
	}

	id := uuid.New().String()
	entry := &contextEntry{
		data:      data,
		kind:      kind,
		createdAt: time.Now(),
	}
	s.contextCache.Store(id, entry)
	s.contextTotalSize.Add(int64(len(data)))
	s.contextCount.Add(1)

	return id, nil
}

// LoadContext retrieves a context entry by UUID.
func (s *Server) LoadContext(id string) ([]byte, string, bool) {
	v, ok := s.contextCache.Load(id)
	if !ok {
		return nil, "", false
	}
	entry := v.(*contextEntry)
	if time.Since(entry.createdAt) > contextTTL {
		s.contextCache.Delete(id)
		s.contextTotalSize.Add(-int64(len(entry.data)))
		s.contextCount.Add(-1)
		return nil, "", false
	}
	return entry.data, entry.kind, true
}

func (s *Server) startContextReaper() {
	ticker := time.NewTicker(contextReapInterval)
	for range ticker.C {
		s.reapExpiredContexts()
	}
}

func (s *Server) reapExpiredContexts() {
	now := time.Now()
	s.contextCache.Range(func(k, v any) bool {
		entry := v.(*contextEntry)
		if now.Sub(entry.createdAt) > contextTTL {
			s.contextCache.Delete(k)
			s.contextTotalSize.Add(-int64(len(entry.data)))
			s.contextCount.Add(-1)
		}
		return true
	})
}
