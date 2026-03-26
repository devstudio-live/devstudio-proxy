package main

import (
	"fmt"
	"sync"
	"sync/atomic"
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
	kind      string // "snapshot" or "object"
	createdAt time.Time
}

var (
	contextCache     sync.Map
	contextTotalSize atomic.Int64
	contextCount     atomic.Int32
)

// storeContext stores data in the context cache under a new UUID key.
// kind should be "snapshot" for auto-triggered MCP snapshot fallback,
// or "object" for explicitly stored arbitrary objects.
func storeContext(data []byte, kind string) (string, error) {
	if len(data) > maxEntrySize {
		return "", fmt.Errorf("payload too large: %d bytes (max %d)", len(data), maxEntrySize)
	}
	if int(contextTotalSize.Load())+len(data) > maxTotalSize {
		return "", fmt.Errorf("context cache full: total size limit (%d bytes) would be exceeded", maxTotalSize)
	}
	if int(contextCount.Load()) >= maxContextEntries {
		return "", fmt.Errorf("context cache full: entry limit (%d) reached", maxContextEntries)
	}

	id := uuid.New().String()
	entry := &contextEntry{
		data:      data,
		kind:      kind,
		createdAt: time.Now(),
	}
	contextCache.Store(id, entry)
	contextTotalSize.Add(int64(len(data)))
	contextCount.Add(1)

	return id, nil
}

// loadContext retrieves a context entry by UUID.
// Returns (data, kind, true) on hit, ("", "", false) on miss or expiry.
func loadContext(id string) ([]byte, string, bool) {
	v, ok := contextCache.Load(id)
	if !ok {
		return nil, "", false
	}
	entry := v.(*contextEntry)
	if time.Since(entry.createdAt) > contextTTL {
		contextCache.Delete(id)
		contextTotalSize.Add(-int64(len(entry.data)))
		contextCount.Add(-1)
		return nil, "", false
	}
	return entry.data, entry.kind, true
}

// startContextReaper starts a background goroutine that evicts expired entries.
// Mirrors the startPoolReaper pattern in db_pool.go.
func startContextReaper() {
	ticker := time.NewTicker(contextReapInterval)
	for range ticker.C {
		reapExpiredContexts()
	}
}

func reapExpiredContexts() {
	now := time.Now()
	contextCache.Range(func(k, v any) bool {
		entry := v.(*contextEntry)
		if now.Sub(entry.createdAt) > contextTTL {
			contextCache.Delete(k)
			contextTotalSize.Add(-int64(len(entry.data)))
			contextCount.Add(-1)
		}
		return true
	})
}
