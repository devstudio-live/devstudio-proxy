package proxycore

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisPoolEntry struct {
	client   *redis.Client
	lastUsed time.Time
}

// buildRedisOptions constructs *redis.Options from a DBConnection.
// The Database field is treated as a DB index (0–15).
func buildRedisOptions(conn DBConnection) *redis.Options {
	port := conn.Port
	if port == 0 {
		port = 6379
	}

	db := 0
	if conn.Database != "" {
		fmt.Sscanf(conn.Database, "%d", &db)
	}

	opts := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", conn.Host, port),
		Password: conn.Password,
		DB:       db,
	}
	if conn.User != "" {
		opts.Username = conn.User
	}
	if conn.SSL == "require" || conn.SSL == "prefer" {
		opts.TLSConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec
	}
	return opts
}

// redisConnectionKey returns a SHA-256 hash of the connection config for pool keying.
func redisConnectionKey(conn DBConnection) string {
	b, _ := json.Marshal(conn)
	sum := sha256.Sum256(b)
	return "redis:" + hex.EncodeToString(sum[:])
}

// getPooledRedisClient returns a pooled *redis.Client, creating one if needed.
func (s *Server) getPooledRedisClient(conn DBConnection) (*redis.Client, error) {
	key := redisConnectionKey(conn)

	if v, ok := s.redisPool.Load(key); ok {
		entry := v.(*redisPoolEntry)
		entry.lastUsed = time.Now()
		return entry.client, nil
	}

	s.redisPoolMu.Lock()
	defer s.redisPoolMu.Unlock()

	// Double-check after acquiring the lock.
	if v, ok := s.redisPool.Load(key); ok {
		entry := v.(*redisPoolEntry)
		entry.lastUsed = time.Now()
		return entry.client, nil
	}

	if s.redisPoolSize >= maxPoolSize {
		return nil, fmt.Errorf("Redis connection pool full (%d/%d)", s.redisPoolSize, maxPoolSize)
	}

	client := redis.NewClient(buildRedisOptions(conn))

	entry := &redisPoolEntry{client: client, lastUsed: time.Now()}
	s.redisPool.Store(key, entry)
	s.redisPoolSize++

	return client, nil
}

// startRedisPoolReaper starts a background goroutine that closes idle Redis clients.
func (s *Server) startRedisPoolReaper() {
	ticker := time.NewTicker(reaperInterval)
	for range ticker.C {
		s.reapIdleRedisConnections()
	}
}

func (s *Server) reapIdleRedisConnections() {
	now := time.Now()
	var toEvict []string

	s.redisPool.Range(func(k, v any) bool {
		entry := v.(*redisPoolEntry)
		if now.Sub(entry.lastUsed) > idleExpiry {
			toEvict = append(toEvict, k.(string))
		}
		return true
	})

	if len(toEvict) == 0 {
		return
	}

	s.redisPoolMu.Lock()
	defer s.redisPoolMu.Unlock()

	for _, key := range toEvict {
		if v, ok := s.redisPool.Load(key); ok {
			entry := v.(*redisPoolEntry)
			if now.Sub(entry.lastUsed) > idleExpiry {
				_ = entry.client.Close()
				s.redisPool.Delete(key)
				s.redisPoolSize--
			}
		}
	}
}
