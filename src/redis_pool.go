package main

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisPoolEntry struct {
	client   *redis.Client
	lastUsed time.Time
}

var (
	redisPool     sync.Map
	redisPoolMu   sync.Mutex
	redisPoolSize int
)

// buildRedisOptions constructs *redis.Options from a dbConnection.
// The Database field is treated as a DB index (0–15).
func buildRedisOptions(conn dbConnection) *redis.Options {
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
func redisConnectionKey(conn dbConnection) string {
	b, _ := json.Marshal(conn)
	sum := sha256.Sum256(b)
	return "redis:" + hex.EncodeToString(sum[:])
}

// getPooledRedisClient returns a pooled *redis.Client, creating one if needed.
func getPooledRedisClient(conn dbConnection) (*redis.Client, error) {
	key := redisConnectionKey(conn)

	if v, ok := redisPool.Load(key); ok {
		entry := v.(*redisPoolEntry)
		entry.lastUsed = time.Now()
		return entry.client, nil
	}

	redisPoolMu.Lock()
	defer redisPoolMu.Unlock()

	// Double-check after acquiring the lock.
	if v, ok := redisPool.Load(key); ok {
		entry := v.(*redisPoolEntry)
		entry.lastUsed = time.Now()
		return entry.client, nil
	}

	if redisPoolSize >= maxPoolSize {
		return nil, fmt.Errorf("Redis connection pool full (%d/%d)", redisPoolSize, maxPoolSize)
	}

	client := redis.NewClient(buildRedisOptions(conn))

	entry := &redisPoolEntry{client: client, lastUsed: time.Now()}
	redisPool.Store(key, entry)
	redisPoolSize++

	return client, nil
}

// startRedisPoolReaper starts a background goroutine that closes idle Redis clients.
func startRedisPoolReaper() {
	ticker := time.NewTicker(reaperInterval)
	for range ticker.C {
		reapIdleRedisConnections()
	}
}

func reapIdleRedisConnections() {
	now := time.Now()
	var toEvict []string

	redisPool.Range(func(k, v any) bool {
		entry := v.(*redisPoolEntry)
		if now.Sub(entry.lastUsed) > idleExpiry {
			toEvict = append(toEvict, k.(string))
		}
		return true
	})

	if len(toEvict) == 0 {
		return
	}

	redisPoolMu.Lock()
	defer redisPoolMu.Unlock()

	for _, key := range toEvict {
		if v, ok := redisPool.Load(key); ok {
			entry := v.(*redisPoolEntry)
			if now.Sub(entry.lastUsed) > idleExpiry {
				_ = entry.client.Close()
				redisPool.Delete(key)
				redisPoolSize--
			}
		}
	}
}
