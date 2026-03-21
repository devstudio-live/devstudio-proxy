package main

import (
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

type elasticPoolEntry struct {
	client   *elasticsearch.Client
	lastUsed time.Time
}

var (
	elasticPool     sync.Map
	elasticPoolMu   sync.Mutex
	elasticPoolSize int
)

// buildElasticAddresses returns the base URL for the Elasticsearch node.
func buildElasticAddresses(conn dbConnection) []string {
	port := conn.Port
	if port == 0 {
		port = 9200
	}
	scheme := "http"
	if conn.SSL == "require" || conn.SSL == "https" {
		scheme = "https"
	}
	return []string{fmt.Sprintf("%s://%s:%d", scheme, conn.Host, port)}
}

// elasticConnectionKey returns a SHA-256 hash of the connection config for pool keying.
func elasticConnectionKey(conn dbConnection) string {
	b, _ := json.Marshal(conn)
	sum := sha256.Sum256(b)
	return "elastic:" + hex.EncodeToString(sum[:])
}

// getPooledElasticClient returns a cached *elasticsearch.Client, creating one if needed.
func getPooledElasticClient(conn dbConnection) (*elasticsearch.Client, error) {
	key := elasticConnectionKey(conn)

	if v, ok := elasticPool.Load(key); ok {
		entry := v.(*elasticPoolEntry)
		entry.lastUsed = time.Now()
		return entry.client, nil
	}

	elasticPoolMu.Lock()
	defer elasticPoolMu.Unlock()

	// Double-check after acquiring the lock.
	if v, ok := elasticPool.Load(key); ok {
		entry := v.(*elasticPoolEntry)
		entry.lastUsed = time.Now()
		return entry.client, nil
	}

	if elasticPoolSize >= maxPoolSize {
		return nil, fmt.Errorf("Elasticsearch connection pool full (%d/%d)", elasticPoolSize, maxPoolSize)
	}

	cfg := elasticsearch.Config{
		Addresses: buildElasticAddresses(conn),
		// Skip TLS verification for dev use (self-signed certs).
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
		},
	}
	if conn.User != "" {
		cfg.Username = conn.User
		cfg.Password = conn.Password
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("elasticsearch client: %w", err)
	}

	entry := &elasticPoolEntry{client: client, lastUsed: time.Now()}
	elasticPool.Store(key, entry)
	elasticPoolSize++

	return client, nil
}

// startElasticPoolReaper starts a background goroutine that evicts idle Elasticsearch clients.
func startElasticPoolReaper() {
	ticker := time.NewTicker(reaperInterval)
	for range ticker.C {
		reapIdleElasticConnections()
	}
}

func reapIdleElasticConnections() {
	now := time.Now()
	var toEvict []string

	elasticPool.Range(func(k, v any) bool {
		entry := v.(*elasticPoolEntry)
		if now.Sub(entry.lastUsed) > idleExpiry {
			toEvict = append(toEvict, k.(string))
		}
		return true
	})

	if len(toEvict) == 0 {
		return
	}

	elasticPoolMu.Lock()
	defer elasticPoolMu.Unlock()

	for _, key := range toEvict {
		if v, ok := elasticPool.Load(key); ok {
			entry := v.(*elasticPoolEntry)
			// Re-check under lock — it may have been used since the range scan.
			if now.Sub(entry.lastUsed) > idleExpiry {
				// Elasticsearch client has no explicit close — just evict from pool.
				elasticPool.Delete(key)
				elasticPoolSize--
			}
		}
	}
}
