package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// dbConnection holds the parameters needed to open a database connection.
type dbConnection struct {
	Driver   string `json:"driver"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	User     string `json:"user"`
	Password string `json:"password"`
	SSL      string `json:"ssl"`
	// File is used by SQLite instead of host/port.
	File string `json:"file"`
	// AuthSource is the authentication database for MongoDB (default: "admin").
	AuthSource string `json:"authSource,omitempty"`
}

const (
	maxPoolSize    = 20
	idleExpiry     = 5 * time.Minute
	reaperInterval = 30 * time.Second
)

type poolEntry struct {
	db        *sql.DB
	lastUsed  time.Time
}

var (
	pool   sync.Map // map[string]*poolEntry
	poolMu sync.Mutex
	// poolSize tracks the current number of open connections for cap enforcement.
	poolSize int
)

// connectionKey returns a stable SHA-256 hash of the connection config,
// used as the pool map key.
func connectionKey(conn dbConnection) string {
	b, _ := json.Marshal(conn)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// getPooledDB returns a pooled *sql.DB for the given connection config,
// opening a new connection if none exists or if the existing one is unhealthy.
func getPooledDB(conn dbConnection) (*sql.DB, error) {
	key := connectionKey(conn)

	if v, ok := pool.Load(key); ok {
		entry := v.(*poolEntry)
		entry.lastUsed = time.Now()
		return entry.db, nil
	}

	// Open a new connection under the pool mutex to avoid duplicate opens.
	poolMu.Lock()
	defer poolMu.Unlock()

	// Double-check after acquiring the lock.
	if v, ok := pool.Load(key); ok {
		entry := v.(*poolEntry)
		entry.lastUsed = time.Now()
		return entry.db, nil
	}

	if poolSize >= maxPoolSize {
		return nil, fmt.Errorf("connection pool full (%d/%d): close unused connections first", poolSize, maxPoolSize)
	}

	dsn, err := buildDSN(conn)
	if err != nil {
		return nil, err
	}

	driverName, err := driverNameFor(conn.Driver)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", conn.Driver, err)
	}

	// Conservative pool settings — this proxy serves a single developer.
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(10 * time.Minute)
	db.SetConnMaxIdleTime(idleExpiry)

	entry := &poolEntry{db: db, lastUsed: time.Now()}
	pool.Store(key, entry)
	poolSize++

	return db, nil
}

// startPoolReaper starts a background goroutine that closes idle connections.
func startPoolReaper() {
	ticker := time.NewTicker(reaperInterval)
	for range ticker.C {
		reapIdleConnections()
	}
}

func reapIdleConnections() {
	now := time.Now()
	var toEvict []string

	pool.Range(func(k, v any) bool {
		entry := v.(*poolEntry)
		if now.Sub(entry.lastUsed) > idleExpiry {
			toEvict = append(toEvict, k.(string))
		}
		return true
	})

	if len(toEvict) == 0 {
		return
	}

	poolMu.Lock()
	defer poolMu.Unlock()

	for _, key := range toEvict {
		if v, ok := pool.Load(key); ok {
			entry := v.(*poolEntry)
			// Re-check under lock — it may have been used since the range scan.
			if now.Sub(entry.lastUsed) > idleExpiry {
				entry.db.Close()
				pool.Delete(key)
				poolSize--
			}
		}
	}
}
