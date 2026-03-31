package proxycore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// DBConnection holds the parameters needed to open a database connection.
type DBConnection struct {
	Driver           string `json:"driver"`
	Host             string `json:"host"`
	Port             int    `json:"port"`
	Database         string `json:"database"`
	User             string `json:"user"`
	Password         string `json:"password"`
	SSL              string `json:"ssl"`
	File             string `json:"file"`
	AuthSource       string `json:"authSource,omitempty"`
	ConnectionString string `json:"connectionString,omitempty"`
}

const (
	maxPoolSize    = 20
	idleExpiry     = 5 * time.Minute
	reaperInterval = 30 * time.Second
)

type poolEntry struct {
	db       *sql.DB
	lastUsed time.Time
}

func connectionKey(conn DBConnection) string {
	b, _ := json.Marshal(conn)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func (s *Server) getPooledDB(conn DBConnection) (*sql.DB, error) {
	key := connectionKey(conn)

	if v, ok := s.sqlPool.Load(key); ok {
		entry := v.(*poolEntry)
		entry.lastUsed = time.Now()
		return entry.db, nil
	}

	s.sqlPoolMu.Lock()
	defer s.sqlPoolMu.Unlock()

	if v, ok := s.sqlPool.Load(key); ok {
		entry := v.(*poolEntry)
		entry.lastUsed = time.Now()
		return entry.db, nil
	}

	if s.sqlPoolSize >= maxPoolSize {
		return nil, fmt.Errorf("connection pool full (%d/%d): close unused connections first", s.sqlPoolSize, maxPoolSize)
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

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(10 * time.Minute)
	db.SetConnMaxIdleTime(idleExpiry)

	entry := &poolEntry{db: db, lastUsed: time.Now()}
	s.sqlPool.Store(key, entry)
	s.sqlPoolSize++

	return db, nil
}

func (s *Server) startPoolReaper() {
	ticker := time.NewTicker(reaperInterval)
	for range ticker.C {
		s.reapIdleConnections()
	}
}

func (s *Server) reapIdleConnections() {
	now := time.Now()
	var toEvict []string

	s.sqlPool.Range(func(k, v any) bool {
		entry := v.(*poolEntry)
		if now.Sub(entry.lastUsed) > idleExpiry {
			toEvict = append(toEvict, k.(string))
		}
		return true
	})

	if len(toEvict) == 0 {
		return
	}

	s.sqlPoolMu.Lock()
	defer s.sqlPoolMu.Unlock()

	for _, key := range toEvict {
		if v, ok := s.sqlPool.Load(key); ok {
			entry := v.(*poolEntry)
			if now.Sub(entry.lastUsed) > idleExpiry {
				entry.db.Close()
				s.sqlPool.Delete(key)
				s.sqlPoolSize--
			}
		}
	}
}
