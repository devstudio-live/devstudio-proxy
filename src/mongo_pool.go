package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mongoPoolEntry struct {
	client   *mongo.Client
	lastUsed time.Time
}

var (
	mongoPool     sync.Map
	mongoPoolMu   sync.Mutex
	mongoPoolSize int
)

// buildMongoURI constructs a MongoDB connection URI from a dbConnection.
func buildMongoURI(conn dbConnection) string {
	// Use the raw connection string when provided so that all options
	// (e.g. directConnection=true, authMechanism) are preserved as-is.
	// This is required for mongodb+srv:// URIs (SRV-only hostnames) and for
	// plain mongodb:// URIs that include options like directConnection=true,
	// which prevent the driver from doing replica-set discovery and trying to
	// resolve internal hostnames that are unreachable from outside the cluster.
	if conn.ConnectionString != "" {
		return conn.ConnectionString
	}
	port := conn.Port
	if port == 0 {
		port = 27017
	}
	if conn.User != "" && conn.Password != "" {
		authSource := conn.AuthSource
		if authSource == "" {
			authSource = "admin"
		}
		u := &url.URL{
			Scheme:   "mongodb",
			User:     url.UserPassword(conn.User, conn.Password),
			Host:     fmt.Sprintf("%s:%d", conn.Host, port),
			Path:     "/" + conn.Database,
			RawQuery: "authSource=" + url.QueryEscape(authSource),
		}
		return u.String()
	}
	return fmt.Sprintf("mongodb://%s:%d/%s", conn.Host, port, conn.Database)
}

// mongoConnectionKey returns a SHA-256 hash of the connection config for pool keying.
func mongoConnectionKey(conn dbConnection) string {
	b, _ := json.Marshal(conn)
	sum := sha256.Sum256(b)
	return "mongo:" + hex.EncodeToString(sum[:])
}

// getPooledMongoClient returns a pooled *mongo.Client, creating one if needed.
func getPooledMongoClient(conn dbConnection) (*mongo.Client, error) {
	key := mongoConnectionKey(conn)

	if v, ok := mongoPool.Load(key); ok {
		entry := v.(*mongoPoolEntry)
		entry.lastUsed = time.Now()
		return entry.client, nil
	}

	mongoPoolMu.Lock()
	defer mongoPoolMu.Unlock()

	// Double-check after acquiring the lock.
	if v, ok := mongoPool.Load(key); ok {
		entry := v.(*mongoPoolEntry)
		entry.lastUsed = time.Now()
		return entry.client, nil
	}

	if mongoPoolSize >= maxPoolSize {
		return nil, fmt.Errorf("MongoDB connection pool full (%d/%d)", mongoPoolSize, maxPoolSize)
	}

	uri := buildMongoURI(conn)
	opts := options.Client().ApplyURI(uri)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("mongodb connect: %w", err)
	}

	entry := &mongoPoolEntry{client: client, lastUsed: time.Now()}
	mongoPool.Store(key, entry)
	mongoPoolSize++

	return client, nil
}

// startMongoPoolReaper starts a background goroutine that disconnects idle MongoDB clients.
func startMongoPoolReaper() {
	ticker := time.NewTicker(reaperInterval)
	for range ticker.C {
		reapIdleMongoConnections()
	}
}

func reapIdleMongoConnections() {
	now := time.Now()
	var toEvict []string

	mongoPool.Range(func(k, v any) bool {
		entry := v.(*mongoPoolEntry)
		if now.Sub(entry.lastUsed) > idleExpiry {
			toEvict = append(toEvict, k.(string))
		}
		return true
	})

	if len(toEvict) == 0 {
		return
	}

	mongoPoolMu.Lock()
	defer mongoPoolMu.Unlock()

	for _, key := range toEvict {
		if v, ok := mongoPool.Load(key); ok {
			entry := v.(*mongoPoolEntry)
			if now.Sub(entry.lastUsed) > idleExpiry {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = entry.client.Disconnect(ctx)
				cancel()
				mongoPool.Delete(key)
				mongoPoolSize--
			}
		}
	}
}
