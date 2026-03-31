package proxycore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mongoPoolEntry struct {
	client   *mongo.Client
	lastUsed time.Time
}

// buildMongoURI constructs a MongoDB connection URI from a DBConnection.
func buildMongoURI(conn DBConnection) string {
	// Use the raw connection string when provided so that all options
	// (e.g. directConnection=true, authMechanism) are preserved as-is.
	// This is required for mongodb+srv:// URIs (SRV-only hostnames) and for
	// plain mongodb:// URIs that include options like directConnection=true,
	// which prevent the driver from doing replica-set discovery and trying to
	// resolve internal hostnames that are unreachable from outside the cluster.
	if conn.ConnectionString != "" {
		encoded := encodeMongoCredentials(conn.ConnectionString)
		log.Printf("mongo: connection string (sent to driver): %s", redactMongoURI(encoded))
		return encoded
	}
	port := conn.Port
	if port == 0 {
		port = 27017
	}
	var uri string
	if conn.User != "" && conn.Password != "" {
		// Decode first in case the credentials are already percent-encoded,
		// then re-encode via url.UserPassword to avoid double-encoding.
		decodedUser, err := url.PathUnescape(conn.User)
		if err != nil {
			decodedUser = conn.User
		}
		decodedPass, err := url.PathUnescape(conn.Password)
		if err != nil {
			decodedPass = conn.Password
		}
		u := &url.URL{
			Scheme: "mongodb",
			User:   url.UserPassword(decodedUser, decodedPass),
			Host:   fmt.Sprintf("%s:%d", conn.Host, port),
			Path:   "/",
		}
		uri = u.String()
		log.Printf("mongo: built URI (sent to driver): %s", redactMongoURI(uri))
	} else {
		uri = fmt.Sprintf("mongodb://%s:%d/", conn.Host, port)
		log.Printf("mongo: built URI (no credentials): %s", uri)
	}
	return uri
}

// encodeMongoCredentials extracts the username and password from a MongoDB
// connection string using string manipulation (not url.Parse, which fails when
// the password contains unencoded special characters like @, /, ?), then
// re-encodes them with url.UserPassword so the driver receives a valid URI.
func encodeMongoCredentials(cs string) string {
	// Find scheme ("mongodb://" or "mongodb+srv://").
	schemeEnd := strings.Index(cs, "://")
	if schemeEnd < 0 {
		return cs
	}
	scheme := cs[:schemeEnd+3]
	rest := cs[schemeEnd+3:]

	// The authority section ends at the first "/" or "?".
	// Find the last "@" within that section to split userinfo from host.
	slashIdx := strings.IndexAny(rest, "/?")
	authority := rest
	afterAuthority := ""
	if slashIdx >= 0 {
		authority = rest[:slashIdx]
		afterAuthority = rest[slashIdx:]
	}

	atIdx := strings.LastIndex(authority, "@")
	if atIdx < 0 {
		// No credentials in the connection string.
		return cs
	}

	userinfo := authority[:atIdx]
	hostpart := authority[atIdx+1:]

	// Split userinfo at the first ":" to get username and password.
	colonIdx := strings.Index(userinfo, ":")
	var username, password string
	if colonIdx < 0 {
		username = userinfo
	} else {
		username = userinfo[:colonIdx]
		password = userinfo[colonIdx+1:]
	}

	// Decode first in case the credentials are already percent-encoded,
	// then re-encode via url.UserPassword to avoid double-encoding.
	decodedUser, err := url.PathUnescape(username)
	if err != nil {
		decodedUser = username
	}
	decodedPass, err := url.PathUnescape(password)
	if err != nil {
		decodedPass = password
	}

	log.Printf("mongo: encoding credentials — username=%q", decodedUser)

	// url.UserPassword encodes both fields using the same rules the Go HTTP
	// library applies to userinfo, percent-encoding all special characters.
	encoded := scheme + url.UserPassword(decodedUser, decodedPass).String() + "@" + hostpart + afterAuthority
	return encoded
}

// redactMongoURI replaces the password in a MongoDB URI with "***".
func redactMongoURI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil || u.User == nil {
		return uri
	}
	if _, hasPassword := u.User.Password(); hasPassword {
		u.User = url.UserPassword(u.User.Username(), "***")
	}
	return u.String()
}

// mongoConnectionKey returns a SHA-256 hash of the connection config for pool keying.
func mongoConnectionKey(conn DBConnection) string {
	b, _ := json.Marshal(conn)
	sum := sha256.Sum256(b)
	return "mongo:" + hex.EncodeToString(sum[:])
}

// getPooledMongoClient returns a pooled *mongo.Client, creating one if needed.
func (s *Server) getPooledMongoClient(conn DBConnection) (*mongo.Client, error) {
	key := mongoConnectionKey(conn)

	if v, ok := s.mongoPool.Load(key); ok {
		entry := v.(*mongoPoolEntry)
		entry.lastUsed = time.Now()
		log.Printf("mongo: reusing pooled client for key=%s", key[:16])
		return entry.client, nil
	}

	s.mongoPoolMu.Lock()
	defer s.mongoPoolMu.Unlock()

	// Double-check after acquiring the lock.
	if v, ok := s.mongoPool.Load(key); ok {
		entry := v.(*mongoPoolEntry)
		entry.lastUsed = time.Now()
		log.Printf("mongo: reusing pooled client (post-lock) for key=%s", key[:16])
		return entry.client, nil
	}

	if s.mongoPoolSize >= maxPoolSize {
		return nil, fmt.Errorf("MongoDB connection pool full (%d/%d)", s.mongoPoolSize, maxPoolSize)
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
	s.mongoPool.Store(key, entry)
	s.mongoPoolSize++

	return client, nil
}

// evictMongoClient removes a client from the pool and disconnects it.
// Called when an operation fails so the next request gets a fresh connection.
func (s *Server) evictMongoClient(conn DBConnection) {
	key := mongoConnectionKey(conn)
	s.mongoPoolMu.Lock()
	defer s.mongoPoolMu.Unlock()
	if v, ok := s.mongoPool.Load(key); ok {
		entry := v.(*mongoPoolEntry)
		log.Printf("mongo: evicting failed client for key=%s", key[:16])
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = entry.client.Disconnect(ctx)
		cancel()
		s.mongoPool.Delete(key)
		s.mongoPoolSize--
	}
}

// startMongoPoolReaper starts a background goroutine that disconnects idle MongoDB clients.
func (s *Server) startMongoPoolReaper() {
	ticker := time.NewTicker(reaperInterval)
	for range ticker.C {
		s.reapIdleMongoConnections()
	}
}

func (s *Server) reapIdleMongoConnections() {
	now := time.Now()
	var toEvict []string

	s.mongoPool.Range(func(k, v any) bool {
		entry := v.(*mongoPoolEntry)
		if now.Sub(entry.lastUsed) > idleExpiry {
			toEvict = append(toEvict, k.(string))
		}
		return true
	})

	if len(toEvict) == 0 {
		return
	}

	s.mongoPoolMu.Lock()
	defer s.mongoPoolMu.Unlock()

	for _, key := range toEvict {
		if v, ok := s.mongoPool.Load(key); ok {
			entry := v.(*mongoPoolEntry)
			if now.Sub(entry.lastUsed) > idleExpiry {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_ = entry.client.Disconnect(ctx)
				cancel()
				s.mongoPool.Delete(key)
				s.mongoPoolSize--
			}
		}
	}
}
