package proxycore

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
)

// TrafficRecord holds metadata about a single proxied HTTP request.
type TrafficRecord struct {
	ID          int64             `json:"id"`
	Timestamp   int64             `json:"ts"`                        // UnixMilli
	Method      string            `json:"method"`
	Path        string            `json:"path"`
	Protocol    string            `json:"protocol"`                  // sql, mongo, k8s, fs, hprof, elastic, redis, mcp, forward, tunnel, admin, health
	Status      int               `json:"status"`
	DurationUS  int64             `json:"duration_us"`
	ReqSize     int64             `json:"req_size"`
	RespSize    int64             `json:"resp_size"`
	ReqHeaders  map[string]string `json:"req_headers,omitempty"`
	RespHeaders map[string]string `json:"resp_headers,omitempty"`
	Error       string            `json:"error,omitempty"`
}

// TrafficRing is a fixed-capacity ring buffer for traffic records with pub/sub support.
type TrafficRing struct {
	mu          sync.Mutex
	records     []TrafficRecord
	pos         int
	nextID      int64
	cap         int
	subscribers map[chan TrafficRecord]struct{}
}

// NewTrafficRing creates a TrafficRing with the given capacity.
func NewTrafficRing(cap int) *TrafficRing {
	return &TrafficRing{
		records:     make([]TrafficRecord, 0, cap),
		cap:         cap,
		subscribers: make(map[chan TrafficRecord]struct{}),
	}
}

// Push adds a record, auto-increments ID, and broadcasts to subscribers.
func (r *TrafficRing) Push(rec TrafficRecord) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec.ID = r.nextID + 1
	r.nextID = rec.ID
	if len(r.records) < r.cap {
		r.records = append(r.records, rec)
	} else {
		r.records[r.pos] = rec
		r.pos = (r.pos + 1) % r.cap
	}
	for ch := range r.subscribers {
		select {
		case ch <- rec:
		default:
		}
	}
}

// Snapshot returns a copy of the buffer in chronological order.
func (r *TrafficRing) Snapshot() []TrafficRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]TrafficRecord, len(r.records))
	if len(r.records) < r.cap {
		copy(out, r.records)
		return out
	}
	copy(out, r.records[r.pos:])
	copy(out[r.cap-r.pos:], r.records[:r.pos])
	return out
}

// SnapshotSince returns records with ID > since.
func (r *TrafficRing) SnapshotSince(since int64) []TrafficRecord {
	all := r.Snapshot()
	if since <= 0 {
		return all
	}
	out := make([]TrafficRecord, 0, len(all))
	for _, rec := range all {
		if rec.ID > since {
			out = append(out, rec)
		}
	}
	return out
}

// Len returns the current number of records in the buffer.
func (r *TrafficRing) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.records)
}

// Subscribe registers a channel for live traffic records.
func (r *TrafficRing) Subscribe(ch chan TrafficRecord) {
	r.mu.Lock()
	r.subscribers[ch] = struct{}{}
	r.mu.Unlock()
}

// Unsubscribe removes a subscriber channel.
func (r *TrafficRing) Unsubscribe(ch chan TrafficRecord) {
	r.mu.Lock()
	delete(r.subscribers, ch)
	r.mu.Unlock()
}

// MarshalTrafficSSEFrame formats a TrafficRecord as a named SSE frame.
func MarshalTrafficSSEFrame(rec TrafficRecord) []byte {
	b, _ := json.Marshal(rec)
	return []byte("event: traffic\ndata: " + string(b) + "\n\n")
}

// detectProtocol determines the gateway protocol from the request.
func detectProtocol(r *http.Request) string {
	if proto := r.Header.Get("X-DevStudio-Gateway-Protocol"); proto != "" {
		return proto
	}
	path := r.URL.Path
	switch {
	case path == "/health":
		return "health"
	case strings.HasPrefix(path, "/admin/"):
		return "admin"
	case strings.HasPrefix(path, "/mcp"):
		return "mcp"
	case strings.Contains(path, "/k8s/exec"):
		return "k8s"
	case r.Method == http.MethodConnect:
		return "tunnel"
	default:
		return "forward"
	}
}

// flattenHeaders collapses multi-value HTTP headers into a single-value map.
func flattenHeaders(h http.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, vs := range h {
		out[k] = strings.Join(vs, ", ")
	}
	return out
}
