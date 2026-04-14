package proxycore

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// TrafficRecord holds metadata about a single proxied HTTP request.
type TrafficRecord struct {
	ID          int64             `json:"id"`
	Timestamp   int64             `json:"ts"`      // UnixMilli
	Method      string            `json:"method"`
	Path        string            `json:"path"`
	Protocol    string            `json:"protocol"` // sql, mongo, k8s, fs, hprof, elastic, redis, mcp, forward, tunnel, admin, health
	Operation   string            `json:"operation,omitempty"` // e.g., "query", "exec", "list" (gateway-specific)
	Target      string            `json:"target,omitempty"`    // e.g., DB alias, k8s context, tunnel host
	RemoteAddr  string            `json:"remote_addr,omitempty"`
	Status      int               `json:"status"`
	DurationUS  int64             `json:"duration_us"`
	ReqSize     int64             `json:"req_size"`
	RespSize    int64             `json:"resp_size"`
	ReqHeaders  map[string]string `json:"req_headers,omitempty"`
	RespHeaders map[string]string `json:"resp_headers,omitempty"`
	ReqBody     string            `json:"req_body,omitempty"`  // truncated preview, verbose only
	RespBody    string            `json:"resp_body,omitempty"` // truncated preview, verbose only
	ReqBodyTrunc bool             `json:"req_body_trunc,omitempty"`
	RespBodyTrunc bool            `json:"resp_body_trunc,omitempty"`
	WSUpgrade   bool              `json:"ws_upgrade,omitempty"`
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

// Clear removes all records but preserves the running ID counter
// (so subscribers and SnapshotSince callers don't see stale IDs reappear).
func (r *TrafficRing) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = r.records[:0]
	r.pos = 0
}

// TrafficStats contains aggregate metrics computed over the current buffer.
type TrafficStats struct {
	Count         int             `json:"count"`
	ErrorCount    int             `json:"error_count"`
	ErrorRate     float64         `json:"error_rate"` // 0.0-1.0
	AvgDurationUS int64           `json:"avg_duration_us"`
	P50DurationUS int64           `json:"p50_duration_us"`
	P95DurationUS int64           `json:"p95_duration_us"`
	P99DurationUS int64           `json:"p99_duration_us"`
	TotalReqBytes int64           `json:"total_req_bytes"`
	TotalRespBytes int64          `json:"total_resp_bytes"`
	ByProtocol    map[string]int  `json:"by_protocol"`
	ByStatusClass map[string]int  `json:"by_status_class"` // "2xx", "3xx", "4xx", "5xx", "other"
	RPS1m         float64         `json:"rps_1m"`          // requests per second, last minute
	WindowSeconds int             `json:"window_seconds"`
	OldestTS      int64           `json:"oldest_ts,omitempty"`
	NewestTS      int64           `json:"newest_ts,omitempty"`
}

// Stats computes aggregate metrics over the current buffer snapshot.
// Durations are linear-interpolated percentiles computed on a sorted copy.
func (r *TrafficRing) Stats() TrafficStats {
	snap := r.Snapshot()
	st := TrafficStats{
		ByProtocol:    make(map[string]int),
		ByStatusClass: make(map[string]int),
	}
	st.Count = len(snap)
	if st.Count == 0 {
		return st
	}

	durations := make([]int64, 0, st.Count)
	var totalDur int64
	now := snap[len(snap)-1].Timestamp
	oneMinAgo := now - 60_000
	rpsCount := 0

	for _, rec := range snap {
		durations = append(durations, rec.DurationUS)
		totalDur += rec.DurationUS
		st.TotalReqBytes += rec.ReqSize
		st.TotalRespBytes += rec.RespSize
		st.ByProtocol[rec.Protocol]++
		switch {
		case rec.Status >= 500:
			st.ByStatusClass["5xx"]++
			st.ErrorCount++
		case rec.Status >= 400:
			st.ByStatusClass["4xx"]++
			st.ErrorCount++
		case rec.Status >= 300:
			st.ByStatusClass["3xx"]++
		case rec.Status >= 200:
			st.ByStatusClass["2xx"]++
		default:
			st.ByStatusClass["other"]++
		}
		if rec.Timestamp >= oneMinAgo {
			rpsCount++
		}
	}
	st.AvgDurationUS = totalDur / int64(st.Count)
	st.ErrorRate = float64(st.ErrorCount) / float64(st.Count)

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	st.P50DurationUS = percentile(durations, 0.50)
	st.P95DurationUS = percentile(durations, 0.95)
	st.P99DurationUS = percentile(durations, 0.99)

	windowSecs := 60
	if firstAfterCutoff := snap[0].Timestamp; firstAfterCutoff > oneMinAgo {
		secs := int((now - firstAfterCutoff) / 1000)
		if secs < 1 {
			secs = 1
		}
		windowSecs = secs
	}
	st.WindowSeconds = windowSecs
	if windowSecs > 0 {
		st.RPS1m = float64(rpsCount) / float64(windowSecs)
	}
	st.OldestTS = snap[0].Timestamp
	st.NewestTS = snap[len(snap)-1].Timestamp
	return st
}

// percentile returns the value at the given quantile in [0,1] using
// nearest-rank. sorted must be ascending.
func percentile(sorted []int64, q float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * q)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
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
