package proxycore

import (
	"encoding/json"
	"sync"
	"time"
)

// ── Proxy event ring buffer with SSE subscriptions ──────────────────────────

// ProxyEvent represents a typed event published to subscribers.
type ProxyEvent struct {
	ID        int64  `json:"id"`
	Type      string `json:"type"`
	Payload   any    `json:"payload"`
	Timestamp int64  `json:"ts"`
}

// EventRing is a fixed-capacity ring buffer for proxy events with pub/sub.
type EventRing struct {
	mu          sync.Mutex
	events      []ProxyEvent
	pos         int
	nextID      int64
	cap         int
	subscribers map[chan ProxyEvent]struct{}
}

// NewEventRing creates an EventRing with the given capacity.
func NewEventRing(cap int) *EventRing {
	return &EventRing{
		events:      make([]ProxyEvent, 0, cap),
		cap:         cap,
		subscribers: make(map[chan ProxyEvent]struct{}),
	}
}

// Push adds an event, auto-increments ID, and broadcasts to subscribers.
func (r *EventRing) Push(evt ProxyEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	evt.ID = r.nextID + 1
	r.nextID = evt.ID
	if evt.Timestamp == 0 {
		evt.Timestamp = time.Now().Unix()
	}
	if len(r.events) < r.cap {
		r.events = append(r.events, evt)
	} else {
		r.events[r.pos] = evt
		r.pos = (r.pos + 1) % r.cap
	}
	for ch := range r.subscribers {
		select {
		case ch <- evt:
		default:
		}
	}
}

// Snapshot returns a copy of the buffer in chronological order.
func (r *EventRing) Snapshot() []ProxyEvent {
	out := make([]ProxyEvent, len(r.events))
	if len(r.events) < r.cap {
		copy(out, r.events)
		return out
	}
	copy(out, r.events[r.pos:])
	copy(out[r.cap-r.pos:], r.events[:r.pos])
	return out
}

// Subscribe registers a channel for live proxy events.
func (r *EventRing) Subscribe(ch chan ProxyEvent) {
	r.mu.Lock()
	r.subscribers[ch] = struct{}{}
	r.mu.Unlock()
}

// Unsubscribe removes a subscriber channel.
func (r *EventRing) Unsubscribe(ch chan ProxyEvent) {
	r.mu.Lock()
	delete(r.subscribers, ch)
	r.mu.Unlock()
}

// Len returns the current number of events in the buffer.
func (r *EventRing) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

// Lock / Unlock expose the mutex for snapshot-under-lock patterns.
func (r *EventRing) Lock()   { r.mu.Lock() }
func (r *EventRing) Unlock() { r.mu.Unlock() }

// ── Health ticker ───────────────────────────────────────────────────────────

func (s *Server) startHealthTicker() {
	for {
		uptime := int64(time.Since(s.ServerStartTime).Seconds())
		s.EventBuf.Push(ProxyEvent{
			Type:    "health",
			Payload: map[string]any{"status": "ok", "uptime": uptime},
		})
		time.Sleep(30 * time.Second)
	}
}

// MarshalSSEFrame formats a ProxyEvent as an SSE frame with a named event type.
func MarshalSSEFrame(evt ProxyEvent) []byte {
	b, _ := json.Marshal(evt)
	return []byte("event: " + evt.Type + "\ndata: " + string(b) + "\n\n")
}
