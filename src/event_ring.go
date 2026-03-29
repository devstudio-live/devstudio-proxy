package main

import (
	"encoding/json"
	"sync"
	"time"
)

// ── Proxy event ring buffer with SSE subscriptions ──────────────────────────

const eventBufCap = 100

type proxyEvent struct {
	ID        int64  `json:"id"`
	Type      string `json:"type"`
	Payload   any    `json:"payload"`
	Timestamp int64  `json:"ts"`
}

type eventRing struct {
	mu          sync.Mutex
	events      []proxyEvent
	pos         int
	nextID      int64
	subscribers map[chan proxyEvent]struct{}
}

var eventBuf = &eventRing{
	events:      make([]proxyEvent, 0, eventBufCap),
	subscribers: make(map[chan proxyEvent]struct{}),
}

func (r *eventRing) push(evt proxyEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	evt.ID = r.nextID + 1
	r.nextID = evt.ID
	if evt.Timestamp == 0 {
		evt.Timestamp = time.Now().Unix()
	}
	if len(r.events) < eventBufCap {
		r.events = append(r.events, evt)
	} else {
		r.events[r.pos] = evt
		r.pos = (r.pos + 1) % eventBufCap
	}
	for ch := range r.subscribers {
		select {
		case ch <- evt:
		default:
		}
	}
}

func (r *eventRing) snapshot() []proxyEvent {
	out := make([]proxyEvent, len(r.events))
	if len(r.events) < eventBufCap {
		copy(out, r.events)
		return out
	}
	copy(out, r.events[r.pos:])
	copy(out[eventBufCap-r.pos:], r.events[:r.pos])
	return out
}

func (r *eventRing) subscribe(ch chan proxyEvent) {
	r.mu.Lock()
	r.subscribers[ch] = struct{}{}
	r.mu.Unlock()
}

func (r *eventRing) unsubscribe(ch chan proxyEvent) {
	r.mu.Lock()
	delete(r.subscribers, ch)
	r.mu.Unlock()
}

// ── Health ticker ───────────────────────────────────────────────────────────

var serverStartTime = time.Now()

func startHealthTicker() {
	for {
		uptime := int64(time.Since(serverStartTime).Seconds())
		eventBuf.push(proxyEvent{
			Type:    "health",
			Payload: map[string]any{"status": "ok", "uptime": uptime},
		})
		time.Sleep(30 * time.Second)
	}
}

// marshalSSEFrame formats a proxyEvent as an SSE frame with a named event type.
func marshalSSEFrame(evt proxyEvent) []byte {
	b, _ := json.Marshal(evt)
	return []byte("event: " + evt.Type + "\ndata: " + string(b) + "\n\n")
}
