package main

import (
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInitLogOutput_PushesLogLinesToRingBuffer(t *testing.T) {
	logBuf = &logRing{
		lines:       make([]logEntry, 0, logBufCap),
		subscribers: make(map[chan string]struct{}),
	}

	prevWriter := log.Writer()
	prevFlags := log.Flags()
	prevPrefix := log.Prefix()
	log.SetFlags(0)
	log.SetPrefix("")
	defer func() {
		log.SetOutput(prevWriter)
		log.SetFlags(prevFlags)
		log.SetPrefix(prevPrefix)
	}()

	initLogOutput()
	log.Print("proxy: startup test")

	entries := logBuf.snapshot()
	lines := make([]string, 0, len(entries))
	for _, entry := range entries {
		lines = append(lines, entry.Line)
	}
	got := strings.Join(lines, "")
	if !strings.Contains(got, "proxy: startup test") {
		t.Fatalf("expected ring buffer to contain startup log, got %q", got)
	}
}

func TestLoggingResponseWriter_ImplementsFlusher(t *testing.T) {
	rec := httptest.NewRecorder()
	lrw := &loggingResponseWriter{ResponseWriter: rec, status: 200}

	if _, ok := interface{}(lrw).(http.Flusher); !ok {
		t.Fatal("expected loggingResponseWriter to implement http.Flusher")
	}
}
