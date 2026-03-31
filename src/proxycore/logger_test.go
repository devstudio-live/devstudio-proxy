package proxycore

import (
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInitLogOutput_PushesLogLinesToRingBuffer(t *testing.T) {
	srv := NewServer(Options{Port: 0})

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

	srv.initLogOutput()
	log.Print("proxy: startup test")

	entries := srv.LogBuf.Snapshot()
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
	lrw := &LoggingResponseWriter{ResponseWriter: rec}

	if _, ok := interface{}(lrw).(http.Flusher); !ok {
		t.Fatal("expected LoggingResponseWriter to implement http.Flusher")
	}
}
