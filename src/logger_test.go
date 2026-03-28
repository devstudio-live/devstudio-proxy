package main

import (
	"log"
	"strings"
	"testing"
)

func TestInitLogOutput_PushesLogLinesToRingBuffer(t *testing.T) {
	logBuf = &logRing{
		lines:       make([]string, 0, logBufCap),
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

	got := strings.Join(logBuf.snapshot(), "")
	if !strings.Contains(got, "proxy: startup test") {
		t.Fatalf("expected ring buffer to contain startup log, got %q", got)
	}
}
