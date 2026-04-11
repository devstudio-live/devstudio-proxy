package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"

	"devstudio/proxy/proxycore"

	"github.com/google/uuid"
	"github.com/wailsapp/wails/v2/pkg/runtime"
)

const maxProxyResponseSize = 5 << 20 // 5 MB

type App struct {
	ctx           context.Context
	proxy         *proxycore.Server
	sshSessions   sync.Map // bridge sessionID → *sshBridgeSession
	sftpDownloads sync.Map // bridge sessionID → *sftpDownloadState
	sftpUploads   sync.Map // bridge sessionID → *sftpUploadState
}

func NewApp() *App {
	return &App{}
}

func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
	a.proxy = proxycore.NewServer(proxycore.Options{Port: 0})
	a.proxy.LogEnabled.Store(true)
	a.proxy.Start(5 * time.Minute)

	go a.emitLogStream()
	go a.emitEventStream()
}

func (a *App) shutdown(ctx context.Context) {
	if a.proxy != nil {
		a.proxy.Shutdown()
	}
}

// ── IPC Bindings ─────────────────────────────────────────────────────────────

func (a *App) GetVersion() string {
	return proxycore.Version
}

func (a *App) HealthCheck() map[string]any {
	return map[string]any{
		"status": "ok",
		"mode":   "wails",
	}
}

func (a *App) GetConfig() string {
	req := httptest.NewRequest(http.MethodGet, "/admin/config", nil)
	rec := httptest.NewRecorder()
	a.proxy.Handler.ServeHTTP(rec, req)
	return rec.Body.String()
}

func (a *App) UpdateConfig(opts string) string {
	req := httptest.NewRequest(http.MethodPost, "/admin/config", strings.NewReader(opts))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	a.proxy.Handler.ServeHTTP(rec, req)
	return rec.Body.String()
}

func (a *App) Restart() string {
	// In Wails mode, restart means recreating the proxy server in-process.
	// For now, return ok — the frontend can reload the window.
	return `{"ok":true}`
}

func (a *App) TrustCert() string {
	req := httptest.NewRequest(http.MethodPost, "/admin/trust-cert", nil)
	rec := httptest.NewRecorder()
	a.proxy.Handler.ServeHTTP(rec, req)
	return rec.Body.String()
}

func (a *App) GetLogSnapshot() string {
	a.proxy.LogBuf.Lock()
	entries := a.proxy.LogBuf.Snapshot()
	a.proxy.LogBuf.Unlock()
	b, _ := json.Marshal(map[string]any{"lines": entries})
	return string(b)
}

func (a *App) GetEventSnapshot() string {
	a.proxy.EventBuf.Lock()
	events := a.proxy.EventBuf.Snapshot()
	a.proxy.EventBuf.Unlock()
	b, _ := json.Marshal(map[string]any{"events": events})
	return string(b)
}

func (a *App) StoreContext(payload string) string {
	req := httptest.NewRequest(http.MethodPost, "/mcp/context", strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	a.proxy.Handler.ServeHTTP(rec, req)
	return rec.Body.String()
}

func (a *App) LoadContext(id string) string {
	req := httptest.NewRequest(http.MethodGet, "/mcp/context/"+id, nil)
	rec := httptest.NewRecorder()
	a.proxy.Handler.ServeHTTP(rec, req)
	return rec.Body.String()
}

func (a *App) MCPGateway(body string) string {
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	a.proxy.Handler.ServeHTTP(rec, req)
	return rec.Body.String()
}

func (a *App) DBGateway(protocol string, endpoint string, body string) string {
	req := httptest.NewRequest(http.MethodPost, "/"+endpoint, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-DevStudio-Gateway-Route", "true")
	req.Header.Set("X-DevStudio-Gateway-Protocol", protocol)
	rec := httptest.NewRecorder()
	a.proxy.Handler.ServeHTTP(rec, req)
	return rec.Body.String()
}

// SSHGateway dispatches SSH control-plane requests (test / connect / disconnect
// / sessions / tunnel/*) through the in-process proxy handler, matching the
// HTTP path + gateway headers the browser build uses.
func (a *App) SSHGateway(endpoint string, body string) string {
	req := httptest.NewRequest(http.MethodPost, "/"+endpoint, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-DevStudio-Gateway-Route", "true")
	req.Header.Set("X-DevStudio-Gateway-Protocol", "ssh")
	rec := httptest.NewRecorder()
	a.proxy.Handler.ServeHTTP(rec, req)
	return rec.Body.String()
}

// FSGateway dispatches filesystem gateway requests (list / read / write /
// stat / mkdir / delete / rename) through the in-process proxy handler.
func (a *App) FSGateway(endpoint string, body string) string {
	req := httptest.NewRequest(http.MethodPost, "/"+endpoint, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-DevStudio-Gateway-Route", "true")
	req.Header.Set("X-DevStudio-Gateway-Protocol", "fs")
	rec := httptest.NewRecorder()
	a.proxy.Handler.ServeHTTP(rec, req)
	return rec.Body.String()
}

// HprofGateway dispatches non-streaming hprof endpoints (hprof/result/...,
// hprof/query, hprof/report, hprof/status/..., hprof/cancel/...) through the
// in-process proxy handler. The streaming hprof/analyze endpoint is handled
// separately by HprofAnalyzeStream.
func (a *App) HprofGateway(method string, endpoint string, body string) string {
	if method == "" {
		method = http.MethodGet
	}
	var bodyReader io.Reader
	if body != "" {
		bodyReader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, "/"+endpoint, bodyReader)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("X-DevStudio-Gateway-Route", "true")
	req.Header.Set("X-DevStudio-Gateway-Protocol", "hprof")
	rec := httptest.NewRecorder()
	a.proxy.Handler.ServeHTTP(rec, req)
	return rec.Body.String()
}

// hprofStreamWriter is an http.ResponseWriter + http.Flusher that emits each
// Write() as a Wails event tagged by streamId. Used so the SSE-streaming
// hprof/analyze endpoint can be consumed by the frontend over Wails IPC
// without opening any TCP listener.
type hprofStreamWriter struct {
	ctx      context.Context
	streamId string
	header   http.Header
	status   int
}

func (w *hprofStreamWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *hprofStreamWriter) WriteHeader(status int) {
	w.status = status
}

func (w *hprofStreamWriter) Write(data []byte) (int, error) {
	runtime.EventsEmit(w.ctx, "hprof-stream-"+w.streamId, string(data))
	return len(data), nil
}

func (w *hprofStreamWriter) Flush() {}

// HprofAnalyzeStream starts an hprof analyze job in a goroutine. SSE frames
// produced by the gateway are emitted as Wails events on the channel
// "hprof-stream-{streamId}", terminated by "hprof-stream-end-{streamId}".
// The caller is expected to generate streamId and subscribe to both events
// BEFORE invoking this method, otherwise early frames may be lost.
func (a *App) HprofAnalyzeStream(streamId string, path string) string {
	if streamId == "" {
		streamId = uuid.New().String()
	}
	go func() {
		body, _ := json.Marshal(map[string]string{"path": path})
		req := httptest.NewRequest(http.MethodPost, "/hprof/analyze", strings.NewReader(string(body)))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-DevStudio-Gateway-Route", "true")
		req.Header.Set("X-DevStudio-Gateway-Protocol", "hprof")
		w := &hprofStreamWriter{ctx: a.ctx, streamId: streamId, status: 200}
		a.proxy.Handler.ServeHTTP(w, req)
		runtime.EventsEmit(a.ctx, "hprof-stream-end-"+streamId, w.status)
	}()
	return streamId
}


// ProxyResponse is the structured return value for ProxyRequest.
type ProxyResponse struct {
	StatusCode   int               `json:"statusCode"`
	Headers      map[string]string `json:"headers"`
	Body         string            `json:"body"`
	BodyEncoding string            `json:"bodyEncoding"` // "text" or "base64"
}

func (a *App) ProxyRequest(method, url string, headers map[string]string, body string) ProxyResponse {
	var bodyReader io.Reader
	if body != "" {
		bodyReader = strings.NewReader(body)
	}

	outReq, err := http.NewRequest(method, url, bodyReader)
	if err != nil {
		return ProxyResponse{StatusCode: 0, Body: "request error: " + err.Error(), BodyEncoding: "text"}
	}
	for k, v := range headers {
		outReq.Header.Set(k, v)
	}

	resp, err := proxycore.Transport.RoundTrip(outReq)
	if err != nil {
		return ProxyResponse{StatusCode: 0, Body: "upstream error: " + err.Error(), BodyEncoding: "text"}
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxProxyResponseSize+1))
	if err != nil {
		return ProxyResponse{StatusCode: resp.StatusCode, Body: "read error: " + err.Error(), BodyEncoding: "text"}
	}
	if len(respBody) > maxProxyResponseSize {
		return ProxyResponse{StatusCode: resp.StatusCode, Body: "response too large (>5MB)", BodyEncoding: "text"}
	}

	respHeaders := make(map[string]string, len(resp.Header))
	for k := range resp.Header {
		respHeaders[k] = resp.Header.Get(k)
	}

	ct := resp.Header.Get("Content-Type")
	isText := ct == "" || strings.HasPrefix(ct, "text/") ||
		strings.Contains(ct, "json") || strings.Contains(ct, "xml") ||
		strings.Contains(ct, "javascript") || strings.Contains(ct, "html")

	if isText {
		return ProxyResponse{
			StatusCode:   resp.StatusCode,
			Headers:      respHeaders,
			Body:         string(respBody),
			BodyEncoding: "text",
		}
	}
	return ProxyResponse{
		StatusCode:   resp.StatusCode,
		Headers:      respHeaders,
		Body:         base64.StdEncoding.EncodeToString(respBody),
		BodyEncoding: "base64",
	}
}

// ── SSE → Wails Events ──────────────────────────────────────────────────────

func (a *App) emitLogStream() {
	// Emit snapshot first so the frontend gets backfill on connect.
	a.proxy.LogBuf.Lock()
	snapshot := a.proxy.LogBuf.Snapshot()
	a.proxy.LogBuf.Unlock()
	for _, entry := range snapshot {
		runtime.EventsEmit(a.ctx, "proxy-log", entry.Line)
	}

	ch := make(chan string, 64)
	a.proxy.LogBuf.Subscribe(ch)
	defer a.proxy.LogBuf.Unsubscribe(ch)
	for line := range ch {
		runtime.EventsEmit(a.ctx, "proxy-log", line)
	}
}

func (a *App) emitEventStream() {
	// Emit snapshot first so the frontend gets backfill on connect.
	a.proxy.EventBuf.Lock()
	snapshot := a.proxy.EventBuf.Snapshot()
	a.proxy.EventBuf.Unlock()
	for _, evt := range snapshot {
		b, _ := json.Marshal(evt)
		runtime.EventsEmit(a.ctx, "proxy-event", string(b))
	}

	ch := make(chan proxycore.ProxyEvent, 64)
	a.proxy.EventBuf.Subscribe(ch)
	defer a.proxy.EventBuf.Unsubscribe(ch)
	for evt := range ch {
		b, _ := json.Marshal(evt)
		runtime.EventsEmit(a.ctx, "proxy-event", string(b))
	}
}
