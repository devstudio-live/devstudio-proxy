package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/dop251/goja"
)

const (
	mcpScriptURL = "https://www.devstudio.live/resources/mcp_routines.js"
	mcpBaseURL   = "https://devstudio.live"
	mcpFetchTimeout = 15 * time.Second
)

// mcpFallbackEnabled controls whether failed local MCP execution falls back
// to remote forwarding. Disabled by default; enable with -mcp-fallback.
var mcpFallbackEnabled bool

// mcpRuntime holds the Goja VM and associated state for local MCP execution.
var mcpRuntime struct {
	mu     sync.Mutex
	vm     *goja.Runtime
	script string // current JS source (used to detect changes on refresh)
	ready  bool
}

// initMCPRuntime fetches mcp_routines.js and initialises the Goja VM.
// If refreshInterval > 0, a background goroutine periodically re-fetches
// the script and hot-swaps the VM when the source changes.
func initMCPRuntime(refreshInterval time.Duration) {
	src, err := fetchMCPScript()
	if err != nil {
		log.Printf("mcp: init failed (will fall back to remote): %v", err)
		return
	}

	if err := loadMCPScript(src); err != nil {
		log.Printf("mcp: script load failed (will fall back to remote): %v", err)
		return
	}

	log.Printf("mcp: local runtime ready (%d bytes)", len(src))

	if refreshInterval > 0 {
		go mcpRefreshLoop(refreshInterval)
	}
}

// fetchMCPScript downloads the MCP routines JS source.
func fetchMCPScript() (string, error) {
	client := &http.Client{Timeout: mcpFetchTimeout}
	resp, err := client.Get(mcpScriptURL)
	if err != nil {
		return "", fmt.Errorf("fetch %s: %w", mcpScriptURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("fetch %s: status %d", mcpScriptURL, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", mcpScriptURL, err)
	}

	return string(body), nil
}

// loadMCPScript creates a new Goja VM, executes the script (which defines
// the MCP object and its _TOOL_BY_NAME lookup), and installs it as the
// active runtime.
func loadMCPScript(src string) error {
	vm := goja.New()

	// Inject Go-backed bridge functions before running the script so they are
	// available to any top-level initialisation code in mcp_routines.js.

	// __storeContext(jsonString) — auto snapshot fallback called by _buildSnapshotUrl.
	vm.Set("__storeContext", func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) == 0 {
			return goja.Null()
		}
		jsonStr := call.Arguments[0].String()
		id, err := storeContext([]byte(jsonStr), "snapshot")
		if err != nil {
			return goja.Null()
		}
		return vm.ToValue(id)
	})

	// __storeObject(jsObject) — explicit large-object store for MCP tool routines.
	vm.Set("__storeObject", func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) == 0 {
			return goja.Null()
		}
		obj := call.Arguments[0].Export()
		data, err := json.Marshal(obj)
		if err != nil {
			return goja.Null()
		}
		id, err := storeContext(data, "object")
		if err != nil {
			return goja.Null()
		}
		return vm.ToValue(id)
	})

	if _, err := vm.RunString(src); err != nil {
		return fmt.Errorf("goja eval: %w", err)
	}

	// Verify the MCP object exists
	mcpObj := vm.Get("MCP")
	if mcpObj == nil || goja.IsUndefined(mcpObj) || goja.IsNull(mcpObj) {
		return fmt.Errorf("MCP object not found after eval")
	}

	mcpRuntime.mu.Lock()
	mcpRuntime.vm = vm
	mcpRuntime.script = src
	mcpRuntime.ready = true
	mcpRuntime.mu.Unlock()

	return nil
}

// mcpRefreshLoop periodically re-fetches the MCP script and hot-swaps the
// Goja VM when the source has changed.
func mcpRefreshLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		src, err := fetchMCPScript()
		if err != nil {
			log.Printf("mcp: refresh fetch error: %v", err)
			continue
		}

		mcpRuntime.mu.Lock()
		same := src == mcpRuntime.script
		mcpRuntime.mu.Unlock()

		if same {
			continue
		}

		if err := loadMCPScript(src); err != nil {
			log.Printf("mcp: refresh load error (keeping current): %v", err)
			continue
		}

		log.Printf("mcp: runtime refreshed (%d bytes)", len(src))
	}
}

// handleMCPGateway handles /mcp requests by executing them locally in the
// Goja VM. Falls back to remote forwarding if the runtime is not ready or
// if execution fails.
func handleMCPGateway(w http.ResponseWriter, r *http.Request) {
	mcpRuntime.mu.Lock()
	ready := mcpRuntime.ready
	mcpRuntime.mu.Unlock()

	if !ready {
		if mcpFallbackEnabled {
			mcpForwardFallback(w, r)
			return
		}
		setCORS(w, r)
		http.Error(w, "mcp runtime not ready", http.StatusServiceUnavailable)
		return
	}

	// Read and parse the request body
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		setCORS(w, r)
		http.Error(w, "failed to read request body", http.StatusBadRequest)
		return
	}

	var reqBody interface{}
	if err := json.Unmarshal(bodyBytes, &reqBody); err != nil {
		setCORS(w, r)
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}

	// Execute MCP.handleRequest(baseUrl, body) in the Goja VM
	mcpRuntime.mu.Lock()
	result, err := callMCPHandleRequest(mcpRuntime.vm, reqBody)
	mcpRuntime.mu.Unlock()

	if err != nil {
		log.Printf("mcp: goja execution error: %v", err)
		if mcpFallbackEnabled {
			r.Body = io.NopCloser(newBytesReader(bodyBytes))
			mcpForwardFallback(w, r)
			return
		}
		setCORS(w, r)
		http.Error(w, "mcp execution failed", http.StatusInternalServerError)
		return
	}

	// Write the response
	setCORS(w, r)
	w.Header().Set("Content-Type", "application/json")

	status := 200
	if s, ok := result["status"].(int64); ok {
		status = int(s)
	} else if s, ok := result["status"].(float64); ok {
		status = int(s)
	}

	respBody := result["body"]
	if respBody == nil && status == 204 {
		w.WriteHeader(status)
		return
	}

	out, err := json.Marshal(respBody)
	if err != nil {
		http.Error(w, "failed to marshal response", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(status)
	w.Write(out) //nolint:errcheck
}

// callMCPHandleRequest invokes MCP.handleRequest(baseUrl, body) on the given
// Goja VM and returns the result as a Go map. Must be called under mcpRuntime.mu.
func callMCPHandleRequest(vm *goja.Runtime, body interface{}) (map[string]interface{}, error) {
	mcpObj := vm.Get("MCP")
	if mcpObj == nil || goja.IsUndefined(mcpObj) {
		return nil, fmt.Errorf("MCP object not available")
	}

	handleReq, ok := goja.AssertFunction(mcpObj.ToObject(vm).Get("handleRequest"))
	if !ok {
		return nil, fmt.Errorf("MCP.handleRequest is not a function")
	}

	jsBody := vm.ToValue(body)
	jsBaseURL := vm.ToValue(mcpBaseURL)

	res, err := handleReq(mcpObj, jsBaseURL, jsBody)
	if err != nil {
		return nil, fmt.Errorf("handleRequest call: %w", err)
	}

	exported := res.Export()
	result, ok := exported.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("handleRequest returned %T, expected object", exported)
	}

	return result, nil
}

// mcpForwardFallback falls back to remote MCP forwarding (original behavior).
func mcpForwardFallback(w http.ResponseWriter, r *http.Request) {
	r.URL.Scheme = "https"
	r.URL.Host = "devstudio.live"
	r.Host = "devstudio.live"
	handleForward(w, r)
}

// newBytesReader is a helper to create a bytes.Reader from a byte slice.
func newBytesReader(b []byte) *bytesReader {
	return &bytesReader{data: b}
}

type bytesReader struct {
	data []byte
	pos  int
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
