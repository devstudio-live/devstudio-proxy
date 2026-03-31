package main

import (
	"embed"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
)

// ProxyHandler forwards requests to the remote DevStudio site,
// falling back to embedded assets when the upstream is unreachable.
type ProxyHandler struct {
	proxy    *httputil.ReverseProxy
	fallback fs.FS
}

func NewProxyHandler(target *url.URL, fallbackAssets embed.FS) *ProxyHandler {
	proxy := httputil.NewSingleHostReverseProxy(target)

	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)
		req.Host = target.Host
		req.Header.Set("Accept-Encoding", "identity")
	}

	proxy.ModifyResponse = func(resp *http.Response) error {
		resp.Header.Del("Content-Security-Policy")
		resp.Header.Del("X-Frame-Options")
		return nil
	}

	fallbackFS, _ := fs.Sub(fallbackAssets, "frontend/dist")

	h := &ProxyHandler{
		proxy:    proxy,
		fallback: fallbackFS,
	}
	proxy.ErrorHandler = h.handleProxyError
	return h
}

func (h *ProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.proxy.ServeHTTP(w, r)
}

func (h *ProxyHandler) handleProxyError(w http.ResponseWriter, r *http.Request, err error) {
	log.Printf("Proxy error for %s: %v, serving offline fallback", r.URL.Path, err)
	h.serveFallback(w, r)
}

func (h *ProxyHandler) serveFallback(w http.ResponseWriter, r *http.Request) {
	if h.fallback == nil {
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
		return
	}
	f, err := h.fallback.Open("index.html")
	if err != nil {
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
		return
	}
	defer f.Close()

	stat, _ := f.Stat()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	http.ServeContent(w, r, "index.html", stat.ModTime(), f.(io.ReadSeeker))
}
