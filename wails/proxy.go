package main

import (
	"bytes"
	"embed"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
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

		ct := resp.Header.Get("Content-Type")
		if strings.Contains(ct, "text/html") {
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return err
			}
			inject := []byte(`<script>window.__WAILS__=true;</script>`)
			if idx := bytes.Index(body, []byte("<head>")); idx >= 0 {
				pos := idx + len("<head>")
				body = append(body[:pos], append(inject, body[pos:]...)...)
			} else if idx := bytes.Index(body, []byte("<HEAD>")); idx >= 0 {
				pos := idx + len("<HEAD>")
				body = append(body[:pos], append(inject, body[pos:]...)...)
			}
			resp.Body = io.NopCloser(bytes.NewReader(body))
			resp.ContentLength = int64(len(body))
			resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
		}
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
