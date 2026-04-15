package proxycore

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// handleURLOpenerInspect handles POST /urlopener/inspect.
// Body:  { "url": "https://example.com", "include": ["ssl","network","phishing"] }
// If "include" is absent, all three are attempted.
// Response shape is stable; any group that couldn't be populated is returned
// with a nested "error" string but still present so the client can display
// partial results.
func (s *Server) handleURLOpenerInspect(w http.ResponseWriter, r *http.Request) {
	setCORS(w, r)
	w.Header().Set("Content-Type", "application/json")

	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{"error": "method not allowed"}) //nolint:errcheck
		return
	}

	var body struct {
		URL     string   `json:"url"`
		Include []string `json:"include"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid json: " + err.Error()}) //nolint:errcheck
		return
	}

	u, err := url.Parse(strings.TrimSpace(body.URL))
	if err != nil || u.Host == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid url"}) //nolint:errcheck
		return
	}

	want := func(name string) bool {
		if len(body.Include) == 0 {
			return true
		}
		for _, n := range body.Include {
			if n == name {
				return true
			}
		}
		return false
	}

	ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
	defer cancel()

	result := map[string]interface{}{"url": u.String()}
	if want("ssl") {
		result["ssl"] = inspectTLS(ctx, u)
	}
	if want("network") {
		result["network"] = inspectNetwork(ctx, u)
	}
	if want("phishing") {
		result["phishing"] = inspectPhishing(ctx, u)
	}

	json.NewEncoder(w).Encode(result) //nolint:errcheck
}

// ── SSL / TLS ─────────────────────────────────────────────────────────────

func inspectTLS(ctx context.Context, u *url.URL) map[string]interface{} {
	out := map[string]interface{}{
		"issuer":     nil,
		"subject":    nil,
		"notBefore":  nil,
		"notAfter":   nil,
		"daysLeft":   nil,
		"sigAlg":     nil,
		"keyBits":    nil,
		"dnsNames":   nil,
		"selfSigned": nil,
		"error":      nil,
	}
	if u.Scheme != "https" {
		out["error"] = "not https"
		return out
	}

	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "443"
	}

	dialer := &net.Dialer{Timeout: 6 * time.Second}
	rawConn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	defer rawConn.Close()

	tlsConn := tls.Client(rawConn, &tls.Config{
		ServerName:         host,
		InsecureSkipVerify: true, //nolint:gosec  -- we report results, don't trust
	})
	if deadline, ok := ctx.Deadline(); ok {
		_ = tlsConn.SetDeadline(deadline)
	}
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		out["error"] = err.Error()
		return out
	}
	defer tlsConn.Close()

	state := tlsConn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		out["error"] = "no peer certificate"
		return out
	}
	leaf := state.PeerCertificates[0]

	keyBits := 0
	if rsaKey, ok := leaf.PublicKey.(*rsa.PublicKey); ok {
		keyBits = rsaKey.N.BitLen()
	}

	daysLeft := int(time.Until(leaf.NotAfter).Hours() / 24)
	out["issuer"] = leaf.Issuer.String()
	out["subject"] = leaf.Subject.String()
	out["notBefore"] = leaf.NotBefore.UTC().Format(time.RFC3339)
	out["notAfter"] = leaf.NotAfter.UTC().Format(time.RFC3339)
	out["daysLeft"] = daysLeft
	out["sigAlg"] = leaf.SignatureAlgorithm.String()
	if keyBits > 0 {
		out["keyBits"] = keyBits
	}
	out["dnsNames"] = leaf.DNSNames
	out["selfSigned"] = leaf.Issuer.String() == leaf.Subject.String()
	out["tlsVersion"] = tlsVersionString(state.Version)
	return out
}

func tlsVersionString(v uint16) string {
	switch v {
	case tls.VersionTLS13:
		return "TLS 1.3"
	case tls.VersionTLS12:
		return "TLS 1.2"
	case tls.VersionTLS11:
		return "TLS 1.1"
	case tls.VersionTLS10:
		return "TLS 1.0"
	}
	return fmt.Sprintf("0x%04x", v)
}

// ── Network / DNS / CDN ───────────────────────────────────────────────────

// known CDN hints — matched against CNAME + IP reverse DNS.
// Lightweight heuristic; ASN/GeoIP lookups require a DB we don't ship yet.
var cdnHints = []struct {
	name    string
	pattern string
}{
	{"Cloudflare", "cloudflare"},
	{"Akamai", "akamai"},
	{"Akamai", "edgekey"},
	{"Akamai", "edgesuite"},
	{"Fastly", "fastly"},
	{"AWS CloudFront", "cloudfront"},
	{"Azure CDN", "azureedge"},
	{"Google Cloud", "googleusercontent"},
	{"Google Cloud", "ghs.googlehosted"},
	{"Netlify", "netlify"},
	{"Vercel", "vercel-dns"},
	{"Vercel", "vercel.app"},
	{"GitHub Pages", "github.io"},
	{"Bunny CDN", "b-cdn.net"},
	{"Cloudinary", "cloudinary"},
	{"jsDelivr", "jsdelivr"},
}

func detectCDN(hints ...string) string {
	for _, s := range hints {
		lc := strings.ToLower(s)
		for _, c := range cdnHints {
			if strings.Contains(lc, c.pattern) {
				return c.name
			}
		}
	}
	return ""
}

func inspectNetwork(ctx context.Context, u *url.URL) map[string]interface{} {
	out := map[string]interface{}{
		"ip":        nil,
		"ipVersion": nil,
		"allIps":    nil,
		"cname":     nil,
		"asn":       nil, // reserved for future GeoIP DB integration
		"asnOrg":    nil,
		"country":   nil,
		"cdn":       nil,
		"error":     nil,
	}

	host := u.Hostname()
	if host == "" {
		out["error"] = "no host"
		return out
	}

	resolver := &net.Resolver{PreferGo: true}

	ips, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	if len(ips) == 0 {
		out["error"] = "no ips"
		return out
	}

	allIPs := make([]string, 0, len(ips))
	for _, ip := range ips {
		allIPs = append(allIPs, ip.IP.String())
	}
	primary := ips[0].IP
	out["ip"] = primary.String()
	out["allIps"] = allIPs
	if primary.To4() != nil {
		out["ipVersion"] = 4
	} else {
		out["ipVersion"] = 6
	}

	cname, _ := resolver.LookupCNAME(ctx, host)
	cname = strings.TrimSuffix(cname, ".")
	if cname != "" && cname != host {
		out["cname"] = cname
	}

	// Reverse DNS on primary IP adds context for CDN detection.
	var reverseNames []string
	if names, err := resolver.LookupAddr(ctx, primary.String()); err == nil {
		reverseNames = names
	}

	if cdn := detectCDN(append([]string{cname, host}, reverseNames...)...); cdn != "" {
		out["cdn"] = cdn
	}
	return out
}

// ── Phishing (URLhaus) ────────────────────────────────────────────────────

// inspectPhishing queries URLhaus (abuse.ch) for the given URL.
// Free, no API key required. Response docs: https://urlhaus-api.abuse.ch/
func inspectPhishing(ctx context.Context, u *url.URL) map[string]interface{} {
	out := map[string]interface{}{
		"verdict":    "unknown",
		"source":     "urlhaus",
		"reason":     nil,
		"threat":     nil,
		"tags":       nil,
		"reportedAt": nil,
		"error":      nil,
	}

	form := url.Values{}
	form.Set("url", u.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://urlhaus-api.abuse.ch/v1/url/", strings.NewReader(form.Encode()))
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 8 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		out["error"] = err.Error()
		return out
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	var payload struct {
		QueryStatus string   `json:"query_status"`
		URLStatus   string   `json:"url_status"`
		Threat      string   `json:"threat"`
		Tags        []string `json:"tags"`
		DateAdded   string   `json:"date_added"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		out["error"] = "urlhaus: " + err.Error()
		return out
	}

	switch payload.QueryStatus {
	case "no_results":
		out["verdict"] = "clean"
	case "ok":
		out["verdict"] = "malicious"
		out["threat"] = payload.Threat
		out["tags"] = payload.Tags
		out["reportedAt"] = payload.DateAdded
		out["reason"] = "URLhaus listed (" + payload.URLStatus + ")"
	case "invalid_url":
		out["verdict"] = "unknown"
		out["reason"] = "urlhaus: invalid_url"
	default:
		out["verdict"] = "unknown"
		out["reason"] = "urlhaus: " + payload.QueryStatus
	}
	return out
}
