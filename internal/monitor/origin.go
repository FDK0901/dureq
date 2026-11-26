package monitor

import (
	"fmt"
	"net"
	"net/http"
	"strings"
)

// originGuard keeps web pages of other origins from using the API, unless they are allowed:
// the API has no authentication, and a browser reaches whatever its user does. Whoever is not
// a browser is not affected.
type originGuard struct {
	protection *http.CrossOriginProtection
	allowed    map[string]bool
	any        bool
}

// newOriginGuard allows origins such as "https://durequi.example.com". "*" allows all.
func newOriginGuard(origins []string) (*originGuard, error) {
	g := &originGuard{protection: http.NewCrossOriginProtection(), allowed: make(map[string]bool)}
	for _, o := range origins {
		if o == "*" {
			g.any = true
			continue
		}
		o = strings.TrimSuffix(o, "/")
		if err := g.protection.AddTrustedOrigin(o); err != nil {
			return nil, fmt.Errorf("allowed origin: %w", err)
		}
		g.allowed[o] = true
	}
	return g, nil
}

func (g *originGuard) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && (g.any || g.allowed[origin]) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		}
		w.Header().Add("Vary", "Origin")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if !g.any && !g.allows(r) {
			writeServiceError(w, &ApiError{Msg: "requests from another origin are not allowed; see allowedOrigins", StatusCode: http.StatusForbidden})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// allows reports whether a request is not from a web page of another origin. A request without
// an origin that does not change anything may be: the page cannot read the response.
func (g *originGuard) allows(r *http.Request) bool {
	check := *r
	if r.Header.Get("Origin") != "" {
		// A GET may open a WebSocket, which has no same-origin policy.
		check.Method = http.MethodPost
	}
	if g.protection.Check(&check) == nil {
		return true
	}
	// Behind a proxy, the origin is the host that the proxy was asked for. A web page cannot
	// set this header.
	if host := r.Header.Get("X-Forwarded-Host"); host != "" {
		host, _, _ = strings.Cut(host, ",")
		check.Host = strings.TrimSpace(host)
		return g.protection.Check(&check) == nil
	}
	return false
}

// hostGuard answers only requests for the hosts that it allows, if any are: with DNS rebinding,
// a web page of another origin reaches the API under a name of its own, and to the browser the
// page and the API are of the same origin.
type hostGuard struct {
	names map[string]bool // allowed with any port
	hosts map[string]bool // allowed with this port
}

// newHostGuard allows hosts such as "dureq.internal" (with any port) or "localhost:8080". With
// none, all are allowed.
func newHostGuard(hosts []string) (*hostGuard, error) {
	if len(hosts) == 0 {
		return nil, nil
	}
	g := &hostGuard{names: make(map[string]bool), hosts: make(map[string]bool)}
	for _, h := range hosts {
		h = strings.ToLower(strings.TrimSpace(h))
		if h == "" || strings.ContainsAny(h, "/?#@ ") {
			return nil, fmt.Errorf("allowed host %q: want a name, or a name and port", h)
		}
		if name, port, err := net.SplitHostPort(h); err == nil {
			if name == "" || port == "" {
				return nil, fmt.Errorf("allowed host %q: want a name, or a name and port", h)
			}
			g.hosts[net.JoinHostPort(name, port)] = true
			continue
		}
		g.names[strings.TrimSuffix(strings.TrimPrefix(h, "["), "]")] = true
	}
	return g, nil
}

func (g *hostGuard) allows(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	if name, port, err := net.SplitHostPort(host); err == nil {
		return g.names[name] || g.hosts[net.JoinHostPort(name, port)]
	}
	return g.names[strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")]
}

func (g *hostGuard) middleware(next http.Handler) http.Handler {
	if g == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ok := g.allows(r.Host)
		// Behind a proxy, it is the host that the proxy was asked for.
		if forwarded := r.Header.Get("X-Forwarded-Host"); ok && forwarded != "" {
			forwarded, _, _ = strings.Cut(forwarded, ",")
			ok = g.allows(forwarded)
		}
		if !ok {
			writeServiceError(w, &ApiError{Msg: "requests for this host are not allowed; see allowedHosts", StatusCode: http.StatusForbidden})
			return
		}
		next.ServeHTTP(w, r)
	})
}
