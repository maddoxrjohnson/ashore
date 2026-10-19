// Package router is the daemon's public HTTP front door. It picks a backend
// by the request's Host header and proxies to it with httputil.ReverseProxy.
//
// The routing table is an immutable map behind an atomic pointer. Every
// request loads the pointer once; the rare writer (a deploy) builds a new map
// and swaps it in. The hot path therefore takes no lock, and a request that
// started before a swap finishes against the map it loaded.
package router

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/crypto/acme/autocert"

	"github.com/maddoxrjohnson/ashore/internal/serve"
)

const (
	// DefaultMaxBody caps a request body. Apps are small web services; a
	// client that needs more is not one of ours.
	DefaultMaxBody = 32 << 20

	dialTimeout = 5 * time.Second
)

// backend is one routing table entry. The URL is built once, at Set, so the
// hot path does no parsing.
type backend struct {
	app  string
	addr string
	url  *url.URL
}

// Observer is told about every request the router handles; the metrics
// package implements it. Methods are called on the hot path and must be
// cheap. app is "" for a request no route matched.
type Observer interface {
	Request(app string, status int, d time.Duration)
	UpstreamError(app string)
}

// Options configure a Router. The zero value is a plain HTTP router with
// the default body cap, no rate limit, no TLS, and the default logger.
type Options struct {
	Log      *slog.Logger
	Observer Observer   // sees every request; nil means nobody
	MaxBody  int64      // the largest request body accepted, in bytes; 0 means DefaultMaxBody
	RateRPS  int        // requests per second per client IP; 0 means no limit
	TLS      *TLSConfig // nil means plain HTTP only
}

// Router routes by host. Create it with New; it is safe for concurrent use.
type Router struct {
	maxBody  int64
	observer Observer

	table atomic.Pointer[map[string]backend]
	mu    sync.Mutex // serializes writers; readers never take it
	proxy *httputil.ReverseProxy
	log   *slog.Logger
	limit *limiter // nil: no rate limit

	acme    *autocert.Manager // nil: TLS off
	tlsConf *tls.Config
}

// ctxKey carries the backend ServeHTTP chose into the proxy's Rewrite.
type ctxKey struct{}

// New returns a router with an empty table.
func New(o Options) *Router {
	log := o.Log
	if log == nil {
		log = slog.Default()
	}
	if o.MaxBody <= 0 {
		o.MaxBody = DefaultMaxBody
	}
	r := &Router{maxBody: o.MaxBody, observer: o.Observer, log: log}
	r.table.Store(&map[string]backend{})
	r.SetRateLimit(o.RateRPS)
	if o.TLS != nil {
		r.EnableTLS(*o.TLS)
	}
	r.proxy = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			b := pr.In.Context().Value(ctxKey{}).(backend)
			pr.SetURL(b.url)
			// The stdlib has already dropped the client's X-Forwarded-*
			// headers, so these are ours alone.
			pr.SetXForwarded()
			// SetURL made the outbound Host the backend's address; the app
			// should see the name it is reached by.
			pr.Out.Host = pr.In.Host
		},
		// Our own transport: never an HTTP_PROXY from the environment
		// between the router and a loopback backend.
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: dialTimeout}).DialContext,
			MaxIdleConnsPerHost: 32,
			IdleConnTimeout:     90 * time.Second,
		},
		// Flush after every write, or a response without a Content-Length
		// (server-sent events, chunked output) sits in a buffer.
		FlushInterval: -1,
		ErrorHandler:  r.upstreamError,
		ErrorLog:      slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	return r
}

// SetRateLimit caps each client IP at rps requests per second with a burst
// of twice that; 0 turns the limit off. Set it before the first request.
func (r *Router) SetRateLimit(rps int) {
	if rps <= 0 {
		r.limit = nil
		return
	}
	r.limit = newLimiter(rps, 2*rps)
}

// Set routes host to addr, a host:port the router can dial, on behalf of
// app, which is how the request is labelled in the metrics and the log.
func (r *Router) Set(host, app, addr string) {
	host = hostOf(host)
	b := backend{app: app, addr: addr, url: &url.URL{Scheme: "http", Host: addr}}
	r.mu.Lock()
	defer r.mu.Unlock()
	next := maps.Clone(*r.table.Load())
	next[host] = b
	r.table.Store(&next)
	r.log.Info("route set", "host", host, "app", app, "addr", addr)
}

// Remove drops the route for host. Removing an unknown host does nothing.
func (r *Router) Remove(host string) {
	host = hostOf(host)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := (*r.table.Load())[host]; !ok {
		return
	}
	next := maps.Clone(*r.table.Load())
	delete(next, host)
	r.table.Store(&next)
	r.log.Info("route removed", "host", host)
}

// Get returns the address host routes to.
func (r *Router) Get(host string) (addr string, ok bool) {
	b, ok := (*r.table.Load())[hostOf(host)]
	return b.addr, ok
}

// ServeHTTP is the hot path: one pointer load, one map lookup, then the
// proxy. Every response carries X-Request-Id and every request is logged.
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	start := time.Now()
	id := requestID()
	w.Header().Set("X-Request-Id", id)
	rw := &serve.Recorder{ResponseWriter: w}
	host := hostOf(req.Host)

	b, ok := (*r.table.Load())[host]
	switch {
	case r.limited(rw, req):
		// Per client, before the route lookup: an unknown host is the
		// cheapest request to flood with.
	case ok:
		req.Header.Set("X-Request-Id", id) // the proxy clones headers into the outbound request
		req.Body = http.MaxBytesReader(w, req.Body, r.maxBody)
		r.proxy.ServeHTTP(rw, req.WithContext(context.WithValue(req.Context(), ctxKey{}, b)))
	default:
		http.Error(rw, fmt.Sprintf("no app at %s (request %s)", host, id), http.StatusNotFound)
	}

	elapsed := time.Since(start)
	if r.observer != nil {
		r.observer.Request(b.app, rw.Status, elapsed)
	}
	r.log.Info("http", "host", host, "app", b.app, "method", req.Method, "path", req.URL.Path,
		"status", rw.Status, "bytes", rw.Bytes, "duration", elapsed,
		"id", id, "remote", req.RemoteAddr)
}

// upstreamError answers when the backend could not. The request id is in
// the body so a user can quote it and the operator can grep for it.
func (r *Router) upstreamError(w http.ResponseWriter, req *http.Request, err error) {
	id := w.Header().Get("X-Request-Id")
	if r.observer != nil {
		r.observer.UpstreamError(req.Context().Value(ctxKey{}).(backend).app)
	}
	var tooBig *http.MaxBytesError
	switch {
	case errors.As(err, &tooBig):
		http.Error(w, fmt.Sprintf("request body larger than %d bytes (request %s)", tooBig.Limit, id),
			http.StatusRequestEntityTooLarge)
	case errors.Is(err, context.Canceled):
		// The client hung up; there is no one to tell. Logged by ServeHTTP.
		w.WriteHeader(http.StatusBadGateway)
	default:
		r.log.Warn("upstream error", "host", hostOf(req.Host), "id", id, "err", err)
		http.Error(w, fmt.Sprintf("bad gateway (request %s)", id), http.StatusBadGateway)
	}
}

// Serve runs the plain HTTP server on ln until ctx is done, then shuts it
// down, giving in-flight requests shutdownTimeout to finish. With TLS
// enabled this listener only answers ACME challenges and redirects.
func (r *Router) Serve(ctx context.Context, ln net.Listener) error {
	return r.serve(ctx, ln, r.httpHandler(), "http")
}

func (r *Router) serve(ctx context.Context, ln net.Listener, h http.Handler, name string) error {
	return serve.HTTP(ctx, ln, h, name, r.log)
}

// limited refuses the request with a 429 when its client is over the rate
// limit, and reports whether it did.
func (r *Router) limited(w http.ResponseWriter, req *http.Request) bool {
	if r.limit == nil || r.limit.allow(clientIP(req.RemoteAddr), time.Now()) {
		return false
	}
	w.Header().Set("Retry-After", "1")
	http.Error(w, fmt.Sprintf("too many requests (request %s)", w.Header().Get("X-Request-Id")), http.StatusTooManyRequests)
	return true
}

// clientIP is the peer's address without the port, the rate limiter's
// key. The router is the edge, so X-Forwarded-For is never trusted here.
func clientIP(remoteAddr string) string {
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		return host
	}
	return remoteAddr
}

// hostOf normalizes a Host header for the table: no port, no trailing dot,
// lower case. An empty header (HTTP/1.0) stays empty and matches nothing.
func hostOf(h string) string {
	if host, _, err := net.SplitHostPort(h); err == nil {
		h = host
	}
	return strings.ToLower(strings.TrimSuffix(h, "."))
}

// requestID is 8 random bytes as hex: short enough to read out loud, random
// enough never to repeat in one log file.
func requestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:]) // never fails on supported platforms
	return hex.EncodeToString(b[:])
}
