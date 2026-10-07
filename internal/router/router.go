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
)

const (
	// DefaultMaxBody caps a request body. Apps are small web services; a
	// client that needs more is not one of ours.
	DefaultMaxBody = 32 << 20

	readHeaderTimeout = 10 * time.Second
	idleTimeout       = 2 * time.Minute
	dialTimeout       = 5 * time.Second
	shutdownTimeout   = 10 * time.Second
)

// backend is one routing table entry. The URL is built once, at Set, so the
// hot path does no parsing.
type backend struct {
	addr string
	url  *url.URL
}

// Router routes by host. Create it with New; it is safe for concurrent use.
type Router struct {
	// MaxBody is the largest request body accepted, in bytes. Set it
	// before the first request.
	MaxBody int64

	table atomic.Pointer[map[string]backend]
	mu    sync.Mutex // serializes writers; readers never take it
	proxy *httputil.ReverseProxy
	log   *slog.Logger
}

// ctxKey carries the backend ServeHTTP chose into the proxy's Rewrite.
type ctxKey struct{}

// New returns a router with an empty table. A nil logger means slog.Default.
func New(log *slog.Logger) *Router {
	if log == nil {
		log = slog.Default()
	}
	r := &Router{MaxBody: DefaultMaxBody, log: log}
	r.table.Store(&map[string]backend{})
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

// Set routes host to addr, a host:port the router can dial.
func (r *Router) Set(host, addr string) {
	host = hostOf(host)
	b := backend{addr: addr, url: &url.URL{Scheme: "http", Host: addr}}
	r.mu.Lock()
	defer r.mu.Unlock()
	next := maps.Clone(*r.table.Load())
	next[host] = b
	r.table.Store(&next)
	r.log.Info("route set", "host", host, "addr", addr)
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
	rw := &recorder{ResponseWriter: w}
	host := hostOf(req.Host)

	if b, ok := (*r.table.Load())[host]; ok {
		req.Header.Set("X-Request-Id", id) // the proxy clones headers into the outbound request
		req.Body = http.MaxBytesReader(w, req.Body, r.MaxBody)
		r.proxy.ServeHTTP(rw, req.WithContext(context.WithValue(req.Context(), ctxKey{}, b)))
	} else {
		http.Error(rw, fmt.Sprintf("no app at %s (request %s)", host, id), http.StatusNotFound)
	}

	r.log.Info("http", "host", host, "method", req.Method, "path", req.URL.Path,
		"status", rw.status, "bytes", rw.bytes, "duration", time.Since(start),
		"id", id, "remote", req.RemoteAddr)
}

// upstreamError answers when the backend could not. The request id is in
// the body so a user can quote it and the operator can grep for it.
func (r *Router) upstreamError(w http.ResponseWriter, req *http.Request, err error) {
	id := w.Header().Get("X-Request-Id")
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

// Serve runs an HTTP server on ln until ctx is done, then shuts it down,
// giving in-flight requests shutdownTimeout to finish.
func (r *Router) Serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{
		Handler:           r,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		ErrorLog:          slog.NewLogLogger(r.log.Handler(), slog.LevelWarn),
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case err := <-errc:
		return fmt.Errorf("http serve: %w", err)
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		r.log.Warn("http shutdown", "err", err)
		_ = srv.Close()
	}
	<-errc // Serve has returned ErrServerClosed
	return nil
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

// recorder captures the status and size for the access log. Unwrap lets
// http.ResponseController reach the real writer, which is how the proxy
// flushes streaming responses and hijacks WebSocket upgrades.
type recorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (rw *recorder) WriteHeader(code int) {
	if rw.status == 0 {
		rw.status = code
	}
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *recorder) Write(p []byte) (int, error) {
	if rw.status == 0 {
		rw.status = http.StatusOK
	}
	n, err := rw.ResponseWriter.Write(p)
	rw.bytes += int64(n)
	return n, err
}

func (rw *recorder) Unwrap() http.ResponseWriter { return rw.ResponseWriter }
