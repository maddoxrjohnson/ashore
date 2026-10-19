// Package serve is what the daemon's listeners share: an accept loop that
// ends with its context and waits for the connections it spawned, an HTTP
// server that shuts down with a deadline, and a ResponseWriter wrapper that
// records what was sent, for access logs and metrics.
package serve

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// Timeouts every listener uses. A client that is slow to send headers or
// idle for long holds a connection; nothing here needs either.
const (
	ReadHeaderTimeout = 10 * time.Second
	IdleTimeout       = 2 * time.Minute
	ShutdownTimeout   = 10 * time.Second
)

// Accept takes connections from ln and hands each to handle in its own
// goroutine until ctx is done, then closes the listener and waits for the
// handlers, which see the cancelled context. name labels the accept error.
func Accept(ctx context.Context, ln net.Listener, name string, handle func(ctx context.Context, c net.Conn)) error {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel() // runs before wg.Wait, so connections are told to stop first
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("%s accept: %w", name, err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			handle(ctx, c)
		}()
	}
}

// HTTP runs h on ln until ctx is done, then shuts the server down, giving
// in-flight requests ShutdownTimeout to finish before closing them. name
// labels errors and log lines ("http", "https", "api").
func HTTP(ctx context.Context, ln net.Listener, h http.Handler, name string, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}
	srv := &http.Server{
		Handler:           h,
		ReadHeaderTimeout: ReadHeaderTimeout,
		IdleTimeout:       IdleTimeout,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case err := <-errc:
		return fmt.Errorf("%s serve: %w", name, err)
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		log.Warn(name+" shutdown", "err", err)
		_ = srv.Close()
	}
	<-errc // Serve has returned ErrServerClosed
	return nil
}

// Recorder captures the status and size of a response. Unwrap lets
// http.ResponseController reach the real writer, which is how a proxy
// flushes streaming responses and hijacks WebSocket upgrades through it.
type Recorder struct {
	http.ResponseWriter
	Status int
	Bytes  int64
}

func (r *Recorder) WriteHeader(code int) {
	if r.Status == 0 {
		r.Status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *Recorder) Write(p []byte) (int, error) {
	if r.Status == 0 {
		r.Status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(p)
	r.Bytes += int64(n)
	return n, err
}

func (r *Recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }
