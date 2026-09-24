package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/maddoxrjohnson/ashore/internal/config"
	"github.com/maddoxrjohnson/ashore/internal/gitserver"
	"github.com/maddoxrjohnson/ashore/internal/store"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ashored:", err)
		os.Exit(1)
	}
}

func run() error {
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println("ashored", version)
		return nil
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}
	slog.SetDefault(newLogger(os.Stderr, cfg))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return fmt.Errorf("data dir: %w", err)
	}
	st, err := store.Open(ctx, filepath.Join(cfg.DataDir, "ashore.db"))
	if err != nil {
		return err
	}

	gs, err := gitserver.New(st, cfg.DataDir, cfg.SSHHostKey)
	if err != nil {
		_ = st.Close()
		return err
	}
	ln, err := net.Listen("tcp", cfg.SSHAddr)
	if err != nil {
		_ = st.Close()
		return fmt.Errorf("ssh listen: %w", err)
	}
	errc := make(chan error, 1)
	go func() { errc <- gs.Serve(ctx, ln) }()
	slog.Info("ssh listening", "addr", ln.Addr().String())
	slog.Info("ready", "version", version)

	var serveErr error
	select {
	case <-ctx.Done():
		slog.Info("shutting down")
		serveErr = <-errc // Serve closes the listener and waits for its connections
	case serveErr = <-errc:
	}
	if err := st.Close(); err != nil {
		return err
	}
	return serveErr
}

// newLogger builds the daemon's logger: JSON for machines, text for a
// terminal. Config values themselves are never logged.
func newLogger(w io.Writer, cfg config.Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	if cfg.LogFormat == config.LogFormatText {
		return slog.New(slog.NewTextHandler(w, opts))
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}
