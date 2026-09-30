package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"golang.org/x/sync/errgroup"

	"github.com/maddoxrjohnson/ashore/internal/config"
	"github.com/maddoxrjohnson/ashore/internal/gitserver"
	"github.com/maddoxrjohnson/ashore/internal/hook"
	"github.com/maddoxrjohnson/ashore/internal/store"
)

var version = "dev"

func main() {
	// "ashored hook ..." is the pre-receive hook git runs. It has its own exit
	// codes and talks to the user itself, so it never goes through run.
	if len(os.Args) > 1 && os.Args[1] == "hook" {
		os.Exit(hook.Run(os.Args[2:], os.Stdin, os.Stdout, os.Stderr, os.Getenv))
	}
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

	// The script every app repository's core.hooksPath points at. Written
	// before the SSH listener opens so no push can arrive without it.
	if err := hook.InstallScript(cfg.DataDir); err != nil {
		_ = st.Close()
		return fmt.Errorf("hook script: %w", err)
	}

	hs, err := hook.NewServer(cfg.DataDir, noDeployer{})
	if err != nil {
		_ = st.Close()
		return err
	}
	hln, err := hs.Listen()
	if err != nil {
		_ = st.Close()
		return err
	}
	gs, err := gitserver.New(st, hs, cfg.DataDir, cfg.SSHHostKey)
	if err != nil {
		_ = hln.Close()
		_ = st.Close()
		return err
	}
	ln, err := net.Listen("tcp", cfg.SSHAddr)
	if err != nil {
		_ = hln.Close()
		_ = st.Close()
		return fmt.Errorf("ssh listen: %w", err)
	}

	// Each Serve returns nil once its context ends and closes its listener
	// on the way out. The group cancels that context on the first failure,
	// so one server going down takes the other with it.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return hs.Serve(gctx, hln) })
	g.Go(func() error { return gs.Serve(gctx, ln) })
	g.Go(func() error {
		<-gctx.Done()
		slog.Info("shutting down")
		return nil
	})
	slog.Info("hook socket", "path", hs.Socket())
	slog.Info("ssh listening", "addr", ln.Addr().String())
	slog.Info("ready", "version", version)

	serveErr := g.Wait()
	if err := st.Close(); err != nil {
		return err
	}
	return serveErr
}

// noDeployer stands in until the supervisor exists. It tells the pusher what
// arrived and rejects the push, so no repository holds a commit that never
// deployed.
type noDeployer struct{}

func (noDeployer) Deploy(_ context.Context, app, sha, _ string, out io.Writer) error {
	_, _ = fmt.Fprintf(out, "-----> received %s (%s)\n", app, sha[:7])
	return errors.New("deploying is not implemented yet")
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
