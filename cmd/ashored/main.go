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
	"strconv"
	"syscall"
	"time"

	"github.com/moby/moby/client"
	"golang.org/x/sync/errgroup"

	"github.com/maddoxrjohnson/ashore/internal/builder"
	"github.com/maddoxrjohnson/ashore/internal/config"
	"github.com/maddoxrjohnson/ashore/internal/gitserver"
	"github.com/maddoxrjohnson/ashore/internal/hook"
	"github.com/maddoxrjohnson/ashore/internal/router"
	dockerrt "github.com/maddoxrjohnson/ashore/internal/runtime/docker"
	"github.com/maddoxrjohnson/ashore/internal/store"
	"github.com/maddoxrjohnson/ashore/internal/supervisor"
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

	if flag.NArg() > 0 {
		return admin(ctx, cfg, flag.Args(), os.Stdout)
	}

	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return fmt.Errorf("data dir: %w", err)
	}
	st, err := store.Open(ctx, filepath.Join(cfg.DataDir, "ashore.db"))
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	// The script every app repository's core.hooksPath points at. Written
	// before the SSH listener opens so no push can arrive without it.
	if err := hook.InstallScript(cfg.DataDir); err != nil {
		return fmt.Errorf("hook script: %w", err)
	}

	docker, err := newDocker(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { _ = docker.Close() }()

	rt := router.New(slog.Default())
	sup := supervisor.New(st, builder.New(docker, cfg.DataDir, cfg.BuildTimeout), dockerrt.New(docker), rt,
		supervisor.Config{
			DataDir:          cfg.DataDir,
			Domain:           cfg.Domain,
			HTTPPort:         portOf(cfg.HTTPAddr),
			BuildConcurrency: cfg.BuildConcurrency,
			HealthTimeout:    cfg.HealthTimeout,
			StopGrace:        cfg.StopGrace,
		}, slog.Default())

	hs, err := hook.NewServer(cfg.DataDir, sup)
	if err != nil {
		return err
	}
	hln, err := hs.Listen()
	if err != nil {
		return err
	}
	gs, err := gitserver.New(st, hs, cfg.DataDir, cfg.SSHHostKey)
	if err != nil {
		_ = hln.Close()
		return err
	}
	sln, err := net.Listen("tcp", cfg.SSHAddr)
	if err != nil {
		_ = hln.Close()
		return fmt.Errorf("ssh listen: %w", err)
	}
	httpLn, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		_ = hln.Close()
		_ = sln.Close()
		return fmt.Errorf("http listen: %w", err)
	}

	// Each Serve returns nil once its context ends and closes its listener
	// on the way out. The group cancels that context on the first failure,
	// so one server going down takes the others with it.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return hs.Serve(gctx, hln) })
	g.Go(func() error { return gs.Serve(gctx, sln) })
	g.Go(func() error { return rt.Serve(gctx, httpLn) })
	g.Go(func() error { return sup.Run(gctx) })
	g.Go(func() error {
		<-gctx.Done()
		slog.Info("shutting down")
		return nil
	})
	slog.Info("hook socket", "path", hs.Socket())
	slog.Info("ssh listening", "addr", sln.Addr().String())
	slog.Info("http listening", "addr", httpLn.Addr().String(), "domain", cfg.Domain)
	slog.Info("ready", "version", version)
	return g.Wait()
}

// newDocker connects to the Engine API. An unreachable daemon is reported
// but does not stop the start: routes for running apps must come back
// first, and the next deploy will say what is wrong.
func newDocker(ctx context.Context, cfg config.Config) (*client.Client, error) {
	opts := []client.Opt{client.FromEnv}
	if cfg.DockerHost != "" {
		opts = append(opts, client.WithHost(cfg.DockerHost))
	}
	docker, err := client.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("docker client: %w", err)
	}
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := docker.Ping(pctx, client.PingOptions{}); err != nil {
		slog.Warn("docker daemon unreachable; deploys will fail until it is", "host", docker.DaemonHost(), "err", err)
	} else {
		slog.Info("docker", "host", docker.DaemonHost())
	}
	return docker, nil
}

// portOf returns the port of a listen address, 0 if it has none.
func portOf(addr string) int {
	_, p, err := net.SplitHostPort(addr)
	if err != nil {
		return 0
	}
	n, _ := strconv.Atoi(p)
	return n
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
