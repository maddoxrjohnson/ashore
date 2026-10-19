package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/moby/moby/client"
	"golang.org/x/sync/errgroup"

	"github.com/maddoxrjohnson/ashore/internal/api"
	"github.com/maddoxrjohnson/ashore/internal/builder"
	"github.com/maddoxrjohnson/ashore/internal/config"
	"github.com/maddoxrjohnson/ashore/internal/gitserver"
	"github.com/maddoxrjohnson/ashore/internal/hook"
	"github.com/maddoxrjohnson/ashore/internal/logs"
	"github.com/maddoxrjohnson/ashore/internal/metrics"
	"github.com/maddoxrjohnson/ashore/internal/router"
	"github.com/maddoxrjohnson/ashore/internal/runtime"
	dockerrt "github.com/maddoxrjohnson/ashore/internal/runtime/docker"
	"github.com/maddoxrjohnson/ashore/internal/runtime/native"
	"github.com/maddoxrjohnson/ashore/internal/site"
	"github.com/maddoxrjohnson/ashore/internal/store"
	"github.com/maddoxrjohnson/ashore/internal/supervisor"
	"github.com/maddoxrjohnson/ashore/internal/web"
)

var version = "dev"

func main() {
	// "ashored hook ..." is the pre-receive hook git runs. It has its own exit
	// codes and talks to the user itself, so it never goes through run.
	if len(os.Args) > 1 && os.Args[1] == "hook" {
		os.Exit(hook.Run(os.Args[2:], os.Stdin, os.Stdout, os.Stderr, os.Getenv))
	}
	// "ashored runtime-init" is the native runtime's container init: the
	// daemon re-executed into new namespaces, which builds the container's
	// filesystem and execs the app.
	if len(os.Args) > 1 && os.Args[1] == "runtime-init" {
		os.Exit(native.Init(os.Args[2:]))
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

	if flag.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %q; the ashore command talks to the daemon", flag.Args())
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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

	mets := metrics.New(version, cfg.Runtime)
	rt := router.New(slog.Default())
	rt.Observer = mets
	rt.SetRateLimit(cfg.RateLimitRPS)
	tlsOn := cfg.HTTPSAddr != ""
	where := site.Site{Domain: cfg.Domain, HTTPPort: site.PortOf(cfg.HTTPAddr), SSHAddr: cfg.SSHAddr}
	if tlsOn {
		where.HTTPSPort = site.PortOf(cfg.HTTPSAddr)
		rt.EnableTLS(router.TLSConfig{Email: cfg.ACMEEmail, CacheDir: filepath.Join(cfg.DataDir, "certs")})
	}
	// Both runtimes build with Docker; only the native one runs without it.
	// Docker gives each container its own network, so apps listen on a
	// fixed port inside; the native runtime shares the host's, so the app
	// listens on the host port itself.
	var drt runtime.Runtime = dockerrt.New(docker)
	containerPort := 8080
	if cfg.Runtime == config.RuntimeNative {
		drt = native.New(docker, cfg.DataDir, cfg.CgroupRoot, slog.Default())
		containerPort = 0
	}
	hub := logs.New(cfg.DataDir, drt, slog.Default())
	hub.Observer = mets
	defer func() { _ = hub.Close() }()
	sup := supervisor.New(supervisor.Deps{
		Store: st, Builder: builder.New(docker, cfg.DataDir, cfg.BuildTimeout), Runtime: drt, Router: rt, Logs: hub, Observer: mets, Log: slog.Default(),
	},
		supervisor.Config{
			DataDir:          cfg.DataDir,
			Site:             where,
			BuildConcurrency: cfg.BuildConcurrency,
			HealthTimeout:    cfg.HealthTimeout,
			StopGrace:        cfg.StopGrace,
			DrainGrace:       cfg.DrainGrace,
			ContainerPort:    containerPort,
		})

	// Routes live in memory, so they are rebuilt from what is running
	// before any listener opens; a daemon restart is then invisible from
	// outside. An unreachable Docker makes this a warning: the daemon still
	// starts, and the next deploy says what is wrong.
	if rep, err := sup.Reconcile(ctx); err != nil {
		slog.Warn("reconcile", "err", err, "partial", rep.String())
	}

	// The admin token is created on the first run; the CLI reads the file.
	token, err := api.LoadToken(cfg.DataDir)
	if err != nil {
		return err
	}
	as := api.New(st, sup, hub, token, api.Config{DataDir: cfg.DataDir, Site: where}, slog.Default())
	dash := web.New(st, sup, hub, token, web.Config{DataDir: cfg.DataDir, Site: where, Version: version}, slog.Default())
	// One internal listener: the API under /api/, the dashboard elsewhere.
	internal := http.NewServeMux()
	internal.Handle("/api/", as)
	internal.Handle("GET /metrics", mets.Handler())
	internal.HandleFunc("GET /healthz", mets.Healthz)
	internal.Handle("/", dash)

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
	apiLn, err := net.Listen("tcp", cfg.InternalAddr)
	if err != nil {
		_ = hln.Close()
		_ = sln.Close()
		_ = httpLn.Close()
		return fmt.Errorf("internal listen: %w", err)
	}
	var httpsLn net.Listener
	if tlsOn {
		httpsLn, err = net.Listen("tcp", cfg.HTTPSAddr)
		if err != nil {
			_ = hln.Close()
			_ = sln.Close()
			_ = httpLn.Close()
			_ = apiLn.Close()
			return fmt.Errorf("https listen: %w", err)
		}
	}

	// Each Serve returns nil once its context ends and closes its listener
	// on the way out. The group cancels that context on the first failure,
	// so one server going down takes the others with it.
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error { return hs.Serve(gctx, hln) })
	g.Go(func() error { return gs.Serve(gctx, sln) })
	g.Go(func() error { return rt.Serve(gctx, httpLn) })
	if tlsOn {
		g.Go(func() error { return rt.ServeTLS(gctx, httpsLn) })
	}
	g.Go(func() error { return as.Serve(gctx, apiLn, internal) })
	g.Go(func() error { return sup.Run(gctx) })
	g.Go(func() error {
		<-gctx.Done()
		slog.Info("shutting down")
		return nil
	})
	slog.Info("hook socket", "path", hs.Socket())
	slog.Info("ssh listening", "addr", sln.Addr().String())
	slog.Info("http listening", "addr", httpLn.Addr().String(), "domain", cfg.Domain, "tls", tlsOn)
	if tlsOn {
		slog.Info("https listening", "addr", httpsLn.Addr().String(), "acme_email", cfg.ACMEEmail)
	}
	slog.Info("api listening", "addr", apiLn.Addr().String())
	mets.SetReady(true)
	slog.Info("ready", "version", version, "runtime", cfg.Runtime)
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

// newLogger builds the daemon's logger: JSON for machines, text for a
// terminal. Config values themselves are never logged.
func newLogger(w io.Writer, cfg config.Config) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.LogLevel}
	if cfg.LogFormat == config.LogFormatText {
		return slog.New(slog.NewTextHandler(w, opts))
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}
