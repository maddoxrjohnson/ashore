// Package daemon wires the pieces of ashored together and runs them: the
// store, the Docker client, the runtime, the supervisor, and the four
// listeners (hook socket, SSH, HTTP with optional HTTPS, internal). It is
// what cmd/ashored calls once it has parsed its flags and configuration.
package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
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

// dockerContainerPort is the port apps listen on inside a Docker
// container; the runtime publishes it on the host port. The native
// runtime shares the host's network, so there the app listens on the host
// port itself.
const dockerContainerPort = 8080

// Run starts everything and blocks until ctx ends or a listener fails,
// then shuts down in order: listeners stop accepting, the supervisor's
// actors return, the store closes. version labels the ready line and the
// metrics; log receives every line the daemon writes.
func Run(ctx context.Context, cfg config.Config, version string, log *slog.Logger) error {
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

	docker, err := newDocker(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer func() { _ = docker.Close() }()

	where := site.Site{Domain: cfg.Domain, HTTPPort: site.PortOf(cfg.HTTPAddr), SSHAddr: cfg.SSHAddr}
	tlsOn := cfg.HTTPSAddr != ""
	ropts := router.Options{Log: log, RateRPS: cfg.RateLimitRPS}
	if tlsOn {
		where.HTTPSPort = site.PortOf(cfg.HTTPSAddr)
		ropts.TLS = &router.TLSConfig{Email: cfg.ACMEEmail, CacheDir: filepath.Join(cfg.DataDir, "certs")}
	}
	mets := metrics.New(version, cfg.Runtime)
	ropts.Observer = mets
	rt := router.New(ropts)
	drt, containerPort := newRuntime(cfg, docker, log)
	hub := logs.New(cfg.DataDir, drt, logs.Options{Log: log, Observer: mets})
	defer func() { _ = hub.Close() }()
	sup := supervisor.New(supervisor.Deps{
		Store: st, Builder: builder.New(docker, cfg.DataDir, cfg.BuildTimeout, log), Runtime: drt, Router: rt,
		Logs: hub, Observer: mets, Log: log,
	}, supervisor.Config{
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
		log.Warn("reconcile", "err", err, "partial", rep.String())
	}

	// The admin token is created on the first run; the CLI reads the file.
	token, created, err := api.LoadToken(cfg.DataDir)
	if err != nil {
		return err
	}
	if created {
		log.Info("admin token created", "path", filepath.Join(cfg.DataDir, api.TokenFile))
	}
	as := api.New(st, sup, hub, token, api.Config{DataDir: cfg.DataDir, Site: where}, log)
	dash := web.New(st, sup, hub, token, web.Config{DataDir: cfg.DataDir, Site: where, Version: version}, log)
	// One internal listener: the API under /api/, the dashboard elsewhere.
	internal := http.NewServeMux()
	internal.Handle("/api/", as)
	internal.Handle("GET /metrics", mets.Handler())
	internal.HandleFunc("GET /healthz", mets.Healthz)
	internal.Handle("/", dash)

	hs, err := hook.NewServer(cfg.DataDir, sup, log)
	if err != nil {
		return err
	}
	gs, err := gitserver.New(st, hs, cfg.DataDir, cfg.SSHHostKey, log)
	if err != nil {
		return err
	}
	var lns listeners
	defer lns.closeAll()
	hln, err := hs.Listen()
	if err != nil {
		return err
	}
	lns.add(hln)
	sln, err := lns.tcp("ssh", cfg.SSHAddr)
	if err != nil {
		return err
	}
	httpLn, err := lns.tcp("http", cfg.HTTPAddr)
	if err != nil {
		return err
	}
	apiLn, err := lns.tcp("internal", cfg.InternalAddr)
	if err != nil {
		return err
	}
	var httpsLn net.Listener
	if tlsOn {
		if httpsLn, err = lns.tcp("https", cfg.HTTPSAddr); err != nil {
			return err
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
		log.Info("shutting down")
		return nil
	})
	log.Info("hook socket", "path", hs.Socket())
	log.Info("ssh listening", "addr", sln.Addr().String())
	log.Info("http listening", "addr", httpLn.Addr().String(), "domain", cfg.Domain, "tls", tlsOn)
	if tlsOn {
		log.Info("https listening", "addr", httpsLn.Addr().String(), "acme_email", cfg.ACMEEmail)
	}
	log.Info("api listening", "addr", apiLn.Addr().String())
	mets.SetReady(true)
	log.Info("ready", "version", version, "runtime", cfg.Runtime)
	return g.Wait()
}

// listeners collects what was opened so a failure to open the next one
// closes the rest. Once every Serve has taken its listener, closeAll is a
// no-op in practice: each Serve closes its own on the way out.
type listeners []net.Listener

func (l *listeners) add(ln net.Listener) { *l = append(*l, ln) }

func (l *listeners) tcp(name, addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("%s listen: %w", name, err)
	}
	l.add(ln)
	return ln, nil
}

func (l *listeners) closeAll() {
	for _, ln := range *l {
		_ = ln.Close()
	}
}

// newRuntime picks the runtime the configuration asks for. Both build with
// Docker; only the native one runs without it. The second result is the
// port apps listen on inside their instance, 0 meaning the host port.
func newRuntime(cfg config.Config, docker *client.Client, log *slog.Logger) (runtime.Runtime, int) {
	if cfg.Runtime == config.RuntimeNative {
		return native.New(docker, cfg.DataDir, cfg.CgroupRoot, log), 0
	}
	return dockerrt.New(docker), dockerContainerPort
}

// newDocker connects to the Engine API. An unreachable daemon is reported
// but does not stop the start: routes for running apps must come back
// first, and the next deploy will say what is wrong.
func newDocker(ctx context.Context, cfg config.Config, log *slog.Logger) (*client.Client, error) {
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
		log.Warn("docker daemon unreachable; deploys will fail until it is", "host", docker.DaemonHost(), "err", err)
	} else {
		log.Info("docker", "host", docker.DaemonHost())
	}
	return docker, nil
}
