// Package supervisor owns the lifecycle of an app's releases: build the
// image, start an instance, wait until it answers, point the router at it,
// stop what it replaced, record the result.
//
// One goroutine per app (its actor) reads commands from a channel, so at
// most one deploy per app runs at a time and no mutex guards the deploy
// state; a second push to the same app waits its turn and is told so.
// Different apps deploy concurrently, bounded only by the build semaphore.
package supervisor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/builder"
	"github.com/maddoxrjohnson/ashore/internal/gitserver"
	"github.com/maddoxrjohnson/ashore/internal/manifest"
	"github.com/maddoxrjohnson/ashore/internal/runtime"
	"github.com/maddoxrjohnson/ashore/internal/store"
)

// Store is the slice of the database the supervisor uses.
type Store interface {
	GetApp(ctx context.Context, name string) (store.App, error)
	GetConfig(ctx context.Context, appID int64) (map[string]string, error)
	LiveRelease(ctx context.Context, appID int64) (store.Release, error)
	CreateRelease(ctx context.Context, appID int64, n store.NewRelease) (store.Release, error)
	MarkBuilt(ctx context.Context, id int64, image string) error
	MarkFailed(ctx context.Context, id int64) error
	SetLive(ctx context.Context, appID, releaseID int64) error
}

// Builder turns a commit into an image; *builder.Builder satisfies it.
type Builder interface {
	Build(ctx context.Context, job builder.Job, out io.Writer) (string, error)
}

// Router is the routing table the supervisor swaps; *router.Router satisfies it.
type Router interface {
	Set(host, addr string)
	Get(host string) (addr string, ok bool)
}

// Config is what a deploy needs to know about its surroundings.
type Config struct {
	DataDir string
	// Domain and HTTPPort make the URL printed when a release goes live.
	Domain   string
	HTTPPort int
	// BuildConcurrency caps builds in flight across all apps.
	BuildConcurrency int
	// HealthTimeout is how long a new instance has to pass its health
	// check and StopGrace is SIGTERM-to-SIGKILL when stopping one, unless
	// the app's ashore.toml says otherwise.
	HealthTimeout time.Duration
	StopGrace     time.Duration
}

// Instance defaults for what ashore.toml leaves out.
const (
	containerPort = 8080
	defaultMemory = 256 << 20
	defaultCPU    = 1.0
	healthEvery   = 250 * time.Millisecond // default probe interval
	healthHold    = 300 * time.Millisecond // how long a TCP probe connection must stay open
	probeTimeout  = 2 * time.Second        // one HTTP probe, connect to response
	logTail       = 20                     // lines shown when an instance dies during its health check
)

// manifestReader is manifest.ReadRepo's shape; tests substitute a function
// that returns a file without a repository.
type manifestReader func(ctx context.Context, repo, sha, quarantine string) (data []byte, found bool, err error)

// Supervisor runs deploys. Create it with New, call Deploy from any
// goroutine, and run Run to tie its lifetime to the daemon's.
type Supervisor struct {
	store   Store
	builder Builder
	runtime runtime.Runtime
	router  Router
	cfg     Config
	log     *slog.Logger
	builds  chan struct{} // the build semaphore

	readManifest manifestReader

	// ctx is the lifetime of every actor; Run cancels it. It lives in the
	// struct because actors start lazily, from whichever goroutine first
	// deploys an app.
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	apps   map[string]*actor
	actors sync.WaitGroup
}

// New wires the supervisor. A nil logger means slog.Default.
func New(st Store, b Builder, rt runtime.Runtime, r Router, cfg Config, log *slog.Logger) *Supervisor {
	if log == nil {
		log = slog.Default()
	}
	if cfg.BuildConcurrency < 1 {
		cfg.BuildConcurrency = 1
	}
	sup := &Supervisor{
		store: st, builder: b, runtime: rt, router: r, cfg: cfg, log: log,
		builds:       make(chan struct{}, cfg.BuildConcurrency),
		apps:         make(map[string]*actor),
		readManifest: manifest.ReadRepo,
	}
	sup.ctx, sup.cancel = context.WithCancel(context.Background())
	return sup
}

// Run blocks until ctx is done, then ends every actor and waits for them.
// Deploys in progress see a cancelled context and fail; nothing is stopped
// on the way out, since containers outlive the daemon on purpose.
func (s *Supervisor) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
	case <-s.ctx.Done():
	}
	s.cancel()
	s.actors.Wait()
	return nil
}

var errNotRunning = errors.New("supervisor is not running")

// Deploy implements hook.Deployer: build sha, run it, and make it the app's
// live release. Lines written to out reach the pusher. A cancelled ctx
// withdraws a push still waiting its turn; once the actor has taken it,
// Deploy waits for the verdict (the actor sees the same ctx and stops
// promptly), so nothing writes to out after Deploy returns.
func (s *Supervisor) Deploy(ctx context.Context, app, sha, quarantine string, out io.Writer) error {
	return s.send(ctx, app, deployCmd{kind: kindPush, sha: sha, quarantine: quarantine}, out)
}

// Redeploy makes a new release of the app's live image with its current
// config vars and runs it, the way a push would. It is what a config change
// does. note is the release note, such as "config:set GREETING". An app
// with no live release has nothing to redeploy and gets an error.
func (s *Supervisor) Redeploy(ctx context.Context, app, note string, out io.Writer) error {
	return s.send(ctx, app, deployCmd{kind: kindRedeploy, note: note}, out)
}

// send hands cmd to the app's actor and waits for the verdict.
func (s *Supervisor) send(ctx context.Context, app string, cmd deployCmd, out io.Writer) error {
	a, err := s.actorFor(app)
	if err != nil {
		return err
	}
	cmd.ctx, cmd.out, cmd.reply = ctx, out, make(chan error, 1)
	if a.busy.Load() {
		_, _ = fmt.Fprintln(out, "-----> waiting for the current deploy to finish")
	}
	select {
	case a.cmds <- cmd:
	case <-ctx.Done():
		return ctx.Err()
	case <-a.done:
		return errNotRunning
	}
	return <-cmd.reply
}

// actorFor returns the app's actor, starting it on first use. The lookup
// does not touch the database: an unknown app fails inside deploy, with the
// pusher watching.
func (s *Supervisor) actorFor(app string) (*actor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return nil, errNotRunning
	}
	a, ok := s.apps[app]
	if !ok {
		a = &actor{sup: s, name: app, cmds: make(chan deployCmd), done: make(chan struct{})}
		s.apps[app] = a
		s.actors.Add(1)
		go a.run(s.ctx)
	}
	return a, nil
}

// actor is the one goroutine that owns an app's deploy state.
type actor struct {
	sup  *Supervisor
	name string
	cmds chan deployCmd
	done chan struct{} // closed when the actor has returned
	busy atomic.Bool   // a deploy is in progress; the next pusher is told to wait

	current *running // what the router points at, if this daemon started it
}

// running is an instance this actor started and what it needs to stop it.
type running struct {
	inst  runtime.Instance
	grace time.Duration // its manifest's process.grace
}

// kind is what a deploy starts from.
type kind int

const (
	kindPush     kind = iota // a pushed commit: build it, then run it
	kindRedeploy             // the live image again, with the config as it is now
)

type deployCmd struct {
	ctx   context.Context
	kind  kind
	sha   string // kindPush
	note  string // kindRedeploy: the release note
	out   io.Writer
	reply chan error // buffered: the actor never waits for the caller

	quarantine string // kindPush
}

func (a *actor) run(ctx context.Context) {
	defer a.sup.actors.Done()
	defer close(a.done)
	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-a.cmds:
			a.busy.Store(true)
			cmd.reply <- a.deploy(cmd)
			a.busy.Store(false)
		}
	}
}

// deploy is one run of the algorithm; the error it returns is the last line
// the pusher sees. A panic is turned into an error so one bad deploy cannot
// take the daemon down.
func (a *actor) deploy(cmd deployCmd) (err error) {
	defer func() {
		if r := recover(); r != nil {
			a.sup.log.Error("deploy panic", "app", a.name, "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("internal error: %v", r)
		}
	}()
	s := a.sup
	ctx, out := cmd.ctx, cmd.out
	short := cmd.sha
	if len(short) > 7 {
		short = short[:7]
	}

	app, err := s.store.GetApp(ctx, a.name)
	if err != nil {
		return err
	}

	// What the release is made of. A push has a commit, its ashore.toml,
	// and no image yet; a redeploy reuses the live release's image, commit,
	// and manifest. Both snapshot the config vars as they are now, so the
	// row says exactly what ran. A manifest that does not parse is refused
	// here, before a release exists: it is a mistake in the file, not a
	// deploy that was attempted.
	var n store.NewRelease
	var usingManifest string
	switch cmd.kind {
	case kindPush:
		data, found, err := s.readManifest(ctx, gitserver.RepoPath(s.cfg.DataDir, a.name), cmd.sha, cmd.quarantine)
		if err != nil {
			return err
		}
		m, err := manifest.Parse(data, s.defaults())
		if err != nil {
			return err
		}
		if found {
			usingManifest = m.String()
		}
		n = store.NewRelease{GitSHA: cmd.sha, ManifestJSON: m.JSON(), Note: "push " + short}
	case kindRedeploy:
		live, err := s.store.LiveRelease(ctx, app.ID)
		if errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("%s has no live release to redeploy; push first", a.name)
		}
		if err != nil {
			return err
		}
		n = store.NewRelease{Image: live.Image, GitSHA: live.GitSHA, ManifestJSON: live.ManifestJSON, Note: cmd.note}
	}
	vars, err := s.store.GetConfig(ctx, app.ID)
	if err != nil {
		return err
	}
	n.ConfigJSON = store.MarshalConfig(vars)

	// The release row comes first, so the version is fixed and announced
	// before any work starts. From here on every failure must end the row
	// (failed), with a context that still works after the pusher's has
	// been cancelled.
	rel, err := s.store.CreateRelease(ctx, app.ID, n)
	if err != nil {
		return err
	}
	if cmd.kind == kindPush {
		_, _ = fmt.Fprintf(out, "-----> release %s (%s)\n", rel.Name(), short)
	} else {
		_, _ = fmt.Fprintf(out, "-----> release %s (%s)\n", rel.Name(), cmd.note)
	}
	if usingManifest != "" {
		_, _ = fmt.Fprintf(out, "-----> %s: %s\n", manifest.FileName, usingManifest)
	}

	// Build, under the global cap, unless the image already exists.
	if rel.Image == "" {
		if err := s.acquireBuild(ctx, out); err != nil {
			a.fail(ctx, rel)
			return err
		}
		_, _ = fmt.Fprintf(out, "-----> building %s\n", a.name)
		image, err := s.builder.Build(ctx, builder.Job{
			App: a.name, Repo: gitserver.RepoPath(s.cfg.DataDir, a.name), SHA: cmd.sha, Quarantine: cmd.quarantine,
		}, out)
		<-s.builds
		if err == nil {
			err = s.store.MarkBuilt(ctx, rel.ID, image)
		}
		if err != nil {
			a.fail(ctx, rel)
			return err
		}
		rel.Image, rel.Status = image, store.StatusStarting // what MarkBuilt wrote; start needs the image
	}

	// Start and wait until it answers. The environment and the manifest
	// are the row's snapshots, not the values read above, so what runs is
	// what the release says ran.
	env, err := store.UnmarshalConfig(rel.ConfigJSON)
	if err != nil {
		a.fail(ctx, rel)
		return err
	}
	m, err := manifest.FromJSON(rel.ManifestJSON, s.defaults())
	if err != nil {
		a.fail(ctx, rel)
		return err
	}
	inst, err := a.start(ctx, rel, env, m, app.Host, out)
	if err == nil && ctx.Err() != nil {
		// The pusher is gone, so git will not record this commit. Going
		// live anyway would leave what runs and what is pushed disagreeing.
		err = ctx.Err()
		if serr := s.runtime.Stop(context.WithoutCancel(ctx), inst.ID, time.Duration(m.Process.Grace)); serr != nil {
			s.log.Warn("stop abandoned instance", "app", a.name, "id", inst.ID, "err", serr)
		}
	}
	if err != nil {
		a.fail(ctx, rel)
		return err
	}

	// The swap. Nothing after it may fail the push: the new release is
	// serving, and git must record the commit it came from.
	old := a.current
	s.router.Set(app.Host, inst.Addr)
	a.current = &running{inst: inst, grace: time.Duration(m.Process.Grace)}
	s.log.Info("route swapped", "app", a.name, "release", rel.Name(), "addr", inst.Addr)
	_, _ = fmt.Fprintf(out, "-----> live at %s\n", s.url(app.Host))

	bg := context.WithoutCancel(ctx)
	if old != nil {
		if err := s.runtime.Stop(bg, old.inst.ID, old.grace); err != nil {
			s.log.Warn("stop old instance", "app", a.name, "id", old.inst.ID, "err", err)
		}
	}
	if err := s.store.SetLive(bg, app.ID, rel.ID); err != nil {
		s.log.Error("mark release live", "app", a.name, "release", rel.Name(), "err", err)
	}
	return nil
}

// fail ends a release that will not go live. It uses a context that
// survives the pusher's cancellation, since that is one of the reasons.
func (a *actor) fail(ctx context.Context, rel store.Release) {
	if err := a.sup.store.MarkFailed(context.WithoutCancel(ctx), rel.ID); err != nil {
		a.sup.log.Error("mark release failed", "app", a.name, "release", rel.Name(), "err", err)
	}
}

// acquireBuild takes a build slot, telling the pusher when it has to wait.
func (s *Supervisor) acquireBuild(ctx context.Context, out io.Writer) error {
	select {
	case s.builds <- struct{}{}:
		return nil
	default:
	}
	_, _ = fmt.Fprintln(out, "-----> waiting for a build slot")
	select {
	case s.builds <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// defaults are the manifest values for what an app's ashore.toml leaves out.
func (s *Supervisor) defaults() manifest.Defaults {
	return manifest.Defaults{
		Timeout: s.cfg.HealthTimeout, Interval: healthEvery, Grace: s.cfg.StopGrace,
		Memory: defaultMemory, CPU: defaultCPU,
	}
}

// start runs the release with env as its environment and m's limits, and
// waits for it to pass m's health check. host is the app's hostname, sent
// as the Host header of an HTTP probe. On any failure the instance is
// stopped and removed, so a rejected push leaves nothing running.
func (a *actor) start(ctx context.Context, rel store.Release, env map[string]string, m manifest.Manifest, host string, out io.Writer) (runtime.Instance, error) {
	s := a.sup
	port, err := runtime.FreePort()
	if err != nil {
		return runtime.Instance{}, err
	}
	inst, err := s.runtime.Start(ctx, runtime.Spec{
		App: a.name, Release: rel.Name(), Image: rel.Image, Env: env,
		HostPort: port, ContainerPort: containerPort,
		Memory: int64(m.Process.Memory), CPU: float64(m.Process.CPU),
	})
	if err != nil {
		return runtime.Instance{}, err
	}
	s.log.Info("instance started", "app", a.name, "release", rel.Name(), "id", inst.ID, "addr", inst.Addr)
	if m.Health.Path == "" {
		_, _ = fmt.Fprintf(out, "-----> waiting for %s to listen on port %d\n", rel.Name(), containerPort)
	} else {
		_, _ = fmt.Fprintf(out, "-----> waiting for %s to answer GET %s on port %d\n", rel.Name(), m.Health.Path, containerPort)
	}

	if err := a.waitHealthy(ctx, inst, m.Health, host); err != nil {
		bg := context.WithoutCancel(ctx)
		if a.exited(bg, inst.ID) {
			a.tail(bg, inst.ID, out)
		}
		if serr := s.runtime.Stop(bg, inst.ID, time.Duration(m.Process.Grace)); serr != nil {
			s.log.Warn("stop failed instance", "app", a.name, "id", inst.ID, "err", serr)
		}
		return runtime.Instance{}, err
	}
	return inst, nil
}

// waitHealthy probes the instance every h.Interval until a probe passes,
// the instance exits, h.Timeout passes, or ctx ends. The probe is a TCP
// connect-and-hold, or GET h.Path expecting 2xx when the manifest names one.
func (a *actor) waitHealthy(ctx context.Context, inst runtime.Instance, h manifest.Health, host string) error {
	s := a.sup
	timeout, interval := time.Duration(h.Timeout), time.Duration(h.Interval)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	exited := make(chan int, 1)
	go func() {
		if code, err := s.runtime.Wait(ctx, inst.ID); err == nil {
			exited <- code
		}
	}()

	tick := time.NewTicker(interval)
	defer tick.Stop()
	start := time.Now()
	var last string // what the latest HTTP probe saw, for the timeout message
	for {
		var ok bool
		if h.Path == "" {
			ok = listening(ctx, inst.Addr, interval)
		} else {
			var detail string
			ok, detail = answers(ctx, inst.Addr, host, h.Path)
			if ctx.Err() == nil {
				last = detail // a probe cut off by the deadline says nothing about the app
			}
		}
		if ok {
			s.log.Info("instance healthy", "app", a.name, "id", inst.ID, "after", time.Since(start))
			return nil
		}
		select {
		case code := <-exited:
			return fmt.Errorf("%s exited with code %d before passing its health check", inst.Release, code)
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				if h.Path == "" {
					return fmt.Errorf("%s did not accept connections on port %d within %s", inst.Release, containerPort, timeout)
				}
				return fmt.Errorf("%s did not answer 2xx on GET %s within %s (last: %s)", inst.Release, h.Path, timeout, last)
			}
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// listening reports whether something at addr accepts a connection and
// keeps it. A connect alone is not enough: Docker's userland proxy accepts
// on the host port as soon as the container starts and closes the
// connection at once when nothing listens inside yet. So the probe reads
// with a short deadline; the deadline passing (or the app speaking first)
// means a server holds the connection, EOF or a reset means nothing does.
func listening(ctx context.Context, addr string, dialTimeout time.Duration) bool {
	c, err := (&net.Dialer{Timeout: dialTimeout}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	defer func() { _ = c.Close() }()
	_ = c.SetReadDeadline(time.Now().Add(healthHold))
	_, err = c.Read(make([]byte, 1))
	return err == nil || errors.Is(err, os.ErrDeadlineExceeded)
}

// probeClient makes HTTP probes. No keep-alive: each probe is one
// connection, so a probe never holds a socket to an instance that is about
// to be stopped, and no connection pool is shared between apps.
var probeClient = &http.Client{
	Timeout:   probeTimeout,
	Transport: &http.Transport{DisableKeepAlives: true},
	// A health endpoint that redirects is not healthy; report the 3xx.
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

// answers reports whether GET path on the instance returns 2xx, and what it
// saw when it did not. The Host header is the app's own hostname, so an app
// that routes by host answers the same way it will for real requests.
func answers(ctx context.Context, addr, host, path string) (ok bool, detail string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		return false, err.Error()
	}
	req.Host = host
	resp, err := probeClient.Do(req)
	if err != nil {
		return false, err.Error()
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	return resp.StatusCode/100 == 2, resp.Status
}

// exited reports whether the instance's process has already ended.
func (a *actor) exited(ctx context.Context, id string) bool {
	list, err := a.sup.runtime.List(ctx)
	if err != nil {
		return false
	}
	for _, inst := range list {
		if inst.ID == id {
			return !inst.Running
		}
	}
	return false
}

// tail shows the pusher the last lines an instance wrote before it died.
func (a *actor) tail(ctx context.Context, id string, out io.Writer) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var buf bytes.Buffer
	_ = a.sup.runtime.Logs(ctx, id, time.Time{}, &buf, &buf)
	lines := bytes.Split(bytes.TrimRight(buf.Bytes(), "\n"), []byte{'\n'})
	if len(lines) > logTail {
		lines = lines[len(lines)-logTail:]
	}
	for _, l := range lines {
		if len(l) > 0 {
			_, _ = fmt.Fprintf(out, "       %s\n", l)
		}
	}
}

// url is where the app is reachable through the router.
func (s *Supervisor) url(host string) string {
	if s.cfg.HTTPPort == 80 || s.cfg.HTTPPort == 0 {
		return "http://" + host
	}
	return "http://" + net.JoinHostPort(host, strconv.Itoa(s.cfg.HTTPPort))
}
