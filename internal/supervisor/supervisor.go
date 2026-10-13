// Package supervisor owns the lifecycle of an app's releases: build the
// image, start an instance, wait until it answers, point the router at it,
// stop what it replaced, record the result, and keep it running.
//
// One goroutine per app (its actor) reads commands from a channel, so at
// most one deploy per app runs at a time and no mutex guards the deploy
// state; a second push to the same app waits its turn and is told so.
// Different apps deploy concurrently, bounded only by the build semaphore.
// A watcher goroutine per instance waits for its process to exit and tells
// the actor, which restarts it with exponential backoff and gives up after
// too many failures in a window.
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
	// DrainGrace is how long a replaced instance keeps running after the
	// route moved away from it, so requests it was already handling finish.
	DrainGrace time.Duration
}

// Crash handling: an instance that exits on its own is restarted after
// backoff(n) for its n-th failure inside crashWindow; the crashThreshold-th
// failure marks the app crashed and nothing more is tried until a restart
// is asked for.
const (
	crashWindow    = 10 * time.Minute
	crashThreshold = 10
	maxBackoff     = 60 * time.Second
)

// backoff is the wait before the n-th restart (n from 1): 1, 2, 4, ... s,
// capped at maxBackoff.
func backoff(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	if n > 7 { // 1<<6 s is already past the cap
		return maxBackoff
	}
	return min(time.Second<<(n-1), maxBackoff)
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
	clock        clock

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
		clock:        realClock{},
	}
	sup.ctx, sup.cancel = context.WithCancel(context.Background())
	return sup
}

// Run blocks until ctx is done, then ends every actor and waits for them.
// Deploys in progress see a cancelled context and fail; nothing live is
// stopped on the way out, since containers outlive the daemon on purpose.
// An instance that was already replaced and is draining is stopped at once
// rather than left for reconcile to find.
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
		a = &actor{sup: s, name: app, cmds: make(chan deployCmd), exits: make(chan exitEvent, 8), done: make(chan struct{})}
		s.apps[app] = a
		s.actors.Add(1)
		go a.run(s.ctx)
	}
	return a, nil
}

// Status reports an app's instance as the supervisor sees it. ok is false
// for an app nothing has been started for since the daemon came up.
func (s *Supervisor) Status(app string) (Status, bool) {
	s.mu.Lock()
	a, ok := s.apps[app]
	s.mu.Unlock()
	if !ok {
		return Status{}, false
	}
	return a.status()
}

// actor is the one goroutine that owns an app's deploy state.
type actor struct {
	sup   *Supervisor
	name  string
	cmds  chan deployCmd
	exits chan exitEvent // from watchers; buffered so a watcher never waits on a deploy
	done  chan struct{}  // closed when the actor has returned
	busy  atomic.Bool    // a deploy is in progress; the next pusher is told to wait

	current  *running         // what the router points at, if this daemon started it
	retry    <-chan time.Time // the scheduled restart, nil when none
	failures []time.Time      // unexpected exits inside crashWindow
	restarts int              // automatic restarts since the daemon started

	mu   sync.Mutex
	snap Status // what Status reports; the actor writes, anyone reads
}

// running is an instance this actor started and what it needs to stop it.
type running struct {
	inst   runtime.Instance
	grace  time.Duration // its manifest's process.grace
	exited bool          // its process is gone; onExit set this
}

// exitEvent is a watcher's report: the instance's process ended.
type exitEvent struct {
	id   string
	code int
	err  error
}

// kind is what a deploy starts from.
type kind int

const (
	kindPush     kind = iota // a pushed commit: build it, then run it
	kindRedeploy             // the live image again, with the config as it is now
	kindRestart              // the live release again, as it is: no new row
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
		case ev := <-a.exits:
			a.onExit(ctx, ev)
		case <-a.retry:
			a.retry = nil
			a.restart(ctx)
		}
	}
}

// watch waits for one instance to exit and reports it. The actor decides
// what the exit means; the watcher only knows that it happened.
func (a *actor) watch(ctx context.Context, inst runtime.Instance) {
	defer a.sup.actors.Done()
	code, err := a.sup.runtime.Wait(ctx, inst.ID)
	if ctx.Err() != nil {
		return
	}
	select {
	case a.exits <- exitEvent{id: inst.ID, code: code, err: err}:
	case <-ctx.Done():
	}
}

// onExit handles an instance's exit. Only the current instance matters: a
// replaced instance exits because retire stopped it, and the runtime may
// report an instance twice. The dead container is removed, the failure is
// counted, and a restart is scheduled or the app is given up on.
func (a *actor) onExit(ctx context.Context, ev exitEvent) {
	cur := a.current
	if cur == nil || cur.inst.ID != ev.id || cur.exited {
		return
	}
	cur.exited = true
	why := fmt.Sprintf("exited with code %d", ev.code)
	if ev.err != nil {
		why = "exited: " + ev.err.Error()
	}
	a.sup.log.Warn("instance exited", "app", a.name, "id", cur.inst.ID, "release", cur.inst.Release, "code", ev.code, "err", ev.err)
	if err := a.sup.runtime.Stop(context.WithoutCancel(ctx), cur.inst.ID, 0); err != nil {
		a.sup.log.Warn("remove exited instance", "app", a.name, "id", cur.inst.ID, "err", err)
	}
	a.failed(why)
}

// failed counts one failure of the running release and either schedules the
// next restart or, at the threshold, stops trying. The route is left as it
// is: a 502 with a request id says "the app is here and it is down", which
// a 404 would not.
func (a *actor) failed(why string) {
	s := a.sup
	now := s.clock.Now()
	kept := a.failures[:0]
	for _, t := range a.failures {
		if now.Sub(t) < crashWindow {
			kept = append(kept, t)
		}
	}
	a.failures = append(kept, now)
	n := len(a.failures)
	if n >= crashThreshold {
		s.log.Error("app crashed; not restarting until asked", "app", a.name, "failures", n, "window", crashWindow, "last", why)
		a.setStatus(StateCrashed, fmt.Sprintf("%s; %d failures in %s, giving up", why, n, crashWindow))
		return
	}
	wait := backoff(n)
	s.log.Warn("scheduling restart", "app", a.name, "reason", why, "attempt", n, "in", wait)
	a.setStatus(StateRestarting, fmt.Sprintf("%s; restart %d in %s", why, n, wait))
	a.retry = s.clock.After(wait)
}

// restart is the scheduled restart: the live release again. A restart that
// does not come up is one more failure.
func (a *actor) restart(ctx context.Context) {
	a.busy.Store(true)
	defer a.busy.Store(false)
	if err := a.deploy(deployCmd{ctx: ctx, kind: kindRestart, out: io.Discard}); err != nil {
		a.sup.log.Warn("restart failed", "app", a.name, "err", err)
		a.failed("restart failed: " + err.Error())
	}
}

// setStatus records what ps will show.
func (a *actor) setStatus(state, detail string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.snap = Status{App: a.name, State: state, Detail: detail, Restarts: a.restarts}
	if cur := a.current; cur != nil {
		a.snap.Release, a.snap.Instance, a.snap.Addr, a.snap.StartedAt = cur.inst.Release, cur.inst.ID, cur.inst.Addr, cur.inst.StartedAt
	}
}

func (a *actor) status() (Status, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.snap, a.snap.State != ""
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
	case kindRestart:
		rel, err := s.store.LiveRelease(ctx, app.ID)
		if errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("%s has no live release to restart", a.name)
		}
		if err != nil {
			return err
		}
		return a.launchAndSwap(cmd, app, rel, out)
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

	return a.launchAndSwap(cmd, app, rel, out)
}

// launchAndSwap runs a release that has an image and makes it the one the
// router points at. For a new release (push, redeploy) a failure ends the
// row as failed and success marks it live; a restart reuses the live row
// and touches neither.
func (a *actor) launchAndSwap(cmd deployCmd, app store.App, rel store.Release, out io.Writer) error {
	s := a.sup
	ctx := cmd.ctx
	fail := func(err error) error {
		if cmd.kind != kindRestart {
			a.fail(ctx, rel)
		}
		return err
	}

	// Start and wait until it answers. The environment and the manifest
	// are the row's snapshots, not the values read when the row was made,
	// so what runs is what the release says ran.
	env, err := store.UnmarshalConfig(rel.ConfigJSON)
	if err != nil {
		return fail(err)
	}
	m, err := manifest.FromJSON(rel.ManifestJSON, s.defaults())
	if err != nil {
		return fail(err)
	}
	if a.current == nil || a.current.exited {
		a.setStatus(StateStarting, "starting "+rel.Name())
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
		return fail(err)
	}

	// The swap. Nothing after it may fail the push: the new release is
	// serving, and git must record the commit it came from. A pending
	// restart is for the instance just replaced, so it is dropped; a new
	// release starts with a clean failure history.
	old := a.current
	s.router.Set(app.Host, inst.Addr)
	a.current = &running{inst: inst, grace: time.Duration(m.Process.Grace)}
	a.retry = nil
	if cmd.kind == kindRestart {
		a.restarts++
	} else {
		a.failures = nil
	}
	a.setStatus(StateRunning, "")
	s.actors.Add(1)
	go a.watch(s.ctx, inst)
	s.log.Info("route swapped", "app", a.name, "release", rel.Name(), "addr", inst.Addr)
	_, _ = fmt.Fprintf(out, "-----> live at %s\n", s.url(app.Host))

	if old != nil && !old.exited {
		a.retire(old)
	}
	if cmd.kind != kindRestart {
		if err := s.store.SetLive(context.WithoutCancel(ctx), app.ID, rel.ID); err != nil {
			s.log.Error("mark release live", "app", a.name, "release", rel.Name(), "err", err)
		}
	}
	return nil
}

// retire stops an instance the route no longer points at, after the drain
// grace, so requests the proxy had already sent it finish. It runs in its
// own goroutine so the actor is free for the next command at once; the
// supervisor's lifetime bounds it, and a daemon shutting down stops the
// instance without the rest of the wait, because it is superseded either
// way and reconcile would only have to find it later.
func (a *actor) retire(old *running) {
	s := a.sup
	s.actors.Add(1)
	go func() {
		defer s.actors.Done()
		select {
		case <-time.After(s.cfg.DrainGrace):
		case <-s.ctx.Done():
		}
		if err := s.runtime.Stop(context.WithoutCancel(s.ctx), old.inst.ID, old.grace); err != nil {
			s.log.Warn("stop old instance", "app", a.name, "id", old.inst.ID, "err", err)
			return
		}
		s.log.Info("old instance stopped", "app", a.name, "id", old.inst.ID, "release", old.inst.Release, "drained", s.cfg.DrainGrace)
	}()
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
