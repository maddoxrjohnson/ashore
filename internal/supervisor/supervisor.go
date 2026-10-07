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
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/builder"
	"github.com/maddoxrjohnson/ashore/internal/gitserver"
	"github.com/maddoxrjohnson/ashore/internal/runtime"
	"github.com/maddoxrjohnson/ashore/internal/store"
)

// Store is the slice of the database the supervisor uses.
type Store interface {
	GetApp(ctx context.Context, name string) (store.App, error)
	CreateRelease(ctx context.Context, appID int64, image, gitSHA, note string) (store.Release, error)
	SetReleaseStatus(ctx context.Context, id int64, status string) error
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
	// HealthTimeout is how long a new instance has to accept a TCP
	// connection; StopGrace is SIGTERM-to-SIGKILL when stopping one.
	HealthTimeout time.Duration
	StopGrace     time.Duration
}

// Instance defaults until ashore.toml exists (Phase 2).
const (
	containerPort = 8080
	defaultMemory = 256 << 20
	defaultCPU    = 1.0
	healthEvery   = 250 * time.Millisecond
	healthHold    = 300 * time.Millisecond // how long a probe connection must stay open
	logTail       = 20                     // lines shown when an instance dies during its health check
)

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
		builds: make(chan struct{}, cfg.BuildConcurrency),
		apps:   make(map[string]*actor),
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
	a, err := s.actorFor(app)
	if err != nil {
		return err
	}
	cmd := deployCmd{ctx: ctx, sha: sha, quarantine: quarantine, out: out, reply: make(chan error, 1)}
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

	current *runtime.Instance // the instance the router points at, if this daemon started it
}

type deployCmd struct {
	ctx        context.Context
	sha        string
	quarantine string
	out        io.Writer
	reply      chan error // buffered: the actor never waits for the caller
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

	// Build, under the global cap.
	if err := s.acquireBuild(ctx, out); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "-----> building %s (%s)\n", a.name, short)
	image, err := s.builder.Build(ctx, builder.Job{
		App: a.name, Repo: gitserver.RepoPath(s.cfg.DataDir, a.name), SHA: cmd.sha, Quarantine: cmd.quarantine,
	}, out)
	<-s.builds
	if err != nil {
		return err
	}

	rel, err := s.store.CreateRelease(ctx, app.ID, image, cmd.sha, "push "+short)
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "-----> release %s\n", rel.Name())

	// Start and wait until it answers. From here on a failure must also
	// mark the release and remove the instance, with a context that still
	// works after the pusher's has been cancelled.
	inst, err := a.start(ctx, rel, out)
	if err == nil && ctx.Err() != nil {
		// The pusher is gone, so git will not record this commit. Going
		// live anyway would leave what runs and what is pushed disagreeing.
		err = ctx.Err()
		if serr := s.runtime.Stop(context.WithoutCancel(ctx), inst.ID, s.cfg.StopGrace); serr != nil {
			s.log.Warn("stop abandoned instance", "app", a.name, "id", inst.ID, "err", serr)
		}
	}
	if err != nil {
		_ = s.store.SetReleaseStatus(context.WithoutCancel(ctx), rel.ID, store.StatusFailed)
		return err
	}

	// The swap. Nothing after it may fail the push: the new release is
	// serving, and git must record the commit it came from.
	old := a.current
	s.router.Set(app.Host, inst.Addr)
	a.current = &inst
	s.log.Info("route swapped", "app", a.name, "release", rel.Name(), "addr", inst.Addr)
	_, _ = fmt.Fprintf(out, "-----> live at %s\n", s.url(app.Host))

	bg := context.WithoutCancel(ctx)
	if old != nil {
		if err := s.runtime.Stop(bg, old.ID, s.cfg.StopGrace); err != nil {
			s.log.Warn("stop old instance", "app", a.name, "id", old.ID, "err", err)
		}
	}
	if err := s.store.SetLive(bg, app.ID, rel.ID); err != nil {
		s.log.Error("mark release live", "app", a.name, "release", rel.Name(), "err", err)
	}
	return nil
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

// start runs the release and waits for it to accept a connection. On any
// failure the instance is stopped and removed, so a rejected push leaves
// nothing running.
func (a *actor) start(ctx context.Context, rel store.Release, out io.Writer) (runtime.Instance, error) {
	s := a.sup
	port, err := runtime.FreePort()
	if err != nil {
		return runtime.Instance{}, err
	}
	inst, err := s.runtime.Start(ctx, runtime.Spec{
		App: a.name, Release: rel.Name(), Image: rel.Image,
		HostPort: port, ContainerPort: containerPort,
		Memory: defaultMemory, CPU: defaultCPU,
	})
	if err != nil {
		return runtime.Instance{}, err
	}
	s.log.Info("instance started", "app", a.name, "release", rel.Name(), "id", inst.ID, "addr", inst.Addr)
	_, _ = fmt.Fprintf(out, "-----> waiting for %s to listen on port %d\n", rel.Name(), containerPort)

	if err := a.waitHealthy(ctx, inst); err != nil {
		bg := context.WithoutCancel(ctx)
		if a.exited(bg, inst.ID) {
			a.tail(bg, inst.ID, out)
		}
		if serr := s.runtime.Stop(bg, inst.ID, s.cfg.StopGrace); serr != nil {
			s.log.Warn("stop failed instance", "app", a.name, "id", inst.ID, "err", serr)
		}
		return runtime.Instance{}, err
	}
	return inst, nil
}

// waitHealthy tries a TCP connect every healthEvery until one succeeds,
// the instance exits, HealthTimeout passes, or ctx ends.
func (a *actor) waitHealthy(ctx context.Context, inst runtime.Instance) error {
	s := a.sup
	ctx, cancel := context.WithTimeout(ctx, s.cfg.HealthTimeout)
	defer cancel()

	exited := make(chan int, 1)
	go func() {
		if code, err := s.runtime.Wait(ctx, inst.ID); err == nil {
			exited <- code
		}
	}()

	tick := time.NewTicker(healthEvery)
	defer tick.Stop()
	start := time.Now()
	for {
		if listening(ctx, inst.Addr) {
			s.log.Info("instance healthy", "app", a.name, "id", inst.ID, "after", time.Since(start))
			return nil
		}
		select {
		case code := <-exited:
			return fmt.Errorf("%s exited with code %d before accepting connections", inst.Release, code)
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("%s did not accept connections on port %d within %s", inst.Release, containerPort, s.cfg.HealthTimeout)
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
func listening(ctx context.Context, addr string) bool {
	c, err := (&net.Dialer{Timeout: healthEvery}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	defer func() { _ = c.Close() }()
	_ = c.SetReadDeadline(time.Now().Add(healthHold))
	_, err = c.Read(make([]byte, 1))
	return err == nil || errors.Is(err, os.ErrDeadlineExceeded)
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
