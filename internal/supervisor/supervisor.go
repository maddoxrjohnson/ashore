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
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/builder"
	"github.com/maddoxrjohnson/ashore/internal/manifest"
	"github.com/maddoxrjohnson/ashore/internal/runtime"
	"github.com/maddoxrjohnson/ashore/internal/site"
	"github.com/maddoxrjohnson/ashore/internal/store"
)

// Store is the slice of the database the supervisor uses.
type Store interface {
	GetApp(ctx context.Context, name string) (store.App, error)
	ListApps(ctx context.Context) ([]store.App, error)
	OpenReleases(ctx context.Context) ([]store.Release, error)
	GetConfig(ctx context.Context, appID int64) (map[string]string, error)
	LiveRelease(ctx context.Context, appID int64) (store.Release, error)
	ReleaseByVersion(ctx context.Context, appID int64, version int) (store.Release, error)
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
	Set(host, app, addr string)
	Remove(host string)
}

// Logs collects what instances print; *logs.Hub satisfies it. Collect is
// run in a goroutine of its own for every instance the supervisor starts
// or adopts, with the supervisor's lifetime as its context. A nil Logs
// collects nothing.
type Logs interface {
	Collect(ctx context.Context, inst runtime.Instance, since time.Time) error
	Forget(app string) error
}

// Config is what a deploy needs to know about its surroundings.
type Config struct {
	DataDir string
	// Site makes the URL printed when a release goes live.
	Site site.Site
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
	// ContainerPort is the port apps listen on inside their container
	// (8080 under Docker). 0 means the runtime shares the host's network
	// and the app listens on the host port itself (the native runtime).
	ContainerPort int
}

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
	logs    Logs
	obs     Observer
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

// Deps is what the supervisor is built from. Store, Builder, Runtime, and
// Router are required; Logs, Observer, and Log may be nil (no log
// collection, no metrics, the default logger).
type Deps struct {
	Store    Store
	Builder  Builder
	Runtime  runtime.Runtime
	Router   Router
	Logs     Logs
	Observer Observer
	Log      *slog.Logger
}

// New wires the supervisor.
func New(d Deps, cfg Config) *Supervisor {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.Observer == nil {
		d.Observer = noObserver{}
	}
	if cfg.BuildConcurrency < 1 {
		cfg.BuildConcurrency = 1
	}
	sup := &Supervisor{
		store: d.Store, builder: d.Builder, runtime: d.Runtime, router: d.Router, logs: d.Logs, obs: d.Observer, cfg: cfg, log: d.Log,
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

// Rollback makes a new release from an earlier one's image and config
// snapshot and runs it. The old row is never revived: the history shows the
// rollback as its own release, noted "rollback vN". Only a release that was
// built can be rolled back to; a failed build has no image.
func (s *Supervisor) Rollback(ctx context.Context, app string, version int, out io.Writer) error {
	return s.send(ctx, app, deployCmd{kind: kindRollback, version: version}, out)
}

// Restart replaces the live release's instance with a fresh one of the same
// release: no new row, no build. It also clears a crashed app's failure
// history, which is how an operator ends a crash loop after fixing its
// cause, and drops any restart the supervisor had scheduled itself.
func (s *Supervisor) Restart(ctx context.Context, app string, out io.Writer) error {
	return s.send(ctx, app, deployCmd{kind: kindRestart, manual: true}, out)
}

// Destroy stops the app's instance, removes its route, and retires its
// actor, so nothing of the app runs or is reachable. The caller removes the
// rows and the repository afterwards; the order means a failure there
// leaves an app that exists but serves nothing, never one that serves
// without existing.
func (s *Supervisor) Destroy(ctx context.Context, app string) error {
	a, err := s.store.GetApp(ctx, app)
	if err != nil {
		return err
	}
	s.mu.Lock()
	act, ok := s.apps[app]
	s.mu.Unlock()
	if !ok {
		// Nothing was started since the daemon came up; there may still be
		// a route from reconcile.
		s.router.Remove(a.Host)
		return nil
	}
	cmd := deployCmd{ctx: ctx, kind: kindDestroy, host: a.Host, reply: make(chan error, 1)}
	select {
	case act.cmds <- cmd:
	case <-ctx.Done():
		return ctx.Err()
	case <-act.done:
		s.router.Remove(a.Host) // retired already; the route is all that could be left
		return nil
	}
	return <-cmd.reply
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

// forget drops an actor that has returned, so a later command for the same
// name starts a fresh one instead of finding a closed channel.
func (s *Supervisor) forget(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.apps[name]; ok && a.done != nil {
		select {
		case <-a.done:
		default:
			// run defers forget before close(done); the channel closes
			// right after this returns. The map entry still goes.
		}
		delete(s.apps, name)
	}
}
