// The actor: the one goroutine that owns an app's deploy state. Commands
// arrive on a channel, exits arrive from watchers, and a scheduled restart
// arrives from the clock; nothing else touches the actor's fields.
package supervisor

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/runtime"
)

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
	kindRollback             // an earlier release's image and config, as a new row
	kindDestroy              // stop everything and retire the actor
	kindAdopt                // take over an instance found running at start
)

type deployCmd struct {
	ctx     context.Context
	kind    kind
	sha     string // kindPush
	note    string // kindRedeploy: the release note
	version int    // kindRollback: the release to go back to
	out     io.Writer
	reply   chan error // buffered: the actor never waits for the caller

	quarantine string // kindPush
	manual     bool   // kindRestart: asked for, not scheduled; resets the failure history
	host       string // kindDestroy: the route to remove
	adopt      *running
	adoptHost  string // kindAdopt: the instance and the route to restore
}

func (a *actor) run(ctx context.Context) {
	defer a.sup.actors.Done()
	defer close(a.done)
	defer a.sup.forget(a.name)
	for {
		select {
		case <-ctx.Done():
			return
		case cmd := <-a.cmds:
			if cmd.kind == kindDestroy {
				cmd.reply <- a.destroy(cmd)
				return
			}
			if cmd.kind == kindAdopt {
				cmd.reply <- a.adopt(cmd)
				continue
			}
			a.busy.Store(true)
			cmd.reply <- a.counted(cmd)
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
// what the exit means; the watcher only knows that it happened. A watcher
// whose actor has gone (destroy) has no one to tell and ends.
func (a *actor) watch(ctx context.Context, inst runtime.Instance) {
	defer a.sup.actors.Done()
	code, err := a.sup.runtime.Wait(ctx, inst.ID)
	if ctx.Err() != nil {
		return
	}
	select {
	case a.exits <- exitEvent{id: inst.ID, code: code, err: err}:
	case <-a.done:
	case <-ctx.Done():
	}
}

// adopt makes an instance found running at start the actor's current one:
// route restored, watcher started, no release change. Reconcile is the only
// caller, before any deploy for the app, so nothing is replaced.
func (a *actor) adopt(cmd deployCmd) error {
	s := a.sup
	if a.current != nil {
		return fmt.Errorf("%s already has an instance", a.name)
	}
	a.current = cmd.adopt
	s.router.Set(cmd.adoptHost, a.name, cmd.adopt.inst.Addr)
	a.setStatus(StateRunning, "adopted at start")
	s.actors.Add(1)
	go a.watch(s.ctx, cmd.adopt.inst)
	a.collect(cmd.adopt.inst, s.clock.Now()) // what it printed while no daemon listened is in Docker, not here
	s.log.Info("instance adopted", "app", a.name, "id", cmd.adopt.inst.ID, "release", cmd.adopt.inst.Release, "addr", cmd.adopt.inst.Addr)
	return nil
}

// destroy is the actor's last act: stop what runs, drop the route, and
// report nothing running. The instance gets its own grace, no drain: the
// route is gone at the same moment, and there is no new release for
// requests to move to.
func (a *actor) destroy(cmd deployCmd) error {
	s := a.sup
	a.retry = nil
	s.router.Remove(cmd.host)
	if cur := a.current; cur != nil && !cur.exited {
		if err := s.runtime.Stop(context.WithoutCancel(cmd.ctx), cur.inst.ID, cur.grace); err != nil {
			return fmt.Errorf("stop %s: %w", cur.inst.Release, err)
		}
		s.log.Info("instance stopped", "app", a.name, "id", cur.inst.ID, "release", cur.inst.Release, "reason", "destroy")
	}
	a.current = nil
	a.setStatus(StateStopped, "destroyed")
	s.obs.InstanceGone(a.name)
	if s.logs != nil {
		if err := s.logs.Forget(a.name); err != nil {
			s.log.Warn("forget logs", "app", a.name, "err", err)
		}
	}
	return nil
}

// collect starts the instance's log collector. It runs from the moment the
// instance exists, so the lines of a release that then fails its health
// check are on record too; it ends when the instance exits or the
// supervisor stops.
func (a *actor) collect(inst runtime.Instance, since time.Time) {
	s := a.sup
	if s.logs == nil {
		return
	}
	s.actors.Add(1)
	go func() {
		defer s.actors.Done()
		_ = s.logs.Collect(s.ctx, inst, since)
	}()
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
	if err := a.counted(deployCmd{ctx: ctx, kind: kindRestart, out: io.Discard}); err != nil {
		a.sup.log.Warn("restart failed", "app", a.name, "err", err)
		a.failed("restart failed: " + err.Error())
	}
}

// counted runs deploy and reports its result to the metrics.
func (a *actor) counted(cmd deployCmd) error {
	err := a.deploy(cmd)
	result := ResultLive
	if err != nil {
		result = ResultFailed
	}
	a.sup.obs.Deploy(a.name, result)
	return err
}

// setStatus records what ps will show.
func (a *actor) setStatus(state, detail string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.snap = Status{App: a.name, State: state, Detail: detail, Restarts: a.restarts}
	if cur := a.current; cur != nil {
		a.snap.Release, a.snap.Instance, a.snap.Addr, a.snap.StartedAt = cur.inst.Release, cur.inst.ID, cur.inst.Addr, cur.inst.StartedAt
	}
	a.sup.obs.InstanceState(a.name, state)
}

func (a *actor) status() (Status, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.snap, a.snap.State != ""
}
