// One run of the deploy algorithm: make the release row, build if there is
// no image, start and health-check an instance, swap the route, retire
// the old instance after the drain, and record the result.
package supervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/builder"
	"github.com/maddoxrjohnson/ashore/internal/gitcmd"
	"github.com/maddoxrjohnson/ashore/internal/gitserver"
	"github.com/maddoxrjohnson/ashore/internal/manifest"
	"github.com/maddoxrjohnson/ashore/internal/store"
)

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
	short := gitcmd.ShortSHA(cmd.sha)

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
		if cmd.manual {
			a.retry, a.failures = nil, nil
			_, _ = fmt.Fprintf(out, "-----> restarting %s\n", rel.Name())
		}
		return a.launchAndSwap(cmd, app, rel, out)
	case kindRollback:
		target, err := s.store.ReleaseByVersion(ctx, app.ID, cmd.version)
		if err != nil {
			return fmt.Errorf("%s: %w", a.name, err)
		}
		if target.Image == "" {
			return fmt.Errorf("%s %s has no image to roll back to (status %s)", a.name, target.Name(), target.Status)
		}
		if live, err := s.store.LiveRelease(ctx, app.ID); err == nil && live.ID == target.ID {
			return fmt.Errorf("%s %s is already live", a.name, target.Name())
		}
		n = store.NewRelease{Image: target.Image, GitSHA: target.GitSHA, ConfigJSON: target.ConfigJSON,
			ManifestJSON: target.ManifestJSON, Note: "rollback " + target.Name()}
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
	if cmd.kind != kindRollback {
		// A rollback runs the old snapshot; everything else runs the config
		// as it is now.
		vars, err := s.store.GetConfig(ctx, app.ID)
		if err != nil {
			return err
		}
		n.ConfigJSON = store.MarshalConfig(vars)
	}

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
		_, _ = fmt.Fprintf(out, "-----> release %s (%s)\n", rel.Name(), rel.Note)
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
		buildStart := time.Now()
		image, err := s.builder.Build(ctx, builder.Job{
			App: a.name, Repo: gitserver.RepoPath(s.cfg.DataDir, a.name), SHA: cmd.sha, Quarantine: cmd.quarantine,
		}, out)
		<-s.builds
		s.obs.Stage(StageBuild, time.Since(buildStart))
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
	swapStart := time.Now()
	old := a.current
	s.router.Set(app.Host, a.name, inst.Addr)
	a.current = &running{inst: inst, grace: time.Duration(m.Process.Grace)}
	a.retry = nil
	if cmd.kind == kindRestart {
		a.restarts++
		if !cmd.manual {
			s.obs.Restart(a.name)
		}
	} else {
		a.failures = nil
	}
	a.setStatus(StateRunning, "")
	s.actors.Add(1)
	go a.watch(s.ctx, inst)
	s.obs.Stage(StageSwap, time.Since(swapStart))
	s.log.Info("route swapped", "app", a.name, "release", rel.Name(), "addr", inst.Addr)
	_, _ = fmt.Fprintf(out, "-----> live at %s\n", s.cfg.Site.AppURL(app.Host))

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
