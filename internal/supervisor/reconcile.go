package supervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/manifest"
	"github.com/maddoxrjohnson/ashore/internal/runtime"
	"github.com/maddoxrjohnson/ashore/internal/store"
)

// Report is what Reconcile found and did.
type Report struct {
	Adopted   int // running instances of a live release: route restored, watcher started
	Restarted int // live releases with nothing running: a start was scheduled
	Orphans   int // running instances that should not be: stopped and removed
	Removed   int // exited instances: removed
	Failed    int // releases left building or starting by a cut-off deploy: marked failed
}

func (r Report) String() string {
	return fmt.Sprintf("adopted=%d restarted=%d orphans=%d removed=%d failed=%d", r.Adopted, r.Restarted, r.Orphans, r.Removed, r.Failed)
}

// Reconcile makes what runs agree with what the store says should run,
// after a daemon start. The runtime's list of labelled containers is the
// observed state, the apps' live releases the desired one. A running
// instance of an app's live release is adopted; anything else running is
// an orphan and is stopped; exited instances are removed; a live release
// with nothing running is started again, in the background, through the
// normal deploy path; releases cut off mid-deploy are marked failed. It
// returns what it did, and the first error that stopped it from looking.
func (s *Supervisor) Reconcile(ctx context.Context) (Report, error) {
	var rep Report
	insts, err := s.runtime.List(ctx)
	if err != nil {
		return rep, fmt.Errorf("reconcile: %w", err)
	}
	apps, err := s.store.ListApps(ctx)
	if err != nil {
		return rep, fmt.Errorf("reconcile: %w", err)
	}
	byApp := make(map[string][]runtime.Instance)
	for _, inst := range insts {
		byApp[inst.App] = append(byApp[inst.App], inst)
	}

	for _, app := range apps {
		live, err := s.store.LiveRelease(ctx, app.ID)
		hasLive := err == nil
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			return rep, fmt.Errorf("reconcile %s: %w", app.Name, err)
		}
		var keep *runtime.Instance
		for _, inst := range byApp[app.Name] {
			switch {
			case !inst.Running:
				s.remove(ctx, inst, "exited")
				rep.Removed++
			case hasLive && inst.Release == live.Name() && inst.Addr != "" && keep == nil:
				keep = &inst
			default:
				s.remove(ctx, inst, "orphan")
				rep.Orphans++
			}
		}
		delete(byApp, app.Name)
		if !hasLive {
			continue
		}
		if keep != nil {
			if err := s.adopt(ctx, app, live, *keep); err != nil {
				s.log.Error("adopt instance", "app", app.Name, "id", keep.ID, "err", err)
				continue
			}
			rep.Adopted++
			continue
		}
		// Nothing runs for a release that should: start one, but not here.
		// A health wait per app would hold the whole start.
		s.log.Warn("live release has no instance; restarting", "app", app.Name, "release", live.Name())
		rep.Restarted++
		go func(name string) {
			if err := s.send(s.ctx, name, deployCmd{kind: kindRestart}, io.Discard); err != nil {
				s.log.Error("restart at reconcile", "app", name, "err", err)
			}
		}(app.Name)
	}
	// Instances of apps the store does not know.
	for _, insts := range byApp {
		for _, inst := range insts {
			s.remove(ctx, inst, "no such app")
			rep.Orphans++
		}
	}

	open, err := s.store.OpenReleases(ctx)
	if err != nil {
		return rep, fmt.Errorf("reconcile: %w", err)
	}
	for _, rel := range open {
		if err := s.store.MarkFailed(ctx, rel.ID); err != nil {
			s.log.Error("fail cut-off release", "release", rel.ID, "err", err)
			continue
		}
		rep.Failed++
	}
	s.log.Info("reconciled", "adopted", rep.Adopted, "restarted", rep.Restarted, "orphans", rep.Orphans, "removed", rep.Removed, "failed", rep.Failed)
	return rep, nil
}

// remove stops and removes an instance that should not be there.
func (s *Supervisor) remove(ctx context.Context, inst runtime.Instance, why string) {
	if err := s.runtime.Stop(ctx, inst.ID, s.cfg.StopGrace); err != nil {
		s.log.Warn("remove instance", "app", inst.App, "id", inst.ID, "why", why, "err", err)
		return
	}
	s.log.Info("instance removed", "app", inst.App, "id", inst.ID, "release", inst.Release, "why", why)
}

// adopt hands a running instance of the live release to the app's actor.
func (s *Supervisor) adopt(ctx context.Context, app store.App, live store.Release, inst runtime.Instance) error {
	m, err := manifest.FromJSON(live.ManifestJSON, s.defaults())
	if err != nil {
		return err
	}
	a, err := s.actorFor(app.Name)
	if err != nil {
		return err
	}
	cmd := deployCmd{ctx: ctx, kind: kindAdopt, adopt: &running{inst: inst, grace: time.Duration(m.Process.Grace)}, adoptHost: app.Host, reply: make(chan error, 1)}
	select {
	case a.cmds <- cmd:
	case <-ctx.Done():
		return ctx.Err()
	}
	return <-cmd.reply
}
