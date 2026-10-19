// Living with a started instance: waiting for it, tailing its output,
// stopping it, and listing what is on disk, for this daemon's own children
// and for instances a previous daemon started.
package native

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/runtime"
	"golang.org/x/sys/unix"
)

// Wait blocks until the instance exits. For a child of this daemon the
// exit code is the real one; for an instance adopted after a daemon
// restart the kernel will not tell us (we are not its parent), so Wait
// polls for the process to disappear and reports -1.
func (r *Runtime) Wait(ctx context.Context, id string) (int, error) {
	r.mu.Lock()
	p, ok := r.procs[id]
	r.mu.Unlock()
	if ok {
		select {
		case <-p.done:
			return p.code, nil
		case <-ctx.Done():
			return -1, ctx.Err()
		}
	}
	st, err := readState(filepath.Join(r.dir, id))
	if err != nil {
		return -1, fmt.Errorf("wait %s: %w", runtime.Short(id), runtime.ErrNotFound)
	}
	for alive(st) {
		select {
		case <-time.After(250 * time.Millisecond):
		case <-ctx.Done():
			return -1, ctx.Err()
		}
	}
	return -1, nil
}

// Logs copies the instance's stdout and stderr files to the writers and
// keeps copying until the process exits or ctx ends. The zero since means
// from the beginning; any other since starts at the current end of the
// files, since the lines carry no timestamps of their own.
func (r *Runtime) Logs(ctx context.Context, id string, since time.Time, stdout, stderr io.Writer) error {
	dir := filepath.Join(r.dir, id)
	st, err := readState(dir)
	if err != nil {
		return fmt.Errorf("logs %s: %w", runtime.Short(id), runtime.ErrNotFound)
	}
	out, err := os.Open(filepath.Join(dir, "stdout.log"))
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	errf, err := os.Open(filepath.Join(dir, "stderr.log"))
	if err != nil {
		return err
	}
	defer func() { _ = errf.Close() }()
	if !since.IsZero() {
		_, _ = out.Seek(0, io.SeekEnd)
		_, _ = errf.Seek(0, io.SeekEnd)
	}
	buf := make([]byte, 32*1024)
	drain := func() (bool, error) {
		got := false
		for _, pair := range []struct {
			f *os.File
			w io.Writer
		}{{out, stdout}, {errf, stderr}} {
			for {
				n, rerr := pair.f.Read(buf)
				if n > 0 {
					got = true
					if _, werr := pair.w.Write(buf[:n]); werr != nil {
						return got, werr
					}
				}
				if rerr != nil {
					break // EOF for now
				}
			}
		}
		return got, nil
	}
	for {
		if _, err := drain(); err != nil {
			return err
		}
		if !r.running(id, st) {
			_, err := drain() // whatever was written between the last read and the exit
			return err
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// running reports whether the instance's process is alive: by our own
// bookkeeping for a child, by /proc otherwise.
func (r *Runtime) running(id string, st state) bool {
	r.mu.Lock()
	p, ok := r.procs[id]
	r.mu.Unlock()
	if ok {
		select {
		case <-p.done:
			return false
		default:
			return true
		}
	}
	return alive(st)
}

// Stop ends the instance and removes everything it had: SIGTERM, then
// SIGKILL once grace has passed, then its cgroup and directory. The app is
// PID 1 of its namespace, and PID 1 ignores a signal it has no handler
// for, so an app that does not handle SIGTERM lives until the SIGKILL,
// exactly as under docker stop. A cancelled ctx cuts the grace short.
// Stopping an instance that is already gone is not an error.
func (r *Runtime) Stop(ctx context.Context, id string, grace time.Duration) error {
	dir := filepath.Join(r.dir, id)
	st, err := readState(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	r.mu.Lock()
	p, ok := r.procs[id]
	r.mu.Unlock()
	if ok {
		r.stopChild(ctx, p, grace)
		r.mu.Lock()
		delete(r.procs, id)
		r.mu.Unlock()
	} else {
		r.stopAdopted(ctx, st, grace)
	}
	if r.cg != nil && st.Cgroup != "" {
		if err := r.cg.remove(st.Cgroup); err != nil {
			r.log.Warn("remove cgroup", "id", runtime.Short(id), "err", err)
		}
	}
	r.log.Info("native instance stopped", "app", st.App, "id", runtime.Short(id))
	return os.RemoveAll(dir)
}

// stopChild ends a child of this daemon, whose exit the reaper reports.
func (r *Runtime) stopChild(ctx context.Context, p *proc, grace time.Duration) {
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Signal(unix.SIGTERM)
	select {
	case <-p.done:
		return
	case <-time.After(grace):
	case <-ctx.Done():
	}
	_ = p.cmd.Process.Kill()
	<-p.done
}

// stopAdopted ends an instance a previous daemon started: only /proc says
// whether it is still there.
func (r *Runtime) stopAdopted(ctx context.Context, st state, grace time.Duration) {
	if !alive(st) {
		return
	}
	_ = unix.Kill(st.Pid, unix.SIGTERM)
	deadline := time.Now().Add(grace)
	for alive(st) && time.Now().Before(deadline) && ctx.Err() == nil {
		time.Sleep(50 * time.Millisecond)
	}
	for alive(st) {
		_ = unix.Kill(st.Pid, unix.SIGKILL)
		time.Sleep(50 * time.Millisecond)
	}
}

// List reads every instance's state file: this daemon's children and the
// instances of daemons before it, for reconcile. Running is decided from
// /proc for the latter, with the start time guarding against a reused pid.
func (r *Runtime) List(context.Context) ([]runtime.Instance, error) {
	entries, err := os.ReadDir(r.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []runtime.Instance
	for _, e := range entries {
		st, err := readState(filepath.Join(r.dir, e.Name()))
		if err != nil {
			continue // a directory mid-creation or mid-removal
		}
		inst := runtime.Instance{ID: st.ID, App: st.App, Release: st.Release, StartedAt: st.CreatedAt, Running: r.running(st.ID, st)}
		if inst.Running {
			inst.Addr = net.JoinHostPort("127.0.0.1", strconv.Itoa(st.Port))
		}
		out = append(out, inst)
	}
	return out, nil
}
