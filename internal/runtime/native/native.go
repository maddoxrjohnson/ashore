// Package native runs instances without Docker at runtime: an image's
// filesystem is exported once, and each instance is the daemon binary
// re-executed into new namespaces, where it mounts an overlay over that
// filesystem, pivots into it, drops its privileges, and execs the app.
// Docker still builds the image; this package only needs it to export one.
package native

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/runtime"
)

// Runtime runs instances as re-executed children of the daemon in their
// own namespaces. Create it with New.
type Runtime struct {
	exporter *exporter
	dir      string // <dataDir>/instances
	rootless bool
	cg       *cgroups // nil: limits are not enforced
	log      *slog.Logger

	mu    sync.Mutex
	procs map[string]*proc // instances this daemon started and can wait on
}

var _ runtime.Runtime = (*Runtime)(nil)

// New returns a runtime keeping its state under <dataDir>/instances and
// exporting images under <dataDir>/rootfs. cgroupRoot is where instance
// cgroups are made (a directory on the cgroup v2 filesystem, such as
// /sys/fs/cgroup/ashore); it is used when the daemon is root and the
// filesystem is cgroup v2, and limits are not enforced otherwise. Without
// root the runtime is rootless: every instance lives in a user namespace
// where it is root while the host sees the daemon's user.
func New(d Docker, dataDir, cgroupRoot string, log *slog.Logger) *Runtime {
	if log == nil {
		log = slog.Default()
	}
	r := &Runtime{
		exporter: newExporter(d, dataDir),
		dir:      filepath.Join(dataDir, "instances"),
		rootless: os.Geteuid() != 0,
		log:      log,
		procs:    make(map[string]*proc),
	}
	switch {
	case r.rootless:
		log.Warn("native runtime without root: user namespaces instead of privileges; memory and cpu limits are not enforced")
	case cgroupRoot == "" || !cgroup2Mounted(filepath.Dir(cgroupRoot)):
		log.Warn("native runtime without cgroup v2: memory, cpu, and pids limits are not enforced", "root", cgroupRoot)
	default:
		r.cg = &cgroups{root: cgroupRoot}
		if err := r.cg.setup(); err != nil {
			log.Warn("native runtime: limits are not enforced", "err", err)
			r.cg = nil
		}
	}
	return r
}

// Start exports the image if needed, prepares the instance's directories,
// and re-executes the daemon as the container's init. It returns once the
// child has exec'd the app, or with the child's reason when it could not.
func (r *Runtime) Start(ctx context.Context, s runtime.Spec) (runtime.Instance, error) {
	if err := s.Validate(); err != nil {
		return runtime.Instance{}, err
	}
	im, err := r.exporter.Prepare(ctx, s.Image)
	if err != nil {
		return runtime.Instance{}, fmt.Errorf("start %s %s: %w", s.App, s.Release, err)
	}
	argv := im.Config.Argv()
	if len(argv) == 0 {
		return runtime.Instance{}, fmt.Errorf("start %s %s: image %s has no entrypoint or cmd", s.App, s.Release, s.Image)
	}
	id, err := newID()
	if err != nil {
		return runtime.Instance{}, err
	}
	dir := filepath.Join(r.dir, id)
	for _, sub := range []string{"upper", "work", "merged"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return runtime.Instance{}, err
		}
	}
	uid, gid, err := lookupUser(im.Rootfs(), im.Config.User)
	if err != nil {
		_ = os.RemoveAll(dir)
		return runtime.Instance{}, fmt.Errorf("start %s %s: %w", s.App, s.Release, err)
	}
	spec := initSpec{
		ID: id, Hostname: id[:12],
		Lower: im.Rootfs(), Upper: filepath.Join(dir, "upper"), Work: filepath.Join(dir, "work"), Merged: filepath.Join(dir, "merged"),
		Argv: argv, Env: environ(im.Config.Env, s.Env, s.HostPort), Dir: im.Config.WorkingDir,
		UID: uid, GID: gid, Rootless: r.rootless,
	}
	st := state{ID: id, App: s.App, Release: s.Release, Image: s.Image, Port: s.HostPort, CreatedAt: time.Now(), Labels: s.AllLabels()}

	p, err := r.spawn(ctx, dir, spec, &st, s)
	if err != nil {
		_ = os.RemoveAll(dir)
		return runtime.Instance{}, fmt.Errorf("start %s %s: %w", s.App, s.Release, err)
	}
	r.mu.Lock()
	r.procs[id] = p
	r.mu.Unlock()
	r.log.Info("native instance started", "app", s.App, "release", s.Release, "id", id, "pid", st.Pid, "rootless", r.rootless)
	return runtime.Instance{ID: id, App: s.App, Release: s.Release, Addr: net.JoinHostPort("127.0.0.1", strconv.Itoa(s.HostPort)),
		StartedAt: st.CreatedAt, Running: true}, nil
}

// environ is the app's environment: the image's, then the config vars
// over it, then PORT and HOST, which the runtime owns. The app shares the
// host's network, so PORT is the host port and HOST says to bind loopback.
func environ(image []string, vars map[string]string, port int) []string {
	env := make(map[string]string, len(image)+len(vars)+2)
	for _, kv := range image {
		k, v, _ := strings.Cut(kv, "=")
		env[k] = v
	}
	for k, v := range vars {
		env[k] = v
	}
	if _, ok := env["PATH"]; !ok {
		env["PATH"] = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
	}
	env["PORT"] = strconv.Itoa(port)
	env["HOST"] = "127.0.0.1"
	out := make([]string, 0, len(env))
	for k, v := range env {
		out = append(out, k+"="+v)
	}
	sort.Strings(out)
	return out
}

func newID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
