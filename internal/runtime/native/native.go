package native

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

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

// proc is a child this daemon started.
type proc struct {
	cmd  *exec.Cmd
	done chan struct{} // closed once Wait has reaped it
	code int
}

// state is what the runtime remembers about an instance on disk, so a
// restarted daemon can find it again.
type state struct {
	ID        string            `json:"id"`
	App       string            `json:"app"`
	Release   string            `json:"release"`
	Image     string            `json:"image"`
	Pid       int               `json:"pid"`
	StartTime uint64            `json:"start_time"` // the kernel's, from /proc/<pid>/stat, against pid reuse
	Port      int               `json:"port"`
	Cgroup    string            `json:"cgroup,omitempty"` // the instance's cgroup directory, when limits are enforced
	CreatedAt time.Time         `json:"created_at"`
	Labels    map[string]string `json:"labels,omitempty"`
}

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
	if s.HostPort < 1 || s.HostPort > 65535 {
		return runtime.Instance{}, fmt.Errorf("start %s %s: host port %d out of range", s.App, s.Release, s.HostPort)
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
	labels := make(map[string]string, len(s.Labels)+2)
	for k, v := range s.Labels {
		labels[k] = v
	}
	labels[runtime.LabelApp], labels[runtime.LabelRelease] = s.App, s.Release
	st := state{ID: id, App: s.App, Release: s.Release, Image: s.Image, Port: s.HostPort, CreatedAt: time.Now(), Labels: labels}

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

// spawn starts the child and waits for its verdict on the error pipe.
func (r *Runtime) spawn(ctx context.Context, dir string, spec initSpec, st *state, s runtime.Spec) (*proc, error) {
	stdout, err := os.OpenFile(filepath.Join(dir, "stdout.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stdout.Close() }()
	stderr, err := os.OpenFile(filepath.Join(dir, "stderr.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	defer func() { _ = stderr.Close() }()
	specR, specW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	defer func() { _ = specR.Close() }()
	errR, errW, err := os.Pipe()
	if err != nil {
		_ = specW.Close()
		return nil, err
	}
	defer func() { _ = errR.Close() }()

	cmd := exec.Command("/proc/self/exe", "runtime-init")
	cmd.Env = []string{"PATH=/usr/bin:/bin"} // the child's own environment; the app's comes from the spec
	cmd.Stdin = nil
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.ExtraFiles = []*os.File{specR, errW} // fds 3 and 4 in the child
	cmd.SysProcAttr = &syscall.SysProcAttr{
		// A new session: the daemon's terminal signals never reach the app.
		Setsid:     true,
		Cloneflags: unix.CLONE_NEWPID | unix.CLONE_NEWNS | unix.CLONE_NEWUTS | unix.CLONE_NEWIPC,
	}
	if r.rootless {
		// Without privileges, a user namespace grants them inside: the
		// daemon's user becomes uid 0 there, which is enough for mounts
		// and pivot_root but maps to nothing more on the host.
		cmd.SysProcAttr.Cloneflags |= unix.CLONE_NEWUSER
		cmd.SysProcAttr.UidMappings = []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getuid(), Size: 1}}
		cmd.SysProcAttr.GidMappings = []syscall.SysProcIDMap{{ContainerID: 0, HostID: os.Getgid(), Size: 1}}
		cmd.SysProcAttr.GidMappingsEnableSetgroups = false
	}
	cgroupFD, err := r.place(cmd, st, s)
	if err != nil {
		_ = specW.Close()
		_ = errW.Close()
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		_ = specW.Close()
		_ = errW.Close()
		_ = cgroupFD.Close()
		r.unplace(st)
		return nil, fmt.Errorf("start init: %w", err)
	}
	_ = cgroupFD.Close()
	_ = errW.Close() // only the child holds the write end now
	err = json.NewEncoder(specW).Encode(spec)
	_ = specW.Close()
	p := &proc{cmd: cmd, done: make(chan struct{})}
	go p.reap()
	if err == nil {
		err = readError(errR) // EOF once the child has exec'd
	}
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		_ = cmd.Process.Kill()
		<-p.done
		r.unplace(st)
		return nil, err
	}
	st.Pid = cmd.Process.Pid
	st.StartTime, _ = startTime(st.Pid)
	if err := writeState(dir, *st); err != nil {
		_ = cmd.Process.Kill()
		<-p.done
		r.unplace(st)
		return nil, err
	}
	return p, nil
}

// place creates the instance's cgroup with the spec's limits and arranges
// for the child to be cloned straight into it. The returned file is the
// cgroup directory, to be closed once the child has started. With no
// cgroups it does nothing.
func (r *Runtime) place(cmd *exec.Cmd, st *state, s runtime.Spec) (*os.File, error) {
	if r.cg == nil {
		return nil, nil
	}
	dir, err := r.cg.create(st.ID, s.Memory, s.CPU, s.Pids)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(dir, os.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		_ = r.cg.remove(dir)
		return nil, fmt.Errorf("open cgroup: %w", err)
	}
	st.Cgroup = dir
	cmd.SysProcAttr.UseCgroupFD = true
	cmd.SysProcAttr.CgroupFD = int(f.Fd())
	return f, nil
}

// unplace removes a cgroup made for a child that did not get going.
func (r *Runtime) unplace(st *state) {
	if r.cg != nil && st.Cgroup != "" {
		_ = r.cg.remove(st.Cgroup)
	}
}

// reap waits for the child so it never becomes a zombie, and records how
// it ended. A process killed by a signal reports 128+signal, as a shell
// would.
func (p *proc) reap() {
	err := p.cmd.Wait()
	p.code = p.cmd.ProcessState.ExitCode()
	if p.code < 0 {
		if ws, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			p.code = 128 + int(ws.Signal())
		} else if err != nil {
			p.code = 1
		}
	}
	close(p.done)
}

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
		return -1, fmt.Errorf("wait %s: %w", short(id), runtime.ErrNotFound)
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
		return fmt.Errorf("logs %s: %w", short(id), runtime.ErrNotFound)
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
		any := false
		for _, pair := range []struct {
			f *os.File
			w io.Writer
		}{{out, stdout}, {errf, stderr}} {
			for {
				n, rerr := pair.f.Read(buf)
				if n > 0 {
					any = true
					if _, werr := pair.w.Write(buf[:n]); werr != nil {
						return any, werr
					}
				}
				if rerr != nil {
					break // EOF for now
				}
			}
		}
		return any, nil
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
			r.log.Warn("remove cgroup", "id", short(id), "err", err)
		}
	}
	r.log.Info("native instance stopped", "app", st.App, "id", short(id))
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

func short(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

func writeState(dir string, st state) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, "state.json.tmp")
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(dir, "state.json"))
}

func readState(dir string) (state, error) {
	data, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		return state{}, err
	}
	var st state
	if err := json.Unmarshal(data, &st); err != nil {
		return state{}, fmt.Errorf("%s: %w", dir, err)
	}
	return st, nil
}

// startTime is field 22 of /proc/<pid>/stat: when the process started, in
// clock ticks since boot. A pid can be reused; a pid with this start time
// cannot.
func startTime(pid int) (uint64, error) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return 0, err
	}
	// The command name is in parentheses and may contain spaces; fields
	// are counted after the closing one.
	i := strings.LastIndexByte(string(data), ')')
	if i < 0 {
		return 0, fmt.Errorf("/proc/%d/stat: unexpected format", pid)
	}
	fields := strings.Fields(string(data[i+1:]))
	if len(fields) < 20 {
		return 0, fmt.Errorf("/proc/%d/stat: unexpected format", pid)
	}
	return strconv.ParseUint(fields[19], 10, 64) // field 22 overall; 20th after the name
}

// alive reports whether the process the state describes still exists,
// start time included, so a reused pid is not mistaken for it.
func alive(st state) bool {
	if st.Pid <= 0 {
		return false
	}
	t, err := startTime(st.Pid)
	return err == nil && (st.StartTime == 0 || t == st.StartTime)
}
