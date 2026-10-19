// Starting the child: the pipes, the clone flags, the cgroup placement,
// and the wait for its verdict.
package native

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/maddoxrjohnson/ashore/internal/runtime"
	"golang.org/x/sys/unix"
)

// proc is a child this daemon started.
type proc struct {
	cmd  *exec.Cmd
	done chan struct{} // closed once Wait has reaped it
	code int
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
