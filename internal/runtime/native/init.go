package native

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// initSpec is what the parent hands the child over a pipe: everything the
// init step needs to build the container's world and exec the app.
type initSpec struct {
	ID       string   `json:"id"`
	Hostname string   `json:"hostname"`
	Lower    string   `json:"lower"`  // the image's rootfs, read-only
	Upper    string   `json:"upper"`  // this instance's writes
	Work     string   `json:"work"`   // overlayfs scratch
	Merged   string   `json:"merged"` // the mount point that becomes /
	Argv     []string `json:"argv"`
	Env      []string `json:"env"`
	Dir      string   `json:"dir"` // working directory inside the container
	UID      int      `json:"uid"`
	GID      int      `json:"gid"`
	// Rootless is set when the daemon has no root: the child runs in a
	// user namespace where it is uid 0 but the host sees the daemon's
	// user. Some mounts and the uid switch are then done differently or
	// not at all.
	Rootless bool `json:"rootless"`
}

// File descriptors the parent passes beyond stdin, stdout, stderr.
const (
	specFD = 3 // the initSpec as JSON; closed by the parent after writing
	errFD  = 4 // the child's error, if any; closed on a successful exec
)

// Init is the child side: "ashored runtime-init". It runs already inside
// the new namespaces (the parent passed the clone flags), builds the
// filesystem, moves into it, drops privileges, and execs the app, which
// then is PID 1 of the container. It never returns on success; the exit
// status it returns is for the failure paths.
func Init(args []string) int {
	if len(args) != 0 {
		fmt.Fprintln(os.Stderr, "ashored runtime-init takes no arguments")
		return 2
	}
	errPipe := os.NewFile(errFD, "errpipe")
	fail := func(err error) int {
		_, _ = fmt.Fprint(errPipe, err.Error())
		_ = errPipe.Close()
		return 1
	}
	// A successful exec must close the error pipe so the parent sees EOF.
	if _, err := unix.FcntlInt(errFD, unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
		return fail(fmt.Errorf("errpipe cloexec: %w", err))
	}
	var spec initSpec
	specFile := os.NewFile(specFD, "spec")
	if err := json.NewDecoder(specFile).Decode(&spec); err != nil {
		return fail(fmt.Errorf("reading spec: %w", err))
	}
	_ = specFile.Close()
	if err := setup(spec); err != nil {
		return fail(err)
	}
	// The command is looked up on the app's PATH inside the new root, not
	// on this process's.
	for _, kv := range spec.Env {
		if v, ok := strings.CutPrefix(kv, "PATH="); ok {
			_ = os.Setenv("PATH", v)
		}
	}
	path, err := exec.LookPath(spec.Argv[0])
	if err != nil {
		return fail(fmt.Errorf("%s: %w", spec.Argv[0], err))
	}
	if err := unix.Exec(path, spec.Argv, spec.Env); err != nil {
		return fail(fmt.Errorf("exec %s: %w", path, err))
	}
	return 1 // unreachable
}

// setup builds the container's view of the world, in the order the
// kernel requires: hostname, private mount propagation, the overlay, the
// pseudo filesystems inside it, then the pivot that makes it /.
func setup(spec initSpec) error {
	if err := unix.Sethostname([]byte(spec.Hostname)); err != nil {
		return fmt.Errorf("sethostname: %w", err)
	}
	// Our mount namespace is a copy of the host's; without this, mounts
	// made here would propagate back to the host through shared peers.
	if err := unix.Mount("", "/", "", unix.MS_REC|unix.MS_PRIVATE, ""); err != nil {
		return fmt.Errorf("make / private: %w", err)
	}
	opts := "lowerdir=" + spec.Lower + ",upperdir=" + spec.Upper + ",workdir=" + spec.Work
	if spec.Rootless {
		// Unprivileged overlayfs keeps its bookkeeping in user.* xattrs.
		opts += ",userxattr"
	}
	if err := unix.Mount("overlay", spec.Merged, "overlay", 0, opts); err != nil {
		return fmt.Errorf("mount overlay on %s: %w", spec.Merged, err)
	}
	if err := mountSpecial(spec); err != nil {
		return err
	}
	if spec.Dir != "" {
		if err := os.MkdirAll(filepath.Join(spec.Merged, spec.Dir), 0o755); err != nil {
			return fmt.Errorf("working dir: %w", err)
		}
	}
	if err := pivot(spec.Merged); err != nil {
		return err
	}
	dir := spec.Dir
	if dir == "" {
		dir = "/"
	}
	if err := os.Chdir(dir); err != nil {
		return fmt.Errorf("chdir %s: %w", dir, err)
	}
	return harden(spec)
}

// mountSpecial fills the container's /proc, /sys, /dev, and /tmp, and
// gives it the host's DNS configuration, since it shares the host's
// network (D7).
func mountSpecial(spec initSpec) error {
	m := spec.Merged
	mount := func(src, dst, typ string, flags uintptr, data string) error {
		if err := os.MkdirAll(dst, 0o755); err != nil {
			return err
		}
		if err := unix.Mount(src, dst, typ, flags, data); err != nil {
			return fmt.Errorf("mount %s on %s: %w", typ, dst, err)
		}
		return nil
	}
	const noSUID = unix.MS_NOSUID | unix.MS_NOEXEC | unix.MS_NODEV
	if err := mount("proc", filepath.Join(m, "proc"), "proc", noSUID, ""); err != nil {
		return err
	}
	if err := mount("sysfs", filepath.Join(m, "sys"), "sysfs", noSUID|unix.MS_RDONLY, ""); err != nil {
		if !spec.Rootless {
			return err
		}
		// A user namespace may not mount sysfs unless it owns the network
		// namespace, which a rootless instance does not (it shares the
		// host's). A read-only bind of the host's /sys is the fallback.
		sys := filepath.Join(m, "sys")
		if err := unix.Mount("/sys", sys, "", unix.MS_BIND|unix.MS_REC, ""); err != nil {
			return fmt.Errorf("bind /sys: %w", err)
		}
		_ = unix.Mount("", sys, "", unix.MS_BIND|unix.MS_REMOUNT|unix.MS_RDONLY|noSUID, "")
	}
	dev := filepath.Join(m, "dev")
	if err := mount("tmpfs", dev, "tmpfs", unix.MS_NOSUID|unix.MS_STRICTATIME, "mode=755,size=65536k"); err != nil {
		return err
	}
	if err := makeDevices(dev, spec.Rootless); err != nil {
		return err
	}
	if err := mount("tmpfs", filepath.Join(dev, "shm"), "tmpfs", noSUID, "mode=1777,size=65536k"); err != nil {
		return err
	}
	if err := mount("tmpfs", filepath.Join(m, "tmp"), "tmpfs", unix.MS_NOSUID|unix.MS_NODEV, "mode=1777"); err != nil {
		return err
	}
	for _, name := range []string{"resolv.conf", "hosts"} {
		if err := bindFile("/etc/"+name, filepath.Join(m, "etc", name)); err != nil {
			return err
		}
	}
	return nil
}

// device is one node under /dev that every container gets.
type device struct {
	name         string
	major, minor uint32
}

var devices = []device{{"null", 1, 3}, {"zero", 1, 5}, {"full", 1, 7}, {"random", 1, 8}, {"urandom", 1, 9}, {"tty", 5, 0}}

// makeDevices creates the minimal /dev. With root the nodes are made;
// without (rootless), mknod is refused, so the host's nodes are
// bind-mounted onto empty files, which is what rootless runtimes do.
func makeDevices(dev string, rootless bool) error {
	for _, d := range devices {
		path := filepath.Join(dev, d.name)
		if rootless {
			if err := bindFile("/dev/"+d.name, path); err != nil {
				return err
			}
			continue
		}
		if err := unix.Mknod(path, unix.S_IFCHR|0o666, int(unix.Mkdev(d.major, d.minor))); err != nil {
			return fmt.Errorf("mknod %s: %w", path, err)
		}
	}
	for name, target := range map[string]string{"fd": "/proc/self/fd", "stdin": "/proc/self/fd/0", "stdout": "/proc/self/fd/1", "stderr": "/proc/self/fd/2"} {
		if err := os.Symlink(target, filepath.Join(dev, name)); err != nil {
			return err
		}
	}
	return nil
}

// bindFile bind-mounts the host file src onto dst inside the container,
// creating dst (and its directory) first. A missing src is skipped.
func bindFile(src, dst string) error {
	if _, err := os.Stat(src); err != nil {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_ = f.Close()
	if err := unix.Mount(src, dst, "", unix.MS_BIND, ""); err != nil {
		return fmt.Errorf("bind %s: %w", src, err)
	}
	return nil
}

// pivot makes merged the root of this mount namespace and discards the
// old one. pivot_root(".", ".") stacks the old root on top of the new one
// at the same path; detaching the top mount reveals the new root, so no
// /oldroot directory ever exists inside the container.
func pivot(merged string) error {
	if err := os.Chdir(merged); err != nil {
		return err
	}
	if err := unix.PivotRoot(".", "."); err != nil {
		return fmt.Errorf("pivot_root: %w", err)
	}
	if err := unix.Unmount(".", unix.MNT_DETACH); err != nil {
		return fmt.Errorf("unmount old root: %w", err)
	}
	return os.Chdir("/")
}

// readError returns what the child wrote to its error pipe: nothing on a
// successful exec.
func readError(r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("reading init status: %w", err)
	}
	if msg := strings.TrimSpace(string(b)); msg != "" {
		return errors.New(msg)
	}
	return nil
}
