package native

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	"github.com/maddoxrjohnson/ashore/internal/runtime"
)

// cgroups manages one cgroup v2 directory per instance under root, such
// as /sys/fs/cgroup/ashore/<id>. The files inside are the knobs: memory.max,
// cpu.max, pids.max. The child is placed in its cgroup by the kernel at
// clone time (CLONE_INTO_CGROUP via SysProcAttr.CgroupFD), so the limits
// hold from its first instruction and no process ever has to be moved.
type cgroups struct {
	root string
}

// controllers the instances need, enabled in the parent and in root so
// the per-instance directories get the files.
const controllers = "+cpu +memory +pids"

// cpuPeriod is the cpu.max period in microseconds; the quota is cpu × this.
const cpuPeriod = 100000

// cgroup2Mounted reports whether path is on a cgroup v2 filesystem.
func cgroup2Mounted(path string) bool {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return false
	}
	return st.Type == unix.CGROUP2_SUPER_MAGIC
}

// setup creates root and enables the controllers for its children. The
// parent of root must already have them enabled for root itself, which
// the system's root cgroup allows; under systemd a delegated slice does
// the same.
func (c *cgroups) setup() error {
	if err := os.MkdirAll(c.root, 0o755); err != nil {
		return fmt.Errorf("cgroup root: %w", err)
	}
	for _, dir := range []string{filepath.Dir(c.root), c.root} {
		if err := os.WriteFile(filepath.Join(dir, "cgroup.subtree_control"), []byte(controllers), 0o644); err != nil {
			return fmt.Errorf("enable controllers in %s: %w", dir, err)
		}
	}
	return nil
}

// create makes the instance's cgroup with its limits and returns its path.
// A zero memory or cpu means no limit; a zero pids means the default.
func (c *cgroups) create(id string, memory int64, cpu float64, pids int64) (string, error) {
	dir := filepath.Join(c.root, id)
	if err := os.Mkdir(dir, 0o755); err != nil {
		return "", fmt.Errorf("cgroup %s: %w", id, err)
	}
	if pids <= 0 {
		pids = runtime.DefaultPids
	}
	mem, swap := "max", "max"
	if memory > 0 {
		// No swap beyond the limit, or 256m behaves like 512m, as the
		// Docker runtime also arranges.
		mem, swap = strconv.FormatInt(memory, 10), "0"
	}
	cpuMax := "max " + strconv.Itoa(cpuPeriod)
	if cpu > 0 {
		cpuMax = strconv.Itoa(int(cpu*cpuPeriod)) + " " + strconv.Itoa(cpuPeriod)
	}
	for name, value := range map[string]string{
		"memory.max":       mem,
		"memory.swap.max":  swap,
		"memory.oom.group": "1", // an OOM kills the whole instance, not one thread of it
		"cpu.max":          cpuMax,
		"pids.max":         strconv.FormatInt(pids, 10),
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(value), 0o644); err != nil {
			_ = os.Remove(dir)
			return "", fmt.Errorf("cgroup %s: %s: %w", id, name, err)
		}
	}
	return dir, nil
}

// remove deletes the instance's cgroup. The kernel refuses while a
// process is still in it (EBUSY), and a process that just exited may take
// a moment to leave, so it retries briefly. A cgroup that is already gone
// is not an error.
func (c *cgroups) remove(dir string) error {
	var err error
	for range 50 {
		err = os.Remove(dir)
		if err == nil || errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if !errors.Is(err, unix.EBUSY) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("remove cgroup %s: %w", dir, err)
}

// readEvents parses a cgroup's memory.events: a map of event name (low,
// high, max, oom, oom_kill) to count.
func readEvents(dir string) (map[string]int64, error) {
	f, err := os.Open(filepath.Join(dir, "memory.events"))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	events := make(map[string]int64)
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		name, value, ok := strings.Cut(sc.Text(), " ")
		if !ok {
			continue
		}
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("memory.events: %q", sc.Text())
		}
		events[name] = n
	}
	return events, sc.Err()
}
