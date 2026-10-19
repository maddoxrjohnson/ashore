package native

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The cgroup code is a set of file writes; against a temp directory the
// writes land in regular files, which is enough to check the knobs.
func TestCgroupFiles(t *testing.T) {
	parent := t.TempDir()
	c := &cgroups{root: filepath.Join(parent, "ashore")}
	if err := c.setup(); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{parent, c.root} {
		b, err := os.ReadFile(filepath.Join(dir, "cgroup.subtree_control"))
		if err != nil || string(b) != "+cpu +memory +pids" {
			t.Errorf("%s/cgroup.subtree_control: %q %v", dir, b, err)
		}
	}

	dir, err := c.create("abc", 64<<20, 0.5, 0)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{
		"memory.max": "67108864", "memory.swap.max": "0", "memory.oom.group": "1",
		"cpu.max": "50000 100000", "pids.max": "256",
	} {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil || string(b) != want {
			t.Errorf("%s = %q, want %q (%v)", name, b, want, err)
		}
	}
	// A real cgroup directory is empty as far as rmdir is concerned; the
	// temp directory holds regular files, so clear them first.
	clearFiles(t, dir)
	if err := c.remove(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("cgroup dir still there: %v", err)
	}
	if err := c.remove(dir); err != nil {
		t.Fatalf("removing twice: %v", err)
	}

	// No limits: "max", and the pids default.
	dir, err = c.create("free", 0, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"memory.max": "max", "memory.swap.max": "max", "cpu.max": "max 100000", "pids.max": "1000"} {
		if b, _ := os.ReadFile(filepath.Join(dir, name)); string(b) != want {
			t.Errorf("%s = %q, want %q", name, b, want)
		}
	}
	// A second create of the same id fails rather than reusing a cgroup
	// that may still hold a process.
	if _, err := c.create("free", 0, 0, 0); err == nil {
		t.Error("duplicate cgroup created")
	}
}

func TestReadEvents(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "memory.events"), []byte("low 0\nhigh 3\nmax 12\noom 1\noom_kill 1\n"), 0o644)
	ev, err := readEvents(dir)
	if err != nil || ev["oom_kill"] != 1 || ev["max"] != 12 || ev["low"] != 0 {
		t.Fatalf("%v %v", ev, err)
	}
	_ = os.WriteFile(filepath.Join(dir, "memory.events"), []byte("oom x\n"), 0o644)
	if _, err := readEvents(dir); err == nil || !strings.Contains(err.Error(), "memory.events") {
		t.Fatalf("bad file: %v", err)
	}
}

func clearFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		_ = os.Remove(filepath.Join(dir, e.Name()))
	}
}
