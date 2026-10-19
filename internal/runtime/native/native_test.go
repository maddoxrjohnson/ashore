package native

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/runtime"
)

// TestMain lets the test binary act as the container init: Start re-executes
// /proc/self/exe, which is this binary, with "runtime-init".
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "runtime-init" {
		os.Exit(Init(os.Args[2:]))
	}
	code := m.Run()
	if probeDir != "" {
		_ = os.RemoveAll(probeDir)
	}
	os.Exit(code)
}

// The program every instance test runs: it reports what it sees, writes a
// file, and then serves TCP connections on $PORT until killed. Built once
// per test binary run with the host's Go toolchain, statically, into an
// image directory of its own.
const probeSource = `package main

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

func main() {
	if os.Getenv("HOG") != "" {
		var held [][]byte
		for {
			chunk := make([]byte, 4<<20)
			for i := 0; i < len(chunk); i += 4096 {
				chunk[i] = 1
			}
			held = append(held, chunk)
			fmt.Printf("holding %d MiB\n", len(held)*4)
		}
	}
	if os.Getenv("IGNORE") != "" {
		signal.Ignore(syscall.SIGTERM)
	}
	if os.Getenv("TRAP") != "" {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM)
		go func() {
			<-ch
			fmt.Println("got SIGTERM, leaving")
			os.Exit(0)
		}()
	}
	host, _ := os.Hostname()
	wd, _ := os.Getwd()
	fmt.Printf("pid=%d hostname=%s uid=%d gid=%d cwd=%s port=%s host=%s greeting=%s\n",
		os.Getpid(), host, os.Getuid(), os.Getgid(), wd, os.Getenv("PORT"), os.Getenv("HOST"), os.Getenv("GREETING"))
	entries, _ := os.ReadDir("/")
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	fmt.Printf("root=%s\n", strings.Join(names, ","))
	_, oldroot := os.Stat("/oldroot")
	fmt.Printf("oldroot_missing=%v\n", os.IsNotExist(oldroot))
	if err := os.WriteFile("/written-by-app", []byte("hello"), 0o644); err != nil {
		fmt.Printf("write=%v\n", err)
	}
	if _, err := os.Stat("/proc/1/status"); err != nil {
		fmt.Printf("proc=%v\n", err)
	}
	fmt.Fprintln(os.Stderr, "probe: this goes to stderr")
	ln, err := net.Listen("tcp", "127.0.0.1:"+os.Getenv("PORT"))
	if err != nil {
		fmt.Println("listen:", err)
		os.Exit(3)
	}
	fmt.Println("listening")
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		fmt.Fprintln(c, "pong")
		c.Close()
	}
}
`

var (
	probeOnce sync.Once
	probeDir  string // an image directory (rootfs + config.json + done) holding the probe
	probeErr  error
)

// probeImage builds the probe into an exported-image layout under a
// directory that outlives the test, so every test shares one build.
func probeImage(t *testing.T) string {
	t.Helper()
	probeOnce.Do(func() {
		goBin, err := exec.LookPath("go")
		if err != nil {
			probeErr = err
			return
		}
		dir, err := os.MkdirTemp("", "ashore-probe-*")
		if err != nil {
			probeErr = err
			return
		}
		src := filepath.Join(dir, "src")
		_ = os.MkdirAll(src, 0o755)
		_ = os.WriteFile(filepath.Join(src, "main.go"), []byte(probeSource), 0o644)
		_ = os.WriteFile(filepath.Join(src, "go.mod"), []byte("module probe\n\ngo 1.22\n"), 0o644)
		rootfs := filepath.Join(dir, "rootfs")
		_ = os.MkdirAll(filepath.Join(rootfs, "bin"), 0o755)
		_ = os.MkdirAll(filepath.Join(rootfs, "etc"), 0o755)
		_ = os.WriteFile(filepath.Join(rootfs, "etc", "passwd"), []byte("root:x:0:0::/:/bin/sh\napp:x:1234:5678::/srv:/bin/sh\n"), 0o644)
		_ = os.WriteFile(filepath.Join(rootfs, "etc", "group"), []byte("root:x:0:\nstaff:x:4321:\n"), 0o644)
		_ = os.WriteFile(filepath.Join(rootfs, "marker-from-image"), []byte("x"), 0o644)
		cmd := exec.Command(goBin, "build", "-o", filepath.Join(rootfs, "bin", "probe"), ".")
		cmd.Dir = src
		cmd.Env = append(os.Environ(), "CGO_ENABLED=0", "GOFLAGS=-mod=mod")
		if out, err := cmd.CombinedOutput(); err != nil {
			probeErr = fmt.Errorf("building probe: %v\n%s", err, out)
			return
		}
		cfg := ImageConfig{Cmd: []string{"/bin/probe"}, Env: []string{"PATH=/bin"}, WorkingDir: "/srv"}
		data, _ := jsonMarshal(cfg)
		_ = os.WriteFile(filepath.Join(dir, configFile), data, 0o644)
		_ = os.WriteFile(filepath.Join(dir, doneFile), nil, 0o644)
		probeDir = dir
	})
	if probeErr != nil {
		t.Skipf("no probe image: %v", probeErr)
	}
	return probeDir
}

// newTestRuntime returns a runtime whose exporter already holds the probe
// image under the id "probe", so Prepare needs no Docker.
func newTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	if !namespacesWork(t) {
		t.Skip("cannot create user namespaces here; run as root for the privileged path")
	}
	img := probeImage(t)
	dataDir := t.TempDir()
	rootfsDir := filepath.Join(dataDir, "rootfs")
	_ = os.MkdirAll(rootfsDir, 0o755)
	if err := os.Symlink(img, filepath.Join(rootfsDir, "probe")); err != nil {
		t.Fatal(err)
	}
	d := &fakeDocker{id: "probe", tarball: func() []byte { t.Fatal("export should not run"); return nil }}
	r := New(d, dataDir, filepath.Join(dataDir, "cgroup", "ashore"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() {
		if list, _ := r.List(context.Background()); len(list) > 0 {
			for _, inst := range list {
				_ = r.Stop(context.Background(), inst.ID, 0)
			}
		}
	})
	return r
}

// namespacesWork reports whether this process may create the namespaces
// Start needs: as root always; otherwise only with unprivileged user
// namespaces enabled.
func namespacesWork(t *testing.T) bool {
	t.Helper()
	if os.Geteuid() == 0 {
		return true
	}
	cmd := exec.Command("unshare", "-Urmpf", "true")
	return cmd.Run() == nil
}

func freePort(t *testing.T) int {
	t.Helper()
	p, err := runtime.FreePort()
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// waitPong connects to addr until something answers "pong".
func waitPong(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			line, _ := bufio.NewReader(c).ReadString('\n')
			_ = c.Close()
			if strings.TrimSpace(line) == "pong" {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("nothing answered on %s", addr)
}

func TestStartRunsTheAppInItsOwnWorld(t *testing.T) {
	r := newTestRuntime(t)
	ctx := context.Background()
	port := freePort(t)
	inst, err := r.Start(ctx, runtime.Spec{App: "probe", Release: "v1", Image: "probe:latest", HostPort: port,
		Env: map[string]string{"GREETING": "hi", "PORT": "ignored"}})
	if err != nil {
		t.Fatal(err)
	}
	if inst.Addr != fmt.Sprintf("127.0.0.1:%d", port) || !inst.Running || inst.App != "probe" {
		t.Fatalf("instance %+v", inst)
	}
	waitPong(t, inst.Addr)

	var out, errOut bytes.Buffer
	lctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	_ = r.Logs(lctx, inst.ID, time.Time{}, &out, &errOut)
	got := out.String()
	for _, want := range []string{
		"pid=1 ", "hostname=" + inst.ID[:12] + " ", "cwd=/srv ", fmt.Sprintf("port=%d ", port), "host=127.0.0.1 ", "greeting=hi",
		"root=bin,dev,etc,marker-from-image,proc,srv,sys,tmp\n",
		"oldroot_missing=true", "listening",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("stdout lacks %q:\n%s", want, got)
		}
	}
	for _, bad := range []string{"write=", "proc=", "listen:"} {
		if strings.Contains(got, bad) {
			t.Errorf("stdout reports a failure %q:\n%s", bad, got)
		}
	}
	if !strings.Contains(errOut.String(), "probe: this goes to stderr") {
		t.Errorf("stderr: %q", errOut.String())
	}
	if r.rootless {
		if !strings.Contains(got, "uid=0 gid=0") {
			t.Errorf("rootless instance should be uid 0 in its namespace:\n%s", got)
		}
	} else if !strings.Contains(got, "uid=65534 gid=65534") {
		t.Errorf("image without USER should run as nobody:\n%s", got)
	}

	// The app's write landed in the upper layer, not in the image.
	upper := filepath.Join(r.dir, inst.ID, "upper", "written-by-app")
	if b, err := os.ReadFile(upper); err != nil || string(b) != "hello" {
		t.Errorf("upper layer: %v %q", err, b)
	}
	if _, err := os.Stat(filepath.Join(probeImage(t), "rootfs", "written-by-app")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the image was modified: %v", err)
	}
	// The host sees none of the container's mounts.
	if mounts := hostMounts(t); strings.Contains(mounts, inst.ID) {
		t.Errorf("container mounts leaked to the host:\n%s", mounts)
	}

	// List and Wait agree it is running; Stop ends it and removes it.
	list, err := r.List(ctx)
	if err != nil || len(list) != 1 || list[0].ID != inst.ID || !list[0].Running || list[0].Addr != inst.Addr {
		t.Fatalf("list: %+v %v", list, err)
	}
	wctx, wcancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer wcancel()
	if _, err := r.Wait(wctx, inst.ID); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Wait on a running instance returned early: %v", err)
	}
	if err := r.Stop(ctx, inst.ID, time.Second); err != nil {
		t.Fatal(err)
	}
	if list, _ := r.List(ctx); len(list) != 0 {
		t.Errorf("still listed after stop: %+v", list)
	}
	if _, err := os.Stat(filepath.Join(r.dir, inst.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("instance dir left: %v", err)
	}
	if _, err := r.Wait(ctx, inst.ID); !errors.Is(err, runtime.ErrNotFound) {
		t.Errorf("Wait after stop: %v", err)
	}
	if err := r.Stop(ctx, inst.ID, 0); err != nil {
		t.Errorf("second stop: %v", err)
	}
}

func TestInitFailureIsReported(t *testing.T) {
	r := newTestRuntime(t)
	ctx := context.Background()
	// An image whose command does not exist: the child reports it over the
	// error pipe and Start fails with that message, leaving nothing behind.
	bad := filepath.Join(filepath.Dir(r.dir), "rootfs", "bad")
	_ = os.MkdirAll(filepath.Join(bad, "rootfs"), 0o755)
	data, _ := jsonMarshal(ImageConfig{Cmd: []string{"/nonexistent"}})
	_ = os.WriteFile(filepath.Join(bad, configFile), data, 0o644)
	_ = os.WriteFile(filepath.Join(bad, doneFile), nil, 0o644)
	r.exporter.docker = &fakeDocker{id: "bad"}
	_, err := r.Start(ctx, runtime.Spec{App: "x", Release: "v1", Image: "bad", HostPort: freePort(t)})
	if err == nil || !strings.Contains(err.Error(), "/nonexistent") {
		t.Fatalf("err = %v", err)
	}
	if list, _ := r.List(ctx); len(list) != 0 {
		t.Errorf("failed start left %+v", list)
	}
	entries, _ := os.ReadDir(r.dir)
	if len(entries) != 0 {
		t.Errorf("failed start left directories: %v", entries)
	}
}

func TestWaitReportsTheExitCode(t *testing.T) {
	r := newTestRuntime(t)
	ctx := context.Background()
	// The probe exits 3 when it cannot listen; give it a port that is taken.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	port := ln.Addr().(*net.TCPAddr).Port
	inst, err := r.Start(ctx, runtime.Spec{App: "probe", Release: "v1", Image: "probe:latest", HostPort: port})
	if err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	code, err := r.Wait(wctx, inst.ID)
	if err != nil || code != 3 {
		t.Fatalf("wait: %d %v", code, err)
	}
	list, _ := r.List(ctx)
	if len(list) != 1 || list[0].Running || list[0].Addr != "" {
		t.Errorf("exited instance listed as %+v", list)
	}
	var out bytes.Buffer
	if err := r.Logs(ctx, inst.ID, time.Time{}, &out, io.Discard); err != nil || !strings.Contains(out.String(), "listen:") {
		t.Errorf("logs after exit: %v %q", err, out.String())
	}
	if err := r.Stop(ctx, inst.ID, 0); err != nil {
		t.Fatal(err)
	}
}

func TestEnviron(t *testing.T) {
	got := environ([]string{"PATH=/x", "A=image", "B=image"}, map[string]string{"B": "config", "PORT": "9", "C=": "odd"}, 4000)
	want := "A=image B=config C==odd HOST=127.0.0.1 PATH=/x PORT=4000"
	if strings.Join(got, " ") != want {
		t.Fatalf("got %q", strings.Join(got, " "))
	}
	if got := environ(nil, nil, 1); !strings.Contains(strings.Join(got, " "), "PATH=/usr/local/sbin") {
		t.Fatalf("default PATH missing: %q", got)
	}
}

func TestLookupUser(t *testing.T) {
	rootfs := filepath.Join(probeImage(t), "rootfs")
	for _, tc := range []struct {
		user     string
		uid, gid int
		err      bool
	}{
		{"", 65534, 65534, false},
		{"1000", 1000, 65534, false},
		{"1000:2000", 1000, 2000, false},
		{"app", 1234, 5678, false},
		{"app:staff", 1234, 4321, false},
		{"app:9", 1234, 9, false},
		{"nobody-here", 0, 0, true},
		{"app:nogroup", 0, 0, true},
		{"-1", 0, 0, true},
	} {
		uid, gid, err := lookupUser(rootfs, tc.user)
		if (err != nil) != tc.err || uid != tc.uid || gid != tc.gid {
			t.Errorf("%q: %d %d %v, want %d %d err=%v", tc.user, uid, gid, err, tc.uid, tc.gid, tc.err)
		}
	}
	if _, _, err := lookupUser(t.TempDir(), "app"); err == nil || !strings.Contains(err.Error(), "no passwd") {
		t.Errorf("no passwd file: %v", err)
	}
}

func TestStartTime(t *testing.T) {
	ts, err := startTime(os.Getpid())
	if err != nil || ts == 0 {
		t.Fatalf("own start time: %d %v", ts, err)
	}
	if alive(state{Pid: os.Getpid(), StartTime: ts + 1}) {
		t.Error("a different start time must not count as alive")
	}
	if !alive(state{Pid: os.Getpid(), StartTime: ts}) || !alive(state{Pid: os.Getpid()}) {
		t.Error("own pid should be alive")
	}
	if alive(state{Pid: 0}) {
		t.Error("pid 0 is never alive")
	}
}

func hostMounts(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// TestMemoryLimitKillsTheHog needs root and cgroup v2: the probe in HOG
// mode allocates until the kernel's OOM killer ends it at memory.max, and
// the cgroup's memory.events says so. It is the Phase 4 acceptance's
// memhog check in miniature.
func TestMemoryLimitKillsTheHog(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("needs root: sudo go test -run TestMemoryLimit ./internal/runtime/native/")
	}
	if !cgroup2Mounted("/sys/fs/cgroup") {
		t.Skip("/sys/fs/cgroup is not cgroup v2")
	}
	img := probeImage(t)
	dataDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(dataDir, "rootfs"), 0o755)
	_ = os.Symlink(img, filepath.Join(dataDir, "rootfs", "probe"))
	r := New(&fakeDocker{id: "probe"}, dataDir, "/sys/fs/cgroup/ashore-test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if r.cg == nil {
		t.Fatal("cgroups not enabled as root on cgroup v2")
	}
	ctx := context.Background()
	inst, err := r.Start(ctx, runtime.Spec{App: "hog", Release: "v1", Image: "probe", HostPort: freePort(t),
		Memory: 32 << 20, CPU: 1, Env: map[string]string{"HOG": "1"}})
	if err != nil {
		t.Fatal(err)
	}
	wctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	code, err := r.Wait(wctx, inst.ID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	st, _ := readState(filepath.Join(r.dir, inst.ID))
	events, eerr := readEvents(st.Cgroup)
	if eerr != nil || events["oom_kill"] < 1 {
		t.Errorf("memory.events: %v %v", events, eerr)
	}
	if code != 137 { // 128 + SIGKILL
		t.Errorf("exit code %d, want 137", code)
	}
	var out bytes.Buffer
	_ = r.Logs(ctx, inst.ID, time.Time{}, &out, io.Discard)
	if !strings.Contains(out.String(), "holding 4 MiB") || strings.Contains(out.String(), "holding 64 MiB") {
		t.Errorf("hog output:\n%s", out.String())
	}
	if err := r.Stop(ctx, inst.ID, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(st.Cgroup); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("cgroup left behind: %v", err)
	}
	_ = os.Remove("/sys/fs/cgroup/ashore-test")
}

func TestStopIsGracefulThenForceful(t *testing.T) {
	r := newTestRuntime(t)
	ctx := context.Background()

	// An app that handles SIGTERM leaves at once.
	inst, err := r.Start(ctx, runtime.Spec{App: "probe", Release: "v1", Image: "probe:latest", HostPort: freePort(t), Env: map[string]string{"TRAP": "1"}})
	if err != nil {
		t.Fatal(err)
	}
	waitPong(t, inst.Addr)
	var out syncBuffer
	lctx, cancel := context.WithCancel(ctx)
	logsDone := make(chan struct{})
	go func() { _ = r.Logs(lctx, inst.ID, time.Time{}, &out, io.Discard); close(logsDone) }()
	for !strings.Contains(out.String(), "listening") { // the reader is attached before the stop
		time.Sleep(10 * time.Millisecond)
	}
	start := time.Now()
	if err := r.Stop(ctx, inst.ID, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("stop of a SIGTERM-aware app took %s", took)
	}
	// The reader ends by itself once the process is gone, after a final
	// drain; cancelling first would cut that drain short.
	select {
	case <-logsDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Logs did not return after the instance exited")
	}
	cancel()
	if !strings.Contains(out.String(), "got SIGTERM, leaving") {
		t.Errorf("the app did not see SIGTERM:\n%s", out.String())
	}

	// An app that ignores SIGTERM (or, as PID 1, has no handler for it: the
	// Go runtime installs one, so the probe ignores it explicitly) lives
	// until the SIGKILL after the grace.
	inst, err = r.Start(ctx, runtime.Spec{App: "probe", Release: "v2", Image: "probe:latest", HostPort: freePort(t), Env: map[string]string{"IGNORE": "1"}})
	if err != nil {
		t.Fatal(err)
	}
	waitPong(t, inst.Addr)
	start = time.Now()
	if err := r.Stop(ctx, inst.ID, 500*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took < 500*time.Millisecond || took > 5*time.Second {
		t.Errorf("stop of a deaf app took %s, want about the grace", took)
	}
	if list, _ := r.List(ctx); len(list) != 0 {
		t.Errorf("after stops: %+v", list)
	}
}

func TestAdoptedInstanceAfterARestart(t *testing.T) {
	r := newTestRuntime(t)
	ctx := context.Background()
	inst, err := r.Start(ctx, runtime.Spec{App: "probe", Release: "v1", Image: "probe:latest", HostPort: freePort(t)})
	if err != nil {
		t.Fatal(err)
	}
	waitPong(t, inst.Addr)

	// A new runtime over the same data dir knows nothing of the child but
	// finds it through the state file and /proc.
	later := New(&fakeDocker{id: "probe"}, filepath.Dir(r.dir), filepath.Join(filepath.Dir(r.dir), "cgroup", "ashore"), slog.New(slog.NewTextHandler(io.Discard, nil)))
	list, err := later.List(ctx)
	if err != nil || len(list) != 1 || !list[0].Running || list[0].Addr != inst.Addr || list[0].App != "probe" {
		t.Fatalf("list after restart: %+v %v", list, err)
	}
	var out bytes.Buffer
	lctx, cancel := context.WithTimeout(ctx, time.Second)
	_ = later.Logs(lctx, inst.ID, time.Time{}, &out, io.Discard)
	cancel()
	if !strings.Contains(out.String(), "listening") {
		t.Errorf("logs after restart:\n%s", out.String())
	}
	wctx, wcancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer wcancel()
	if _, err := later.Wait(wctx, inst.ID); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Wait on a running adopted instance: %v", err)
	}

	// Stop through the later runtime signals by pid; the first runtime's
	// reaper sees the exit.
	done := make(chan error, 1)
	go func() { _, err := r.Wait(ctx, inst.ID); done <- err }()
	if err := later.Stop(ctx, inst.ID, 300*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("first runtime's Wait: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the child was not reaped")
	}
	if list, _ := later.List(ctx); len(list) != 0 {
		t.Errorf("after stop: %+v", list)
	}
	// A stale state file for a pid that is gone lists as not running, and
	// Wait returns at once with -1.
	dir := filepath.Join(r.dir, "stale")
	_ = os.MkdirAll(dir, 0o755)
	_ = writeState(dir, state{ID: "stale", App: "old", Release: "v1", Pid: 2_000_000, StartTime: 1, Port: 1})
	_ = os.WriteFile(filepath.Join(dir, "stdout.log"), nil, 0o600)
	_ = os.WriteFile(filepath.Join(dir, "stderr.log"), nil, 0o600)
	list, _ = later.List(ctx)
	if len(list) != 1 || list[0].Running || list[0].ID != "stale" {
		t.Errorf("stale instance: %+v", list)
	}
	if code, err := later.Wait(ctx, "stale"); err != nil || code != -1 {
		t.Errorf("wait on a stale instance: %d %v", code, err)
	}
	if err := later.Stop(ctx, "stale", time.Second); err != nil {
		t.Errorf("stop of a stale instance: %v", err)
	}
}

// syncBuffer is a bytes.Buffer safe for a writer and a reader.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}
