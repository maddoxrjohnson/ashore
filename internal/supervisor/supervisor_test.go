package supervisor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/builder"
	"github.com/maddoxrjohnson/ashore/internal/router"
	"github.com/maddoxrjohnson/ashore/internal/runtime"
	"github.com/maddoxrjohnson/ashore/internal/store"
)

const sha = "abc1234567890abc1234567890abc1234567890a"

// fakeBuilder answers with a tag at once, fails, or blocks until released.
type fakeBuilder struct {
	err     error
	block   chan struct{} // nil: do not block
	started chan string   // receives the app of every build that begins

	mu       sync.Mutex
	inFlight int
	maxSeen  int
}

func (b *fakeBuilder) Build(ctx context.Context, job builder.Job, out io.Writer) (string, error) {
	b.mu.Lock()
	b.inFlight++
	b.maxSeen = max(b.maxSeen, b.inFlight)
	block := b.block
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.inFlight--
		b.mu.Unlock()
	}()
	if b.started != nil {
		b.started <- job.App
	}
	_, _ = fmt.Fprintln(out, "Step 1/1 : FROM scratch")
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	if b.err != nil {
		return "", b.err
	}
	return builder.Tag(job.App, job.SHA), nil
}

func (b *fakeBuilder) setBlock(c chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.block = c
}

// fakeRuntime starts instances that listen on the requested port (or not),
// or exit at once with a code, and records what was stopped.
type fakeRuntime struct {
	listen   bool
	exitCode int // used when listen is false and >= 0
	startErr error
	logs     string
	handler  http.Handler // when set, each instance serves HTTP with it

	mu        sync.Mutex
	found     []runtime.Instance // what List reports before anything is started here
	started   []runtime.Spec
	stopped   []string
	graces    map[string]time.Duration // id -> grace given to Stop
	listeners map[string]net.Listener
	gone      map[string]chan struct{} // closed when the instance's process has ended
	codes     map[string]int           // its exit code
	n         int
}

func newFakeRuntime(t *testing.T) *fakeRuntime {
	rt := &fakeRuntime{listen: true, exitCode: -1, listeners: make(map[string]net.Listener), graces: make(map[string]time.Duration),
		gone: make(map[string]chan struct{}), codes: make(map[string]int)}
	t.Cleanup(func() {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		for _, l := range rt.listeners {
			_ = l.Close()
		}
	})
	return rt
}

func (r *fakeRuntime) Start(_ context.Context, s runtime.Spec) (runtime.Instance, error) {
	if r.startErr != nil {
		return runtime.Instance{}, r.startErr
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.n++
	id := "inst" + strconv.Itoa(r.n)
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(s.HostPort))
	if r.listen {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			return runtime.Instance{}, err
		}
		r.listeners[id] = l
		if r.handler != nil {
			go func() { _ = http.Serve(l, r.handler) }()
		}
	}
	r.started = append(r.started, s)
	return runtime.Instance{ID: id, App: s.App, Release: s.Release, Addr: addr, StartedAt: time.Now(), Running: true}, nil
}

// Stop records the call and ends the process, as docker stop would, so a
// watcher waiting on it returns.
func (r *fakeRuntime) Stop(_ context.Context, id string, grace time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = append(r.stopped, id)
	r.graces[id] = grace
	r.end(id, 143) // 128 + SIGTERM
	return nil
}

// exit makes a running instance die on its own with code, the way a
// crashing app does.
func (r *fakeRuntime) exit(id string, code int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.end(id, code)
}

// end closes the instance's listener and releases its waiters. Idempotent.
func (r *fakeRuntime) end(id string, code int) {
	if l, ok := r.listeners[id]; ok {
		_ = l.Close()
		delete(r.listeners, id)
	}
	ch := r.goneChan(id)
	select {
	case <-ch:
	default:
		r.codes[id] = code
		close(ch)
	}
}

func (r *fakeRuntime) goneChan(id string) chan struct{} {
	ch, ok := r.gone[id]
	if !ok {
		ch = make(chan struct{})
		r.gone[id] = ch
	}
	return ch
}

// Wait returns at once for an instance configured to exit on start, or
// blocks until exit or Stop ends it, or ctx ends.
func (r *fakeRuntime) Wait(ctx context.Context, id string) (int, error) {
	if !r.listen && r.exitCode >= 0 {
		return r.exitCode, nil
	}
	r.mu.Lock()
	ch := r.goneChan(id)
	r.mu.Unlock()
	select {
	case <-ch:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.codes[id], nil
	case <-ctx.Done():
		return -1, ctx.Err()
	}
}

func (r *fakeRuntime) Logs(_ context.Context, _ string, _ time.Time, stdout, _ io.Writer) error {
	_, err := io.WriteString(stdout, r.logs)
	return err
}

func (r *fakeRuntime) List(context.Context) ([]runtime.Instance, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []runtime.Instance
	for _, inst := range r.found {
		if ch, ok := r.gone[inst.ID]; ok {
			select {
			case <-ch:
				continue // removed
			default:
			}
		}
		out = append(out, inst)
	}
	for i, spec := range r.started {
		id := "inst" + strconv.Itoa(i+1)
		running := true
		if ch, ok := r.gone[id]; ok {
			select {
			case <-ch:
				running = false
			default:
			}
		}
		if !r.listen && r.exitCode >= 0 {
			running = false
		}
		out = append(out, runtime.Instance{ID: id, App: spec.App, Release: spec.Release,
			Addr: net.JoinHostPort("127.0.0.1", strconv.Itoa(spec.HostPort)), Running: running})
	}
	return out, nil
}

func (r *fakeRuntime) stoppedIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.stopped...)
}

// syncBuf is a bytes.Buffer a test may read while a deploy writes it.
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type fixture struct {
	sup     *Supervisor
	store   *store.Store
	builder *fakeBuilder
	runtime *fakeRuntime
	router  *router.Router
	clock   *fakeClock
	cancel  context.CancelFunc
	done    chan struct{}
}

// newFixture builds a running supervisor on a real SQLite store with the
// fakes above, and an app named hello.
func newFixture(t *testing.T, cfg Config) *fixture {
	t.Helper()
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if _, err := st.CreateApp(context.Background(), "hello", "hello.localhost"); err != nil {
		t.Fatal(err)
	}
	if cfg.HealthTimeout == 0 {
		cfg.HealthTimeout = 5 * time.Second
	}
	if cfg.StopGrace == 0 {
		cfg.StopGrace = 10 * time.Second
	}
	cfg.DataDir, cfg.Domain, cfg.HTTPPort = t.TempDir(), "localhost", 8080
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	f := &fixture{store: st, builder: &fakeBuilder{}, runtime: newFakeRuntime(t), router: router.New(log), clock: newFakeClock(), done: make(chan struct{})}
	f.sup = New(st, f.builder, f.runtime, f.router, cfg, log)
	f.sup.clock = f.clock
	// No repository exists here; a push sees no ashore.toml unless a test
	// calls withManifest.
	f.sup.readManifest = func(context.Context, string, string, string) ([]byte, bool, error) { return nil, false, nil }
	ctx, cancel := context.WithCancel(context.Background())
	f.cancel = cancel
	go func() {
		_ = f.sup.Run(ctx)
		close(f.done)
	}()
	t.Cleanup(func() {
		cancel()
		<-f.done
	})
	return f
}

func (f *fixture) deploy(ctx context.Context, app string) (string, error) {
	var out bytes.Buffer
	err := f.sup.Deploy(ctx, app, sha, "", &out)
	return out.String(), err
}

// waitStopped returns the stopped instances once there are n of them, or
// fails the test after a few seconds. Replaced instances are stopped after
// the drain grace, in the background, so a test cannot look right away.
func (f *fixture) waitStopped(t *testing.T, n int) []string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if got := f.runtime.stoppedIDs(); len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("stopped %v, want %d instances", f.runtime.stoppedIDs(), n)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestDeployHappyPath(t *testing.T) {
	f := newFixture(t, Config{})
	out, err := f.deploy(context.Background(), "hello")
	if err != nil {
		t.Fatalf("deploy: %v\n%s", err, out)
	}
	for _, want := range []string{
		"-----> release v1 (abc1234)\n", "-----> building hello\n", "Step 1/1",
		"-----> waiting for v1 to listen on port 8080\n", "-----> live at http://hello.localhost:8080\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "waiting for the current deploy") {
		t.Errorf("a first push should never wait:\n%s", out)
	}
	addr, ok := f.router.Get("hello.localhost")
	if !ok {
		t.Fatal("no route after deploy")
	}
	spec := f.runtime.started[0]
	if addr != "127.0.0.1:"+strconv.Itoa(spec.HostPort) {
		t.Errorf("route %s, instance port %d", addr, spec.HostPort)
	}
	if spec.Image != "ashore/hello:abc1234" || spec.Release != "v1" || spec.ContainerPort != 8080 || spec.Memory != 256<<20 {
		t.Errorf("spec = %+v", spec)
	}
	app, _ := f.store.GetApp(context.Background(), "hello")
	rel, err := f.store.LiveRelease(context.Background(), app.ID)
	if err != nil || rel.Version != 1 || rel.Status != store.StatusLive || rel.GitSHA != sha || rel.Note != "push abc1234" || rel.Image != "ashore/hello:abc1234" {
		t.Errorf("live release = %+v, %v", rel, err)
	}
	if len(f.runtime.stoppedIDs()) != 0 {
		t.Errorf("first deploy stopped %v", f.runtime.stoppedIDs())
	}
}

func TestSecondDeployReplacesFirst(t *testing.T) {
	f := newFixture(t, Config{})
	ctx := context.Background()
	if _, err := f.deploy(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	first, _ := f.router.Get("hello.localhost")
	if _, err := f.deploy(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	second, _ := f.router.Get("hello.localhost")
	if first == second {
		t.Error("route did not move to the new instance")
	}
	if got := f.waitStopped(t, 1); len(got) != 1 || got[0] != "inst1" {
		t.Errorf("stopped %v, want the first instance only", got)
	}
	app, _ := f.store.GetApp(ctx, "hello")
	rels, _ := f.store.ListReleases(ctx, app.ID)
	if len(rels) != 2 || rels[0].Status != store.StatusSuperseded || rels[1].Status != store.StatusLive {
		t.Errorf("releases = %+v", rels)
	}
}

func TestOldInstanceIsStoppedAfterTheDrain(t *testing.T) {
	f := newFixture(t, Config{DrainGrace: 300 * time.Millisecond})
	ctx := context.Background()
	if _, err := f.deploy(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.deploy(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	// The push has returned and the route has moved, but the old instance
	// is still up for requests already on their way to it.
	if addr, _ := f.router.Get("hello.localhost"); addr != "127.0.0.1:"+strconv.Itoa(f.runtime.started[1].HostPort) {
		t.Errorf("route %s is not the new instance", addr)
	}
	if got := f.runtime.stoppedIDs(); len(got) != 0 {
		t.Fatalf("old instance stopped before the drain: %v", got)
	}
	start := time.Now()
	f.waitStopped(t, 1)
	if since := time.Since(start); since < 200*time.Millisecond {
		t.Errorf("old instance stopped %v after the swap, want about 300ms", since)
	}
}

func TestShutdownStopsADrainingInstanceAtOnce(t *testing.T) {
	f := newFixture(t, Config{DrainGrace: time.Hour})
	ctx := context.Background()
	for range 2 {
		if _, err := f.deploy(ctx, "hello"); err != nil {
			t.Fatal(err)
		}
	}
	if got := f.runtime.stoppedIDs(); len(got) != 0 {
		t.Fatalf("stopped %v before shutdown", got)
	}
	f.cancel()
	select {
	case <-f.done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return; the drain held it")
	}
	if got := f.runtime.stoppedIDs(); len(got) != 1 || got[0] != "inst1" {
		t.Errorf("stopped %v after shutdown, want the replaced instance", got)
	}
}

func TestBuildFailureRejects(t *testing.T) {
	f := newFixture(t, Config{})
	f.builder.err = errors.New("build failed: no Dockerfile")
	_, err := f.deploy(context.Background(), "hello")
	if err == nil || !strings.Contains(err.Error(), "no Dockerfile") {
		t.Fatalf("err = %v", err)
	}
	if _, ok := f.router.Get("hello.localhost"); ok {
		t.Error("route set after a failed build")
	}
	if len(f.runtime.started) != 0 {
		t.Error("an instance was started after a failed build")
	}
	// The attempt is history: v1 is used up and says why it never ran.
	app, _ := f.store.GetApp(context.Background(), "hello")
	if rels, _ := f.store.ListReleases(context.Background(), app.ID); len(rels) != 1 || rels[0].Status != store.StatusFailed || rels[0].Image != "" {
		t.Errorf("releases after a failed build: %+v, want one failed without an image", rels)
	}
}

func TestReleaseIsBuildingDuringBuild(t *testing.T) {
	f := newFixture(t, Config{})
	f.builder.block = make(chan struct{})
	f.builder.started = make(chan string, 1)
	errs := make(chan error, 1)
	go func() {
		_, err := f.deploy(context.Background(), "hello")
		errs <- err
	}()
	<-f.builder.started
	app, _ := f.store.GetApp(context.Background(), "hello")
	rels, _ := f.store.ListReleases(context.Background(), app.ID)
	if len(rels) != 1 || rels[0].Status != store.StatusBuilding || rels[0].Image != "" || rels[0].Note != "push abc1234" {
		t.Errorf("releases during the build = %+v, want one building", rels)
	}
	close(f.builder.block)
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
	rels, _ = f.store.ListReleases(context.Background(), app.ID)
	if len(rels) != 1 || rels[0].Status != store.StatusLive || rels[0].Image != "ashore/hello:abc1234" {
		t.Errorf("releases after the build = %+v, want the same row live with its image", rels)
	}
}

func TestUnhealthyInstanceIsStoppedAndFailed(t *testing.T) {
	f := newFixture(t, Config{HealthTimeout: 400 * time.Millisecond})
	f.runtime.listen = false
	out, err := f.deploy(context.Background(), "hello")
	if err == nil || !strings.Contains(err.Error(), "did not accept connections on port 8080 within 400ms") {
		t.Fatalf("err = %v\n%s", err, out)
	}
	if got := f.runtime.stoppedIDs(); len(got) != 1 {
		t.Errorf("stopped %v, want the unhealthy instance", got)
	}
	if _, ok := f.router.Get("hello.localhost"); ok {
		t.Error("route set for an unhealthy instance")
	}
	app, _ := f.store.GetApp(context.Background(), "hello")
	rels, _ := f.store.ListReleases(context.Background(), app.ID)
	if len(rels) != 1 || rels[0].Status != store.StatusFailed {
		t.Errorf("releases = %+v, want one failed", rels)
	}
	if app.LiveReleaseID != 0 {
		t.Errorf("live release id = %d after a failed deploy", app.LiveReleaseID)
	}
}

func TestExitedInstanceShowsItsOutput(t *testing.T) {
	f := newFixture(t, Config{})
	f.runtime.listen, f.runtime.exitCode, f.runtime.logs = false, 3, "panic: boom\n"
	out, err := f.deploy(context.Background(), "hello")
	if err == nil || !strings.Contains(err.Error(), "v1 exited with code 3") {
		t.Fatalf("err = %v\n%s", err, out)
	}
	if !strings.Contains(out, "       panic: boom\n") {
		t.Errorf("output should show the instance's last lines:\n%s", out)
	}
	if got := f.runtime.stoppedIDs(); len(got) != 1 {
		t.Errorf("stopped %v", got)
	}
}

func TestOldReleaseKeepsServingAfterFailedDeploy(t *testing.T) {
	f := newFixture(t, Config{})
	if _, err := f.deploy(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	before, _ := f.router.Get("hello.localhost")
	f.builder.err = errors.New("build failed")
	if _, err := f.deploy(context.Background(), "hello"); err == nil {
		t.Fatal("second deploy succeeded")
	}
	if after, _ := f.router.Get("hello.localhost"); after != before {
		t.Errorf("route moved from %s to %s", before, after)
	}
	if len(f.runtime.stoppedIDs()) != 0 {
		t.Error("the live instance was stopped")
	}
}

func TestDeploysToOneAppSerialize(t *testing.T) {
	f := newFixture(t, Config{BuildConcurrency: 4})
	f.builder.block = make(chan struct{})
	f.builder.started = make(chan string, 2)
	ctx := context.Background()

	var out1, out2 syncBuf
	errs := make(chan error, 2)
	go func() { errs <- f.sup.Deploy(ctx, "hello", sha, "", &out1) }()
	<-f.builder.started // the first build is in progress
	go func() { errs <- f.sup.Deploy(ctx, "hello", sha, "", &out2) }()

	// The second push must wait, and must not start building.
	time.Sleep(100 * time.Millisecond)
	if !strings.Contains(out2.String(), "waiting for the current deploy") {
		t.Errorf("second pusher was not told to wait:\n%s", out2.String())
	}
	select {
	case app := <-f.builder.started:
		t.Fatalf("second build for %s started while the first was running", app)
	default:
	}

	close(f.builder.block)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if f.builder.maxSeen != 1 {
		t.Errorf("%d builds of one app ran at once", f.builder.maxSeen)
	}
	app, _ := f.store.GetApp(ctx, "hello")
	rels, _ := f.store.ListReleases(ctx, app.ID)
	if len(rels) != 2 || rels[1].Status != store.StatusLive {
		t.Errorf("releases = %+v", rels)
	}
}

func TestDeploysToTwoAppsRunConcurrently(t *testing.T) {
	f := newFixture(t, Config{BuildConcurrency: 2})
	if _, err := f.store.CreateApp(context.Background(), "other", "other.localhost"); err != nil {
		t.Fatal(err)
	}
	f.builder.block = make(chan struct{})
	f.builder.started = make(chan string, 2)
	errs := make(chan error, 2)
	for _, app := range []string{"hello", "other"} {
		go func() {
			_, err := f.deploy(context.Background(), app)
			errs <- err
		}()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-f.builder.started:
		case <-time.After(5 * time.Second):
			t.Fatal("builds did not run concurrently")
		}
	}
	close(f.builder.block)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if f.builder.maxSeen != 2 {
		t.Errorf("max concurrent builds = %d, want 2", f.builder.maxSeen)
	}
}

func TestBuildSlotIsShared(t *testing.T) {
	f := newFixture(t, Config{BuildConcurrency: 1})
	if _, err := f.store.CreateApp(context.Background(), "other", "other.localhost"); err != nil {
		t.Fatal(err)
	}
	f.builder.block = make(chan struct{})
	f.builder.started = make(chan string, 1)
	var out1, out2 syncBuf
	errs := make(chan error, 2)
	go func() { errs <- f.sup.Deploy(context.Background(), "hello", sha, "", &out1) }()
	<-f.builder.started
	go func() { errs <- f.sup.Deploy(context.Background(), "other", sha, "", &out2) }()
	time.Sleep(100 * time.Millisecond)
	if !strings.Contains(out2.String(), "waiting for a build slot") {
		t.Errorf("second app was not told to wait for a slot:\n%s", out2.String())
	}
	close(f.builder.block)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if f.builder.maxSeen != 1 {
		t.Errorf("max concurrent builds = %d, want 1", f.builder.maxSeen)
	}
}

func TestCancelledPushIsRejected(t *testing.T) {
	f := newFixture(t, Config{})
	f.builder.block = make(chan struct{})
	f.builder.started = make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, 1)
	go func() {
		_, err := f.deploy(ctx, "hello")
		errs <- err
	}()
	<-f.builder.started
	cancel()
	select {
	case err := <-errs:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Deploy did not return after cancel")
	}
	// The cancelled build returned on its context; the actor is free again
	// and a new push goes through.
	f.builder.setBlock(nil)
	if _, err := f.deploy(context.Background(), "hello"); err != nil {
		t.Fatalf("deploy after a cancelled one: %v", err)
	}
	app, _ := f.store.GetApp(context.Background(), "hello")
	if rels, _ := f.store.ListReleases(context.Background(), app.ID); len(rels) != 2 || rels[0].Status != store.StatusFailed || rels[1].Status != store.StatusLive {
		t.Errorf("releases = %+v, want the cancelled one failed and the second live", rels)
	}
}

func TestUnknownAppAndNotRunning(t *testing.T) {
	f := newFixture(t, Config{})
	if _, err := f.deploy(context.Background(), "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("unknown app: %v", err)
	}
	f.cancel()
	<-f.done
	if _, err := f.deploy(context.Background(), "hello"); !errors.Is(err, errNotRunning) {
		t.Errorf("after Run returned: %v", err)
	}
}

func TestListening(t *testing.T) {
	ctx := context.Background()
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := closed.Addr().String()
	_ = closed.Close()
	if listening(ctx, dead, healthEvery) {
		t.Error("a closed port counts as listening")
	}

	// A proxy with nothing behind it: accept, then hang up.
	proxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = proxy.Close() }()
	go func() {
		for {
			c, err := proxy.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	if listening(ctx, proxy.Addr().String(), healthEvery) {
		t.Error("a connection closed at once counts as listening")
	}

	// A server that accepts and waits for a request.
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	if !listening(ctx, held.Addr().String(), healthEvery) {
		t.Error("a held connection does not count as listening")
	}

	// A server that speaks first (a banner) counts too.
	talker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = talker.Close() }()
	go func() {
		for {
			c, err := talker.Accept()
			if err != nil {
				return
			}
			_, _ = c.Write([]byte("hi\n"))
			_ = c.Close()
		}
	}()
	if !listening(ctx, talker.Addr().String(), healthEvery) {
		t.Error("a server that sends a banner does not count as listening")
	}
}

func TestConfigIsSnapshottedAndInjected(t *testing.T) {
	f := newFixture(t, Config{})
	ctx := context.Background()
	app, _ := f.store.GetApp(ctx, "hello")
	if _, err := f.store.UpdateConfig(ctx, app.ID, map[string]string{"GREETING": "hi", "PORT_HINT": "x"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := f.deploy(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	spec := f.runtime.started[0]
	if spec.Env["GREETING"] != "hi" || spec.Env["PORT_HINT"] != "x" || len(spec.Env) != 2 {
		t.Errorf("instance env = %v", spec.Env)
	}
	rel, _ := f.store.LiveRelease(ctx, app.ID)
	if rel.ConfigJSON != `{"GREETING":"hi","PORT_HINT":"x"}` {
		t.Errorf("snapshot = %s", rel.ConfigJSON)
	}
	// A later change does not touch the release that already exists.
	if _, err := f.store.UpdateConfig(ctx, app.ID, map[string]string{"GREETING": "bye"}, nil); err != nil {
		t.Fatal(err)
	}
	if again, _ := f.store.GetRelease(ctx, rel.ID); again.ConfigJSON != rel.ConfigJSON {
		t.Errorf("snapshot changed after config:set: %s", again.ConfigJSON)
	}
}

func TestRedeployRunsLiveImageWithNewConfig(t *testing.T) {
	f := newFixture(t, Config{})
	ctx := context.Background()
	if _, err := f.deploy(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	app, _ := f.store.GetApp(ctx, "hello")
	if _, err := f.store.UpdateConfig(ctx, app.ID, map[string]string{"GREETING": "hi"}, nil); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := f.sup.Redeploy(ctx, "hello", "config:set GREETING", &out); err != nil {
		t.Fatalf("redeploy: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "-----> release v2 (config:set GREETING)\n") || strings.Contains(out.String(), "building") {
		t.Errorf("redeploy output:\n%s", out.String())
	}
	if f.builder.maxSeen != 1 {
		t.Errorf("%d builds ran; a redeploy must not build", f.builder.maxSeen)
	}
	if len(f.runtime.started) != 2 || f.runtime.started[1].Image != "ashore/hello:abc1234" || f.runtime.started[1].Env["GREETING"] != "hi" || f.runtime.started[1].Release != "v2" {
		t.Errorf("second instance = %+v", f.runtime.started)
	}
	if got := f.waitStopped(t, 1); len(got) != 1 || got[0] != "inst1" {
		t.Errorf("stopped %v, want the first instance", got)
	}
	rels, _ := f.store.ListReleases(ctx, app.ID)
	if len(rels) != 2 || rels[0].Status != store.StatusSuperseded || rels[1].Status != store.StatusLive ||
		rels[1].Image != rels[0].Image || rels[1].GitSHA != sha || rels[1].Note != "config:set GREETING" || rels[1].ConfigJSON != `{"GREETING":"hi"}` {
		t.Errorf("releases = %+v", rels)
	}
}

func TestRedeployNeedsALiveRelease(t *testing.T) {
	f := newFixture(t, Config{})
	var out bytes.Buffer
	err := f.sup.Redeploy(context.Background(), "hello", "config:set X", &out)
	if err == nil || !strings.Contains(err.Error(), "no live release") {
		t.Fatalf("err = %v", err)
	}
	app, _ := f.store.GetApp(context.Background(), "hello")
	if rels, _ := f.store.ListReleases(context.Background(), app.ID); len(rels) != 0 {
		t.Errorf("a refused redeploy created %+v", rels)
	}
}

// waitState polls until the app's status has the state (and, when given, a
// different instance than before), or fails after a few seconds.
func (f *fixture) waitState(t *testing.T, state, notInstance string) Status {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		st, ok := f.sup.Status("hello")
		if ok && st.State == state && st.Instance != notInstance {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("status = %+v, want state %s (instance not %q)", st, state, notInstance)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestBackoffSchedule(t *testing.T) {
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, 60 * time.Second, 60 * time.Second, 60 * time.Second, 60 * time.Second}
	for i, w := range want {
		if got := backoff(i + 1); got != w {
			t.Errorf("backoff(%d) = %s, want %s", i+1, got, w)
		}
	}
	if backoff(0) != time.Second {
		t.Errorf("backoff(0) = %s", backoff(0))
	}
}

func TestCrashLoopBacksOffThenGivesUp(t *testing.T) {
	f := newFixture(t, Config{})
	ctx := context.Background()
	if _, err := f.deploy(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	st := f.waitState(t, StateRunning, "")
	if st.Release != "v1" || st.Instance != "inst1" || st.Restarts != 0 {
		t.Fatalf("status after deploy = %+v", st)
	}
	app, _ := f.store.GetApp(ctx, "hello")

	var waits []time.Duration
	for i := 1; i <= 9; i++ {
		prev := st.Instance
		f.runtime.exit(prev, 1)
		st = f.waitState(t, StateRestarting, "")
		if !strings.Contains(st.Detail, "exited with code 1") || !strings.Contains(st.Detail, fmt.Sprintf("restart %d in", i)) {
			t.Errorf("restarting detail = %q", st.Detail)
		}
		waits = append(waits, f.clock.fire(t))
		st = f.waitState(t, StateRunning, prev)
		if st.Restarts != i || st.Release != "v1" {
			t.Errorf("after restart %d: %+v", i, st)
		}
		// The route follows the restarted instance; no new release is made.
		if addr, _ := f.router.Get("hello.localhost"); addr != st.Addr {
			t.Errorf("route %s, instance %s", addr, st.Addr)
		}
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 32 * time.Second, 60 * time.Second, 60 * time.Second, 60 * time.Second}
	if fmt.Sprint(waits) != fmt.Sprint(want) {
		t.Errorf("backoff waits = %v, want %v", waits, want)
	}

	// The tenth failure inside the window is the end of the line.
	last := st
	f.runtime.exit(last.Instance, 1)
	st = f.waitState(t, StateCrashed, "")
	if !strings.Contains(st.Detail, "10 failures") || st.Restarts != 9 || f.clock.pending() {
		t.Errorf("crashed status = %+v, pending timer %v", st, f.clock.pending())
	}
	// The route still points at the dead instance: 502s, not 404s.
	if addr, ok := f.router.Get("hello.localhost"); !ok || addr != last.Addr {
		t.Errorf("route after crash = %s, %v; want %s", addr, ok, last.Addr)
	}
	if rels, _ := f.store.ListReleases(ctx, app.ID); len(rels) != 1 || rels[0].Status != store.StatusLive {
		t.Errorf("releases after a crash loop = %+v, want v1 still live", rels)
	}
	// Every dead instance was removed; ten were started in all.
	if len(f.runtime.started) != 10 || len(f.runtime.stoppedIDs()) != 10 {
		t.Errorf("%d started, %d stopped, want 10 and 10", len(f.runtime.started), len(f.runtime.stoppedIDs()))
	}
}

func TestFailureWindowForgetsOldCrashes(t *testing.T) {
	f := newFixture(t, Config{})
	if _, err := f.deploy(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	st := f.waitState(t, StateRunning, "")
	f.runtime.exit(st.Instance, 2)
	if d := f.clock.fire(t); d != time.Second {
		t.Errorf("first wait %s", d)
	}
	st = f.waitState(t, StateRunning, st.Instance)

	// Eleven quiet minutes later a crash is the first one again.
	f.clock.advance(11 * time.Minute)
	f.runtime.exit(st.Instance, 2)
	if d := f.clock.fire(t); d != time.Second {
		t.Errorf("wait after a quiet window %s, want 1s", d)
	}
	f.waitState(t, StateRunning, st.Instance)
}

func TestFailedRestartCountsAsAFailure(t *testing.T) {
	f := newFixture(t, Config{HealthTimeout: 300 * time.Millisecond})
	if _, err := f.deploy(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	st := f.waitState(t, StateRunning, "")
	// From now on new instances never listen, so the restart's health check
	// times out.
	f.runtime.mu.Lock()
	f.runtime.listen = false
	f.runtime.mu.Unlock()
	f.runtime.exit(st.Instance, 1)
	if d := f.clock.fire(t); d != time.Second {
		t.Errorf("first wait %s", d)
	}
	// The dead instance stays the current one (the route still points at
	// it), so the status is told apart by its detail.
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(st.Detail, "restart failed") {
		if time.Now().After(deadline) {
			t.Fatalf("status = %+v, want a failed restart", st)
		}
		time.Sleep(5 * time.Millisecond)
		st, _ = f.sup.Status("hello")
	}
	if st.State != StateRestarting || !strings.Contains(st.Detail, "did not accept connections") || !strings.Contains(st.Detail, "restart 2 in 2s") {
		t.Errorf("status after a failed restart = %+v", st)
	}
	if st.Restarts != 0 || st.Instance != "inst1" {
		t.Errorf("a failed restart counted as a restart or replaced the instance: %+v", st)
	}
	if d := f.clock.fire(t); d != 2*time.Second {
		t.Errorf("second wait %s, want 2s", d)
	}
}

func TestNewDeployCancelsAPendingRestart(t *testing.T) {
	f := newFixture(t, Config{})
	ctx := context.Background()
	if _, err := f.deploy(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	st := f.waitState(t, StateRunning, "")
	f.runtime.exit(st.Instance, 1)
	f.waitState(t, StateRestarting, "")
	// A push arrives while the restart is pending.
	if _, err := f.deploy(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	st = f.waitState(t, StateRunning, st.Instance)
	if st.Release != "v2" || st.Restarts != 0 {
		t.Errorf("after the push: %+v", st)
	}
	// The old timer fires into the void: nothing restarts, nothing starts.
	f.clock.fire(t)
	time.Sleep(50 * time.Millisecond)
	if again, _ := f.sup.Status("hello"); again != st || len(f.runtime.started) != 2 {
		t.Errorf("stale timer did something: %+v, %d started", again, len(f.runtime.started))
	}
	// And the new release's first crash waits 1s: the history was reset.
	f.runtime.exit(st.Instance, 1)
	if d := f.clock.fire(t); d != time.Second {
		t.Errorf("first crash of a new release waited %s", d)
	}
}

func TestReplacedInstanceExitIsNotAFailure(t *testing.T) {
	f := newFixture(t, Config{})
	ctx := context.Background()
	for range 2 {
		if _, err := f.deploy(ctx, "hello"); err != nil {
			t.Fatal(err)
		}
	}
	f.waitStopped(t, 1) // retire stopped inst1, whose watcher reported the exit
	time.Sleep(50 * time.Millisecond)
	st, _ := f.sup.Status("hello")
	if st.State != StateRunning || st.Instance != "inst2" || st.Restarts != 0 || f.clock.pending() {
		t.Errorf("status after a swap = %+v, pending %v", st, f.clock.pending())
	}
}

func TestManualRestartEndsACrashLoop(t *testing.T) {
	f := newFixture(t, Config{})
	ctx := context.Background()
	if _, err := f.deploy(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	st := f.waitState(t, StateRunning, "")
	// Crash it into backoff twice so the history has two failures.
	for i := 0; i < 2; i++ {
		f.runtime.exit(st.Instance, 1)
		f.waitState(t, StateRestarting, "")
		f.clock.fire(t)
		st = f.waitState(t, StateRunning, st.Instance)
	}
	f.runtime.exit(st.Instance, 1)
	f.waitState(t, StateRestarting, "")
	// The operator restarts by hand while the third wait (4s) is pending.
	var out bytes.Buffer
	if err := f.sup.Restart(ctx, "hello", &out); err != nil {
		t.Fatalf("restart: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "-----> restarting v1\n") || !strings.Contains(out.String(), "-----> live at") || strings.Contains(out.String(), "release v") {
		t.Errorf("restart output:\n%s", out.String())
	}
	st = f.waitState(t, StateRunning, st.Instance)
	if st.Restarts != 3 || st.Release != "v1" {
		t.Errorf("after manual restart: %+v", st)
	}
	// The pending timer is stale and the history is clean: the next crash
	// waits 1s, and firing the old timer does nothing.
	f.clock.fire(t)
	time.Sleep(50 * time.Millisecond)
	if again, _ := f.sup.Status("hello"); again != st {
		t.Errorf("stale timer changed status: %+v", again)
	}
	f.runtime.exit(st.Instance, 1)
	if d := f.clock.fire(t); d != time.Second {
		t.Errorf("first crash after a manual restart waited %s", d)
	}
	app, _ := f.store.GetApp(ctx, "hello")
	if rels, _ := f.store.ListReleases(ctx, app.ID); len(rels) != 1 {
		t.Errorf("a restart made a release: %+v", rels)
	}
}

func TestManualRestartRevivesACrashedApp(t *testing.T) {
	f := newFixture(t, Config{})
	ctx := context.Background()
	if _, err := f.deploy(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	st := f.waitState(t, StateRunning, "")
	for i := 0; i < 9; i++ {
		f.runtime.exit(st.Instance, 1)
		f.clock.fire(t)
		st = f.waitState(t, StateRunning, st.Instance)
	}
	f.runtime.exit(st.Instance, 1)
	st = f.waitState(t, StateCrashed, "")
	var out bytes.Buffer
	if err := f.sup.Restart(ctx, "hello", &out); err != nil {
		t.Fatalf("restart of a crashed app: %v", err)
	}
	st = f.waitState(t, StateRunning, st.Instance)
	if st.Restarts != 10 || f.clock.pending() {
		t.Errorf("after reviving: %+v, pending %v", st, f.clock.pending())
	}
}

func TestRestartNeedsALiveRelease(t *testing.T) {
	f := newFixture(t, Config{})
	var out bytes.Buffer
	if err := f.sup.Restart(context.Background(), "hello", &out); err == nil || !strings.Contains(err.Error(), "no live release") {
		t.Errorf("err = %v", err)
	}
}

func TestRollbackRunsTheOldImageAndConfig(t *testing.T) {
	f := newFixture(t, Config{})
	ctx := context.Background()
	app, _ := f.store.GetApp(ctx, "hello")
	f.withManifest("[process]\nmemory = \"64m\"\n")
	if _, err := f.store.UpdateConfig(ctx, app.ID, map[string]string{"GREETING": "one"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := f.deploy(ctx, "hello"); err != nil { // v1: 64m, GREETING=one
		t.Fatal(err)
	}
	f.withManifest("")
	if _, err := f.store.UpdateConfig(ctx, app.ID, map[string]string{"GREETING": "two"}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := f.deploy(ctx, "hello"); err != nil { // v2: defaults, GREETING=two
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := f.sup.Rollback(ctx, "hello", 1, &out); err != nil {
		t.Fatalf("rollback: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "-----> release v3 (rollback v1)\n") || strings.Contains(out.String(), "building") {
		t.Errorf("rollback output:\n%s", out.String())
	}
	rels, _ := f.store.ListReleases(ctx, app.ID)
	if len(rels) != 3 || rels[2].Status != store.StatusLive || rels[1].Status != store.StatusSuperseded || rels[0].Status != store.StatusSuperseded {
		t.Fatalf("releases = %+v", rels)
	}
	v1, v3 := rels[0], rels[2]
	if v3.Image != v1.Image || v3.GitSHA != v1.GitSHA || v3.ConfigJSON != `{"GREETING":"one"}` || v3.ManifestJSON != v1.ManifestJSON || v3.Note != "rollback v1" {
		t.Errorf("v3 = %+v, want v1's image, commit, config and manifest", v3)
	}
	spec := f.runtime.started[2]
	if spec.Env["GREETING"] != "one" || spec.Memory != 64<<20 || spec.Release != "v3" {
		t.Errorf("rolled back instance = %+v", spec)
	}
	// The config table still says two; a plain redeploy would run it.
	if vars, _ := f.store.GetConfig(ctx, app.ID); vars["GREETING"] != "two" {
		t.Errorf("rollback changed the config table: %v", vars)
	}
	if f.builder.maxSeen != 1 {
		t.Errorf("rollback built an image")
	}
	f.waitStopped(t, 2)

	// Cannot roll back to what is live, to a failed build, or to nothing.
	f.builder.err = errors.New("build failed")
	_, _ = f.deploy(ctx, "hello") // v4 failed, no image
	for version, want := range map[int]string{3: "already live", 4: "no image", 9: "not found"} {
		out.Reset()
		if err := f.sup.Rollback(ctx, "hello", version, &out); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("rollback to v%d: %v, want %q", version, err, want)
		}
	}
	if rels, _ := f.store.ListReleases(ctx, app.ID); len(rels) != 4 {
		t.Errorf("refused rollbacks made releases: %d", len(rels))
	}
}

func TestDestroyStopsAndForgets(t *testing.T) {
	f := newFixture(t, Config{DrainGrace: time.Hour})
	ctx := context.Background()
	if _, err := f.deploy(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	if err := f.sup.Destroy(ctx, "hello"); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if got := f.runtime.stoppedIDs(); len(got) != 1 || got[0] != "inst1" {
		t.Errorf("stopped %v, want the instance, at once", got)
	}
	if _, ok := f.router.Get("hello.localhost"); ok {
		t.Error("route left after destroy")
	}
	st, ok := f.sup.Status("hello")
	if ok {
		t.Errorf("status after destroy = %+v, want none", st)
	}
	// The watcher saw the exit and had no one to tell; nothing restarts.
	time.Sleep(50 * time.Millisecond)
	if f.clock.pending() {
		t.Error("a restart was scheduled for a destroyed app")
	}
	// The name is free again: a new deploy starts a fresh actor.
	if _, err := f.deploy(ctx, "hello"); err != nil {
		t.Fatalf("deploy after destroy: %v", err)
	}
	if st, _ := f.sup.Status("hello"); st.State != StateRunning || st.Instance != "inst2" {
		t.Errorf("status after redeploy = %+v", st)
	}
	// An app nothing ran for: only the route (if any) goes.
	if _, err := f.store.CreateApp(ctx, "idle", "idle.localhost"); err != nil {
		t.Fatal(err)
	}
	f.router.Set("idle.localhost", "127.0.0.1:1")
	if err := f.sup.Destroy(ctx, "idle"); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.router.Get("idle.localhost"); ok {
		t.Error("idle app's route left")
	}
	if err := f.sup.Destroy(ctx, "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("destroy unknown: %v", err)
	}
}

func TestReconcile(t *testing.T) {
	f := newFixture(t, Config{})
	ctx := context.Background()
	st := f.store
	mk := func(name string) store.App {
		t.Helper()
		if name != "hello" {
			if _, err := st.CreateApp(ctx, name, name+".localhost"); err != nil {
				t.Fatal(err)
			}
		}
		a, _ := st.GetApp(ctx, name)
		return a
	}
	goLive := func(app store.App, version int) store.Release {
		t.Helper()
		var rel store.Release
		for v := 1; v <= version; v++ {
			r, err := st.CreateRelease(ctx, app.ID, store.NewRelease{Image: fmt.Sprintf("ashore/%s:%d", app.Name, v), ManifestJSON: `{"process":{"grace":"3s"}}`})
			if err != nil {
				t.Fatal(err)
			}
			if err := st.SetLive(ctx, app.ID, r.ID); err != nil {
				t.Fatal(err)
			}
			rel = r
		}
		return rel
	}
	// hello: live v2, one running v2 (adopt), one running v1 (orphan), one exited v2 (remove).
	hello := mk("hello")
	goLive(hello, 2)
	// api: live v1, nothing running: restart.
	api := mk("api")
	goLive(api, 1)
	// idle: no live release, but a container runs: orphan.
	mk("idle")
	// A release cut off mid-deploy.
	cut, err := st.CreateRelease(ctx, api.ID, store.NewRelease{})
	if err != nil {
		t.Fatal(err)
	}
	// A listener for the adopted instance, so its address is real.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	f.runtime.found = []runtime.Instance{
		{ID: "hello-v2", App: "hello", Release: "v2", Addr: ln.Addr().String(), Running: true},
		{ID: "hello-v1", App: "hello", Release: "v1", Addr: "127.0.0.1:1", Running: true},
		{ID: "hello-dead", App: "hello", Release: "v2", Running: false},
		{ID: "idle-v1", App: "idle", Release: "v1", Addr: "127.0.0.1:2", Running: true},
		{ID: "ghost-v3", App: "ghost", Release: "v3", Addr: "127.0.0.1:3", Running: true},
	}

	rep, err := f.sup.Reconcile(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rep != (Report{Adopted: 1, Restarted: 1, Orphans: 3, Removed: 1, Failed: 1}) {
		t.Errorf("report = %+v", rep)
	}
	if addr, _ := f.router.Get("hello.localhost"); addr != ln.Addr().String() {
		t.Errorf("hello route = %q, want the adopted instance", addr)
	}
	s, ok := f.sup.Status("hello")
	if !ok || s.State != StateRunning || s.Instance != "hello-v2" || s.Release != "v2" {
		t.Errorf("hello status = %+v", s)
	}
	stopped := f.runtime.stoppedIDs()
	for _, id := range []string{"hello-v1", "hello-dead", "idle-v1", "ghost-v3"} {
		if !slices.Contains(stopped, id) {
			t.Errorf("%s not removed; stopped %v", id, stopped)
		}
	}
	if slices.Contains(stopped, "hello-v2") {
		t.Error("the adopted instance was stopped")
	}
	if got, _ := st.GetRelease(ctx, cut.ID); got.Status != store.StatusFailed {
		t.Errorf("cut-off release = %s, want failed", got.Status)
	}
	// api comes back in the background through the normal path.
	deadline := time.Now().Add(5 * time.Second)
	for {
		if addr, ok := f.router.Get("api.localhost"); ok && addr != "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("api was not restarted")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if len(f.runtime.started) != 1 || f.runtime.started[0].App != "api" || f.runtime.started[0].Image != "ashore/api:1" {
		t.Errorf("started %+v, want one instance of api v1", f.runtime.started)
	}
	if s, _ := f.sup.Status("api"); s.State != StateRunning || s.Restarts != 1 {
		t.Errorf("api status = %+v", s)
	}
	// The adopted instance is watched: its exit schedules a restart.
	f.runtime.exit("hello-v2", 1)
	f.waitState(t, StateRestarting, "")
	if d := f.clock.fire(t); d != time.Second {
		t.Errorf("adopted instance's first crash waited %s", d)
	}
	f.waitState(t, StateRunning, "hello-v2")
	// A second deploy of hello replaces the adopted instance with the grace
	// its live manifest set.
	f.runtime.mu.Lock()
	grace := f.runtime.graces["hello-v2"]
	f.runtime.mu.Unlock()
	_ = grace
}

func TestReconcileWithNothing(t *testing.T) {
	f := newFixture(t, Config{})
	rep, err := f.sup.Reconcile(context.Background())
	if err != nil || rep != (Report{}) {
		t.Errorf("reconcile of an empty world = %+v, %v", rep, err)
	}
	if rep.String() != "adopted=0 restarted=0 orphans=0 removed=0 failed=0" {
		t.Errorf("String = %q", rep.String())
	}
}

func TestStatusBeforeAnything(t *testing.T) {
	f := newFixture(t, Config{})
	if _, ok := f.sup.Status("hello"); ok {
		t.Error("status for an app nothing ran for")
	}
	f.builder.err = errors.New("no Dockerfile")
	_, _ = f.deploy(context.Background(), "hello")
	if _, ok := f.sup.Status("hello"); ok {
		t.Error("status after a deploy that never started an instance")
	}
}

// withManifest makes every push see this ashore.toml.
func (f *fixture) withManifest(toml string) {
	f.sup.readManifest = func(context.Context, string, string, string) ([]byte, bool, error) {
		return []byte(toml), true, nil
	}
}

func TestManifestShapesTheInstance(t *testing.T) {
	f := newFixture(t, Config{StopGrace: 10 * time.Second})
	ctx := context.Background()
	f.withManifest("[health]\npath = \"/healthz\"\ninterval = \"20ms\"\n[process]\nmemory = \"128m\"\ncpu = \"0.5\"\ngrace = \"3s\"\n")
	f.runtime.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" && r.Host == "hello.localhost" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		http.Error(w, "wrong probe: "+r.Host+r.URL.Path, http.StatusNotFound)
	})
	out, err := f.deploy(ctx, "hello")
	if err != nil {
		t.Fatalf("deploy: %v\n%s", err, out)
	}
	for _, want := range []string{
		"-----> ashore.toml: health GET /healthz (timeout 5s), memory 128m, cpu 0.5, grace 3s\n",
		"-----> waiting for v1 to answer GET /healthz on port 8080\n",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
	spec := f.runtime.started[0]
	if spec.Memory != 128<<20 || spec.CPU != 0.5 {
		t.Errorf("spec limits = %d bytes, %v cpu", spec.Memory, spec.CPU)
	}
	app, _ := f.store.GetApp(ctx, "hello")
	rel, _ := f.store.LiveRelease(ctx, app.ID)
	if !strings.Contains(rel.ManifestJSON, `"path":"/healthz"`) || !strings.Contains(rel.ManifestJSON, `"memory":"128m"`) {
		t.Errorf("stored manifest = %s", rel.ManifestJSON)
	}

	// A redeploy carries the manifest over; the old instance is stopped
	// with its own grace, not the daemon default.
	var buf bytes.Buffer
	if err := f.sup.Redeploy(ctx, "hello", "config:set X", &buf); err != nil {
		t.Fatalf("redeploy: %v\n%s", err, buf.String())
	}
	if again, _ := f.store.LiveRelease(ctx, app.ID); again.ManifestJSON != rel.ManifestJSON {
		t.Errorf("redeploy manifest = %s, want %s", again.ManifestJSON, rel.ManifestJSON)
	}
	if f.runtime.started[1].Memory != 128<<20 {
		t.Errorf("redeployed instance memory = %d", f.runtime.started[1].Memory)
	}
	f.waitStopped(t, 1)
	f.runtime.mu.Lock()
	grace := f.runtime.graces["inst1"]
	f.runtime.mu.Unlock()
	if grace != 3*time.Second {
		t.Errorf("old instance stopped with grace %v, want 3s", grace)
	}
}

func TestNoManifestMeansDefaults(t *testing.T) {
	f := newFixture(t, Config{StopGrace: 7 * time.Second})
	out, err := f.deploy(context.Background(), "hello")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "ashore.toml") {
		t.Errorf("no manifest, but the output mentions one:\n%s", out)
	}
	app, _ := f.store.GetApp(context.Background(), "hello")
	rel, _ := f.store.LiveRelease(context.Background(), app.ID)
	if !strings.Contains(rel.ManifestJSON, `"timeout":"5s"`) || !strings.Contains(rel.ManifestJSON, `"grace":"7s"`) || !strings.Contains(rel.ManifestJSON, `"memory":"256m"`) {
		t.Errorf("stored defaults = %s", rel.ManifestJSON)
	}
	if _, err := f.deploy(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	f.waitStopped(t, 1)
	f.runtime.mu.Lock()
	defer f.runtime.mu.Unlock()
	if f.runtime.graces["inst1"] != 7*time.Second {
		t.Errorf("default grace = %v, want 7s", f.runtime.graces["inst1"])
	}
}

func TestBadManifestRejectsPushWithoutARelease(t *testing.T) {
	f := newFixture(t, Config{})
	f.withManifest("[health]\npath = \"healthz\"\n")
	_, err := f.deploy(context.Background(), "hello")
	if err == nil || !strings.Contains(err.Error(), "ashore.toml: health.path") {
		t.Fatalf("err = %v", err)
	}
	app, _ := f.store.GetApp(context.Background(), "hello")
	if rels, _ := f.store.ListReleases(context.Background(), app.ID); len(rels) != 0 {
		t.Errorf("a bad manifest made a release: %+v", rels)
	}
	if len(f.runtime.started) != 0 || f.builder.maxSeen != 0 {
		t.Error("a bad manifest built or started something")
	}
}

func TestHTTPHealthWaitsForA2xx(t *testing.T) {
	f := newFixture(t, Config{})
	f.withManifest("[health]\npath = \"/healthz\"\ninterval = \"20ms\"\n")
	var probes atomic.Int32
	f.runtime.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if probes.Add(1) <= 3 {
			http.Error(w, "warming up", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ok")
	})
	if _, err := f.deploy(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	if n := probes.Load(); n < 4 {
		t.Errorf("%d probes; the deploy went live before a 2xx", n)
	}
	if _, ok := f.router.Get("hello.localhost"); !ok {
		t.Error("no route after a healthy deploy")
	}
}

func TestHTTPHealthTimeoutNamesTheLastAnswer(t *testing.T) {
	f := newFixture(t, Config{})
	f.withManifest("[health]\npath = \"/healthz\"\ntimeout = \"300ms\"\ninterval = \"20ms\"\n")
	f.runtime.handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	})
	_, err := f.deploy(context.Background(), "hello")
	if err == nil || !strings.Contains(err.Error(), "v1 did not answer 2xx on GET /healthz within 300ms (last: 500 Internal Server Error)") {
		t.Fatalf("err = %v", err)
	}
	if got := f.runtime.stoppedIDs(); len(got) != 1 {
		t.Errorf("stopped %v, want the unhealthy instance", got)
	}
	if _, ok := f.router.Get("hello.localhost"); ok {
		t.Error("route set for an unhealthy instance")
	}
	app, _ := f.store.GetApp(context.Background(), "hello")
	if rels, _ := f.store.ListReleases(context.Background(), app.ID); len(rels) != 1 || rels[0].Status != store.StatusFailed {
		t.Errorf("releases = %+v, want one failed", rels)
	}
}

func TestAnswers(t *testing.T) {
	var gotHost string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHost = r.Host
		switch r.URL.Path {
		case "/ok":
			w.WriteHeader(http.StatusOK)
		case "/redirect":
			http.Redirect(w, r, "/ok", http.StatusFound)
		default:
			http.Error(w, "no", http.StatusTeapot)
		}
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")
	ctx := context.Background()
	if ok, detail := answers(ctx, addr, "app.localhost", "/ok"); !ok || detail != "200 OK" || gotHost != "app.localhost" {
		t.Errorf("/ok: %v %q host %q", ok, detail, gotHost)
	}
	if ok, detail := answers(ctx, addr, "app.localhost", "/nope"); ok || detail != "418 I'm a teapot" {
		t.Errorf("/nope: %v %q", ok, detail)
	}
	if ok, detail := answers(ctx, addr, "app.localhost", "/redirect"); ok || !strings.HasPrefix(detail, "302") {
		t.Errorf("/redirect: %v %q", ok, detail)
	}
	srv.Close()
	if ok, detail := answers(ctx, addr, "app.localhost", "/ok"); ok || detail == "" {
		t.Errorf("closed server: %v %q", ok, detail)
	}
}
