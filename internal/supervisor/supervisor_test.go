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
	started   []runtime.Spec
	stopped   []string
	graces    map[string]time.Duration // id -> grace given to Stop
	listeners map[string]net.Listener
	n         int
}

func newFakeRuntime(t *testing.T) *fakeRuntime {
	rt := &fakeRuntime{listen: true, exitCode: -1, listeners: make(map[string]net.Listener), graces: make(map[string]time.Duration)}
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

func (r *fakeRuntime) Stop(_ context.Context, id string, grace time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = append(r.stopped, id)
	r.graces[id] = grace
	if l, ok := r.listeners[id]; ok {
		_ = l.Close()
		delete(r.listeners, id)
	}
	return nil
}

func (r *fakeRuntime) Wait(ctx context.Context, _ string) (int, error) {
	if !r.listen && r.exitCode >= 0 {
		return r.exitCode, nil
	}
	<-ctx.Done()
	return -1, ctx.Err()
}

func (r *fakeRuntime) Logs(_ context.Context, _ string, _ time.Time, stdout, _ io.Writer) error {
	_, err := io.WriteString(stdout, r.logs)
	return err
}

func (r *fakeRuntime) List(context.Context) ([]runtime.Instance, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []runtime.Instance
	for i := range r.started {
		id := "inst" + strconv.Itoa(i+1)
		_, running := r.listeners[id]
		out = append(out, runtime.Instance{ID: id, Running: running || (r.listen && !slicesContains(r.stopped, id))})
	}
	return out, nil
}

func slicesContains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
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
	f := &fixture{store: st, builder: &fakeBuilder{}, runtime: newFakeRuntime(t), router: router.New(log), done: make(chan struct{})}
	f.sup = New(st, f.builder, f.runtime, f.router, cfg, log)
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
