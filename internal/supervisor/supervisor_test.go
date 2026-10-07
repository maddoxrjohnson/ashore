package supervisor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

	mu        sync.Mutex
	started   []runtime.Spec
	stopped   []string
	listeners map[string]net.Listener
	n         int
}

func newFakeRuntime(t *testing.T) *fakeRuntime {
	rt := &fakeRuntime{listen: true, exitCode: -1, listeners: make(map[string]net.Listener)}
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
	}
	r.started = append(r.started, s)
	return runtime.Instance{ID: id, App: s.App, Release: s.Release, Addr: addr, StartedAt: time.Now(), Running: true}, nil
}

func (r *fakeRuntime) Stop(_ context.Context, id string, _ time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = append(r.stopped, id)
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
	cfg.DataDir, cfg.Domain, cfg.HTTPPort = t.TempDir(), "localhost", 8080
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	f := &fixture{store: st, builder: &fakeBuilder{}, runtime: newFakeRuntime(t), router: router.New(log), done: make(chan struct{})}
	f.sup = New(st, f.builder, f.runtime, f.router, cfg, log)
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

func TestDeployHappyPath(t *testing.T) {
	f := newFixture(t, Config{})
	out, err := f.deploy(context.Background(), "hello")
	if err != nil {
		t.Fatalf("deploy: %v\n%s", err, out)
	}
	for _, want := range []string{
		"-----> building hello (abc1234)\n", "Step 1/1", "-----> release v1\n",
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
	if err != nil || rel.Version != 1 || rel.Status != store.StatusLive || rel.GitSHA != sha || rel.Note != "push abc1234" {
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
	if got := f.runtime.stoppedIDs(); len(got) != 1 || got[0] != "inst1" {
		t.Errorf("stopped %v, want the first instance only", got)
	}
	app, _ := f.store.GetApp(ctx, "hello")
	rels, _ := f.store.ListReleases(ctx, app.ID)
	if len(rels) != 2 || rels[0].Status != store.StatusSuperseded || rels[1].Status != store.StatusLive {
		t.Errorf("releases = %+v", rels)
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
	app, _ := f.store.GetApp(context.Background(), "hello")
	if rels, _ := f.store.ListReleases(context.Background(), app.ID); len(rels) != 0 {
		t.Errorf("releases after a failed build: %+v", rels)
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
	if rels, _ := f.store.ListReleases(context.Background(), app.ID); len(rels) != 1 {
		t.Errorf("releases = %+v, want only the successful one", rels)
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
	if listening(ctx, dead) {
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
	if listening(ctx, proxy.Addr().String()) {
		t.Error("a connection closed at once counts as listening")
	}

	// A server that accepts and waits for a request.
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	if !listening(ctx, held.Addr().String()) {
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
	if !listening(ctx, talker.Addr().String()) {
		t.Error("a server that sends a banner does not count as listening")
	}
}
