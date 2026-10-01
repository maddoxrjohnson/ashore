package docker

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"

	"github.com/maddoxrjohnson/ashore/internal/runtime"
)

// fakeDocker records requests and answers with canned results. The zero
// value succeeds at everything.
type fakeDocker struct {
	createErr, startErr, stopErr, removeErr, logsErr, listErr error

	wait  container.WaitResponse
	wErr  error
	block bool // ContainerWait never answers
	logs  []byte
	items []container.Summary

	mu      sync.Mutex
	create  client.ContainerCreateOptions
	stop    client.ContainerStopOptions
	logOpts client.ContainerLogsOptions
	list    client.ContainerListOptions
	removed []string
}

func (f *fakeDocker) ContainerCreate(_ context.Context, o client.ContainerCreateOptions) (client.ContainerCreateResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.create = o
	if f.createErr != nil {
		return client.ContainerCreateResult{}, f.createErr
	}
	return client.ContainerCreateResult{ID: "c0ffee0123456789abcdef"}, nil
}

func (f *fakeDocker) ContainerStart(context.Context, string, client.ContainerStartOptions) (client.ContainerStartResult, error) {
	return client.ContainerStartResult{}, f.startErr
}

func (f *fakeDocker) ContainerStop(_ context.Context, _ string, o client.ContainerStopOptions) (client.ContainerStopResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stop = o
	return client.ContainerStopResult{}, f.stopErr
}

func (f *fakeDocker) ContainerRemove(_ context.Context, id string, o client.ContainerRemoveOptions) (client.ContainerRemoveResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if o.Force {
		f.removed = append(f.removed, id)
	}
	return client.ContainerRemoveResult{}, f.removeErr
}

func (f *fakeDocker) ContainerWait(context.Context, string, client.ContainerWaitOptions) client.ContainerWaitResult {
	res := make(chan container.WaitResponse, 1)
	errc := make(chan error, 1)
	switch {
	case f.block:
	case f.wErr != nil:
		errc <- f.wErr
	default:
		res <- f.wait
	}
	return client.ContainerWaitResult{Result: res, Error: errc}
}

func (f *fakeDocker) ContainerLogs(_ context.Context, _ string, o client.ContainerLogsOptions) (client.ContainerLogsResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logOpts = o
	if f.logsErr != nil {
		return nil, f.logsErr
	}
	return io.NopCloser(bytes.NewReader(f.logs)), nil
}

func (f *fakeDocker) ContainerList(_ context.Context, o client.ContainerListOptions) (client.ContainerListResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.list = o
	return client.ContainerListResult{Items: f.items}, f.listErr
}

func spec() runtime.Spec {
	return runtime.Spec{
		App: "hello", Release: "v3", Image: "ashore/hello:abc1234",
		Env:      map[string]string{"B": "2", "A": "1", "PORT": "9999"},
		HostPort: 41234, ContainerPort: 8080,
		Memory: 256 << 20, CPU: 0.5,
		Labels: map[string]string{"ashore.test": "1", runtime.LabelApp: "spoofed"},
	}
}

func TestStartCreatesLimitedLoopbackContainer(t *testing.T) {
	f := &fakeDocker{}
	inst, err := New(f).Start(context.Background(), spec())
	if err != nil {
		t.Fatal(err)
	}

	if inst.ID != "c0ffee0123456789abcdef" || inst.App != "hello" || inst.Release != "v3" ||
		inst.Addr != "127.0.0.1:41234" || !inst.Running || inst.StartedAt.IsZero() {
		t.Errorf("instance = %+v", inst)
	}

	o := f.create
	if !strings.HasPrefix(o.Name, "ashore-hello-v3-") || len(o.Name) != len("ashore-hello-v3-")+8 {
		t.Errorf("name = %q", o.Name)
	}
	if o.Config.Image != "ashore/hello:abc1234" {
		t.Errorf("image = %q", o.Config.Image)
	}
	if want := []string{"A=1", "B=2", "PORT=8080"}; !slices.Equal(o.Config.Env, want) {
		t.Errorf("env = %q, want %q (sorted, PORT forced to the container port)", o.Config.Env, want)
	}
	want := map[string]string{"ashore.test": "1", runtime.LabelApp: "hello", runtime.LabelRelease: "v3"}
	if len(o.Config.Labels) != len(want) {
		t.Errorf("labels = %v, want %v", o.Config.Labels, want)
	}
	for k, v := range want {
		if o.Config.Labels[k] != v {
			t.Errorf("label %s = %q, want %q", k, o.Config.Labels[k], v)
		}
	}

	port := network.MustParsePort("8080/tcp")
	if _, ok := o.Config.ExposedPorts[port]; !ok {
		t.Errorf("exposed ports = %v", o.Config.ExposedPorts)
	}
	hc := o.HostConfig
	b := hc.PortBindings[port]
	if len(b) != 1 || b[0].HostIP != netip.MustParseAddr("127.0.0.1") || b[0].HostPort != "41234" {
		t.Errorf("port bindings = %v, want only 127.0.0.1:41234", hc.PortBindings)
	}
	if !slices.Equal(hc.SecurityOpt, []string{"no-new-privileges"}) {
		t.Errorf("security opts = %v", hc.SecurityOpt)
	}
	if hc.Memory != 256<<20 || hc.MemorySwap != 256<<20 {
		t.Errorf("memory = %d, swap = %d", hc.Memory, hc.MemorySwap)
	}
	if hc.NanoCPUs != 500_000_000 {
		t.Errorf("nano cpus = %d", hc.NanoCPUs)
	}
	if hc.PidsLimit == nil || *hc.PidsLimit != runtime.DefaultPids {
		t.Errorf("pids limit = %v, want %d", hc.PidsLimit, runtime.DefaultPids)
	}
}

func TestStartRejectsBadPorts(t *testing.T) {
	for _, tc := range []struct{ host, ctr int }{{0, 8080}, {70000, 8080}, {41234, 0}, {41234, -1}, {41234, 65536}} {
		s := spec()
		s.HostPort, s.ContainerPort = tc.host, tc.ctr
		f := &fakeDocker{}
		if _, err := New(f).Start(context.Background(), s); err == nil {
			t.Errorf("host %d container %d: no error", tc.host, tc.ctr)
		}
		if f.create.Config != nil {
			t.Errorf("host %d container %d: container created anyway", tc.host, tc.ctr)
		}
	}
}

func TestStartFailures(t *testing.T) {
	boom := errors.New("boom")

	f := &fakeDocker{createErr: boom}
	if _, err := New(f).Start(context.Background(), spec()); !errors.Is(err, boom) {
		t.Errorf("create failure: err = %v", err)
	}
	if len(f.removed) != 0 {
		t.Errorf("create failure removed %v", f.removed)
	}

	f = &fakeDocker{startErr: boom}
	if _, err := New(f).Start(context.Background(), spec()); !errors.Is(err, boom) {
		t.Errorf("start failure: err = %v", err)
	}
	if !slices.Equal(f.removed, []string{"c0ffee0123456789abcdef"}) {
		t.Errorf("start failure removed %v, want the created container", f.removed)
	}
}

func TestStop(t *testing.T) {
	for _, tc := range []struct {
		grace time.Duration
		secs  int
	}{{0, 0}, {time.Second, 1}, {1500 * time.Millisecond, 2}, {10 * time.Second, 10}} {
		f := &fakeDocker{}
		if err := New(f).Stop(context.Background(), "abc", tc.grace); err != nil {
			t.Fatal(err)
		}
		if f.stop.Timeout == nil || *f.stop.Timeout != tc.secs {
			t.Errorf("grace %v: timeout = %v, want %d s", tc.grace, f.stop.Timeout, tc.secs)
		}
		if !slices.Equal(f.removed, []string{"abc"}) {
			t.Errorf("grace %v: removed %v", tc.grace, f.removed)
		}
	}

	gone := cerrdefs.ErrNotFound.WithMessage("No such container: abc")
	f := &fakeDocker{stopErr: gone, removeErr: gone}
	if err := New(f).Stop(context.Background(), "abc", time.Second); err != nil {
		t.Errorf("stopping a missing container: %v, want nil", err)
	}

	boom := errors.New("boom")
	f = &fakeDocker{stopErr: boom}
	if err := New(f).Stop(context.Background(), "abc", time.Second); !errors.Is(err, boom) {
		t.Errorf("stop failure: err = %v", err)
	}
	f = &fakeDocker{removeErr: boom}
	if err := New(f).Stop(context.Background(), "abc", time.Second); !errors.Is(err, boom) {
		t.Errorf("remove failure: err = %v", err)
	}
}

func TestWait(t *testing.T) {
	ctx := context.Background()

	code, err := New(&fakeDocker{wait: container.WaitResponse{StatusCode: 3}}).Wait(ctx, "abc")
	if err != nil || code != 3 {
		t.Errorf("exit 3: got %d, %v", code, err)
	}

	_, err = New(&fakeDocker{wErr: cerrdefs.ErrNotFound.WithMessage("No such container: abc")}).Wait(ctx, "abc")
	if !errors.Is(err, runtime.ErrNotFound) {
		t.Errorf("missing container: err = %v, want ErrNotFound", err)
	}

	_, err = New(&fakeDocker{wait: container.WaitResponse{Error: &container.WaitExitError{Message: "oops"}}}).Wait(ctx, "abc")
	if err == nil || !strings.Contains(err.Error(), "oops") {
		t.Errorf("wait error: err = %v", err)
	}

	ctx2, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if _, err := New(&fakeDocker{block: true}).Wait(ctx2, "abc"); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("cancelled wait: err = %v", err)
	}
}

// frame appends one chunk in Docker's multiplexed log format: a stream byte,
// three zero bytes, a big-endian uint32 length, then the payload.
func frame(b []byte, stream stdcopy.StdType, payload string) []byte {
	b = append(b, byte(stream), 0, 0, 0)
	b = binary.BigEndian.AppendUint32(b, uint32(len(payload)))
	return append(b, payload...)
}

func TestLogsSplitsStreams(t *testing.T) {
	var mux []byte
	mux = frame(mux, stdcopy.Stdout, "out 1\n")
	mux = frame(mux, stdcopy.Stderr, "err 1\n")
	mux = frame(mux, stdcopy.Stdout, "out 2\n")

	f := &fakeDocker{logs: mux}
	var stdout, stderr bytes.Buffer
	since := time.Unix(1700000000, 42)
	if err := New(f).Logs(context.Background(), "abc", since, &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "out 1\nout 2\n" || stderr.String() != "err 1\n" {
		t.Errorf("stdout %q, stderr %q", stdout.String(), stderr.String())
	}
	o := f.logOpts
	if !o.Follow || !o.ShowStdout || !o.ShowStderr || o.Since != "1700000000.000000042" {
		t.Errorf("log options = %+v", o)
	}

	f = &fakeDocker{}
	_ = New(f).Logs(context.Background(), "abc", time.Time{}, io.Discard, io.Discard)
	if f.logOpts.Since != "" {
		t.Errorf("zero since sent as %q", f.logOpts.Since)
	}

	f = &fakeDocker{logsErr: cerrdefs.ErrNotFound.WithMessage("No such container: abc")}
	if err := New(f).Logs(context.Background(), "abc", time.Time{}, io.Discard, io.Discard); !errors.Is(err, runtime.ErrNotFound) {
		t.Errorf("missing container: err = %v, want ErrNotFound", err)
	}
}

func TestList(t *testing.T) {
	f := &fakeDocker{items: []container.Summary{
		{
			ID: "running", Created: 1700000000, State: container.StateRunning,
			Labels: map[string]string{runtime.LabelApp: "hello", runtime.LabelRelease: "v3"},
			Ports:  []container.PortSummary{{IP: netip.MustParseAddr("127.0.0.1"), PrivatePort: 8080, PublicPort: 41234, Type: "tcp"}},
		},
		{
			ID: "exited", Created: 1700000100, State: container.StateExited,
			Labels: map[string]string{runtime.LabelApp: "hello", runtime.LabelRelease: "v2"},
		},
	}}
	got, err := New(f).List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []runtime.Instance{
		{ID: "running", App: "hello", Release: "v3", Addr: "127.0.0.1:41234", StartedAt: time.Unix(1700000000, 0), Running: true},
		{ID: "exited", App: "hello", Release: "v2", StartedAt: time.Unix(1700000100, 0)},
	}
	if !slices.Equal(got, want) {
		t.Errorf("list =\n%+v\nwant\n%+v", got, want)
	}
	if !f.list.All || !f.list.Filters["label"][runtime.LabelApp] {
		t.Errorf("list options = %+v, want all containers filtered by %s", f.list, runtime.LabelApp)
	}

	f = &fakeDocker{listErr: errors.New("boom")}
	if _, err := New(f).List(context.Background()); err == nil {
		t.Error("list failure: no error")
	}
}
