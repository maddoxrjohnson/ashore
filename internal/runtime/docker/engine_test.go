package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/netip"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/build"
	"github.com/moby/moby/api/types/jsonstream"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"

	"github.com/maddoxrjohnson/ashore/internal/runtime"
)

// These tests talk to a real Docker daemon and skip without one. Every
// container and image they create carries ashore.test=1.

func dockerClient(t *testing.T) *client.Client {
	t.Helper()
	cli, err := client.New(client.FromEnv)
	if err != nil {
		t.Skipf("docker client: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cli.Ping(ctx, client.PingOptions{}); err != nil {
		_ = cli.Close()
		t.Skipf("docker daemon unavailable: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// The test app: exits with $EXIT if set; otherwise prints one line to each
// stream and answers every TCP connection on $PORT with "pong". The base
// image is the one hello-go builds with, so it is usually cached already.
const testDockerfile = `FROM golang:1.27-alpine
CMD ["sh", "-c", "if [ -n \"$EXIT\" ]; then echo bye; exit $EXIT; fi; echo \"out $GREETING\"; echo err >&2; exec nc -lk -p \"$PORT\" -e echo pong"]
`

const testImage = "ashore/runtime-test:latest"

func buildTestImage(t *testing.T, cli *client.Client) string {
	t.Helper()
	var ctxTar bytes.Buffer
	tw := tar.NewWriter(&ctxTar)
	_ = tw.WriteHeader(&tar.Header{Name: "Dockerfile", Mode: 0o644, Size: int64(len(testDockerfile))})
	_, _ = tw.Write([]byte(testDockerfile))
	_ = tw.Close()

	ctx := context.Background()
	res, err := cli.ImageBuild(ctx, &ctxTar, client.ImageBuildOptions{
		Tags:        []string{testImage},
		Labels:      map[string]string{"ashore.test": "1"},
		Remove:      true,
		ForceRemove: true,
		Version:     build.BuilderV1,
	})
	if err != nil {
		t.Fatalf("build test image: %v", err)
	}
	defer func() { _ = res.Body.Close() }()
	dec := json.NewDecoder(res.Body)
	for {
		var m jsonstream.Message
		if err := dec.Decode(&m); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("build test image: %v", err)
		}
		if m.Error != nil {
			t.Fatalf("build test image: %s", m.Error.Message)
		}
	}
	t.Cleanup(func() {
		_, _ = cli.ImageRemove(context.Background(), testImage, client.ImageRemoveOptions{Force: true, PruneChildren: true})
	})
	return testImage
}

func testSpec(t *testing.T, image string, env map[string]string) runtime.Spec {
	t.Helper()
	port, err := runtime.FreePort()
	if err != nil {
		t.Fatal(err)
	}
	return runtime.Spec{
		App: "rttest", Release: "v1", Image: image, Env: env,
		HostPort: port, ContainerPort: 8080,
		Memory: 64 << 20, CPU: 0.5,
		Labels: map[string]string{"ashore.test": "1"},
	}
}

func listed(t *testing.T, rt *Runtime, id string) (runtime.Instance, bool) {
	t.Helper()
	all, err := rt.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(all, func(in runtime.Instance) bool { return in.ID == id })
	if i < 0 {
		return runtime.Instance{}, false
	}
	return all[i], true
}

func TestRoundTripWithDocker(t *testing.T) {
	cli := dockerClient(t)
	image := buildTestImage(t, cli)
	rt := New(cli)
	ctx := context.Background()

	s := testSpec(t, image, map[string]string{"GREETING": "hi"})
	inst, err := rt.Start(ctx, s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Stop(context.Background(), inst.ID, 0) })

	// The limits and the binding reached the daemon, not just the request.
	ins, err := cli.ContainerInspect(ctx, inst.ID, client.ContainerInspectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	hc := ins.Container.HostConfig
	if hc.Memory != 64<<20 || hc.NanoCPUs != 500_000_000 || hc.PidsLimit == nil || *hc.PidsLimit != runtime.DefaultPids {
		t.Errorf("limits: memory %d, nano cpus %d, pids %v", hc.Memory, hc.NanoCPUs, hc.PidsLimit)
	}
	if !slices.Contains(hc.SecurityOpt, "no-new-privileges") {
		t.Errorf("security opts = %v", hc.SecurityOpt)
	}
	b := hc.PortBindings[network.MustParsePort("8080/tcp")]
	if len(b) != 1 || b[0].HostIP != netip.MustParseAddr("127.0.0.1") {
		t.Errorf("port bindings = %v", hc.PortBindings)
	}

	// The app is reachable through Addr. nc needs a moment to listen.
	var reply string
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", inst.Addr, time.Second); err == nil {
			_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
			buf, _ := io.ReadAll(c)
			_ = c.Close()
			if reply = strings.TrimSpace(string(buf)); reply == "pong" {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if reply != "pong" {
		t.Fatalf("no pong from %s (last reply %q)", inst.Addr, reply)
	}

	got, ok := listed(t, rt, inst.ID)
	if !ok || !got.Running || got.Addr != inst.Addr || got.App != "rttest" || got.Release != "v1" {
		t.Errorf("listed = %+v (found %v), want running at %s", got, ok, inst.Addr)
	}

	// Follow the logs and wait for the exit while Stop runs.
	var stdout, stderr bytes.Buffer
	logsDone := make(chan error, 1)
	go func() { logsDone <- rt.Logs(ctx, inst.ID, time.Time{}, &stdout, &stderr) }()
	type exit struct {
		code int
		err  error
	}
	waitDone := make(chan exit, 1)
	go func() {
		code, err := rt.Wait(ctx, inst.ID)
		waitDone <- exit{code, err}
	}()
	time.Sleep(200 * time.Millisecond) // let both requests reach the daemon

	start := time.Now()
	if err := rt.Stop(ctx, inst.ID, time.Second); err != nil {
		t.Fatal(err)
	}
	t.Logf("stop took %v", time.Since(start).Round(time.Millisecond))

	// busybox nc catches SIGTERM (it prints "punt!") and exits 128 +
	// SIGTERM well inside the grace, so Docker never needs SIGKILL.
	select {
	case e := <-waitDone:
		if e.err != nil || e.code != 143 {
			t.Errorf("wait = %d, %v; want 143", e.code, e.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Wait did not return after Stop")
	}
	select {
	case err := <-logsDone:
		if err != nil {
			t.Errorf("logs: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Logs did not return after Stop")
	}
	if stdout.String() != "out hi\n" || !strings.HasPrefix(stderr.String(), "err\n") {
		t.Errorf("stdout %q, stderr %q", stdout.String(), stderr.String())
	}

	if _, ok := listed(t, rt, inst.ID); ok {
		t.Error("container still listed after Stop")
	}
	if _, err := rt.Wait(ctx, inst.ID); !errors.Is(err, runtime.ErrNotFound) {
		t.Errorf("wait after stop: %v, want ErrNotFound", err)
	}
	if err := rt.Stop(ctx, inst.ID, 0); err != nil {
		t.Errorf("second stop: %v", err)
	}
}

func TestExitedInstanceWithDocker(t *testing.T) {
	cli := dockerClient(t)
	image := buildTestImage(t, cli)
	rt := New(cli)
	ctx := context.Background()

	inst, err := rt.Start(ctx, testSpec(t, image, map[string]string{"EXIT": "3"}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Stop(context.Background(), inst.ID, 0) })

	wctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	code, err := rt.Wait(wctx, inst.ID)
	if err != nil || code != 3 {
		t.Fatalf("wait = %d, %v; want 3", code, err)
	}
	// A second Wait on the exited container answers at once.
	if code, err := rt.Wait(wctx, inst.ID); err != nil || code != 3 {
		t.Errorf("second wait = %d, %v; want 3", code, err)
	}

	got, ok := listed(t, rt, inst.ID)
	if !ok || got.Running || got.Addr != "" {
		t.Errorf("listed = %+v (found %v), want exited with no address", got, ok)
	}

	// Following the logs of an exited container returns what it printed.
	var stdout bytes.Buffer
	if err := rt.Logs(wctx, inst.ID, time.Time{}, &stdout, io.Discard); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "bye\n" {
		t.Errorf("stdout = %q", stdout.String())
	}

	if err := rt.Stop(ctx, inst.ID, time.Second); err != nil {
		t.Fatal(err)
	}
	if _, ok := listed(t, rt, inst.ID); ok {
		t.Error("exited container still listed after Stop")
	}
}
