package native

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	dockerspec "github.com/moby/docker-image-spec/specs-go/v1"
	"github.com/moby/moby/api/types/build"
	"github.com/moby/moby/api/types/image"
	"github.com/moby/moby/api/types/jsonstream"
	"github.com/moby/moby/client"
)

// fakeDocker answers inspect from a fixed config and export from a tar
// built in the test.
type fakeDocker struct {
	id      string
	config  *dockerspec.DockerOCIImageConfig
	tarball func() []byte
	exports atomic.Int32
	removed atomic.Int32
	fail    error
}

func (f *fakeDocker) ImageInspect(_ context.Context, _ string, _ ...client.ImageInspectOption) (client.ImageInspectResult, error) {
	if f.fail != nil {
		return client.ImageInspectResult{}, f.fail
	}
	return client.ImageInspectResult{InspectResponse: image.InspectResponse{ID: "sha256:" + f.id, Config: f.config}}, nil
}

func (f *fakeDocker) ContainerCreate(context.Context, client.ContainerCreateOptions) (client.ContainerCreateResult, error) {
	return client.ContainerCreateResult{ID: "export-container"}, nil
}

func (f *fakeDocker) ContainerExport(context.Context, string, client.ContainerExportOptions) (client.ContainerExportResult, error) {
	f.exports.Add(1)
	return io.NopCloser(bytes.NewReader(f.tarball())), nil
}

func (f *fakeDocker) ContainerRemove(context.Context, string, client.ContainerRemoveOptions) (client.ContainerRemoveResult, error) {
	f.removed.Add(1)
	return client.ContainerRemoveResult{}, nil
}

type entry struct {
	name, link, body string
	typ              byte
	mode             int64
}

func makeTar(t *testing.T, entries []entry) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	for _, e := range entries {
		h := &tar.Header{Name: e.name, Typeflag: e.typ, Mode: e.mode, Linkname: e.link, Size: int64(len(e.body)), Uid: 1000, Gid: 1000}
		if e.typ == 0 {
			h.Typeflag = tar.TypeReg
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(e.body)); err != nil {
			t.Fatal(err)
		}
	}
	_ = tw.Close()
	return buf.Bytes()
}

func testImageTar(t *testing.T) []byte {
	return makeTar(t, []entry{
		{name: "bin/", typ: tar.TypeDir, mode: 0o755},
		{name: "bin/app", body: "#!/bin/sh\necho hi\n", mode: 0o755},
		{name: "bin/app2", typ: tar.TypeLink, link: "bin/app", mode: 0o755},
		{name: "bin/sh", typ: tar.TypeSymlink, link: "app", mode: 0o777},
		{name: "etc/", typ: tar.TypeDir, mode: 0o755},
		{name: "etc/secret", body: "x", mode: 0o600},
		{name: "ro/", typ: tar.TypeDir, mode: 0o555},
		{name: "ro/inside", body: "y", mode: 0o444},
		{name: "dev/null", typ: tar.TypeChar, mode: 0o666},
	})
}

func TestPrepareExportsOnce(t *testing.T) {
	cfg := &dockerspec.DockerOCIImageConfig{}
	cfg.Entrypoint = []string{"/bin/app"}
	cfg.Cmd = []string{"--serve"}
	cfg.Env = []string{"PATH=/bin", "GREETING=hi"}
	cfg.WorkingDir = "/srv"
	cfg.User = "1000:1000"
	d := &fakeDocker{id: "abc123", config: cfg, tarball: func() []byte { return testImageTar(t) }}
	dataDir := t.TempDir()
	e := newExporter(d, dataDir)

	im, err := e.Prepare(context.Background(), "ashore/hello:abc1234")
	if err != nil {
		t.Fatal(err)
	}
	if im.ID != "abc123" || im.Dir != filepath.Join(dataDir, "rootfs", "abc123") || im.Rootfs() != filepath.Join(im.Dir, "rootfs") {
		t.Fatalf("image %+v", im)
	}
	if got := strings.Join(im.Config.Argv(), " "); got != "/bin/app --serve" {
		t.Fatalf("argv %q", got)
	}
	if im.Config.WorkingDir != "/srv" || im.Config.User != "1000:1000" || len(im.Config.Env) != 2 {
		t.Fatalf("config %+v", im.Config)
	}

	// The files, with their modes and links.
	rootfs := im.Rootfs()
	if b, err := os.ReadFile(filepath.Join(rootfs, "bin", "app")); err != nil || !strings.HasPrefix(string(b), "#!/bin/sh") {
		t.Fatalf("bin/app: %v %q", err, b)
	}
	if info, _ := os.Stat(filepath.Join(rootfs, "bin", "app")); info.Mode().Perm() != 0o755 {
		t.Errorf("bin/app mode %o", info.Mode().Perm())
	}
	if info, _ := os.Stat(filepath.Join(rootfs, "etc", "secret")); info.Mode().Perm() != 0o600 {
		t.Errorf("etc/secret mode %o", info.Mode().Perm())
	}
	if link, err := os.Readlink(filepath.Join(rootfs, "bin", "sh")); err != nil || link != "app" {
		t.Errorf("symlink: %q %v", link, err)
	}
	a, _ := os.Stat(filepath.Join(rootfs, "bin", "app"))
	b, _ := os.Stat(filepath.Join(rootfs, "bin", "app2"))
	if !os.SameFile(a, b) {
		t.Error("hard link not made")
	}
	if b, err := os.ReadFile(filepath.Join(rootfs, "ro", "inside")); err != nil || string(b) != "y" {
		t.Errorf("file inside a read-only dir: %v", err)
	}
	if os.Geteuid() != 0 {
		if _, err := os.Lstat(filepath.Join(rootfs, "dev", "null")); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("device node should be skipped without root: %v", err)
		}
	}
	if _, err := os.Stat(filepath.Join(im.Dir, doneFile)); err != nil {
		t.Errorf("done marker: %v", err)
	}
	data, _ := os.ReadFile(filepath.Join(im.Dir, configFile))
	var stored ImageConfig
	if err := json.Unmarshal(data, &stored); err != nil || stored.WorkingDir != "/srv" {
		t.Errorf("config.json: %v %s", err, data)
	}

	// A second Prepare, and two concurrent ones, do not export again; the
	// export container is always removed.
	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.Prepare(context.Background(), "ashore/hello:abc1234"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := d.exports.Load(); n != 1 {
		t.Errorf("exported %d times, want 1", n)
	}
	if d.removed.Load() != 1 {
		t.Errorf("export container removed %d times", d.removed.Load())
	}

	// A fresh exporter (a restarted daemon) reads the stored config and
	// does not export either.
	again, err := newExporter(d, dataDir).Prepare(context.Background(), "ashore/hello:abc1234")
	if err != nil || again.Config.WorkingDir != "/srv" || d.exports.Load() != 1 {
		t.Errorf("after restart: %v %+v exports=%d", err, again.Config, d.exports.Load())
	}

	// Without the marker the export is redone.
	_ = os.Remove(filepath.Join(im.Dir, doneFile))
	if _, err := e.Prepare(context.Background(), "ashore/hello:abc1234"); err != nil || d.exports.Load() != 2 {
		t.Errorf("cut-off export not redone: %v exports=%d", err, d.exports.Load())
	}
}

func TestPrepareConfinesPaths(t *testing.T) {
	// Entries that try to climb out land inside the rootfs instead: the
	// name is rooted and cleaned before it is joined.
	d := &fakeDocker{id: "climb", tarball: func() []byte {
		return makeTar(t, []entry{{name: "../../outside", body: "x", mode: 0o644}})
	}}
	dataDir := t.TempDir()
	im, err := newExporter(d, dataDir).Prepare(context.Background(), "x")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(im.Rootfs(), "outside")); err != nil {
		t.Errorf("confined entry missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "outside")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("entry escaped the rootfs: %v", err)
	}

	// A hard link is confined the same way; one whose target is not in the
	// image fails the export, and the partial export is removed.
	d = &fakeDocker{id: "badlink", tarball: func() []byte {
		return makeTar(t, []entry{{name: "ok", typ: tar.TypeLink, link: "../../etc/passwd", mode: 0o644}})
	}}
	dataDir = t.TempDir()
	if _, err := newExporter(d, dataDir).Prepare(context.Background(), "x"); err == nil || !strings.Contains(err.Error(), "no such file") {
		t.Errorf("link outside the image: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "rootfs", "badlink")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("failed export left %v", err)
	}

	// A symlink pointing outside is fine on disk: it is only followed
	// inside the container's own root after pivot_root.
	d = &fakeDocker{id: "sym", tarball: func() []byte {
		return makeTar(t, []entry{{name: "etc/x", typ: tar.TypeSymlink, link: "/../../anything", mode: 0o777}})
	}}
	if _, err := newExporter(d, t.TempDir()).Prepare(context.Background(), "x"); err != nil {
		t.Errorf("symlink export: %v", err)
	}
	if _, err := secureJoin("/data/rootfs", "../../x"); err != nil {
		t.Errorf("secureJoin should confine, not refuse: %v", err)
	}
}

func TestPrepareInspectError(t *testing.T) {
	d := &fakeDocker{fail: errors.New("no such image")}
	if _, err := newExporter(d, t.TempDir()).Prepare(context.Background(), "nope"); err == nil || !strings.Contains(err.Error(), "no such image") {
		t.Fatalf("err = %v", err)
	}
}

// --- against a real Docker daemon ---

func dockerClient(t *testing.T) *client.Client {
	t.Helper()
	cli, err := client.New(client.FromEnv)
	if err != nil {
		t.Skipf("docker client: %v", err)
	}
	if _, err := cli.Ping(context.Background(), client.PingOptions{}); err != nil {
		_ = cli.Close()
		t.Skipf("docker daemon unavailable: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

const helloImage = "ashore/native-test-hello:latest"

// buildHelloGo builds examples/hello-go with the Docker daemon and returns
// the tag; the image is removed when the test ends.
func buildHelloGo(t *testing.T, cli *client.Client) string {
	t.Helper()
	dir := filepath.Join("..", "..", "..", "examples", "hello-go")
	var ctxTar bytes.Buffer
	tw := tar.NewWriter(&ctxTar)
	for _, name := range []string{"Dockerfile", "go.mod", "main.go"} {
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		_ = tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(data))})
		_, _ = tw.Write(data)
	}
	_ = tw.Close()
	res, err := cli.ImageBuild(context.Background(), &ctxTar, client.ImageBuildOptions{
		Tags: []string{helloImage}, Labels: map[string]string{"ashore.test": "1"},
		Remove: true, ForceRemove: true, Version: build.BuilderV1,
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer func() { _ = res.Body.Close() }()
	dec := json.NewDecoder(res.Body)
	for {
		var m jsonstream.Message
		if err := dec.Decode(&m); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatalf("build: %v", err)
		}
		if m.Error != nil {
			t.Fatalf("build: %s", m.Error.Message)
		}
	}
	t.Cleanup(func() {
		_, _ = cli.ImageRemove(context.Background(), helloImage, client.ImageRemoveOptions{Force: true, PruneChildren: true})
	})
	return helloImage
}

func TestPrepareHelloGoWithDocker(t *testing.T) {
	cli := dockerClient(t)
	tag := buildHelloGo(t, cli)
	im, err := newExporter(cli, t.TempDir()).Prepare(context.Background(), tag)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(im.Config.Argv(), " "); got != "/hello" {
		t.Errorf("argv %q", got)
	}
	info, err := os.Stat(filepath.Join(im.Rootfs(), "hello"))
	if err != nil || info.Mode().Perm()&0o111 == 0 || info.Size() < 1<<20 {
		t.Errorf("rootfs/hello: %v %v", info, err)
	}
	// A scratch image's export still carries the files Docker adds.
	for _, name := range []string{"etc/hostname", "etc/hosts", "etc/resolv.conf", "dev", "proc", "sys"} {
		if _, err := os.Lstat(filepath.Join(im.Rootfs(), name)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	// No export container is left behind.
	list, err := cli.ContainerList(context.Background(), client.ContainerListOptions{All: true, Filters: make(client.Filters).Add("label", "ashore.export")})
	if err != nil || len(list.Items) != 0 {
		t.Errorf("export containers left: %v %v", list.Items, err)
	}
}
