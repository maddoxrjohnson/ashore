package builder

import (
	"archive/tar"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/moby/moby/api/types/build"
	"github.com/moby/moby/client"
)

const zeroSHA = "0000000000000000000000000000000000000000"

// fakeDocker records what the builder sends and answers with a canned
// message stream, an error, or a body that hangs until the context ends.
type fakeDocker struct {
	stream string
	err    error
	block  bool

	mu      sync.Mutex
	called  bool
	options client.ImageBuildOptions
	entries []string // names in the build context tar
}

func (f *fakeDocker) ImageBuild(ctx context.Context, bc io.Reader, o client.ImageBuildOptions) (client.ImageBuildResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.called, f.options = true, o
	tr := tar.NewReader(bc)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return client.ImageBuildResult{}, err
		}
		f.entries = append(f.entries, h.Name)
	}
	if f.err != nil {
		return client.ImageBuildResult{}, f.err
	}
	if f.block {
		return client.ImageBuildResult{Body: ctxBody{ctx}}, nil
	}
	return client.ImageBuildResult{Body: io.NopCloser(strings.NewReader(f.stream))}, nil
}

// ctxBody is a response body that never delivers a byte: the daemon is
// building forever.
type ctxBody struct{ ctx context.Context }

func (b ctxBody) Read([]byte) (int, error) { <-b.ctx.Done(); return 0, b.ctx.Err() }
func (ctxBody) Close() error               { return nil }

// newRepo commits files into a fresh repository and returns its git dir and
// the commit. A working repository's .git serves as "the bare repo" here;
// git archive does not care.
func newRepo(t *testing.T, files map[string]string) (repo, sha string) {
	t.Helper()
	requireGit(t)
	dir := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + dir, "GIT_CONFIG_NOSYSTEM=1",
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com"}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "--quiet", "--initial-branch=main")
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	git("add", ".")
	git("commit", "--quiet", "-m", "one")
	return filepath.Join(dir, ".git"), git("rev-parse", "HEAD")
}

func requireGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
}

// exampleFiles reads examples/hello-go from the repository.
func exampleFiles(t *testing.T) map[string]string {
	t.Helper()
	dir := filepath.Join("..", "..", "examples", "hello-go")
	files := map[string]string{}
	for _, name := range []string{"Dockerfile", "go.mod", "main.go"} {
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		files[name] = string(b)
	}
	return files
}

func readLog(t *testing.T, dataDir, app, sha string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dataDir, "builds", app, sha+".log"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestTag(t *testing.T) {
	if got := Tag("hello", "8196b7e34b2c977d22b640a2ee673db74aefbd10"); got != "ashore/hello:8196b7e" {
		t.Errorf("Tag = %q", got)
	}
	if got := Tag("hello", "abc"); got != "ashore/hello:abc" {
		t.Errorf("Tag with a short sha = %q", got)
	}
}

func TestBuildRelaysStream(t *testing.T) {
	repo, sha := newRepo(t, map[string]string{"Dockerfile": "FROM scratch\n", "main.go": "package main\n"})
	stream := `{"stream":"Step 1/1 : FROM scratch\n"}` +
		`{"status":"Pulling from library/x","id":"latest"}` +
		`{"status":"Downloading","id":"abc","progressDetail":{"current":1,"total":2}}` +
		`{"status":"Pull complete","id":"abc"}` +
		`{"stream":" ---> 123\n"}{"aux":{"ID":"sha256:123"}}` +
		`{"stream":"Successfully tagged ashore/hello:` + sha[:7] + `\n"}`
	d := &fakeDocker{stream: stream}
	dataDir := t.TempDir()
	b := New(d, dataDir, time.Minute)
	b.Labels = map[string]string{"ashore.test": "1"}

	var out strings.Builder
	tag, err := b.Build(context.Background(), Job{App: "hello", Repo: repo, SHA: sha}, &out)
	if err != nil {
		t.Fatal(err)
	}
	if tag != "ashore/hello:"+sha[:7] {
		t.Errorf("tag = %q", tag)
	}
	want := "Step 1/1 : FROM scratch\nlatest: Pulling from library/x\nabc: Pull complete\n ---> 123\nSuccessfully tagged ashore/hello:" + sha[:7] + "\n"
	if out.String() != want {
		t.Errorf("out = %q\nwant %q", out.String(), want)
	}
	if log := readLog(t, dataDir, "hello", sha); log != want {
		t.Errorf("log file = %q\nwant %q", log, want)
	}

	o := d.options
	if len(o.Tags) != 1 || o.Tags[0] != tag || o.Dockerfile != "Dockerfile" || !o.Remove || !o.ForceRemove || o.Version != build.BuilderV1 {
		t.Errorf("options = %+v", o)
	}
	for k, v := range map[string]string{"ashore.app": "hello", "ashore.sha": sha, "ashore.test": "1"} {
		if o.Labels[k] != v {
			t.Errorf("label %s = %q, want %q", k, o.Labels[k], v)
		}
	}
	got := strings.Join(d.entries, " ")
	for _, name := range []string{"Dockerfile", "main.go"} {
		if !strings.Contains(got, name) {
			t.Errorf("build context %q lacks %s", got, name)
		}
	}
	if _, err := filepath.Glob(filepath.Join(os.TempDir(), "ashore-context-*")); err != nil {
		t.Fatal(err)
	}
}

func TestBuildFailures(t *testing.T) {
	repo, sha := newRepo(t, map[string]string{"Dockerfile": "FROM scratch\nRUN false\n"})
	daemonError := `{"stream":"Step 2/2 : RUN false\n"}` +
		`{"errorDetail":{"code":1,"message":"The command '/bin/sh -c false' returned a non-zero code: 1"},"error":"The command '/bin/sh -c false' returned a non-zero code: 1"}`
	cases := []struct {
		name   string
		docker *fakeDocker
		job    Job
		want   string // substring of the error
		called bool   // whether ImageBuild ran
	}{
		{"step fails", &fakeDocker{stream: daemonError}, Job{App: "hello", Repo: repo, SHA: sha}, "build failed: The command '/bin/sh -c false'", true},
		{"docker unreachable", &fakeDocker{err: errors.New("dial unix /var/run/docker.sock: connect: no such file")}, Job{App: "hello", Repo: repo, SHA: sha}, "docker build: dial unix", true},
		{"unknown commit", &fakeDocker{}, Job{App: "hello", Repo: repo, SHA: zeroSHA}, "git archive", false},
		{"timeout", &fakeDocker{block: true}, Job{App: "hello", Repo: repo, SHA: sha}, "build timed out after 50ms", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			b := New(tc.docker, dataDir, 50*time.Millisecond)
			var out strings.Builder
			tag, err := b.Build(context.Background(), tc.job, &out)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want one containing %q", err, tc.want)
			}
			if tag != "" {
				t.Errorf("tag = %q on failure, want empty", tag)
			}
			if tc.docker.called != tc.called {
				t.Errorf("ImageBuild called = %v, want %v", tc.docker.called, tc.called)
			}
			log := readLog(t, dataDir, tc.job.App, tc.job.SHA)
			if !strings.HasPrefix(log, out.String()) || !strings.HasSuffix(log, "error: "+err.Error()+"\n") {
				t.Errorf("log = %q; want the output %q then the error", log, out.String())
			}
		})
	}
}

// TestBuildReadsQuarantine is D23 without Docker: the commit's objects live
// in one repository, the build reads them through another that holds none,
// exactly as receive-pack's quarantine works while pre-receive runs.
func TestBuildReadsQuarantine(t *testing.T) {
	src, sha := newRepo(t, map[string]string{"Dockerfile": "FROM scratch\n"})
	empty := filepath.Join(t.TempDir(), "hello.git")
	if out, err := exec.Command("git", "init", "--quiet", "--bare", empty).CombinedOutput(); err != nil {
		t.Fatalf("git init: %v\n%s", err, out)
	}
	quarantine := filepath.Join(src, "objects")

	d := &fakeDocker{stream: `{"stream":"ok\n"}`}
	b := New(d, t.TempDir(), time.Minute)
	if _, err := b.Build(context.Background(), Job{App: "hello", Repo: empty, SHA: sha}, io.Discard); err == nil || !strings.Contains(err.Error(), "git archive") {
		t.Fatalf("build without the quarantine: err = %v, want git archive to fail", err)
	}
	if _, err := b.Build(context.Background(), Job{App: "hello", Repo: empty, SHA: sha, Quarantine: quarantine}, io.Discard); err != nil {
		t.Fatalf("build with the quarantine: %v", err)
	}
	if strings.Join(d.entries, " ") == "" || !strings.Contains(strings.Join(d.entries, " "), "Dockerfile") {
		t.Errorf("build context entries = %q, want the Dockerfile", d.entries)
	}
}

// dockerClient connects to the local daemon or skips the test.
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

// danglingImages lists untagged image ids. A multi-stage build leaves its
// earlier stages untagged and unlabeled, so the test snapshots the set
// before building and removes what it added.
func danglingImages(t *testing.T, cli *client.Client) map[string]bool {
	t.Helper()
	res, err := cli.ImageList(context.Background(), client.ImageListOptions{Filters: client.Filters{}.Add("dangling", "true")})
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, img := range res.Items {
		ids[img.ID] = true
	}
	return ids
}

func TestBuildHelloGoWithDocker(t *testing.T) {
	cli := dockerClient(t)
	ctx := context.Background()
	dataDir := t.TempDir()
	before := danglingImages(t, cli)
	t.Cleanup(func() {
		for id := range danglingImages(t, cli) {
			if !before[id] {
				_, _ = cli.ImageRemove(ctx, id, client.ImageRemoveOptions{Force: true, PruneChildren: true})
			}
		}
	})
	b := New(cli, dataDir, 10*time.Minute)
	b.Labels = map[string]string{"ashore.test": "1"}

	repo, sha := newRepo(t, exampleFiles(t))
	var out strings.Builder
	tag, err := b.Build(ctx, Job{App: "hello", Repo: repo, SHA: sha}, &out)
	if err != nil {
		t.Fatalf("build: %v\n%s", err, out.String())
	}
	t.Logf("what the pusher would see:\n%s", out.String())
	t.Cleanup(func() {
		_, _ = cli.ImageRemove(ctx, tag, client.ImageRemoveOptions{Force: true, PruneChildren: true})
	})
	for _, want := range []string{"Step 1/", "FROM golang:1.27-alpine", "Successfully tagged " + tag} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("build output lacks %q:\n%s", want, out.String())
		}
	}
	if !strings.Contains(readLog(t, dataDir, "hello", sha), "Successfully tagged "+tag) {
		t.Error("log file lacks the final line")
	}
	img, err := cli.ImageInspect(ctx, tag)
	if err != nil {
		t.Fatalf("image %s not found after the build: %v", tag, err)
	}
	if img.Config == nil || img.Config.Labels["ashore.test"] != "1" || img.Config.Labels["ashore.sha"] != sha {
		t.Errorf("image labels = %v", img.Config)
	}

	t.Run("broken Dockerfile", func(t *testing.T) {
		repo, sha := newRepo(t, map[string]string{"Dockerfile": "FROM scratch\nCOPY missing /\n"})
		var out strings.Builder
		_, err := b.Build(ctx, Job{App: "hello", Repo: repo, SHA: sha}, &out)
		if err == nil || !strings.Contains(err.Error(), "build failed:") {
			t.Fatalf("err = %v, want build failed:\n%s", err, out.String())
		}
		if _, err := cli.ImageInspect(ctx, Tag("hello", sha)); err == nil {
			t.Error("a failed build left a tagged image")
		}
	})
}
