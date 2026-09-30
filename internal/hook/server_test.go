package hook

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/gitserver"
)

// TestPushRunsHook installs this binary as the pre-receive hook (InstallScript
// writes os.Executable), so behave as one when git runs it that way.
func TestMain(m *testing.M) {
	if len(os.Args) > 1 && os.Args[1] == "hook" {
		os.Exit(Run(os.Args[2:], os.Stdin, os.Stdout, os.Stderr, os.Getenv))
	}
	os.Exit(m.Run())
}

type deployCall struct{ app, sha, quarantine string }

// fakeDeployer writes its lines and returns its error. In block mode it waits
// for the context instead and reports the context's error on done.
type fakeDeployer struct {
	mu    sync.Mutex
	lines []string
	err   error
	calls []deployCall

	block bool
	done  chan error
}

func (d *fakeDeployer) set(lines []string, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lines, d.err = lines, err
}

func (d *fakeDeployer) Deploy(ctx context.Context, app, sha, quarantine string, out io.Writer) error {
	d.mu.Lock()
	d.calls = append(d.calls, deployCall{app, sha, quarantine})
	lines, err := d.lines, d.err
	d.mu.Unlock()
	for _, l := range lines {
		_, _ = fmt.Fprintln(out, l)
	}
	if d.block {
		<-ctx.Done()
		d.done <- ctx.Err()
		return ctx.Err()
	}
	return err
}

func (d *fakeDeployer) callList() []deployCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]deployCall(nil), d.calls...)
}

// startServer serves a hook server on a temp data dir until the test ends.
func startServer(t *testing.T, d *fakeDeployer) *Server {
	t.Helper()
	dataDir := t.TempDir() // under /tmp: short enough for sun_path
	s, err := NewServer(dataDir, d)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := s.Listen()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, ln) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Serve: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Serve did not return after cancel")
		}
	})
	return s
}

// quarantineFor is a path git could report for app under the server's data dir.
func quarantineFor(s *Server, app string) string {
	return gitserver.RepoPath(s.dataDir, app) + "/./objects/tmp_objdir-incoming-abc123"
}

// runHook drives the real hook code against the server.
func runHook(t *testing.T, s *Server, app, nonce, quarantine string) (code int, stdout, stderr string) {
	t.Helper()
	env := map[string]string{
		"ASHORE_SOCK": s.Socket(), "ASHORE_NONCE": nonce, "ASHORE_APP": app,
		"GIT_QUARANTINE_PATH": quarantine,
	}
	var out, errb strings.Builder
	stdin := strings.NewReader(oldSHA + " " + newSHA + " refs/heads/main\n")
	code = Run([]string{"pre-receive"}, stdin, &out, &errb, func(k string) string { return env[k] })
	return code, out.String(), errb.String()
}

// rawCall sends body as-is and returns every line the server answered.
func rawCall(t *testing.T, sock, body string) []string {
	t.Helper()
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	if _, err := io.WriteString(c, body); err != nil {
		t.Fatal(err)
	}
	_ = c.(*net.UnixConn).CloseWrite() // done sending; the server sees EOF
	all, err := io.ReadAll(c)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(all), "\n"), "\n")
}

func TestListen(t *testing.T) {
	dataDir := t.TempDir()
	s, err := NewServer(dataDir, &fakeDeployer{})
	if err != nil {
		t.Fatal(err)
	}
	// A crashed daemon leaves its socket file behind; nobody answers on it.
	if err := os.WriteFile(s.Socket(), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	ln, err := s.Listen()
	if err != nil {
		t.Fatalf("Listen over a stale file: %v", err)
	}
	fi, err := os.Stat(s.Socket())
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Type() != os.ModeSocket || fi.Mode().Perm() != 0o600 {
		t.Errorf("socket mode = %v, want a socket with 0600", fi.Mode())
	}
	// While it is live a second daemon must not take it over.
	if _, err := s.Listen(); err == nil || !strings.Contains(err.Error(), "another daemon") {
		t.Errorf("second Listen: err = %v, want another daemon is listening", err)
	}
	_ = ln.Close()
	if _, err := os.Stat(s.Socket()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("socket file after Close: %v, want gone", err)
	}
}

func TestIssueAndRedeem(t *testing.T) {
	s, err := NewServer(t.TempDir(), &fakeDeployer{})
	if err != nil {
		t.Fatal(err)
	}
	n1, revoke1 := s.Issue("hello")
	n2, _ := s.Issue("hello")
	n3, revoke3 := s.Issue("hello")
	if n1 == n2 || len(n1) < 20 {
		t.Fatalf("nonces %q and %q: want distinct and long", n1, n2)
	}
	if !s.redeem(n1, "hello") {
		t.Error("fresh nonce refused")
	}
	if s.redeem(n1, "hello") {
		t.Error("nonce redeemed twice")
	}
	if s.redeem(n2, "other") {
		t.Error("nonce accepted for another app")
	}
	if s.redeem(n2, "hello") {
		t.Error("nonce still valid after a wrong app used it")
	}
	revoke1() // revoking a used nonce is harmless
	revoke3()
	if s.redeem(n3, "hello") {
		t.Error("revoked nonce accepted")
	}
	if len(s.nonces) != 0 {
		t.Errorf("%d nonces left in the table, want 0", len(s.nonces))
	}
}

func TestServeDeploys(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		code    int
		wantOut string
	}{
		{"accepted", nil, 0, "-----> building\n-----> live\n"},
		{"rejected", errors.New("build failed"), 1, "-----> building\n-----> live\n-----> deploy failed: build failed\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := &fakeDeployer{lines: []string{"-----> building", "-----> live"}, err: tc.err}
			s := startServer(t, d)
			nonce, _ := s.Issue("hello")
			code, out, stderr := runHook(t, s, "hello", nonce, quarantineFor(s, "hello"))
			if code != tc.code || out != tc.wantOut || stderr != "" {
				t.Fatalf("exit %d, stdout %q, stderr %q; want %d, %q, empty", code, out, stderr, tc.code, tc.wantOut)
			}
			want := deployCall{"hello", newSHA, filepath.Join(gitserver.RepoPath(s.dataDir, "hello"), "objects", "tmp_objdir-incoming-abc123")}
			if calls := d.callList(); len(calls) != 1 || calls[0] != want {
				t.Fatalf("deploy calls = %+v, want [%+v]", calls, want)
			}
		})
	}
}

func TestServeRefuses(t *testing.T) {
	d := &fakeDeployer{lines: []string{"never"}}
	s := startServer(t, d)
	good, _ := s.Issue("hello")
	used, _ := s.Issue("hello")
	if code, _, _ := runHook(t, s, "hello", used, quarantineFor(s, "hello")); code != 0 {
		t.Fatalf("setup push exit %d", code)
	}
	forOther, _ := s.Issue("other")
	badPaths := map[string]string{
		"outside repo":   s.dataDir + "/repos/other.git/objects/tmp_objdir-incoming-x",
		"relative":       "repos/hello.git/objects/tmp_objdir-incoming-x",
		"dot dot":        gitserver.RepoPath(s.dataDir, "hello") + "/objects/../../../etc",
		"objects itself": gitserver.RepoPath(s.dataDir, "hello") + "/objects",
		"empty":          "",
	}

	cases := []struct {
		name, app, nonce, quarantine, want string
	}{
		{"unknown nonce", "hello", "nope", quarantineFor(s, "hello"), "nonce"},
		{"used nonce", "hello", used, quarantineFor(s, "hello"), "nonce"},
		{"other app's nonce", "hello", forOther, quarantineFor(s, "hello"), "nonce"},
	}
	for name, q := range badPaths {
		n, _ := s.Issue("hello")
		cases = append(cases, struct{ name, app, nonce, quarantine, want string }{name, "hello", n, q, "quarantine"})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out, _ := runHook(t, s, tc.app, tc.nonce, tc.quarantine)
			if code != 1 || !strings.HasPrefix(out, "-----> refused: ") || !strings.Contains(out, tc.want) {
				t.Fatalf("exit %d, stdout %q; want 1 and a refusal mentioning %q", code, out, tc.want)
			}
		})
	}

	raw := []struct{ name, body, want string }{
		{"not json", "hello\n", "bad request"},
		{"bad sha", fmt.Sprintf(`{"app":"hello","sha":"HEAD","quarantine":%q,"nonce":%q}`+"\n", quarantineFor(s, "hello"), good), "object name"},
		{"too long", `{"app":"` + strings.Repeat("a", maxRequest) + "\"}\n", "longer than"},
	}
	for _, tc := range raw {
		t.Run(tc.name, func(t *testing.T) {
			lines := rawCall(t, s.Socket(), tc.body)
			if len(lines) != 2 || !strings.HasPrefix(lines[0], "1-----> refused: ") || !strings.Contains(lines[0], tc.want) || lines[1] != "21" {
				t.Fatalf("response %q; want a refusal mentioning %q then status 1", lines, tc.want)
			}
		})
	}
	t.Run("silent client", func(t *testing.T) {
		c, err := net.Dial("unix", s.Socket())
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = c.Close() }()
		// Say nothing: the server must hang up on its own within requestTimeout.
		_ = c.SetReadDeadline(time.Now().Add(requestTimeout + 5*time.Second))
		if _, err := io.ReadAll(c); err != nil {
			t.Fatalf("server did not close a silent connection: %v", err)
		}
	})
	if calls := d.callList(); len(calls) != 1 {
		t.Fatalf("refused requests reached Deploy: %+v", calls)
	}
}

func TestHookGoneCancelsDeploy(t *testing.T) {
	d := &fakeDeployer{block: true, done: make(chan error, 1)}
	s := startServer(t, d)
	nonce, _ := s.Issue("hello")
	c, err := net.Dial("unix", s.Socket())
	if err != nil {
		t.Fatal(err)
	}
	req := fmt.Sprintf(`{"app":"hello","sha":%q,"quarantine":%q,"nonce":%q}`+"\n", newSHA, quarantineFor(s, "hello"), nonce)
	if _, err := io.WriteString(c, req); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "Deploy to start", func() bool { return len(d.callList()) == 1 })
	_ = c.Close() // git killed the hook, or the pusher's laptop died
	select {
	case err := <-d.done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("deploy ended with %v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("deploy kept running after the hook went away")
	}
}

func TestServeStopsWithActiveDeploy(t *testing.T) {
	d := &fakeDeployer{block: true, done: make(chan error, 1)}
	s, err := NewServer(t.TempDir(), d)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := s.Listen()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- s.Serve(ctx, ln) }()
	nonce, _ := s.Issue("hello")
	result := make(chan int, 1)
	go func() {
		code, _, _ := runHook(t, s, "hello", nonce, quarantineFor(s, "hello"))
		result <- code
	}()
	waitFor(t, "Deploy to start", func() bool { return len(d.callList()) == 1 })

	cancel() // daemon shutdown mid-deploy
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after cancel during a deploy")
	}
	if code := <-result; code != 1 {
		t.Errorf("hook exit after shutdown = %d, want 1 (push rejected)", code)
	}
	if _, err := os.Stat(s.Socket()); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("socket file after Serve returned: %v, want gone", err)
	}
}

func TestLineWriter(t *testing.T) {
	var sb strings.Builder
	lw := &lineWriter{w: &sb}
	for _, chunk := range []string{"one\ntw", "o\r\n", "three", "", "\nfour"} {
		if _, err := lw.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if err := lw.Flush(); err != nil {
		t.Fatal(err)
	}
	if got, want := sb.String(), "1one\n1two\n1three\n1four\n"; got != want {
		t.Errorf("framed = %q, want %q", got, want)
	}

	// Output that never ends a line is split so the hook's scanner is not overrun.
	sb.Reset()
	lw = &lineWriter{w: &sb}
	if _, err := lw.Write([]byte(strings.Repeat("x", maxLine+1))); err != nil {
		t.Fatal(err)
	}
	if err := lw.Flush(); err != nil {
		t.Fatal(err)
	}
	if got, want := sb.String(), "1"+strings.Repeat("x", maxLine)+"\n1x\n"; got != want {
		t.Errorf("long write framed as %d bytes, want %d", len(got), len(want))
	}
}

// TestPushRunsHook is the whole chain without SSH: git push into a bare repo
// whose core.hooksPath holds the installed script, which execs this test
// binary in hook mode, which calls the server over the socket, whose
// deployer's lines come back as remote: lines, and whose verdict decides
// whether the ref moves.
func TestPushRunsHook(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	d := &fakeDeployer{lines: []string{"-----> building hello", "-----> live"}}
	s := startServer(t, d)
	if err := InstallScript(s.dataDir); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	repo, err := gitserver.InitRepo(ctx, s.dataDir, "hello")
	if err != nil {
		t.Fatal(err)
	}

	work := t.TempDir()
	git := func(env []string, args ...string) (string, error) {
		cmd := exec.Command("git", args...)
		cmd.Dir = work
		cmd.Env = append([]string{
			"PATH=" + os.Getenv("PATH"), "HOME=" + work, "GIT_CONFIG_NOSYSTEM=1",
			"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com",
			"GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com",
		}, env...)
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	must := func(out string, err error) string {
		t.Helper()
		if err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
		return strings.TrimSpace(out)
	}
	must(git(nil, "init", "--quiet", "--initial-branch=main"))
	if err := os.WriteFile(filepath.Join(work, "Dockerfile"), []byte("FROM scratch\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	must(git(nil, "add", "Dockerfile"))
	must(git(nil, "commit", "--quiet", "-m", "one"))
	head := must(git(nil, "rev-parse", "HEAD"))
	// What the SSH server puts in receive-pack's environment.
	pushEnv := func(nonce string) []string {
		return []string{"ASHORE_APP=hello", "ASHORE_SOCK=" + s.Socket(), "ASHORE_NONCE=" + nonce}
	}

	nonce, _ := s.Issue("hello")
	out := must(git(pushEnv(nonce), "push", repo, "main"))
	for _, want := range []string{"remote: -----> building hello", "remote: -----> live"} {
		if !strings.Contains(out, want) {
			t.Errorf("push output lacks %q:\n%s", want, out)
		}
	}
	if got := must(git(nil, "--git-dir="+repo, "rev-parse", "refs/heads/main")); got != head {
		t.Errorf("server main = %s, want %s", got, head)
	}
	calls := d.callList()
	if len(calls) != 1 || calls[0].app != "hello" || calls[0].sha != head {
		t.Fatalf("deploy calls = %+v, want one for hello at %s", calls, head)
	}
	if q := calls[0].quarantine; !strings.HasPrefix(q, filepath.Join(repo, "objects", "tmp_objdir-")) {
		t.Errorf("quarantine = %q, want under %s/objects/tmp_objdir-*", q, repo)
	}
	if _, err := os.Stat(calls[0].quarantine); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("quarantine dir after the push: %v, want migrated away", err)
	}

	// A rejected deploy rejects the push and leaves the ref alone.
	d.set([]string{"-----> building hello"}, errors.New("no Dockerfile"))
	if err := os.WriteFile(filepath.Join(work, "Dockerfile"), []byte("FROM scratch\nCMD [\"/x\"]\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	must(git(nil, "commit", "--quiet", "-am", "two"))
	nonce, _ = s.Issue("hello")
	out, err = git(pushEnv(nonce), "push", repo, "main")
	if err == nil {
		t.Fatalf("push succeeded although the deploy failed:\n%s", out)
	}
	for _, want := range []string{"remote: -----> deploy failed: no Dockerfile", "[remote rejected]", "pre-receive hook declined"} {
		if !strings.Contains(out, want) {
			t.Errorf("rejected push output lacks %q:\n%s", want, out)
		}
	}
	if got := must(git(nil, "--git-dir="+repo, "rev-parse", "refs/heads/main")); got != head {
		t.Errorf("server main after rejected push = %s, want still %s", got, head)
	}

	// Without the daemon's variables the hook refuses before connecting.
	out, err = git(nil, "push", repo, "main")
	if err == nil || !strings.Contains(out, "remote: ashore: not started by the daemon") {
		t.Errorf("push without daemon env: err %v, output:\n%s", err, out)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
