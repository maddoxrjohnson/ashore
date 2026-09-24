package gitserver

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/maddoxrjohnson/ashore/internal/store"
)

// testServer is a gitserver without the daemon around it, with one
// authorized client key.
type testServer struct {
	srv     *Server
	st      *store.Store
	dataDir string
	ln      net.Listener
	addr    string
	port    string
	signer  ssh.Signer // the authorized key
	keyFile string     // its private key on disk, for the ssh binary
}

func newTestServer(t *testing.T) (*testServer, context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	dataDir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dataDir, "ashore.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv, err := New(st, dataDir, filepath.Join(dataDir, "host_ed25519"))
	if err != nil {
		t.Fatal(err)
	}

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.AddKey(ctx, "laptop", ssh.FingerprintSHA256(sshPub), string(ssh.MarshalAuthorizedKey(sshPub))); err != nil {
		t.Fatal(err)
	}
	block, err := ssh.MarshalPrivateKey(priv, "")
	if err != nil {
		t.Fatal(err)
	}
	keyFile := filepath.Join(t.TempDir(), "id_ed25519")
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(block), 0o600); err != nil {
		t.Fatal(err)
	}
	return &testServer{srv: srv, st: st, dataDir: dataDir, signer: signer, keyFile: keyFile}, ctx, cancel
}

// startServer serves on a random loopback port until the test ends.
func startServer(t *testing.T) *testServer {
	t.Helper()
	ts, ctx, cancel := newTestServer(t)
	ts.listen(t)
	done := make(chan error, 1)
	go func() { done <- ts.srv.Serve(ctx, ts.ln) }()
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
	return ts
}

func (ts *testServer) listen(t *testing.T) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ts.ln = ln
	ts.addr = ln.Addr().String()
	_, ts.port, _ = net.SplitHostPort(ts.addr)
}

func (ts *testServer) dial(t *testing.T, signer ssh.Signer) *ssh.Client {
	t.Helper()
	client, err := ssh.Dial("tcp", ts.addr, ts.clientConfig(signer))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func (ts *testServer) clientConfig(signer ssh.Signer) *ssh.ClientConfig {
	return &ssh.ClientConfig{
		User:            "ashore",
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.FixedHostKey(ts.srv.hostKey),
		Timeout:         5 * time.Second,
	}
}

// createApp registers an app and creates its bare repository.
func (ts *testServer) createApp(t *testing.T, name string) string {
	t.Helper()
	ctx := context.Background()
	if _, err := ts.st.CreateApp(ctx, name, name+".localhost"); err != nil {
		t.Fatal(err)
	}
	repo, err := InitRepo(ctx, ts.dataDir, name)
	if err != nil {
		t.Fatal(err)
	}
	return repo
}

// clientEnv is the environment for the git and ssh binaries acting as the
// user: a scratch HOME, the authorized identity, and the server's host key
// pinned in known_hosts so a wrong host key would fail the test.
func (ts *testServer) clientEnv(t *testing.T) []string {
	t.Helper()
	home := t.TempDir()
	knownHosts := filepath.Join(home, "known_hosts")
	line := fmt.Sprintf("[127.0.0.1]:%s %s", ts.port, ssh.MarshalAuthorizedKey(ts.srv.hostKey))
	if err := os.WriteFile(knownHosts, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	sshCmd := strings.Join([]string{
		"ssh", "-F", "/dev/null", "-i", ts.keyFile,
		"-o", "IdentitiesOnly=yes", "-o", "UserKnownHostsFile=" + knownHosts,
		"-o", "StrictHostKeyChecking=yes", "-o", "BatchMode=yes",
	}, " ")
	return []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + home,
		"GIT_CONFIG_NOSYSTEM=1",
		"GIT_SSH_COMMAND=" + sshCmd,
		"GIT_AUTHOR_NAME=test", "GIT_AUTHOR_EMAIL=test@example.com",
		"GIT_COMMITTER_NAME=test", "GIT_COMMITTER_EMAIL=test@example.com",
	}
}

func (ts *testServer) url(app string) string {
	return fmt.Sprintf("ssh://ashore@127.0.0.1:%s/%s", ts.port, app)
}

type gitClient struct {
	t   *testing.T
	dir string
	env []string
}

func (g gitClient) run(args ...string) string {
	g.t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = g.dir
	cmd.Env = g.env
	out, err := cmd.CombinedOutput()
	if err != nil {
		g.t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

func serverRef(t *testing.T, repo, ref string) string {
	t.Helper()
	out, err := exec.Command("git", "--git-dir="+repo, "rev-parse", ref).Output()
	if err != nil {
		t.Fatalf("rev-parse %s: %v", ref, err)
	}
	return strings.TrimSpace(string(out))
}

func newKey(t *testing.T) ssh.Signer {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	return signer
}

func newSession(t *testing.T, client *ssh.Client) *ssh.Session {
	t.Helper()
	sess, err := client.NewSession()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	return sess
}

// runOver executes command in a new session and returns the exit status and
// what the server wrote to stderr.
func runOver(t *testing.T, client *ssh.Client, command string) (int, string) {
	t.Helper()
	sess := newSession(t, client)
	var stderr bytes.Buffer
	sess.Stderr = &stderr
	err := sess.Run(command)
	if err == nil {
		return 0, stderr.String()
	}
	var exitErr *ssh.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("run %q: %v", command, err)
	}
	return exitErr.ExitStatus(), stderr.String()
}

func requireBinaries(t *testing.T, names ...string) {
	t.Helper()
	for _, n := range names {
		if _, err := exec.LookPath(n); err != nil {
			t.Skipf("%s is not installed", n)
		}
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

// startReceivePack begins a push over the x/crypto client and returns once
// git's ref advertisement has arrived, which proves git is running.
func startReceivePack(t *testing.T, client *ssh.Client, app string) {
	t.Helper()
	sess := newSession(t, client)
	stdout, err := sess.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := sess.Start(fmt.Sprintf("git-receive-pack '%s'", app)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(stdout, make([]byte, 4)); err != nil {
		t.Fatalf("no ref advertisement: %v", err)
	}
}

func TestUnknownKeyRefused(t *testing.T) {
	ts := startServer(t)
	client, err := ssh.Dial("tcp", ts.addr, ts.clientConfig(newKey(t)))
	if err == nil {
		_ = client.Close()
		t.Fatal("a key that is not in the store was accepted")
	}
}

func TestRefusesEverythingButGit(t *testing.T) {
	ts := startServer(t)
	client := ts.dial(t, ts.signer)

	t.Run("shell", func(t *testing.T) {
		if err := newSession(t, client).Shell(); err == nil {
			t.Fatal("shell request accepted")
		}
	})
	t.Run("pty", func(t *testing.T) {
		if err := newSession(t, client).RequestPty("xterm", 24, 80, ssh.TerminalModes{}); err == nil {
			t.Fatal("pty request accepted")
		}
	})
	t.Run("subsystem", func(t *testing.T) {
		if err := newSession(t, client).RequestSubsystem("sftp"); err == nil {
			t.Fatal("subsystem request accepted")
		}
	})
	t.Run("local forward", func(t *testing.T) {
		if c, err := client.Dial("tcp", "127.0.0.1:1"); err == nil {
			_ = c.Close()
			t.Fatal("direct-tcpip channel accepted")
		}
	})
	t.Run("remote forward", func(t *testing.T) {
		if l, err := client.Listen("tcp", "127.0.0.1:0"); err == nil {
			_ = l.Close()
			t.Fatal("tcpip-forward request accepted")
		}
	})

	execs := []struct{ command, want string }{
		{"ls", "unsupported command"},
		{"git receive-pack 'hello'", "unsupported command"},
		{"git-receive-pack '../../etc'", "invalid app name"},
		{"git-receive-pack 'nope'", "not found"},
	}
	for _, tc := range execs {
		t.Run(tc.command, func(t *testing.T) {
			code, stderr := runOver(t, client, tc.command)
			if code != 1 || !strings.Contains(stderr, tc.want) {
				t.Fatalf("exec %q: exit %d, stderr %q; want exit 1 and %q", tc.command, code, stderr, tc.want)
			}
		})
	}
}

func TestPushWithGit(t *testing.T) {
	requireBinaries(t, "git", "ssh")
	ts := startServer(t)
	repo := ts.createApp(t, "hello")
	// A stand-in hook proves core.hooksPath points at the daemon's directory
	// and that hook output reaches the pusher.
	hooks := HooksPath(ts.dataDir)
	if err := os.MkdirAll(hooks, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hooks, "pre-receive"), []byte("#!/bin/sh\necho stand-in hook ran\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	env := ts.clientEnv(t)
	work := gitClient{t: t, dir: t.TempDir(), env: env}
	work.run("init", "--quiet", "--initial-branch=main")
	readme := filepath.Join(work.dir, "README")
	if err := os.WriteFile(readme, []byte("one\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	work.run("add", "README")
	work.run("commit", "--quiet", "-m", "one")

	out := work.run("push", ts.url("hello"), "main")
	if !strings.Contains(out, "remote: stand-in hook ran") {
		t.Fatalf("push output lacks the hook line:\n%s", out)
	}
	if got, want := serverRef(t, repo, "refs/heads/main"), work.run("rev-parse", "HEAD"); got != want {
		t.Fatalf("server main = %s, want %s", got, want)
	}

	if err := os.WriteFile(readme, []byte("two\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	work.run("commit", "--quiet", "-am", "two")
	work.run("push", ts.url("hello"), "main")
	if got, want := serverRef(t, repo, "refs/heads/main"), work.run("rev-parse", "HEAD"); got != want {
		t.Fatalf("server main after second push = %s, want %s", got, want)
	}

	// Cloning back exercises upload-pack.
	clone := filepath.Join(t.TempDir(), "clone")
	gitClient{t: t, dir: t.TempDir(), env: env}.run("clone", "--quiet", ts.url("hello"), clone)
	data, err := os.ReadFile(filepath.Join(clone, "README"))
	if err != nil || string(data) != "two\n" {
		t.Fatalf("clone README = %q, %v; want %q", data, err, "two\n")
	}
}

func TestConnectionDropStopsGit(t *testing.T) {
	requireBinaries(t, "git")
	ts := startServer(t)
	ts.createApp(t, "hello")
	client := ts.dial(t, ts.signer)
	startReceivePack(t, client, "hello")
	if n := ts.srv.active.Load(); n != 1 {
		t.Fatalf("active sessions = %d, want 1", n)
	}
	_ = client.Close() // the laptop lid closes mid-push
	waitFor(t, "git to exit after the connection dropped", func() bool { return ts.srv.active.Load() == 0 })
}

func TestServeStopsWithActiveSession(t *testing.T) {
	requireBinaries(t, "git")
	ts, ctx, cancel := newTestServer(t)
	defer cancel()
	ts.createApp(t, "hello")
	ts.listen(t)
	done := make(chan error, 1)
	go func() { done <- ts.srv.Serve(ctx, ts.ln) }()
	client := ts.dial(t, ts.signer)
	startReceivePack(t, client, "hello")

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return after cancel while a push was in progress")
	}
	if n := ts.srv.active.Load(); n != 0 {
		t.Fatalf("active sessions after Serve returned = %d", n)
	}
}
