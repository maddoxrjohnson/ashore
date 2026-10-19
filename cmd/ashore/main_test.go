package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/maddoxrjohnson/ashore/internal/api"
	"github.com/maddoxrjohnson/ashore/internal/logs"
	"github.com/maddoxrjohnson/ashore/internal/store"
	"github.com/maddoxrjohnson/ashore/internal/supervisor"
)

const token = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// fakeSup answers the API's long operations with a line each.
type fakeSup struct{ calls []string }

func (f *fakeSup) Redeploy(_ context.Context, app, note string, out io.Writer) error {
	f.calls = append(f.calls, "redeploy "+app)
	_, _ = fmt.Fprintf(out, "-----> release v2 (%s)\n-----> live at http://%s.localhost:8080\n", note, app)
	return nil
}

func (f *fakeSup) Rollback(_ context.Context, app string, version int, out io.Writer) error {
	f.calls = append(f.calls, fmt.Sprintf("rollback %s v%d", app, version))
	_, _ = fmt.Fprintf(out, "-----> release v3 (rollback v%d)\n", version)
	return nil
}

func (f *fakeSup) Restart(_ context.Context, app string, out io.Writer) error {
	f.calls = append(f.calls, "restart "+app)
	_, _ = fmt.Fprintln(out, "-----> restarting v1")
	return errors.New("v1 did not accept connections on port 8080 within 30s")
}

func (f *fakeSup) Destroy(_ context.Context, app string) error {
	f.calls = append(f.calls, "destroy "+app)
	return nil
}

func (f *fakeSup) Status(app string) (supervisor.Status, bool) {
	if app != "hello" {
		return supervisor.Status{}, false
	}
	return supervisor.Status{App: app, Release: "v1", Instance: "abcdef123456789", Addr: "127.0.0.1:41000",
		State: supervisor.StateRestarting, Restarts: 2, Detail: "exited with code 1; restart 3 in 4s"}, true
}

type harness struct {
	t      *testing.T
	server *httptest.Server
	hub    *logs.Hub
	env    map[string]string
}

// newHarness serves the real API over a real store with a fake supervisor,
// and points the CLI's config file at a temp dir.
func newHarness(t *testing.T) *harness {
	t.Helper()
	if _, err := os.Stat("/usr/bin/git"); err != nil {
		t.Skip("git not installed")
	}
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	hub := logs.New(t.TempDir(), nil, nil)
	t.Cleanup(func() { _ = hub.Close() })
	srv := httptest.NewServer(api.New(st, &fakeSup{}, hub, token,
		api.Config{DataDir: t.TempDir(), Domain: "localhost", SSHAddr: "127.0.0.1:2222"},
		slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(srv.Close)
	return &harness{t: t, server: srv, hub: hub, env: map[string]string{"ASHORE_CONFIG": filepath.Join(t.TempDir(), "config")}}
}

// run runs one command and returns stdout, stderr, and the exit status.
func (h *harness) run(args ...string) (string, string, int) {
	h.t.Helper()
	var out, errOut bytes.Buffer
	code := run(args, &out, &errOut, func(k string) string { return h.env[k] })
	return out.String(), errOut.String(), code
}

// ok runs a command that must succeed and returns its stdout.
func (h *harness) ok(args ...string) string {
	h.t.Helper()
	out, errOut, code := h.run(args...)
	if code != 0 {
		h.t.Fatalf("ashore %s: exit %d\n%s%s", strings.Join(args, " "), code, out, errOut)
	}
	return out
}

func TestCLI(t *testing.T) {
	h := newHarness(t)

	// Before login, nothing works, and a bad token is refused at login.
	if _, errOut, code := h.run("apps"); code != 1 || !strings.Contains(errOut, "not logged in") {
		t.Errorf("apps before login: %d %q", code, errOut)
	}
	if _, errOut, code := h.run("login", h.server.URL, "wrong"); code != 1 || !strings.Contains(errOut, "login failed") {
		t.Errorf("login with a wrong token: %d %q", code, errOut)
	}
	out := h.ok("login", h.server.URL+"/", token)
	if !strings.Contains(out, "logged in to "+h.server.URL+";") {
		t.Errorf("login output %q", out)
	}
	if info, err := os.Stat(h.env["ASHORE_CONFIG"]); err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("config file: %v %v", info, err)
	}

	// Apps.
	if out := h.ok("apps"); !strings.Contains(out, "NAME") || strings.Contains(out, "hello") {
		t.Errorf("apps on an empty daemon:\n%s", out)
	}
	out = h.ok("apps:create", "hello")
	if !strings.Contains(out, "created hello\n  git remote add ashore ssh://ashore@127.0.0.1:2222/hello\n  git push ashore main\n") {
		t.Errorf("apps:create:\n%s", out)
	}
	if _, errOut, code := h.run("apps:create", "hello"); code != 1 || !strings.Contains(errOut, "already exists") {
		t.Errorf("duplicate create: %d %q", code, errOut)
	}
	if out := h.ok("apps"); !strings.Contains(out, "hello  hello.localhost  -") {
		t.Errorf("apps:\n%s", out)
	}
	if out := h.ok("apps", "--json"); !strings.Contains(out, `"name": "hello"`) {
		t.Errorf("apps --json:\n%s", out)
	}

	// Config, before and after something is live.
	if out := h.ok("config:get", "hello"); out != "" {
		t.Errorf("empty config printed %q", out)
	}
	out = h.ok("config:set", "hello", "GREETING=hi there", "EMPTY=")
	if !strings.Contains(out, "nothing is running yet") {
		t.Errorf("config:set before a release:\n%s", out)
	}
	if out := h.ok("config:get", "hello"); out != "EMPTY=\nGREETING=hi there\n" {
		t.Errorf("config:get = %q", out)
	}
	if out := h.ok("config:get", "hello", "--json"); !strings.Contains(out, `"GREETING": "hi there"`) {
		t.Errorf("config:get --json = %q", out)
	}
	if _, errOut, code := h.run("config:set", "hello", "NOEQUALS"); code != 1 || !strings.Contains(errOut, "not KEY=value") {
		t.Errorf("bad pair: %d %q", code, errOut)
	}
	if _, errOut, code := h.run("config:set", "hello", "PORT=1"); code != 1 || !strings.Contains(errOut, "reserved") {
		t.Errorf("PORT: %d %q", code, errOut)
	}

	// Releases need rows; make v1 live through the store the daemon shares.
	st, err := store.Open(context.Background(), dbPath(t, h))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	app, _ := st.GetApp(context.Background(), "hello")
	r1, _ := st.CreateRelease(context.Background(), app.ID, store.NewRelease{Image: "ashore/hello:abc1234", GitSHA: strings.Repeat("a", 40), ConfigJSON: `{"GREETING":"hi"}`, Note: "push aaaaaaa"})
	_ = st.SetLive(context.Background(), app.ID, r1.ID)

	out = h.ok("config:unset", "hello", "EMPTY")
	if !strings.Contains(out, "-----> release v2 (config:unset EMPTY)\n") || !strings.Contains(out, "live at") {
		t.Errorf("config:unset stream:\n%s", out)
	}
	// Column widths follow the longest cell, so tables are compared with
	// the padding collapsed.
	flat := func(s string) string { return strings.Join(strings.Fields(s), " ") }
	out = h.ok("releases", "hello")
	if !strings.Contains(out, "VERSION") || !strings.Contains(flat(out), "v1 live") || !strings.Contains(out, "push aaaaaaa") {
		t.Errorf("releases:\n%s", out)
	}
	out = h.ok("releases:info", "hello", "v1")
	info := flat(out)
	for _, want := range []string{"release: v1", "app: hello", "status: live", "image: ashore/hello:abc1234", "config: GREETING=hi", "manifest: {}"} {
		if !strings.Contains(info, want) {
			t.Errorf("releases:info lacks %q:\n%s", want, out)
		}
	}
	if _, errOut, code := h.run("releases:info", "hello", "v9"); code != 1 || !strings.Contains(errOut, "not found") {
		t.Errorf("info v9: %d %q", code, errOut)
	}
	if _, errOut, code := h.run("releases:info", "hello", "1"); code != 1 || !strings.Contains(errOut, "not a release version") {
		t.Errorf("info 1: %d %q", code, errOut)
	}
	if out := h.ok("releases:rollback", "hello", "v1"); out != "-----> release v3 (rollback v1)\n" {
		t.Errorf("rollback: %q", out)
	}

	// ps, restart (the fake fails it), keys.
	out = h.ok("ps", "hello")
	if !strings.Contains(out, "INSTANCE") || !strings.Contains(flat(out), "abcdef123456 v1 restarting 2 - 127.0.0.1:41000 exited with code 1; restart 3 in 4s") {
		t.Errorf("ps:\n%s", out)
	}
	h.ok("apps:create", "idle")
	if out := h.ok("ps", "idle"); !strings.Contains(out, "no instances") {
		t.Errorf("ps idle: %q", out)
	}
	if out, errOut, code := h.run("restart", "hello"); code != 1 || !strings.Contains(out, "-----> restarting v1") || !strings.Contains(errOut, "did not accept connections") {
		t.Errorf("restart: %d\n%s%s", code, out, errOut)
	}
	pub, _, _ := ed25519.GenerateKey(rand.Reader)
	sshPub, _ := ssh.NewPublicKey(pub)
	keyFile := filepath.Join(t.TempDir(), "id.pub")
	_ = os.WriteFile(keyFile, append(bytes.TrimSpace(ssh.MarshalAuthorizedKey(sshPub)), " me@laptop\n"...), 0o600)
	if out := h.ok("keys:add", "me", keyFile); out != "added key me ("+ssh.FingerprintSHA256(sshPub)+")\n" {
		t.Errorf("keys:add: %q", out)
	}
	if _, errOut, code := h.run("keys:add", "me", keyFile+".nope"); code != 1 || !strings.Contains(errOut, "no such file") {
		t.Errorf("missing key file: %d %q", code, errOut)
	}

	// Destroy.
	if out := h.ok("apps:destroy", "idle"); out != "destroyed idle\n" {
		t.Errorf("destroy: %q", out)
	}
	if _, errOut, code := h.run("apps:destroy", "idle"); code != 1 || !strings.Contains(errOut, "not found") {
		t.Errorf("destroy twice: %d %q", code, errOut)
	}

	// Usage errors are exit 2 with the usage text.
	for _, args := range [][]string{nil, {"nope"}, {"apps:create"}, {"config:set", "hello"}, {"releases:info", "hello"},
		{"logs"}, {"logs", "hello", "-n"}, {"logs", "hello", "-n", "x"}, {"logs", "hello", "extra"}, {"logs", "--bogus", "hello"}} {
		if _, errOut, code := h.run(args...); code != 2 || !strings.Contains(errOut, "usage: ashore") {
			t.Errorf("%v: %d %q", args, code, errOut)
		}
	}

	// Logs: the tail, formatted for a terminal or as JSON lines.
	h.hub.Sink("hello").Append(logs.Line{App: "hello", Release: "v1", Stream: logs.StreamStderr, Time: time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC), Text: "warm"})
	h.hub.Sink("hello").Append(logs.Line{App: "hello", Stream: logs.StreamSystem, Time: time.Date(2026, 10, 18, 12, 0, 1, 0, time.UTC), Text: "dropped 3 lines"})
	out = h.ok("logs", "hello")
	if !strings.Contains(out, " v1[stderr]: warm\n") || !strings.Contains(out, " ashore[ashore]: dropped 3 lines\n") {
		t.Errorf("logs:\n%s", out)
	}
	if out := h.ok("logs", "-n", "1", "hello"); strings.Contains(out, "warm") || !strings.Contains(out, "dropped") {
		t.Errorf("logs -n 1:\n%s", out)
	}
	if out := h.ok("logs", "--json", "hello", "-n", "1"); !strings.HasPrefix(out, `{"app":"hello","stream":"ashore","time":"2026-10-18T12:00:01Z","text":"dropped 3 lines"}`) {
		t.Errorf("logs --json:\n%s", out)
	}
	if _, errOut, code := h.run("logs", "nope"); code != 1 || !strings.Contains(errOut, "not found") {
		t.Errorf("logs of an unknown app: %d %q", code, errOut)
	}
	if out, _, code := h.run("--version"); code != 0 || out != "ashore dev\n" {
		t.Errorf("--version: %d %q", code, out)
	}

	// Environment beats the file.
	h.env["ASHORE_URL"], h.env["ASHORE_TOKEN"] = h.server.URL, "wrong"
	if _, errOut, code := h.run("apps"); code != 1 || !strings.Contains(errOut, "wrong token") {
		t.Errorf("env token: %d %q", code, errOut)
	}
}

// dbPath finds the store the test server opened: it is the only ashore.db
// under the harness's temp dirs. Kept simple: the store path is derived the
// same way newHarness built it.
func dbPath(t *testing.T, h *harness) string {
	t.Helper()
	// newHarness opened filepath.Join(t.TempDir(), "test.db"); TempDir is
	// deterministic per call order, so look for it.
	matches, _ := filepath.Glob(filepath.Join(filepath.Dir(h.env["ASHORE_CONFIG"]), "..", "*", "test.db"))
	if len(matches) != 1 {
		t.Fatalf("test.db: %v", matches)
	}
	return matches[0]
}
