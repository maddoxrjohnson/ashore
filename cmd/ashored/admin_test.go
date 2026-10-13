package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/maddoxrjohnson/ashore/internal/api"
	"github.com/maddoxrjohnson/ashore/internal/config"
	"github.com/maddoxrjohnson/ashore/internal/store"
	"github.com/maddoxrjohnson/ashore/internal/supervisor"
)

func TestAdmin(t *testing.T) {
	if _, err := os.Stat("/usr/bin/git"); err != nil {
		t.Skip("git not installed")
	}
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.SSHAddr = "127.0.0.1:2299"
	ctx := context.Background()

	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sshPub, err := ssh.NewPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	keyFile := filepath.Join(t.TempDir(), "id.pub")
	if err := os.WriteFile(keyFile, append(bytes.TrimSpace(ssh.MarshalAuthorizedKey(sshPub)), " me@laptop\n"...), 0o600); err != nil {
		t.Fatal(err)
	}

	// Config commands go through the API of a running daemon; here that is
	// a test server over the same database, with the token file written
	// where the daemon would have left it.
	apiStore, err := store.Open(ctx, filepath.Join(cfg.DataDir, "ashore.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = apiStore.Close() }()
	srv := httptest.NewServer(api.New(apiStore, redeployer{}, "tok", slog.New(slog.NewTextHandler(io.Discard, nil))))
	defer srv.Close()
	cfg.InternalAddr = strings.TrimPrefix(srv.URL, "http://")
	if err := os.WriteFile(filepath.Join(cfg.DataDir, api.TokenFile), []byte("tok\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		args []string
		want string // substring of the output
		err  error  // errors.Is target, nil for success
	}{
		{"no args", nil, "", errUsage},
		{"unknown", []string{"apps", "destroy", "x"}, "", errUsage},
		{"create", []string{"apps", "create", "hello"}, "git remote add ashore ssh://ashore@127.0.0.1:2299/hello", nil},
		{"create again", []string{"apps", "create", "hello"}, "", store.ErrExists},
		{"bad name", []string{"apps", "create", "Hello!"}, "", store.ErrInvalidName},
		{"add key", []string{"keys", "add", "me", keyFile}, "added key me (" + ssh.FingerprintSHA256(sshPub) + ")", nil},
		{"add key again", []string{"keys", "add", "me", keyFile}, "", store.ErrExists},
		{"missing key file", []string{"keys", "add", "me", keyFile + ".nope"}, "", os.ErrNotExist},
		{"list no releases", []string{"releases", "list", "hello"}, "VERSION", nil},
		{"list unknown app", []string{"releases", "list", "nope"}, "", store.ErrNotFound},
		{"info no release", []string{"releases", "info", "hello", "v1"}, "", store.ErrNotFound},
		{"info bad args", []string{"releases", "info", "hello"}, "", errUsage},
		{"config get empty", []string{"config", "get", "hello"}, "", nil},
		{"config set", []string{"config", "set", "hello", "GREETING=hi there", "EMPTY="}, "nothing is running yet", nil},
		{"config get", []string{"config", "get", "hello"}, "EMPTY=\nGREETING=hi there\n", nil},
		{"config set bad pair", []string{"config", "set", "hello", "NOEQUALS"}, "", errNotPair},
		{"config unset", []string{"config", "unset", "hello", "EMPTY"}, "nothing is running yet", nil},
		{"config unset unknown app", []string{"config", "unset", "nope", "X"}, "", errAPI},
		{"config bad args", []string{"config", "set", "hello"}, "", errUsage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			err := admin(ctx, cfg, tc.args, &out)
			if !errors.Is(err, tc.err) {
				t.Fatalf("err = %v, want %v", err, tc.err)
			}
			if !strings.Contains(out.String(), tc.want) {
				t.Errorf("output %q lacks %q", out.String(), tc.want)
			}
		})
	}

	if _, err := os.Stat(filepath.Join(cfg.DataDir, "repos", "hello.git", "HEAD")); err != nil {
		t.Errorf("repository not created: %v", err)
	}
	st, err := store.Open(ctx, filepath.Join(cfg.DataDir, "ashore.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	k, err := st.KeyByFingerprint(ctx, ssh.FingerprintSHA256(sshPub))
	if err != nil {
		t.Fatal(err)
	}
	stored, _, _, _, err := ssh.ParseAuthorizedKey([]byte(k.PubKey))
	if err != nil || !bytes.Equal(stored.Marshal(), sshPub.Marshal()) {
		t.Errorf("stored key %q does not parse back to the key added: %v", k.PubKey, err)
	}
	if strings.Contains(k.PubKey, "me@laptop") {
		t.Errorf("stored key kept the comment: %q", k.PubKey)
	}

	// With a deploy history: a failed v1 and a live v2, listed newest first.
	app, err := st.GetApp(ctx, "hello")
	if err != nil {
		t.Fatal(err)
	}
	r1, err := st.CreateRelease(ctx, app.ID, store.NewRelease{GitSHA: "1111111111111111111111111111111111111111", Note: "push 1111111"})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.MarkFailed(ctx, r1.ID); err != nil {
		t.Fatal(err)
	}
	r2, err := st.CreateRelease(ctx, app.ID, store.NewRelease{Image: "ashore/hello:2222222", GitSHA: "2222222222222222222222222222222222222222", Note: "push 2222222"})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.SetLive(ctx, app.ID, r2.ID); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := admin(ctx, cfg, []string{"releases", "list", "hello"}, &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[1], "v2 ") || !strings.Contains(lines[1], "live") || !strings.HasPrefix(lines[2], "v1 ") || !strings.Contains(lines[2], "failed") {
		t.Errorf("releases list:\n%s", out.String())
	}
	out.Reset()
	if err := admin(ctx, cfg, []string{"releases", "info", "hello", "v2"}, &out); err != nil {
		t.Fatal(err)
	}
	// Column widths follow the longest label, so compare with the padding
	// collapsed.
	info := strings.Join(strings.Fields(out.String()), " ")
	for _, want := range []string{"release: v2", "status: live", "image: ashore/hello:2222222", "commit: 2222222222222222222222222222222222222222", "note: push 2222222", "config: {}", "manifest: {}"} {
		if !strings.Contains(info, want) {
			t.Errorf("releases info lacks %q:\n%s", want, out.String())
		}
	}
	if err := admin(ctx, cfg, []string{"releases", "info", "hello", "2"}, &out); err == nil || !strings.Contains(err.Error(), "not a release version") {
		t.Errorf("bad version: err = %v", err)
	}
}

// errAPI matches any error the daemon answered with.
var errAPI = &api.Error{}

// redeployer is the API's supervisor in TestAdmin; no app is live there,
// so it is never called.
type redeployer struct{}

func (redeployer) Redeploy(context.Context, string, string, io.Writer) error {
	return errors.New("unexpected redeploy")
}

func (redeployer) Status(string) (supervisor.Status, bool) { return supervisor.Status{}, false }

func (redeployer) Restart(context.Context, string, io.Writer) error {
	return errors.New("unexpected restart")
}

func (redeployer) Rollback(context.Context, string, int, io.Writer) error {
	return errors.New("unexpected rollback")
}

func TestPortOf(t *testing.T) {
	for in, want := range map[string]int{":8080": 8080, "127.0.0.1:80": 80, "nope": 0} {
		if got := portOf(in); got != want {
			t.Errorf("portOf(%q) = %d, want %d", in, got, want)
		}
	}
}
