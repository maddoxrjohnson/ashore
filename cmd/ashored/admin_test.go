package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/maddoxrjohnson/ashore/internal/config"
	"github.com/maddoxrjohnson/ashore/internal/store"
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
}

func TestPortOf(t *testing.T) {
	for in, want := range map[string]int{":8080": 8080, "127.0.0.1:80": 80, "nope": 0} {
		if got := portOf(in); got != want {
			t.Errorf("portOf(%q) = %d, want %d", in, got, want)
		}
	}
}
