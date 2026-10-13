package api

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/ssh"

	"github.com/maddoxrjohnson/ashore/internal/store"
)

func TestAppsLifecycle(t *testing.T) {
	if _, err := os.Stat("/usr/bin/git"); err != nil {
		t.Skip("git not installed")
	}
	f := newFixture(t)
	ctx := context.Background()

	apps, err := f.client.Apps(ctx)
	if err != nil || len(apps) != 1 || apps[0].Name != "hello" || apps[0].Live != "" || apps[0].Remote != "ssh://ashore@127.0.0.1:2222/hello" {
		t.Fatalf("apps = %+v, %v", apps, err)
	}
	created, err := f.client.CreateApp(ctx, "api")
	if err != nil || created.Name != "api" || created.Host != "api.localhost" || created.Remote != "ssh://ashore@127.0.0.1:2222/api" {
		t.Fatalf("create = %+v, %v", created, err)
	}
	if _, err := os.Stat(filepath.Join(f.dataDir, "repos", "api.git", "HEAD")); err != nil {
		t.Errorf("repository not created: %v", err)
	}
	if _, err := f.client.CreateApp(ctx, "api"); !errors.Is(err, &Error{Status: http.StatusConflict}) {
		t.Errorf("duplicate: %v", err)
	}
	if _, err := f.client.CreateApp(ctx, "Not Valid"); !errors.Is(err, &Error{Status: http.StatusBadRequest}) {
		t.Errorf("bad name: %v", err)
	}
	f.goLive(t)
	got, err := f.client.App(ctx, "hello")
	if err != nil || got.Live != "v1" {
		t.Errorf("app = %+v, %v", got, err)
	}
	if _, err := f.client.App(ctx, "nope"); !errors.Is(err, &Error{Status: http.StatusNotFound}) {
		t.Errorf("unknown app: %v", err)
	}

	// Destroy: the supervisor is asked first, then rows and repository go.
	if err := f.client.DestroyApp(ctx, "api"); err != nil {
		t.Fatalf("destroy: %v", err)
	}
	if got := f.sup.redeploys(); len(got) != 1 || got[0] != "api: destroy" {
		t.Errorf("supervisor calls = %v", got)
	}
	if _, err := os.Stat(filepath.Join(f.dataDir, "repos", "api.git")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("repository still there: %v", err)
	}
	if _, err := f.client.App(ctx, "api"); !errors.Is(err, &Error{Status: http.StatusNotFound}) {
		t.Errorf("destroyed app still answers: %v", err)
	}
	if err := f.client.DestroyApp(ctx, "api"); !errors.Is(err, &Error{Status: http.StatusNotFound}) {
		t.Errorf("destroy twice: %v", err)
	}
	// A supervisor that cannot stop the instance keeps the app.
	f.sup.err = errors.New("stop v1: docker is down")
	if err := f.client.DestroyApp(ctx, "hello"); err == nil || !strings.Contains(err.Error(), "internal error") {
		t.Errorf("destroy with a failing supervisor: %v", err)
	}
	if _, err := f.client.App(ctx, "hello"); err != nil {
		t.Errorf("app removed although destroy failed: %v", err)
	}
}

func TestReleasesEndpoints(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	rels, err := f.client.Releases(ctx, "hello")
	if err != nil || len(rels) != 0 {
		t.Fatalf("fresh releases = %+v, %v", rels, err)
	}
	r1, _ := f.store.CreateRelease(ctx, f.app.ID, store.NewRelease{GitSHA: "1111111111111111111111111111111111111111", Note: "push 1111111"})
	_ = f.store.MarkFailed(ctx, r1.ID)
	r2, _ := f.store.CreateRelease(ctx, f.app.ID, store.NewRelease{Image: "ashore/hello:2222222", GitSHA: "2222222222222222222222222222222222222222",
		ConfigJSON: `{"GREETING":"hi"}`, ManifestJSON: `{"health":{"path":"/healthz"}}`, Note: "push 2222222"})
	_ = f.store.SetLive(ctx, f.app.ID, r2.ID)

	rels, err = f.client.Releases(ctx, "hello")
	if err != nil || len(rels) != 2 {
		t.Fatalf("releases = %+v, %v", rels, err)
	}
	if rels[0].Version != "v1" || rels[0].Status != "failed" || rels[0].Image != "" || rels[1].Version != "v2" || rels[1].Status != "live" {
		t.Errorf("releases = %+v", rels)
	}
	v2, err := f.client.Release(ctx, "hello", "v2")
	if err != nil || v2.Image != "ashore/hello:2222222" || v2.Commit != r2.GitSHA || v2.Config["GREETING"] != "hi" || string(v2.Manifest) != `{"health":{"path":"/healthz"}}` || v2.Note != "push 2222222" {
		t.Errorf("v2 = %+v, %v", v2, err)
	}
	if v1, _ := f.client.Release(ctx, "hello", "v1"); string(v1.Manifest) != "{}" || len(v1.Config) != 0 {
		t.Errorf("v1 = %+v", v1)
	}
	for name, want := range map[string]int{"v9": http.StatusNotFound, "2": http.StatusBadRequest} {
		if _, err := f.client.Release(ctx, "hello", name); !errors.Is(err, &Error{Status: want}) {
			t.Errorf("release %s: %v, want %d", name, err, want)
		}
	}
	if _, err := f.client.Releases(ctx, "nope"); !errors.Is(err, &Error{Status: http.StatusNotFound}) {
		t.Errorf("releases of unknown app: %v", err)
	}
}

func TestAddKey(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	sshPub, _ := ssh.NewPublicKey(pub)
	line := string(bytes.TrimSpace(ssh.MarshalAuthorizedKey(sshPub))) + " me@laptop"
	k, err := f.client.AddKey(ctx, "me", line)
	if err != nil || k.Name != "me" || k.Fingerprint != ssh.FingerprintSHA256(sshPub) {
		t.Fatalf("add key = %+v, %v", k, err)
	}
	stored, err := f.store.KeyByFingerprint(ctx, k.Fingerprint)
	if err != nil || strings.Contains(stored.PubKey, "me@laptop") || !strings.HasPrefix(stored.PubKey, "ssh-ed25519 ") {
		t.Errorf("stored key = %+v, %v", stored, err)
	}
	if _, err := f.client.AddKey(ctx, "me", line); !errors.Is(err, &Error{Status: http.StatusConflict}) {
		t.Errorf("duplicate key: %v", err)
	}
	if _, err := f.client.AddKey(ctx, "", line); !errors.Is(err, &Error{Status: http.StatusBadRequest}) {
		t.Errorf("no name: %v", err)
	}
	if _, err := f.client.AddKey(ctx, "x", "not a key"); !errors.Is(err, &Error{Status: http.StatusBadRequest}) {
		t.Errorf("garbage key: %v", err)
	}
}
