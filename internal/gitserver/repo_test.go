package gitserver

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maddoxrjohnson/ashore/internal/store"
)

func TestInitRepo(t *testing.T) {
	requireBinaries(t, "git")
	ctx := context.Background()
	dataDir := t.TempDir()

	path, err := InitRepo(ctx, dataDir, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if want := RepoPath(dataDir, "hello"); path != want {
		t.Fatalf("path = %s, want %s", path, want)
	}
	head, err := os.ReadFile(filepath.Join(path, "HEAD"))
	if err != nil || string(head) != "ref: refs/heads/main\n" {
		t.Fatalf("HEAD = %q, %v; want the main branch", head, err)
	}
	if got := gitOutput(t, path, "rev-parse", "--is-bare-repository"); got != "true" {
		t.Fatalf("is-bare-repository = %q", got)
	}
	wantHooks, _ := filepath.Abs(HooksPath(dataDir))
	if got := gitOutput(t, path, "config", "core.hooksPath"); got != wantHooks {
		t.Fatalf("core.hooksPath = %q, want %q", got, wantHooks)
	}

	if _, err := InitRepo(ctx, dataDir, "hello"); !errors.Is(err, ErrRepoExists) {
		t.Fatalf("second InitRepo: %v, want ErrRepoExists", err)
	}
	if _, err := InitRepo(ctx, dataDir, "Hello"); !errors.Is(err, store.ErrInvalidName) {
		t.Fatalf("InitRepo with a bad name: %v, want ErrInvalidName", err)
	}
}

func gitOutput(t *testing.T, repo string, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"--git-dir=" + repo}, args...)...).Output()
	if err != nil {
		t.Fatalf("git %s: %v", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(out))
}
