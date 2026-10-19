package gitcmd

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func requireGit(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
}

func TestEnvIsClean(t *testing.T) {
	t.Setenv("GIT_DIR", "/nope")
	t.Setenv("HOME", "/home/x")
	env := Env("A=1")
	joined := strings.Join(env, "\n")
	if strings.Contains(joined, "GIT_DIR") || !strings.Contains(joined, "LC_ALL=C") || !strings.Contains(joined, "HOME=/home/x") || !strings.HasSuffix(joined, "A=1") {
		t.Fatalf("env %q", env)
	}
	t.Setenv("HOME", "")
	if strings.Contains(strings.Join(Env(), "\n"), "HOME=") {
		t.Fatal("an unset HOME should not be exported")
	}
}

func TestRunAndOutput(t *testing.T) {
	requireGit(t)
	ctx := context.Background()
	repo := filepath.Join(t.TempDir(), "r.git")
	if err := Run(ctx, "", "init", "--quiet", "--bare", "--initial-branch=main", repo); err != nil {
		t.Fatal(err)
	}
	if err := Run(ctx, repo, "config", "core.hooksPath", "/hooks"); err != nil {
		t.Fatal(err)
	}
	out, err := Output(ctx, repo, "", "config", "core.hooksPath")
	if err != nil || strings.TrimSpace(string(out)) != "/hooks" {
		t.Fatalf("output %q %v", out, err)
	}
	if err := Run(ctx, repo, "nonsense-command"); err == nil || !strings.Contains(err.Error(), "git nonsense-command") {
		t.Fatalf("err = %v", err)
	}
	if _, err := Output(ctx, repo, "", "cat-file", "-t", "0000000000000000000000000000000000000000"); err == nil || !strings.Contains(err.Error(), "git cat-file") {
		t.Fatalf("err = %v", err)
	}
	cmd := Command(ctx, repo, "/tmp/q", "status")
	if !strings.Contains(strings.Join(cmd.Env, "\n"), "GIT_ALTERNATE_OBJECT_DIRECTORIES=/tmp/q") || cmd.Args[1] != "--git-dir="+repo {
		t.Fatalf("command %v env %v", cmd.Args, cmd.Env)
	}
	_ = os.RemoveAll(repo)
}

func TestObjectNames(t *testing.T) {
	for name, want := range map[string]bool{
		"abc1234567890abc1234567890abc1234567890a": true,
		strings.Repeat("0", 64):                    true,
		"ABC1234567890abc1234567890abc1234567890a": false,
		"abc1234": false,
		"":        false,
		"abc1234567890abc1234567890abc1234567890g": false,
	} {
		if IsObjectName(name) != want {
			t.Errorf("%q: want %v", name, want)
		}
	}
	if ShortSHA("abc1234567890") != "abc1234" || ShortSHA("ab") != "ab" {
		t.Error("ShortSHA")
	}
}
