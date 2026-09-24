package gitserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/maddoxrjohnson/ashore/internal/store"
)

var ErrRepoExists = errors.New("repository already exists")

// RepoPath is where an app's bare repository lives.
func RepoPath(dataDir, app string) string {
	return filepath.Join(dataDir, "repos", app+".git")
}

// HooksPath is the directory git runs hooks from for every app repository.
// The daemon writes its pre-receive hook there (Phase 1.2).
func HooksPath(dataDir string) string {
	return filepath.Join(dataDir, "hooks")
}

// InitRepo creates the bare repository for app and points core.hooksPath at
// the daemon's hook directory. apps:create calls it after the database row
// exists.
func InitRepo(ctx context.Context, dataDir, app string) (string, error) {
	if err := store.ValidateName(app); err != nil {
		return "", err
	}
	path := RepoPath(dataDir, app)
	if _, err := os.Stat(path); err == nil {
		return "", fmt.Errorf("%s: %w", path, ErrRepoExists)
	}
	// Absolute, because git resolves a relative hooksPath against the repository.
	hooks, err := filepath.Abs(HooksPath(dataDir))
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	if err := git(ctx, "init", "--quiet", "--bare", "--initial-branch=main", path); err != nil {
		return "", err
	}
	if err := git(ctx, "--git-dir="+path, "config", "core.hooksPath", hooks); err != nil {
		_ = os.RemoveAll(path)
		return "", err
	}
	return path, nil
}

// git runs a short git command for its side effect.
func git(ctx context.Context, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Env = gitEnv()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return nil
}
