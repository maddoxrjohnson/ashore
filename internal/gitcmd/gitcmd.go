// Package gitcmd runs the git binary the way the daemon needs it: with a
// clean environment, against a bare repository, and, during a push, with
// the pushed objects' quarantine directory visible.
package gitcmd

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// Env is a child's entire environment: PATH to find git, HOME for its
// config, a fixed locale so messages are parseable, plus extra. Nothing a
// client sent reaches a git process.
func Env(extra ...string) []string {
	env := []string{"PATH=" + os.Getenv("PATH"), "LC_ALL=C"}
	if home := os.Getenv("HOME"); home != "" {
		env = append(env, "HOME="+home)
	}
	return append(env, extra...)
}

// Command builds a git command. repo, when set, is passed as --git-dir;
// quarantine, when set, is GIT_QUARANTINE_PATH from a push in progress,
// exported as an alternate object directory so the pushed commit can be
// read while pre-receive still runs.
func Command(ctx context.Context, repo, quarantine string, args ...string) *exec.Cmd {
	if repo != "" {
		args = append([]string{"--git-dir=" + repo}, args...)
	}
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Env = Env()
	if quarantine != "" {
		cmd.Env = append(cmd.Env, "GIT_ALTERNATE_OBJECT_DIRECTORIES="+quarantine)
	}
	return cmd
}

// Run runs a git command for its side effect; a failure carries git's
// combined output.
func Run(ctx context.Context, repo string, args ...string) error {
	cmd := Command(ctx, repo, "", args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, bytes.TrimSpace(out))
	}
	return nil
}

// Output runs a git command and returns its stdout; a failure carries
// git's stderr.
func Output(ctx context.Context, repo, quarantine string, args ...string) ([]byte, error) {
	cmd := Command(ctx, repo, quarantine, args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", args[0], err, bytes.TrimSpace(stderr.Bytes()))
	}
	return out, nil
}

// IsObjectName reports whether s is a full lowercase hex SHA-1 or SHA-256
// object name. Anything that becomes a git argument or a path component
// must pass this, so nothing else gets through.
func IsObjectName(s string) bool {
	if len(s) != 40 && len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}
	return true
}

// ShortSHA is the seven-character abbreviation git itself prints.
func ShortSHA(sha string) string {
	if len(sha) > 7 {
		return sha[:7]
	}
	return sha
}
