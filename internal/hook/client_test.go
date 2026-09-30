package hook

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const (
	oldSHA  = "1111111111111111111111111111111111111111"
	newSHA  = "2222222222222222222222222222222222222222"
	zeroSHA = "0000000000000000000000000000000000000000"
)

// noStatus makes fakeDaemon hang up without a verdict.
const noStatus = -1

// fakeDaemon listens on a unix socket in a temp dir and answers one connection
// with the given output lines and status. The decoded request comes back on
// the channel so the test can check what the hook sent.
func fakeDaemon(t *testing.T, output []string, status int) (sock string, got <-chan Request) {
	t.Helper()
	sock = filepath.Join(t.TempDir(), "s.sock") // sun_path is 108 bytes; keep it short
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	reqs := make(chan Request, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer func() { _ = c.Close() }()
		var req Request
		if err := json.NewDecoder(c).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		reqs <- req
		for _, l := range output {
			_ = writeLine(c, bandOutput, l)
		}
		if status != noStatus {
			_ = writeLine(c, bandStatus, strconv.Itoa(status))
		}
	}()
	return sock, reqs
}

func TestParseUpdates(t *testing.T) {
	line := func(old, sha, ref string) string { return old + " " + sha + " " + ref + "\n" }
	cases := []struct {
		name    string
		in      string
		want    string // sha on success
		wantErr string // substring of the error otherwise
	}{
		{"one update to main", line(oldSHA, newSHA, "refs/heads/main"), newSHA, ""},
		{"branch creation", line(zeroSHA, newSHA, "refs/heads/main"), newSHA, ""},
		{"empty input", "", "", "no ref updates"},
		{"two refs", line(oldSHA, newSHA, "refs/heads/main") + line(zeroSHA, newSHA, "refs/tags/v1"), "", "exactly one branch"},
		{"other branch", line(oldSHA, newSHA, "refs/heads/dev"), "", "refusing refs/heads/dev"},
		{"tag only", line(zeroSHA, newSHA, "refs/tags/v1"), "", "refusing refs/tags/v1"},
		{"delete", line(newSHA, zeroSHA, "refs/heads/main"), "", "refusing to delete"},
		{"malformed", "just two\n", "", "malformed"},
		{"bad object name", line(oldSHA, "nothex", "refs/heads/main"), "", "bad object name"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseUpdates(strings.NewReader(tc.in), "main")
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want one containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("sha = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRunForwardsOutputAndVerdict(t *testing.T) {
	for _, status := range []int{0, 1} {
		t.Run(fmt.Sprintf("status %d", status), func(t *testing.T) {
			sock, got := fakeDaemon(t, []string{"-----> building", "-----> live"}, status)
			env := map[string]string{
				"ASHORE_SOCK":         sock,
				"ASHORE_NONCE":        "n0nce",
				"ASHORE_APP":          "hello",
				"GIT_QUARANTINE_PATH": "/data/repos/hello.git/objects/incoming-x",
			}
			var stdout, stderr bytes.Buffer
			stdin := strings.NewReader(oldSHA + " " + newSHA + " refs/heads/main\n")
			code := Run([]string{"pre-receive"}, stdin, &stdout, &stderr, func(k string) string { return env[k] })
			if code != status {
				t.Errorf("exit = %d, want %d; stderr %q", code, status, stderr.String())
			}
			if want := "-----> building\n-----> live\n"; stdout.String() != want {
				t.Errorf("stdout = %q, want %q", stdout.String(), want)
			}
			if stderr.Len() != 0 {
				t.Errorf("unexpected stderr %q", stderr.String())
			}
			req := <-got
			want := Request{App: "hello", SHA: newSHA, Quarantine: env["GIT_QUARANTINE_PATH"], Nonce: "n0nce"}
			if req != want {
				t.Errorf("daemon got %+v, want %+v", req, want)
			}
		})
	}
}

func TestRunRefusals(t *testing.T) {
	one := oldSHA + " " + newSHA + " refs/heads/main\n"
	fromDaemon := map[string]string{"ASHORE_SOCK": "unused", "ASHORE_NONCE": "n", "ASHORE_APP": "hello"}
	silent, _ := fakeDaemon(t, []string{"-----> building"}, noStatus)
	cases := []struct {
		name  string
		args  []string
		stdin string
		env   map[string]string
		code  int
		err   string // substring of stderr
	}{
		{"usage", []string{"post-receive"}, one, fromDaemon, 2, "usage"},
		{"not from daemon", []string{"pre-receive"}, one, map[string]string{}, 1, "not started by the daemon"},
		{"two refs", []string{"pre-receive"}, one + zeroSHA + " " + newSHA + " refs/tags/v1\n", fromDaemon, 1, "exactly one branch"},
		{"daemon down", []string{"pre-receive"}, one, map[string]string{
			"ASHORE_SOCK": filepath.Join(t.TempDir(), "none.sock"), "ASHORE_NONCE": "n"}, 1, "no such file"},
		{"no verdict", []string{"pre-receive"}, one, map[string]string{
			"ASHORE_SOCK": silent, "ASHORE_NONCE": "n"}, 1, "without a verdict"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := Run(tc.args, strings.NewReader(tc.stdin), &stdout, &stderr, func(k string) string { return tc.env[k] })
			if code != tc.code {
				t.Errorf("exit = %d, want %d", code, tc.code)
			}
			if !strings.Contains(stderr.String(), tc.err) {
				t.Errorf("stderr %q does not contain %q", stderr.String(), tc.err)
			}
			if !strings.HasPrefix(stderr.String(), "ashore: ") {
				t.Errorf("stderr %q lacks the ashore: prefix", stderr.String())
			}
		})
	}
}
