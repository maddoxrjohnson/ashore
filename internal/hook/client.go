package hook

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// defaultBranch is the only ref a push may update. Anything else is refused
// whole: pre-receive cannot accept one ref and reject another.
const defaultBranch = "main"

const dialTimeout = 5 * time.Second

// Run is the hook's main. args are the words after "hook", stdin is what git
// wrote, and the result is the process exit status, which is git's verdict on
// the push. Everything the process touches is a parameter so tests can drive
// it without a real git or a real daemon.
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer, getenv func(string) string) int {
	if len(args) != 1 || args[0] != "pre-receive" {
		complain(stderr, "usage: ashored hook pre-receive")
		return 2
	}
	sock, nonce := getenv("ASHORE_SOCK"), getenv("ASHORE_NONCE")
	if sock == "" || nonce == "" {
		complain(stderr, "not started by the daemon (ASHORE_SOCK or ASHORE_NONCE unset)")
		return 1
	}
	sha, err := parseUpdates(stdin, defaultBranch)
	if err != nil {
		complain(stderr, "%v", err)
		return 1
	}
	req := Request{
		App:        getenv("ASHORE_APP"),
		SHA:        sha,
		Quarantine: getenv("GIT_QUARANTINE_PATH"),
		Nonce:      nonce,
	}
	code, err := call(context.Background(), sock, req, stdout)
	if err != nil {
		complain(stderr, "%v", err)
	}
	return code
}

// complain writes one "ashore: ..." line for the user; git forwards the hook's
// stderr to them like its stdout.
func complain(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, "ashore: "+format+"\n", args...)
}

// parseUpdates reads git's "<old> <new> <ref>" lines and returns the new
// object name for refs/heads/<branch>. Exactly one line is allowed, it must
// be that branch, and it must not be a deletion (new name all zeros).
func parseUpdates(r io.Reader, branch string) (string, error) {
	want := "refs/heads/" + branch
	var updates [][]string
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) != 3 {
			return "", fmt.Errorf("malformed update line %q", sc.Text())
		}
		updates = append(updates, f)
	}
	if err := sc.Err(); err != nil {
		return "", fmt.Errorf("reading updates: %w", err)
	}
	switch len(updates) {
	case 0:
		return "", errors.New("no ref updates on stdin")
	case 1:
	default:
		return "", fmt.Errorf("push exactly one branch (%s); got %d refs", want, len(updates))
	}
	newSHA, ref := updates[0][1], updates[0][2]
	if ref != want {
		return "", fmt.Errorf("only %s deploys; refusing %s", want, ref)
	}
	if !isObjectName(newSHA) {
		return "", fmt.Errorf("bad object name %q", newSHA)
	}
	if strings.Trim(newSHA, "0") == "" {
		return "", fmt.Errorf("refusing to delete %s", want)
	}
	return newSHA, nil
}

// isObjectName accepts a full lowercase hex SHA-1 or SHA-256 object name. The
// value ends up as a git command argument in the daemon, so nothing else gets
// through.
func isObjectName(s string) bool {
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

// call hands the push to the daemon and relays its answer. Output lines go to
// out as the daemon sent them; the status line's number is the exit code. A
// daemon that cannot be reached, or that hangs up without a status line, is an
// error and exit 1, so the push is rejected rather than silently accepted.
func call(ctx context.Context, sock string, req Request, out io.Writer) (int, error) {
	d := net.Dialer{Timeout: dialTimeout}
	conn, err := d.DialContext(ctx, "unix", sock)
	if err != nil {
		return 1, fmt.Errorf("daemon unreachable: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return 1, fmt.Errorf("sending request: %w", err)
	}
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20) // build output lines can be long
	for sc.Scan() {
		line := sc.Text()
		if line == "" {
			continue
		}
		band, text := line[0], line[1:]
		switch band {
		case bandOutput:
			if _, err := fmt.Fprintln(out, text); err != nil {
				return 1, err
			}
		case bandStatus:
			code, err := strconv.Atoi(text)
			if err != nil || code < 0 {
				return 1, fmt.Errorf("bad status line %q", line)
			}
			return code, nil
		default:
			return 1, fmt.Errorf("bad response line %q", line)
		}
	}
	if err := sc.Err(); err != nil {
		return 1, fmt.Errorf("reading response: %w", err)
	}
	return 1, errors.New("daemon closed the connection without a verdict")
}
