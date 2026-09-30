package hook

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/gitserver"
)

// Deployer is what the daemon does with a push the hook has handed over.
// Everything written to out reaches the pusher's terminal, line by line. A
// nil error accepts the push; an error rejects it and its text is the last
// line the pusher sees.
type Deployer interface {
	Deploy(ctx context.Context, app, sha, quarantine string, out io.Writer) error
}

const (
	// requestTimeout bounds how long a connection may take to send its one
	// request line. The hook sends it immediately after connecting.
	requestTimeout = 5 * time.Second
	// maxRequest is the longest request line accepted. A real one is a few
	// hundred bytes.
	maxRequest = 4096
	// maxLine is where lineWriter splits output that never ends a line, so
	// the hook's scanner (1 MiB) is never overrun.
	maxLine = 64 * 1024
)

// Server is the daemon side of the hook protocol. It issues one nonce per
// git receive-pack, listens on the unix socket in the data directory, and
// turns each hook connection into one Deploy call.
type Server struct {
	deployer Deployer
	dataDir  string // absolute, symlinks resolved, for the quarantine check
	sock     string

	mu     sync.Mutex
	nonces map[string]string // nonce -> app it was issued for
}

// SocketPath is where the daemon listens for its hooks.
func SocketPath(dataDir string) string {
	return filepath.Join(dataDir, "ashore.sock")
}

// NewServer prepares the server for a data directory that already exists.
func NewServer(dataDir string, d Deployer) (*Server, error) {
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}
	// git reports the quarantine path from getcwd(), which resolves
	// symlinks; resolve them here too or the prefix check would fail on a
	// data directory reached through a link.
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return nil, fmt.Errorf("data dir: %w", err)
	}
	return &Server{
		deployer: d,
		dataDir:  real,
		sock:     SocketPath(real),
		nonces:   make(map[string]string),
	}, nil
}

// Socket implements gitserver.Hooks.
func (s *Server) Socket() string { return s.sock }

// Issue implements gitserver.Hooks: a fresh nonce that redeems exactly once,
// for app only. revoke discards it if the hook never called, so a push that
// stalls or is refused before pre-receive leaves nothing behind.
func (s *Server) Issue(app string) (nonce string, revoke func()) {
	nonce = rand.Text()
	s.mu.Lock()
	s.nonces[nonce] = app
	s.mu.Unlock()
	return nonce, func() {
		s.mu.Lock()
		delete(s.nonces, nonce)
		s.mu.Unlock()
	}
}

// redeem consumes nonce and reports whether it was issued for app. A nonce
// presented for the wrong app is consumed as well; a client that gets one
// wrong does not get a second try.
func (s *Server) redeem(nonce, app string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	issuedFor, ok := s.nonces[nonce]
	delete(s.nonces, nonce)
	return ok && issuedFor == app
}

// Listen binds the socket. A file left by a daemon that crashed is removed
// first, but only after checking that nothing answers on it, so a second
// daemon on the same data directory fails instead of stealing the socket.
// The file is then made private to this user; the nonce is the real check,
// the mode is defence in depth.
func (s *Server) Listen() (net.Listener, error) {
	if c, err := net.DialTimeout("unix", s.sock, time.Second); err == nil {
		_ = c.Close()
		return nil, fmt.Errorf("%s: another daemon is listening", s.sock)
	}
	if err := os.Remove(s.sock); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	ln, err := net.Listen("unix", s.sock)
	if err != nil {
		return nil, fmt.Errorf("hook socket: %w", err)
	}
	if err := os.Chmod(s.sock, 0o600); err != nil {
		_ = ln.Close()
		return nil, err
	}
	return ln, nil
}

// Serve accepts hook connections until ctx is done, then closes the listener
// (which removes the socket file) and waits for the deploys in progress,
// which see the cancelled context.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel()
	go func() {
		<-ctx.Done()
		_ = ln.Close()
	}()
	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("hook accept: %w", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handle(ctx, c)
		}()
	}
}

// handle serves one hook: read the request, check it, run the deploy with
// its output framed as bandOutput lines, and finish with the status line.
func (s *Server) handle(ctx context.Context, c net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("hook panic", "panic", r, "stack", string(debug.Stack()))
		}
	}()
	defer func() { _ = c.Close() }()

	req, err := readRequest(c)
	if err == nil {
		err = s.check(&req)
	}
	if err != nil {
		slog.Warn("hook refused", "app", req.App, "err", err)
		refuse(c, err.Error())
		return
	}

	// The hook sends nothing after its request, so any read completing
	// means it is gone: git was killed, or the pusher hung up. Stop the
	// deploy then; a release that went live for a push git will not record
	// would leave "what runs" and "what is pushed" disagreeing.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		_, _ = io.Copy(io.Discard, c)
		cancel()
	}()

	start := time.Now()
	slog.Info("hook connected", "app", req.App, "sha", req.SHA)
	lw := &lineWriter{w: c}
	err = s.deployer.Deploy(ctx, req.App, req.SHA, req.Quarantine, lw)
	_ = lw.Flush()
	if err != nil {
		slog.Warn("deploy failed", "app", req.App, "sha", req.SHA, "err", err, "duration", time.Since(start))
		_ = writeLine(c, bandOutput, "-----> deploy failed: "+err.Error())
		_ = writeLine(c, bandStatus, "1")
		return
	}
	slog.Info("deploy finished", "app", req.App, "sha", req.SHA, "duration", time.Since(start))
	_ = writeLine(c, bandStatus, "0")
}

// refuse answers a bad request, then swallows a bounded amount of whatever
// the client is still sending. Closing a socket with unread bytes queued
// makes the kernel reset the connection, and the client would then read
// "connection reset by peer" instead of the reason. net/http does the same
// before closing on an unread request body.
func refuse(c net.Conn, reason string) {
	_ = writeLine(c, bandOutput, "-----> refused: "+reason)
	_ = writeLine(c, bandStatus, "1")
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	_, _ = io.CopyN(io.Discard, c, maxRequest)
}

// readRequest reads the one JSON line a hook sends, bounded in size and time.
func readRequest(c net.Conn) (Request, error) {
	var req Request
	_ = c.SetReadDeadline(time.Now().Add(requestTimeout))
	defer func() { _ = c.SetReadDeadline(time.Time{}) }()
	line, err := bufio.NewReaderSize(c, maxRequest).ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return req, fmt.Errorf("request longer than %d bytes", maxRequest)
		}
		return req, fmt.Errorf("reading request: %w", err)
	}
	if err := json.Unmarshal(line, &req); err != nil {
		return req, fmt.Errorf("bad request: %w", err)
	}
	return req, nil
}

// check validates a request in the order that matters: the nonce first,
// because it binds the app name to a push the SSH server admitted, then the
// values that become arguments to git. The quarantine path is cleaned and
// must lie inside the app's own object directory (D23).
func (s *Server) check(req *Request) error {
	if !s.redeem(req.Nonce, req.App) {
		return errors.New("bad or expired nonce")
	}
	if !isObjectName(req.SHA) {
		return fmt.Errorf("bad object name %q", req.SHA)
	}
	if req.Quarantine == "" || !filepath.IsAbs(req.Quarantine) {
		return fmt.Errorf("bad quarantine path %q", req.Quarantine)
	}
	// git spells it <repo>/./objects/tmp_objdir-incoming-XXXXXX.
	clean := filepath.Clean(req.Quarantine)
	objects := filepath.Join(gitserver.RepoPath(s.dataDir, req.App), "objects")
	if !strings.HasPrefix(clean, objects+string(filepath.Separator)) {
		return fmt.Errorf("quarantine path %q is outside %s", req.Quarantine, objects)
	}
	req.Quarantine = clean
	return nil
}

// lineWriter frames whatever a Deployer writes as bandOutput lines. A write
// may carry several lines or part of one; only complete lines are sent, and
// Flush sends what is left as a line of its own. Safe for concurrent use,
// since a deploy may write from more than one goroutine.
type lineWriter struct {
	w   io.Writer
	mu  sync.Mutex
	buf []byte
}

func (l *lineWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf = append(l.buf, p...)
	for {
		i := bytes.IndexByte(l.buf, '\n')
		n := i + 1 // consumed: the line and its newline
		if i < 0 {
			if len(l.buf) < maxLine {
				return len(p), nil
			}
			i, n = maxLine, maxLine // no newline in sight: send a chunk as a line
		}
		line := bytes.TrimSuffix(l.buf[:i], []byte{'\r'})
		if err := writeLine(l.w, bandOutput, string(line)); err != nil {
			return 0, err
		}
		l.buf = l.buf[n:]
	}
}

func (l *lineWriter) Flush() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.buf) == 0 {
		return nil
	}
	line := string(bytes.TrimSuffix(l.buf, []byte{'\r'}))
	l.buf = nil
	return writeLine(l.w, bandOutput, line)
}
