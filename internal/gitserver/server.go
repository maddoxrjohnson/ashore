// Package gitserver is the daemon's SSH front door. It authenticates pushers
// by public key and runs git receive-pack or upload-pack on an app's bare
// repository. Nothing else is accepted: no shell, no pty, no subsystems, no
// port forwarding. Every request the handler does not recognise gets a
// "false" reply, so there is no deny list to maintain.
package gitserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/maddoxrjohnson/ashore/internal/store"
)

// Store is the slice of the database the server needs.
type Store interface {
	KeyByFingerprint(ctx context.Context, fingerprint string) (store.Key, error)
	GetApp(ctx context.Context, name string) (store.App, error)
}

const (
	// handshakeTimeout bounds a client that connects and then says nothing;
	// x/crypto/ssh has no timeout of its own for that.
	handshakeTimeout = 30 * time.Second
	lookupTimeout    = 5 * time.Second
	// stopGrace is how long git gets after SIGTERM before it is killed when
	// the client disappears or the daemon shuts down.
	stopGrace = 5 * time.Second

	extKeyName     = "ashore-key-name"
	extFingerprint = "ashore-fingerprint"
)

// Server serves git over SSH. Create it with New and run it with Serve.
type Server struct {
	store   Store
	dataDir string
	config  *ssh.ServerConfig
	hostKey ssh.PublicKey

	// Fingerprint is the host key's SHA256 fingerprint, the string ssh shows
	// a user on first connect.
	Fingerprint string

	active atomic.Int32 // sessions currently running git; tests wait on it
}

// New loads or creates the host key and prepares the SSH configuration.
// dataDir holds repos/<app>.git and is made absolute so that git and the
// hooks never depend on the daemon's working directory.
func New(st Store, dataDir, hostKeyPath string) (*Server, error) {
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}
	signer, created, err := loadOrCreateHostKey(hostKeyPath)
	if err != nil {
		return nil, fmt.Errorf("host key: %w", err)
	}
	s := &Server{
		store:       st,
		dataDir:     abs,
		hostKey:     signer.PublicKey(),
		Fingerprint: ssh.FingerprintSHA256(signer.PublicKey()),
	}
	s.config = &ssh.ServerConfig{
		PublicKeyCallback: s.authenticate,
		ServerVersion:     "SSH-2.0-ashore",
	}
	s.config.AddHostKey(signer)
	slog.Info("ssh host key", "path", hostKeyPath, "fingerprint", s.Fingerprint, "created", created)
	return s, nil
}

// authenticate is called by x/crypto/ssh for every key the client offers,
// once for the query and once for the signed attempt. The lookup is a
// primary-key read, so being called twice is fine.
func (s *Server) authenticate(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
	fp := ssh.FingerprintSHA256(key)
	ctx, cancel := context.WithTimeout(context.Background(), lookupTimeout)
	defer cancel()
	k, err := s.store.KeyByFingerprint(ctx, fp)
	if err != nil {
		slog.Info("ssh auth refused", "remote", conn.RemoteAddr(), "user", conn.User(), "fingerprint", fp, "err", err)
		return nil, err
	}
	// The fingerprint is a hash of the key; compare the stored key material
	// too so a mis-registered row cannot let a different key in.
	stored, _, _, _, err := ssh.ParseAuthorizedKey([]byte(k.PubKey))
	if err != nil {
		return nil, fmt.Errorf("stored key %s: %w", fp, err)
	}
	if !bytes.Equal(stored.Marshal(), key.Marshal()) {
		return nil, fmt.Errorf("key %s: stored key does not match", fp)
	}
	return &ssh.Permissions{Extensions: map[string]string{extKeyName: k.Name, extFingerprint: fp}}, nil
}

// Serve accepts connections until ctx is done, then closes the listener and
// waits for the connections it spawned. Their git processes are terminated
// through the same context.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	defer wg.Wait()
	defer cancel() // runs before wg.Wait, so connections are told to stop first
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
			return fmt.Errorf("ssh accept: %w", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.handleConn(ctx, c)
		}()
	}
}

// handleConn runs the handshake, then hands every session channel to its own
// goroutine and rejects any other channel type.
func (s *Server) handleConn(ctx context.Context, c net.Conn) {
	defer logPanic("connection")
	defer func() { _ = c.Close() }()

	_ = c.SetDeadline(time.Now().Add(handshakeTimeout))
	sconn, chans, reqs, err := ssh.NewServerConn(c, s.config)
	if err != nil {
		slog.Info("ssh handshake failed", "remote", c.RemoteAddr(), "err", err)
		return
	}
	_ = c.SetDeadline(time.Time{})
	defer func() { _ = sconn.Close() }()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { // daemon shutdown ends the connection
		<-ctx.Done()
		_ = sconn.Close()
	}()
	go func() { // client gone: stop its git processes
		_ = sconn.Wait()
		cancel()
	}()
	go ssh.DiscardRequests(reqs) // global requests, i.e. port forwarding: refused

	fp := sconn.Permissions.Extensions[extFingerprint]
	slog.Info("ssh authenticated", "remote", c.RemoteAddr(), "key", sconn.Permissions.Extensions[extKeyName], "fingerprint", fp)

	var sessions sync.WaitGroup
	defer sessions.Wait()
	for nc := range chans {
		if nc.ChannelType() != "session" {
			_ = nc.Reject(ssh.Prohibited, "only session channels are allowed")
			continue
		}
		ch, creqs, err := nc.Accept()
		if err != nil {
			continue
		}
		sessions.Add(1)
		go func() {
			defer sessions.Done()
			s.handleSession(ctx, ch, creqs, fp)
		}()
	}
}

// handleSession waits for the one request that starts something. Only exec
// is accepted; shell, pty-req, env, subsystem and the rest are refused.
func (s *Server) handleSession(ctx context.Context, ch ssh.Channel, reqs <-chan *ssh.Request, fp string) {
	defer logPanic("session")
	defer func() { _ = ch.Close() }()
	for req := range reqs {
		if req.Type != "exec" {
			_ = req.Reply(false, nil)
			continue
		}
		var payload struct{ Command string }
		if err := ssh.Unmarshal(req.Payload, &payload); err != nil {
			_ = req.Reply(false, nil)
			continue
		}
		_ = req.Reply(true, nil)
		// Requests that arrive while git runs are refused in the background;
		// leaving them unread would stall the connection's multiplexer.
		go refuseAll(reqs)
		s.exec(ctx, ch, payload.Command, fp)
		return
	}
}

// exec runs the command or explains the refusal on the client's stderr, then
// reports the exit status. The status must be sent before the channel is
// closed or the client never sees it.
func (s *Server) exec(ctx context.Context, ch ssh.Channel, command, fp string) {
	s.active.Add(1)
	defer s.active.Add(-1)
	code, err := s.run(ctx, ch, command, fp)
	if err != nil {
		slog.Warn("ssh exec refused", "fingerprint", fp, "command", command, "err", err)
		_, _ = fmt.Fprintf(ch.Stderr(), "ashore: %v\n", err)
	}
	_, _ = ch.SendRequest("exit-status", false, ssh.Marshal(struct{ Status uint32 }{uint32(code)}))
}

// run returns the refusal reason as the error, or git's exit status once it
// ran. The channel is git's stdin and stdout; extended data is its stderr.
func (s *Server) run(ctx context.Context, ch ssh.Channel, command, fp string) (int, error) {
	service, app, err := parseCommand(command)
	if err != nil {
		return 1, err
	}
	lookupCtx, cancel := context.WithTimeout(ctx, lookupTimeout)
	defer cancel()
	if _, err := s.store.GetApp(lookupCtx, app); err != nil {
		return 1, err
	}
	repo := RepoPath(s.dataDir, app)
	if _, err := os.Stat(repo); err != nil {
		return 1, fmt.Errorf("app %s: repository missing", app)
	}
	slog.Info("git", "service", service, "app", app, "fingerprint", fp)

	cmd := exec.CommandContext(ctx, "git", service, repo)
	cmd.Env = gitEnv("ASHORE_APP=" + app)
	cmd.Stdout = ch
	cmd.Stderr = ch.Stderr()
	// SIGTERM first so git removes its quarantine directory; Go sends SIGKILL
	// after WaitDelay if that did not do it.
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = stopGrace
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return 1, err
	}
	if err := cmd.Start(); err != nil {
		return 1, fmt.Errorf("start git: %w", err)
	}
	// Copy the client's bytes into git ourselves. With cmd.Stdin = ch, Wait
	// would also wait for that copy, which only ends when the client closes
	// its side of the channel.
	go func() {
		_, _ = io.Copy(stdin, ch)
		_ = stdin.Close()
	}()
	err = cmd.Wait()
	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) && !errors.Is(err, exec.ErrWaitDelay) {
		slog.Error("git wait", "app", app, "err", err)
		return 1, nil
	}
	if code := cmd.ProcessState.ExitCode(); code >= 0 {
		return code, nil
	}
	return 1, nil // killed by a signal: the client dropped or the daemon stopped
}

// gitEnv is the child's entire environment: nothing the client sent, only
// what git needs from the daemon. Phase 1.2 adds the hook's variables.
func gitEnv(extra ...string) []string {
	env := []string{"PATH=" + os.Getenv("PATH"), "LC_ALL=C"}
	if home := os.Getenv("HOME"); home != "" {
		env = append(env, "HOME="+home)
	}
	return append(env, extra...)
}

func refuseAll(reqs <-chan *ssh.Request) {
	for req := range reqs {
		_ = req.Reply(false, nil)
	}
}

// logPanic keeps one misbehaving connection from taking the daemon down.
func logPanic(where string) {
	if r := recover(); r != nil {
		slog.Error("ssh panic", "where", where, "panic", r, "stack", string(debug.Stack()))
	}
}
