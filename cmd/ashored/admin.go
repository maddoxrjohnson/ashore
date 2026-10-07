package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/crypto/ssh"

	"github.com/maddoxrjohnson/ashore/internal/config"
	"github.com/maddoxrjohnson/ashore/internal/gitserver"
	"github.com/maddoxrjohnson/ashore/internal/store"
)

// Admin subcommands run against the data directory directly, with or
// without the daemon running (SQLite's WAL mode allows both). They exist so
// an app can be created and a key registered before the HTTP API and the
// ashore CLI exist; those will replace them.
const adminUsage = `usage:
  ashored apps create <name>          create an app and its repository
  ashored keys add <name> <pubkey>    allow a public key file to push`

var errUsage = errors.New(adminUsage)

func admin(ctx context.Context, cfg config.Config, args []string, out io.Writer) error {
	if len(args) < 2 {
		return errUsage
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return fmt.Errorf("data dir: %w", err)
	}
	st, err := store.Open(ctx, filepath.Join(cfg.DataDir, "ashore.db"))
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	switch args[0] + " " + args[1] {
	case "apps create":
		if len(args) != 3 {
			return errUsage
		}
		return createApp(ctx, st, cfg, args[2], out)
	case "keys add":
		if len(args) != 4 {
			return errUsage
		}
		return addKey(ctx, st, args[2], args[3], out)
	}
	return errUsage
}

// createApp inserts the row, then creates the bare repository. A repository
// that cannot be created takes the row with it, so the two never disagree.
func createApp(ctx context.Context, st *store.Store, cfg config.Config, name string, out io.Writer) error {
	app, err := st.CreateApp(ctx, name, name+"."+cfg.Domain)
	if err != nil {
		return err
	}
	if _, err := gitserver.InitRepo(ctx, cfg.DataDir, name); err != nil {
		_ = st.DeleteApp(ctx, name)
		return err
	}
	_, _ = fmt.Fprintf(out, "created %s\n", app.Name)
	_, _ = fmt.Fprintf(out, "  git remote add ashore %s\n", remoteURL(cfg, name))
	_, _ = fmt.Fprintln(out, "  git push ashore main")
	return nil
}

// remoteURL is what a pusher adds as a remote: the SSH listener's host,
// or the domain when it listens on every interface.
func remoteURL(cfg config.Config, app string) string {
	host, port, err := net.SplitHostPort(cfg.SSHAddr)
	if err != nil {
		return "ssh://ashore@" + cfg.Domain + "/" + app
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = cfg.Domain
	}
	return "ssh://ashore@" + net.JoinHostPort(host, port) + "/" + app
}

// addKey registers the first key in an authorized_keys-style file.
func addKey(ctx context.Context, st *store.Store, name, path string, out io.Writer) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	pub, _, _, _, err := ssh.ParseAuthorizedKey(data)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	fp := ssh.FingerprintSHA256(pub)
	// Store the key as the file had it, minus the comment: one canonical
	// line the SSH server can parse back and compare byte for byte.
	line := pub.Type() + " " + strings.TrimSpace(strings.TrimPrefix(string(ssh.MarshalAuthorizedKey(pub)), pub.Type()))
	if _, err := st.AddKey(ctx, name, fp, line); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "added key %s (%s)\n", name, fp)
	return nil
}
