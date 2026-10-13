package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/maddoxrjohnson/ashore/internal/api"
	"github.com/maddoxrjohnson/ashore/internal/config"
	"github.com/maddoxrjohnson/ashore/internal/gitserver"
	"github.com/maddoxrjohnson/ashore/internal/store"
)

// Admin subcommands run against the data directory directly, with or
// without the daemon running (SQLite's WAL mode allows both). They exist so
// an app can be created and a key registered before the HTTP API and the
// ashore CLI exist; those will replace them.
const adminUsage = `usage:
  ashored apps create <name>            create an app and its repository
  ashored keys add <name> <pubkey>      allow a public key file to push
  ashored releases list <app>           list an app's releases, newest first
  ashored releases info <app> <vN>      show one release
  ashored config get <app>              show config vars
  ashored config set <app> K=V ...      set config vars and redeploy (daemon must be running)
  ashored config unset <app> K ...      unset config vars and redeploy`

var (
	errUsage   = errors.New(adminUsage)
	errNotPair = errors.New("expected KEY=value")
)

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
	case "releases list":
		if len(args) != 3 {
			return errUsage
		}
		return listReleases(ctx, st, args[2], out)
	case "releases info":
		if len(args) != 4 {
			return errUsage
		}
		return releaseInfo(ctx, st, args[2], args[3], out)
	case "config get":
		if len(args) != 3 {
			return errUsage
		}
		return configGet(ctx, cfg, args[2], out)
	case "config set":
		if len(args) < 4 {
			return errUsage
		}
		return configSet(ctx, cfg, args[2], args[3:], out)
	case "config unset":
		if len(args) < 4 {
			return errUsage
		}
		return configUnset(ctx, cfg, args[2], args[3:], out)
	}
	return errUsage
}

// apiClient talks to the running daemon with the token it wrote. Config
// changes have to go through the daemon because it owns the deploy.
func apiClient(cfg config.Config) (*api.Client, error) {
	token, err := api.ReadToken(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	host, port, err := net.SplitHostPort(cfg.InternalAddr)
	if err != nil {
		return nil, fmt.Errorf("ASHORE_INTERNAL_ADDR: %w", err)
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return &api.Client{URL: "http://" + net.JoinHostPort(host, port), Token: token}, nil
}

// configGet prints KEY=value lines, sorted, the way a shell would show them.
func configGet(ctx context.Context, cfg config.Config, app string, out io.Writer) error {
	c, err := apiClient(cfg)
	if err != nil {
		return err
	}
	vars, err := c.Config(ctx, app)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, _ = fmt.Fprintf(out, "%s=%s\n", k, vars[k])
	}
	return nil
}

// configSet parses KEY=value arguments; the value may be empty or contain
// more '=' signs.
func configSet(ctx context.Context, cfg config.Config, app string, pairs []string, out io.Writer) error {
	set := make(map[string]string, len(pairs))
	for _, p := range pairs {
		k, v, ok := strings.Cut(p, "=")
		if !ok || k == "" {
			return fmt.Errorf("%w: %q", errNotPair, p)
		}
		set[k] = v
	}
	c, err := apiClient(cfg)
	if err != nil {
		return err
	}
	return c.UpdateConfig(ctx, app, api.ConfigChange{Set: set}, out)
}

func configUnset(ctx context.Context, cfg config.Config, app string, keys []string, out io.Writer) error {
	c, err := apiClient(cfg)
	if err != nil {
		return err
	}
	return c.UpdateConfig(ctx, app, api.ConfigChange{Unset: keys}, out)
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

// listReleases prints one line per release, newest first, the way a deploy
// history is read.
func listReleases(ctx context.Context, st *store.Store, name string, out io.Writer) error {
	app, err := st.GetApp(ctx, name)
	if err != nil {
		return err
	}
	rels, err := st.ListReleases(ctx, app.ID)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "VERSION\tSTATUS\tCREATED\tNOTE")
	for i := len(rels) - 1; i >= 0; i-- {
		r := rels[i]
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", r.Name(), r.Status, r.CreatedAt.Local().Format(time.DateTime), r.Note)
	}
	return tw.Flush()
}

// releaseInfo prints everything recorded about one release. The config and
// manifest snapshots are shown as stored.
func releaseInfo(ctx context.Context, st *store.Store, name, version string, out io.Writer) error {
	app, err := st.GetApp(ctx, name)
	if err != nil {
		return err
	}
	v, err := store.ParseVersion(version)
	if err != nil {
		return err
	}
	r, err := st.ReleaseByVersion(ctx, app.ID, v)
	if err != nil {
		return fmt.Errorf("%s: %w", name, err)
	}
	tw := tabwriter.NewWriter(out, 0, 0, 1, ' ', 0)
	for _, kv := range [][2]string{
		{"release", r.Name()},
		{"app", app.Name},
		{"status", r.Status},
		{"image", r.Image},
		{"commit", r.GitSHA},
		{"created", r.CreatedAt.Local().Format(time.DateTime)},
		{"note", r.Note},
		{"config", r.ConfigJSON},
		{"manifest", r.ManifestJSON},
	} {
		_, _ = fmt.Fprintf(tw, "%s:\t%s\n", kv[0], kv[1])
	}
	return tw.Flush()
}
