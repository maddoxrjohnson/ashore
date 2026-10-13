// Command ashore is the command-line client for an ashore daemon. It talks
// to the daemon's HTTP API with the admin token; login stores both in a
// config file so every other command needs only its arguments.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/maddoxrjohnson/ashore/internal/api"
)

var version = "dev"

const usage = `usage: ashore <command> [arguments]

  login <url> <token>              save the daemon's address and admin token
  apps                             list apps
  apps:create <name>               create an app and print its git remote
  apps:destroy <name>              stop and remove an app, its releases, and its repository
  config:get <app>                 show config vars
  config:set <app> KEY=value ...   set config vars and redeploy
  config:unset <app> KEY ...       unset config vars and redeploy
  releases <app>                   list releases, newest first
  releases:info <app> <vN>         show one release
  releases:rollback <app> <vN>     new release from vN's image and config
  ps <app>                         the app's instance, its state, and restarts
  restart <app>                    replace the live release's instance
  keys:add <name> <pubkey-file>    allow a public key to push

Flags: --json on list and show commands; --version.
The daemon address and token come from ASHORE_URL and ASHORE_TOKEN when set,
otherwise from the file login writes (ASHORE_CONFIG, or
$XDG_CONFIG_HOME/ashore/config).`

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr, os.Getenv))
}

// run is main with everything injected: arguments, output, environment.
// It returns the exit status: 0, 1 for a failure, 2 for a usage error.
func run(args []string, stdout, stderr io.Writer, getenv func(string) string) int {
	var asJSON bool
	var rest []string
	for _, a := range args {
		switch a {
		case "--json", "-json":
			asJSON = true
		case "--version", "-version":
			_, _ = fmt.Fprintln(stdout, "ashore", version)
			return 0
		case "-h", "--help", "help":
			_, _ = fmt.Fprintln(stdout, usage)
			return 0
		default:
			rest = append(rest, a)
		}
	}
	if len(rest) == 0 {
		_, _ = fmt.Fprintln(stderr, usage)
		return 2
	}
	c := &cli{out: stdout, json: asJSON, getenv: getenv}
	err := c.dispatch(context.Background(), rest[0], rest[1:])
	switch {
	case err == nil:
		return 0
	case errors.Is(err, errUsage):
		_, _ = fmt.Fprintf(stderr, "ashore: %v\n\n%s\n", err, usage)
		return 2
	default:
		_, _ = fmt.Fprintln(stderr, "ashore:", err)
		return 1
	}
}

var errUsage = errors.New("wrong arguments")

// cli carries what every command needs.
type cli struct {
	out    io.Writer
	json   bool
	getenv func(string) string
}

// settings is the config file login writes.
type settings struct {
	URL   string `json:"url"`
	Token string `json:"token"`
}

// configPath is ASHORE_CONFIG, or ashore/config under the user's config
// directory (XDG_CONFIG_HOME, ~/.config).
func (c *cli) configPath() (string, error) {
	if p := c.getenv("ASHORE_CONFIG"); p != "" {
		return p, nil
	}
	if x := c.getenv("XDG_CONFIG_HOME"); x != "" {
		return filepath.Join(x, "ashore", "config"), nil
	}
	home := c.getenv("HOME")
	if home == "" {
		return "", errors.New("HOME is not set; set ASHORE_CONFIG")
	}
	return filepath.Join(home, ".config", "ashore", "config"), nil
}

// client builds the API client from the environment or the config file.
func (c *cli) client() (*api.Client, error) {
	if url, token := c.getenv("ASHORE_URL"), c.getenv("ASHORE_TOKEN"); url != "" && token != "" {
		return &api.Client{URL: url, Token: token}, nil
	}
	path, err := c.configPath()
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errors.New("not logged in: run ashore login <url> <token>")
		}
		return nil, err
	}
	var s settings
	if err := json.Unmarshal(data, &s); err != nil || s.URL == "" || s.Token == "" {
		return nil, fmt.Errorf("%s: not a login file; run ashore login again", path)
	}
	return &api.Client{URL: s.URL, Token: s.Token}, nil
}

// login checks the token against the daemon, then saves both, readable by
// this user only.
func (c *cli) login(ctx context.Context, url, token string) error {
	client := &api.Client{URL: strings.TrimSuffix(url, "/"), Token: token}
	if _, err := client.Apps(ctx); err != nil {
		return fmt.Errorf("login failed: %w", err)
	}
	path, err := c.configPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, _ := json.MarshalIndent(settings{URL: client.URL, Token: token}, "", "  ")
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(c.out, "logged in to %s; saved to %s\n", client.URL, path)
	return nil
}

// printJSON writes v the way the API sent it, indented.
func (c *cli) printJSON(v any) error {
	enc := json.NewEncoder(c.out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
