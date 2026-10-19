package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/api"
	"github.com/maddoxrjohnson/ashore/internal/logs"
	"github.com/maddoxrjohnson/ashore/internal/runtime"
)

// dispatch runs one command. Arity is checked here so every command body
// can index its arguments.
func (c *cli) dispatch(ctx context.Context, name string, args []string) error {
	want := map[string]int{ // exact argument counts; -1 means one or more after the first
		"login": 2, "apps": 0, "apps:create": 1, "apps:destroy": 1, "config:get": 1,
		"config:set": -1, "config:unset": -1, "releases": 1, "releases:info": 2,
		"releases:rollback": 2, "ps": 1, "restart": 1, "keys:add": 2, "logs": -2,
	}
	if name == "logs" {
		return c.logs(ctx, args)
	}
	n, ok := want[name]
	if !ok {
		return fmt.Errorf("%w: unknown command %q", errUsage, name)
	}
	if (n >= 0 && len(args) != n) || (n == -1 && len(args) < 2) {
		return fmt.Errorf("%w for %s", errUsage, name)
	}
	if name == "login" {
		return c.login(ctx, args[0], args[1])
	}
	client, err := c.client()
	if err != nil {
		return err
	}
	switch name {
	case "apps":
		return c.apps(ctx, client)
	case "apps:create":
		return c.appsCreate(ctx, client, args[0])
	case "apps:destroy":
		if err := client.DestroyApp(ctx, args[0]); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(c.out, "destroyed %s\n", args[0])
		return nil
	case "config:get":
		return c.configGet(ctx, client, args[0])
	case "config:set":
		set := make(map[string]string, len(args)-1)
		for _, p := range args[1:] {
			k, v, ok := strings.Cut(p, "=")
			if !ok || k == "" {
				return fmt.Errorf("%q is not KEY=value", p)
			}
			set[k] = v
		}
		return client.UpdateConfig(ctx, args[0], api.ConfigChange{Set: set}, c.out)
	case "config:unset":
		return client.UpdateConfig(ctx, args[0], api.ConfigChange{Unset: args[1:]}, c.out)
	case "releases":
		return c.releases(ctx, client, args[0])
	case "releases:info":
		return c.releaseInfo(ctx, client, args[0], args[1])
	case "releases:rollback":
		return client.Rollback(ctx, args[0], args[1], c.out)
	case "ps":
		return c.ps(ctx, client, args[0])
	case "restart":
		return client.Restart(ctx, args[0], c.out)
	case "keys:add":
		return c.keysAdd(ctx, client, args[0], args[1])
	}
	return fmt.Errorf("%w: unknown command %q", errUsage, name)
}

// logs prints an app's recent lines and, with -f, keeps printing until
// interrupted. -n sets how many recent lines to start with.
func (c *cli) logs(ctx context.Context, args []string) error {
	var app string
	follow, tail := false, api.DefaultTail
	for i := 0; i < len(args); i++ {
		switch a := args[i]; a {
		case "-f", "--follow":
			follow = true
		case "-n", "--tail":
			if i+1 >= len(args) {
				return fmt.Errorf("%w: %s needs a number", errUsage, a)
			}
			n, err := strconv.Atoi(args[i+1])
			if err != nil || n < 0 {
				return fmt.Errorf("%w: %s needs a non-negative number", errUsage, a)
			}
			tail = n
			i++
		default:
			if app != "" || strings.HasPrefix(a, "-") {
				return fmt.Errorf("%w for logs", errUsage)
			}
			app = a
		}
	}
	if app == "" {
		return fmt.Errorf("%w for logs", errUsage)
	}
	client, err := c.client()
	if err != nil {
		return err
	}
	if follow {
		var stop context.CancelFunc
		ctx, stop = signal.NotifyContext(ctx, os.Interrupt)
		defer stop()
	}
	enc := json.NewEncoder(c.out)
	return client.Logs(ctx, app, tail, follow, func(l logs.Line) error {
		if c.json {
			return enc.Encode(l)
		}
		_, err := fmt.Fprintln(c.out, formatLine(l))
		return err
	})
}

// formatLine is one log line for a terminal: when, which release and
// stream, and the text. Lines the daemon adds (dropped-line notices) name
// the daemon instead of a release.
func formatLine(l logs.Line) string {
	who := l.Release
	if l.Stream == logs.StreamSystem {
		who = "ashore"
	}
	return l.Time.Local().Format("2006-01-02T15:04:05.000") + " " + who + "[" + l.Stream + "]: " + l.Text
}

func (c *cli) table(write func(tw *tabwriter.Writer)) error {
	tw := tabwriter.NewWriter(c.out, 0, 0, 2, ' ', 0)
	write(tw)
	return tw.Flush()
}

func local(t time.Time) string { return t.Local().Format(time.DateTime) }

func (c *cli) apps(ctx context.Context, client *api.Client) error {
	apps, err := client.Apps(ctx)
	if err != nil {
		return err
	}
	if c.json {
		return c.printJSON(apps)
	}
	return c.table(func(tw *tabwriter.Writer) {
		_, _ = fmt.Fprintln(tw, "NAME\tHOST\tLIVE\tCREATED")
		for _, a := range apps {
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", a.Name, a.Host, orDash(a.Live), local(a.CreatedAt))
		}
	})
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func (c *cli) appsCreate(ctx context.Context, client *api.Client, name string) error {
	a, err := client.CreateApp(ctx, name)
	if err != nil {
		return err
	}
	if c.json {
		return c.printJSON(a)
	}
	_, _ = fmt.Fprintf(c.out, "created %s\n  git remote add ashore %s\n  git push ashore main\n", a.Name, a.Remote)
	return nil
}

// configGet prints KEY=value lines, sorted, the way a shell shows them.
func (c *cli) configGet(ctx context.Context, client *api.Client, app string) error {
	vars, err := client.Config(ctx, app)
	if err != nil {
		return err
	}
	if c.json {
		return c.printJSON(vars)
	}
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		_, _ = fmt.Fprintf(c.out, "%s=%s\n", k, vars[k])
	}
	return nil
}

// releases lists newest first, the way a deploy history is read.
func (c *cli) releases(ctx context.Context, client *api.Client, app string) error {
	rels, err := client.Releases(ctx, app)
	if err != nil {
		return err
	}
	if c.json {
		return c.printJSON(rels)
	}
	return c.table(func(tw *tabwriter.Writer) {
		_, _ = fmt.Fprintln(tw, "VERSION\tSTATUS\tCREATED\tNOTE")
		for i := len(rels) - 1; i >= 0; i-- {
			r := rels[i]
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", r.Version, r.Status, local(r.CreatedAt), r.Note)
		}
	})
}

func (c *cli) releaseInfo(ctx context.Context, client *api.Client, app, version string) error {
	r, err := client.Release(ctx, app, version)
	if err != nil {
		return err
	}
	if c.json {
		return c.printJSON(r)
	}
	keys := make([]string, 0, len(r.Config))
	for k := range r.Config {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return c.table(func(tw *tabwriter.Writer) {
		_, _ = fmt.Fprintf(tw, "release:\t%s\napp:\t%s\nstatus:\t%s\nimage:\t%s\ncommit:\t%s\ncreated:\t%s\nnote:\t%s\n",
			r.Version, app, r.Status, r.Image, r.Commit, local(r.CreatedAt), r.Note)
		if len(keys) == 0 {
			_, _ = fmt.Fprintln(tw, "config:\t(none)")
		}
		for i, k := range keys {
			label := ""
			if i == 0 {
				label = "config:"
			}
			_, _ = fmt.Fprintf(tw, "%s\t%s=%s\n", label, k, r.Config[k])
		}
		_, _ = fmt.Fprintf(tw, "manifest:\t%s\n", r.Manifest)
	})
}

func (c *cli) ps(ctx context.Context, client *api.Client, app string) error {
	ps, err := client.PS(ctx, app)
	if err != nil {
		return err
	}
	if c.json {
		return c.printJSON(ps)
	}
	if len(ps.Instances) == 0 {
		_, _ = fmt.Fprintf(c.out, "%s: no instances since the daemon started\n", app)
		return nil
	}
	return c.table(func(tw *tabwriter.Writer) {
		_, _ = fmt.Fprintln(tw, "INSTANCE\tRELEASE\tSTATE\tRESTARTS\tUPTIME\tADDR\tDETAIL")
		for _, i := range ps.Instances {
			uptime := "-"
			if i.State == "running" && !i.StartedAt.IsZero() {
				uptime = time.Since(i.StartedAt).Truncate(time.Second).String()
			}
			_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\t%s\n", orDash(runtime.Short(i.Instance)), orDash(i.Release), i.State, i.Restarts, uptime, orDash(i.Addr), i.Detail)
		}
	})
}

// keysAdd sends the first line of an authorized_keys-style file.
func (c *cli) keysAdd(ctx context.Context, client *api.Client, name, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	line, _, _ := strings.Cut(strings.TrimSpace(string(data)), "\n")
	k, err := client.AddKey(ctx, name, line)
	if err != nil {
		return err
	}
	if c.json {
		return c.printJSON(k)
	}
	_, _ = fmt.Fprintf(c.out, "added key %s (%s)\n", k.Name, k.Fingerprint)
	return nil
}
