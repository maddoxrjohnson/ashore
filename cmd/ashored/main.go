// Command ashored is the ashore daemon: it receives git pushes over SSH,
// builds and runs them, and serves them by hostname. It is also, under
// other names, its own pre-receive hook and its own container init.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/maddoxrjohnson/ashore/internal/config"
	"github.com/maddoxrjohnson/ashore/internal/daemon"
	"github.com/maddoxrjohnson/ashore/internal/hook"
	"github.com/maddoxrjohnson/ashore/internal/runtime/native"
)

var version = "dev"

func main() {
	// "ashored hook ..." is the pre-receive hook git runs. It has its own exit
	// codes and talks to the user itself, so it never goes through run.
	if len(os.Args) > 1 && os.Args[1] == "hook" {
		os.Exit(hook.Run(os.Args[2:], os.Stdin, os.Stdout, os.Stderr, os.Getenv))
	}
	// "ashored runtime-init" is the native runtime's container init: the
	// daemon re-executed into new namespaces, which builds the container's
	// filesystem and execs the app.
	if len(os.Args) > 1 && os.Args[1] == "runtime-init" {
		os.Exit(native.Init(os.Args[2:]))
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ashored:", err)
		os.Exit(1)
	}
}

func run() error {
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println("ashored", version)
		return nil
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	if flag.NArg() > 0 {
		return fmt.Errorf("unexpected arguments %q; the ashore command talks to the daemon", flag.Args())
	}
	log := cfg.Logger(os.Stderr)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return daemon.Run(ctx, cfg, version, log)
}
