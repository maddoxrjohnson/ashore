# ashore

[![ci](https://github.com/maddoxrjohnson/ashore/actions/workflows/ci.yml/badge.svg)](https://github.com/maddoxrjohnson/ashore/actions/workflows/ci.yml)

A small self-hosted platform I am building so that `git push` is the whole deploy: the
server receives the push, builds an image, starts the container, routes traffic to it by
hostname, and swaps releases without dropping requests. One Go binary, SQLite for state,
Docker underneath, and later my own container runtime as a second backend.

Work in progress. Today a push deploys. The daemon accepts `git push` over SSH with public
keys from its own key table, keeps one bare repository per app, and runs nothing except
`git receive-pack` and `git upload-pack`. A pre-receive hook hands each push to the daemon
over a unix socket, proving it belongs to that push with a one-time nonce. The daemon builds
the commit into an image through the Docker Engine API, records a numbered release, starts
the image as a container published on 127.0.0.1 only (memory, CPU, and process limits,
`no-new-privileges`), waits until the app holds a TCP connection on its port, points the
reverse proxy at it, stops the previous container, and marks the release live. Every line of
that streams back to the pusher's terminal; a build that fails or an app that never listens
rejects the push and the previous release keeps serving. The proxy routes by hostname from
a copy-on-write table, so requests never take a lock, and every response carries an
`X-Request-Id` that also appears in the access log. Config vars are per-app environment
variables: each release snapshots them when it is created, and changing one makes a new
release of the same image and redeploys it. Zero-downtime swaps, crash restarts, the CLI,
and restoring routes after a daemon restart are next.

An optional `ashore.toml` at the root of the repository tunes the deploy. Every value is
optional; this is the full set with the defaults:

    [health]
    path = "/healthz"     # GET must return 2xx; without it, the check is a held TCP connection
    timeout = "30s"       # how long a new instance has to pass the check
    interval = "250ms"    # how often to probe

    [process]
    grace = "10s"         # SIGTERM to SIGKILL when an instance is stopped
    memory = "256m"       # memory limit (k, m, g, or bytes)
    cpu = "1.0"           # CPU quota

The effective values are stored with each release, so a rollback or a config change runs
with the settings of the release it reuses. A file that does not parse, or names a key the
daemon does not know, rejects the push with the error as the last line.

## Deploying an app locally

With Docker running and the daemon started (`make run`, text logs on stderr):

    ./bin/ashored apps create hello           # app row, bare repo, prints the remote
    ./bin/ashored keys add me ~/.ssh/id_ed25519.pub
    cd examples/hello-go
    git init -b main && git add -A && git commit -m "first"
    git remote add ashore ssh://ashore@localhost:2222/hello
    git push ashore main
    curl http://hello.localhost:8080/

The push prints `release v1`, the build steps, and `live at http://hello.localhost:8080`.
With an `ashore.toml` in the repository the push also prints the settings it read.
Change the string in `main.go`, commit, push again, and curl shows the new one. Break the
`Dockerfile`, push, and the push is rejected while the old version keeps serving.
`./bin/ashored releases list hello` shows the history, the rejected push included as a
`failed` release; `releases info hello v2` shows one release's image, commit, config, and
manifest.

Config vars are set through the daemon, which redeploys the live release with the new
environment and streams the deploy to your terminal:

    ./bin/ashored config set hello GREETING=hi      # release v3, same image, new env
    ./bin/ashored config get hello                  # GREETING=hi
    ./bin/ashored config unset hello GREETING       # release v4

`examples/config-reader` answers every request with `$GREETING`, so a curl after each
command shows the change. The daemon sets `PORT` itself and refuses it as a config var.
Values are never written to the daemon's log; `releases info` shows the snapshot a release
ran with.

The daemon's own API listens on `ASHORE_INTERNAL_ADDR` (default `127.0.0.1:9000`) and
requires the admin token it writes to `data/admin.token` on first start, as a bearer
token. The admin subcommands read that file; the `ashore` CLI will do the same over the
network.

The build context is `git archive` of the pushed commit, so only tracked files reach the
build and `.dockerignore` has no effect. To keep tracked files out of the image, mark them
`export-ignore` in `.gitattributes`.

The admin subcommands (`apps create`, `keys add`, `releases list`, `releases info`,
`config get/set/unset`) are a stopgap until the `ashore` CLI exists. A daemon restart does
not yet re-attach the containers it started;
until reconciliation lands, `docker ps --filter label=ashore.app` shows what is running.

## Build and run

Go 1.27 or newer and make.

    make build      # bin/ashored (daemon) and bin/ashore (CLI)
    make test       # go test -race
    make lint       # go vet and golangci-lint
    make run        # start the daemon locally with text logs

`./bin/ashored --version` prints the version stamped at build time.

## Configuration

Every setting is an environment variable prefixed `ASHORE_`. The full list with defaults is
in `internal/config/config.go`. A bad value stops the daemon at startup with the variable
named in the error.

## License

MIT, see LICENSE.
