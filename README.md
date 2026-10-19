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
release of the same image and redeploys it. A deploy drops no requests: the new instance
gets traffic only after its health check passes, the route moves in one atomic swap, and
the old instance keeps running for a drain period before it is stopped. An instance that
exits on its own is restarted with exponential backoff (1, 2, 4, up to 60 seconds) and
given up on after ten failures in ten minutes, which `ps` reports. The `ashore` command
talks to the daemon's API: apps, config, releases, rollback, restart, `ps`, keys. A daemon
restart changes nothing for the apps: containers keep serving while it is down, and on start
it reconciles what is running with what should be before it opens a single port. Every
line an instance prints is collected once, stamped, appended to a per-app log file, and
kept in memory for the readers that `logs -f` and the dashboard add next.

## Zero-downtime deploys, measured

`examples/slowstart` sleeps ten seconds before it listens and shuts down cleanly on SIGTERM.
With `scripts/loadloop` sending 200 requests a second through the router and
`scripts/deploys.sh` pushing ten deploys back to back:

    requests=24747 ok=24747 failed=0 (none) distinct bodies=11 p50=730µs p99=1.85ms max=15.7ms

Zero failed requests in 24,747 across ten deploys (measured 2026-10-12 on the Docker
runtime in WSL2). Eleven distinct bodies means every release answered during the run, so
the swaps happened; the daemon log shows eleven route swaps and ten old instances stopped
after the five-second drain. To reproduce:

    ./bin/ashored apps create slowstart
    # push examples/slowstart once, then:
    go run ./scripts/loadloop -host slowstart.localhost -rate 200 &
    sh scripts/deploys.sh /path/to/slowstart-checkout 10
    kill -INT %1; wait

`ASHORE_DRAIN_GRACE` (default `5s`) is how long a replaced instance keeps running after the
route moved away from it. The app's own `[process] grace` is the SIGTERM-to-SIGKILL window
once the stop begins.

## Crashes

A watcher per instance waits for its process to exit. An exit the daemon did not ask for
schedules a restart of the same release after 1 s, then 2, 4, 8, 16, 32, and 60 s for later
failures within a ten-minute window; the tenth failure in the window marks the app
`crashed` and nothing more is tried until a restart is asked for. The route stays where it
was, so requests to a crashed app get a 502 with a request id rather than a 404.
`examples/crasher` exits two seconds after it starts when `CRASH_AFTER=2s` is set, and
`ashore ps crasher` shows the state, the restart count, and when the next attempt is due.
`ashore restart crasher` ends a crash loop once its cause is fixed.

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

With Docker running and the daemon started (`make run`, text logs on stderr), log the CLI
in with the token the daemon wrote on its first start, then create an app and push:

    ./bin/ashore login http://127.0.0.1:9000 $(cat data/admin.token)
    ./bin/ashore apps:create hello            # app row, bare repo, prints the remote
    ./bin/ashore keys:add me ~/.ssh/id_ed25519.pub
    cd examples/hello-go
    git init -b main && git add -A && git commit -m "first"
    git remote add ashore ssh://ashore@localhost:2222/hello
    git push ashore main
    curl http://hello.localhost:8080/

The push prints `release v1`, the build steps, and `live at http://hello.localhost:8080`.
With an `ashore.toml` in the repository the push also prints the settings it read.
Change the string in `main.go`, commit, push again, and curl shows the new one. Break the
`Dockerfile`, push, and the push is rejected while the old version keeps serving.
`ashore releases hello` shows the history, the rejected push included as a `failed`
release; `ashore releases:info hello v2` shows one release's image, commit, config, and
manifest.

Config vars are set through the daemon, which redeploys the live release with the new
environment and streams the deploy to your terminal:

    ./bin/ashore config:set hello GREETING=hi       # release v3, same image, new env
    ./bin/ashore config:get hello                   # GREETING=hi
    ./bin/ashore config:unset hello GREETING        # release v4

`examples/config-reader` answers every request with `$GREETING`, so a curl after each
command shows the change. The daemon sets `PORT` itself and refuses it as a config var.
Values are never written to the daemon's log; `releases:info` shows the snapshot a release
ran with.

A rollback is a new release made from an old one's image, config, and manifest, so the
history never rewrites itself; a restart replaces the live release's instance and clears a
crash loop:

    ./bin/ashore releases:rollback hello v2         # release v5 (rollback v2)
    ./bin/ashore restart hello
    ./bin/ashore ps hello                           # instance, state, restarts, uptime
    ./bin/ashore apps:destroy hello                 # stops it, removes rows and repository

Every list and show command takes `--json`. `ashore login` stores the daemon's address and
token in `$XDG_CONFIG_HOME/ashore/config` (or `ASHORE_CONFIG`); `ASHORE_URL` and
`ASHORE_TOKEN` override it, which is how scripts and CI use the CLI.

The daemon's API listens on `ASHORE_INTERNAL_ADDR` (default `127.0.0.1:9000`) and requires
the admin token it writes to `data/admin.token` on first start, as a bearer token. It is
never exposed on the router's port; from another machine, reach it through an SSH tunnel.

The build context is `git archive` of the pushed commit, so only tracked files reach the
build and `.dockerignore` has no effect. To keep tracked files out of the image, mark them
`export-ignore` in `.gitattributes`.

## Logs

A collector goroutine per instance reads the container's output from the moment it
starts, splits it into lines at newlines (chunks from Docker end wherever the read did,
so a partial line waits for the rest), stamps each with the app, release, instance, stream,
and the time it arrived, and appends it to `data/logs/<app>/current.log` (rotated at
50 MB, five kept) and to a ring of the app's last 1000 lines in memory. Lines printed while
a release was still failing its health check are on record too. Readers subscribe to the
ring: each gets a bounded channel, sends to it never block, and a reader that falls behind
loses lines and is then handed one line saying how many, so a stalled tab can never slow
the app or the collector.

    ./bin/ashore logs chatty              # the last 100 lines
    ./bin/ashore logs -f -n 10 chatty     # ten lines, then follow until Ctrl-C
    ./bin/ashore logs --json chatty       # one JSON object per line

`examples/chatty` prints a line a second, so `logs -f` has something to show. The API
serves the same stream as server-sent events at `GET /api/v1/apps/<app>/logs?tail=N&follow=1`,
one `line` event per line with the line as JSON, and a `done` event when the app is
destroyed; the dashboard reads it with `EventSource`.

## Metrics

The internal listener exposes Prometheus metrics at `/metrics` and a readiness check at
`/healthz` (503 until every listener is open, then `ok`). The series are:

| Metric | Type | Labels |
|---|---|---|
| `ashore_http_requests_total` | counter | `app`, `status` |
| `ashore_http_request_duration_seconds` | histogram | `app` |
| `ashore_http_upstream_errors_total` | counter | `app` |
| `ashore_deploys_total` | counter | `app`, `result` (`live`, `failed`) |
| `ashore_deploy_duration_seconds` | histogram | `stage` (`build`, `start`, `health`, `swap`) |
| `ashore_instances` | gauge | `app`, `status` |
| `ashore_instance_restarts_total` | counter | `app` |
| `ashore_log_lines_total` | counter | `app` |
| `ashore_log_lines_dropped_total` | counter | `app` |
| `ashore_build_info` | gauge | `version`, `go`, `runtime` |

plus the Go runtime and process collectors. Requests that match no route count under an
empty `app`. The router, supervisor, and log hub report through small observer interfaces,
so none of them imports the Prometheus client.

## Dashboard

The internal listener also serves a small dashboard at `http://127.0.0.1:9000/`: log in
with the admin token, see the apps, and open one for its instance, releases, config vars
(masked until revealed), build logs, and a live log view. It is server-rendered
`html/template` with a few dozen lines of JavaScript for the log stream and the reveal
buttons, and it is read-only: changes go through the CLI.

## Daemon restarts

The containers belong to Docker, not to the daemon, so they keep serving while `ashored` is
stopped. On start the daemon lists the containers it labelled and compares them with each
app's live release: a running instance of the live release is adopted (route restored,
crash watcher started), anything else running is an orphan and is stopped, exited containers
are removed, a live release with nothing running is started again, and a release left
`building` or `starting` by a cut-off deploy is marked `failed`. One log line reports the
counts:

    msg=reconciled adopted=1 restarted=0 orphans=0 removed=0 failed=0

This runs before any listener opens, so there is no window of 404s. Restart counts live in
memory and start over with the daemon; `ps` shows that.

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
