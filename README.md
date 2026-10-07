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
`X-Request-Id` that also appears in the access log. Zero-downtime swaps, crash restarts,
config vars, the CLI, and restoring routes after a daemon restart are next.

## Deploying an app locally

With Docker running and the daemon started (`make run`, text logs on stderr):

    ./bin/ashored apps create hello           # app row, bare repo, prints the remote
    ./bin/ashored keys add me ~/.ssh/id_ed25519.pub
    cd examples/hello-go
    git init -b main && git add -A && git commit -m "first"
    git remote add ashore ssh://ashore@localhost:2222/hello
    git push ashore main
    curl http://hello.localhost:8080/

The push prints the build steps, `release v1`, and `live at http://hello.localhost:8080`.
Change the string in `main.go`, commit, push again, and curl shows the new one. Break the
`Dockerfile`, push, and the push is rejected while the old version keeps serving.

The build context is `git archive` of the pushed commit, so only tracked files reach the
build and `.dockerignore` has no effect. To keep tracked files out of the image, mark them
`export-ignore` in `.gitattributes`.

The admin subcommands (`apps create`, `keys add`) are a stopgap until the HTTP API and the
`ashore` CLI exist. A daemon restart does not yet re-attach the containers it started;
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
