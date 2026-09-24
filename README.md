# ashore

[![ci](https://github.com/maddoxrjohnson/ashore/actions/workflows/ci.yml/badge.svg)](https://github.com/maddoxrjohnson/ashore/actions/workflows/ci.yml)

A small self-hosted platform I am building so that `git push` is the whole deploy: the
server receives the push, builds an image, starts the container, routes traffic to it by
hostname, and swaps releases without dropping requests. One Go binary, SQLite for state,
Docker underneath, and later my own container runtime as a second backend.

Work in progress. Today the daemon loads its configuration, opens its SQLite database and
brings the schema up to date, and accepts `git push` over SSH: it authenticates with public
keys from its own key table, keeps one bare repository per app, and runs nothing except
`git receive-pack` and `git upload-pack`. Shells, terminals, subsystems, and port forwarding
are refused. It shuts down cleanly on SIGINT or SIGTERM, ending any push in progress. The
deploy hook, builder, and router come next.

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
