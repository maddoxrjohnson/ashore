// Package hook is the pre-receive hook git runs for every push, and the
// unix-socket protocol it uses to hand the push to the daemon.
//
// git runs <DATA_DIR>/hooks/pre-receive, a two-line shell script that execs
// "ashored hook pre-receive" (InstallScript writes it). The hook reads git's
// "<old> <new> <ref>" lines, insists on exactly one update to the deploy
// branch, connects to the socket named by ASHORE_SOCK, and sends one JSON
// line, a Request. The daemon answers with lines that each start with a band
// byte: bandOutput lines are shown to the user (git wraps the hook's stdout
// in sideband, so they appear as "remote: ..."), and one final bandStatus
// line carries the exit code the hook must end with. git moves the ref only
// when that code is 0. The shape follows git's own sideband, and doing the
// work in the daemon rather than the hook is the Gitea design: the daemon
// owns the Docker client, the store, and the locks; the hook owns nothing.
package hook

import (
	"fmt"
	"io"
)

// Request is the one message the hook sends. Quarantine is GIT_QUARANTINE_PATH
// verbatim: while pre-receive runs the pushed objects live there, not in the
// repository, and the builder has to be told where to look. Nonce is the
// per-push secret the daemon put in git's environment; without it any local
// process could trigger a deploy.
type Request struct {
	App        string `json:"app"`
	SHA        string `json:"sha"`
	Quarantine string `json:"quarantine"`
	Nonce      string `json:"nonce"`
}

// Response bands, the first byte of every line the daemon sends. Numbered like
// git's sideband so a raw capture reads the same way.
const (
	bandOutput byte = '1' // one line of text for the user
	bandStatus byte = '2' // the hook's exit code in decimal; always the last line
)

// writeLine frames one response line.
func writeLine(w io.Writer, band byte, text string) error {
	_, err := fmt.Fprintf(w, "%c%s\n", band, text)
	return err
}
