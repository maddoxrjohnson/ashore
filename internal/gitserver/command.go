package gitserver

import (
	"fmt"
	"strings"

	"github.com/maddoxrjohnson/ashore/internal/store"
)

// services maps the program a git client asks for to the subcommand the
// daemon runs.
var services = map[string]string{
	"git-receive-pack": "receive-pack", // git push
	"git-upload-pack":  "upload-pack",  // git clone, git fetch
}

// parseCommand accepts exactly what a git client sends over SSH:
// "git-receive-pack 'hello'" for the remote host:hello, or with a leading
// slash for ssh://host:port/hello, optionally ending in .git. Anything else
// is refused. The app name must pass store.ValidateName, so the repository
// path can never leave the repos directory.
func parseCommand(raw string) (service, app string, err error) {
	name, arg, ok := strings.Cut(strings.TrimSpace(raw), " ")
	if !ok {
		return "", "", fmt.Errorf("unsupported command %q", raw)
	}
	service, ok = services[name]
	if !ok {
		return "", "", fmt.Errorf("unsupported command %q: only git push and git fetch are allowed", name)
	}
	arg = strings.TrimSpace(arg)
	if len(arg) >= 2 && arg[0] == '\'' && arg[len(arg)-1] == '\'' {
		arg = arg[1 : len(arg)-1]
	}
	if arg == "" || strings.ContainsAny(arg, "'\" \t\\") {
		return "", "", fmt.Errorf("bad repository path %q", arg)
	}
	app = strings.TrimSuffix(strings.TrimPrefix(arg, "/"), ".git")
	if err := store.ValidateName(app); err != nil {
		return "", "", err
	}
	return service, app, nil
}
