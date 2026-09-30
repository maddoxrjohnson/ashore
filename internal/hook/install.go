package hook

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/maddoxrjohnson/ashore/internal/gitserver"
)

// InstallScript writes <dataDir>/hooks/pre-receive, the program git runs for
// every push into every app repository (their core.hooksPath points at that
// directory). It is two lines of sh that exec this binary in hook mode, so
// the hook is Go, tested like any package, and never out of step with the
// daemon. Rewritten on every start because the binary's path changes between
// builds and installs.
func InstallScript(dataDir string) error {
	dir := gitserver.HooksPath(dataDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	// Single quotes: sh takes the path literally, whatever it contains.
	script := fmt.Sprintf("#!/bin/sh\nexec '%s' hook pre-receive \"$@\"\n", exe)
	path := filepath.Join(dir, "pre-receive")
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		return err
	}
	// WriteFile keeps the old mode when the file already existed, and git
	// silently skips a hook that is not executable.
	return os.Chmod(path, 0o755)
}
