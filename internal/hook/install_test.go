package hook

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallScript(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "hooks", "pre-receive")

	// A leftover from an older daemon with a bad mode must be fixed, not kept.
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("stale\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := InstallScript(dir); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o755 {
		t.Errorf("mode = %o, want 755", fi.Mode().Perm())
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(body), "#!/bin/sh\n") {
		t.Errorf("script %q has no sh shebang", body)
	}
	for _, want := range []string{"exec '" + exe + "'", "hook pre-receive", `"$@"`} {
		if !strings.Contains(string(body), want) {
			t.Errorf("script %q does not contain %q", body, want)
		}
	}
	if strings.Contains(string(body), "stale") {
		t.Error("old script content survived")
	}
}
