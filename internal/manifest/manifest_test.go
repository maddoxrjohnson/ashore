package manifest

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var defaults = Defaults{Timeout: 30 * time.Second, Interval: 250 * time.Millisecond, Grace: 10 * time.Second, Memory: 256 << 20, CPU: 1}

func TestParse(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want Manifest // zero means "expect an error"
		err  string
	}{
		{"empty file is the defaults", "", Default(defaults), ""},
		{"full", `
[health]
path = "/healthz"
timeout = "45s"
interval = "2s"

[process]
grace = "20s"
memory = "512m"
cpu = "0.5"
`, Manifest{
			Health:  Health{Path: "/healthz", Timeout: Duration(45 * time.Second), Interval: Duration(2 * time.Second)},
			Process: Process{Grace: Duration(20 * time.Second), Memory: 512 << 20, CPU: 0.5},
		}, ""},
		{"partial keeps the rest", "[process]\nmemory = \"1g\"\n", func() Manifest {
			m := Default(defaults)
			m.Process.Memory = 1 << 30
			return m
		}(), ""},
		{"cpu as a number", "[process]\ncpu = 2\n", func() Manifest {
			m := Default(defaults)
			m.Process.CPU = 2
			return m
		}(), ""},
		{"cpu as a float", "[process]\ncpu = 0.25\n", func() Manifest {
			m := Default(defaults)
			m.Process.CPU = 0.25
			return m
		}(), ""},
		{"memory in bytes", "[process]\nmemory = \"33554432\"\n", func() Manifest {
			m := Default(defaults)
			m.Process.Memory = 32 << 20
			return m
		}(), ""},
		{"not toml", "health = [", Manifest{}, "ashore.toml:"},
		{"unknown section", "[helth]\npath = \"/x\"\n", Manifest{}, "unknown key helth"},
		{"unknown key", "[health]\npth = \"/x\"\n", Manifest{}, "unknown key health.pth"},
		{"bad duration", "[health]\ntimeout = \"soon\"\n", Manifest{}, "soon"},
		{"relative path", "[health]\npath = \"healthz\"\n", Manifest{}, "must start with /"},
		{"path with space", "[health]\npath = \"/health z\"\n", Manifest{}, "whitespace"},
		{"tiny timeout", "[health]\ntimeout = \"10ms\"\n", Manifest{}, "shorter than"},
		{"interval over timeout", "[health]\ntimeout = \"1s\"\ninterval = \"2s\"\n", Manifest{}, "longer than"},
		{"zero grace", "[process]\ngrace = \"0s\"\n", Manifest{}, "grace"},
		{"tiny memory", "[process]\nmemory = \"4m\"\n", Manifest{}, "less than 16m"},
		{"bad memory", "[process]\nmemory = \"lots\"\n", Manifest{}, "not a size"},
		{"zero cpu", "[process]\ncpu = 0\n", Manifest{}, "cpu"},
		{"cpu string junk", "[process]\ncpu = \"fast\"\n", Manifest{}, "not a number"},
		{"cpu wrong type", "[process]\ncpu = true\n", Manifest{}, "must be a number"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Parse([]byte(tc.in), defaults)
			if tc.err != "" {
				if err == nil || !strings.Contains(err.Error(), tc.err) {
					t.Fatalf("err = %v, want %q", err, tc.err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("got  %+v\nwant %+v", got, tc.want)
			}
		})
	}
}

func TestJSONRoundTrip(t *testing.T) {
	m, err := Parse([]byte("[health]\npath = \"/healthz\"\n[process]\nmemory = \"128m\"\ncpu = \"0.5\"\n"), defaults)
	if err != nil {
		t.Fatal(err)
	}
	doc := m.JSON()
	want := `{"health":{"path":"/healthz","timeout":"30s","interval":"250ms"},"process":{"grace":"10s","memory":"128m","cpu":"0.5"}}`
	if doc != want {
		t.Errorf("JSON = %s\nwant   %s", doc, want)
	}
	back, err := FromJSON(doc, defaults)
	if err != nil || back != m {
		t.Errorf("FromJSON = %+v, %v", back, err)
	}
	// A release from before manifests existed stores {} and gets the
	// defaults; a document missing a section keeps the defaults for it.
	for _, old := range []string{"", "{}"} {
		if got, err := FromJSON(old, defaults); err != nil || got != Default(defaults) {
			t.Errorf("FromJSON(%q) = %+v, %v", old, got, err)
		}
	}
	got, err := FromJSON(`{"health":{"path":"/up"}}`, defaults)
	if err != nil || got.Health.Path != "/up" || got.Health.Timeout != Duration(30*time.Second) || got.Process.Memory != 256<<20 {
		t.Errorf("partial FromJSON = %+v, %v", got, err)
	}
	if _, err := FromJSON(`{"process":{"memory":"1m"}}`, defaults); err == nil {
		t.Error("an invalid stored manifest was accepted")
	}
	if s := m.String(); s != "health GET /healthz (timeout 30s), memory 128m, cpu 0.5, grace 10s" {
		t.Errorf("String = %q", s)
	}
}

func TestBytesString(t *testing.T) {
	for in, want := range map[Bytes]string{1 << 30: "1g", 256 << 20: "256m", 3 << 20: "3m", 512 << 10: "512k", 1000: "1000", 1536 << 10: "1536k"} {
		if got := in.String(); got != want {
			t.Errorf("Bytes(%d).String() = %q, want %q", int64(in), got, want)
		}
	}
	var b Bytes
	if err := b.UnmarshalText([]byte("-1")); err == nil {
		t.Error("negative size accepted")
	}
	if err := b.UnmarshalText([]byte(" 2G ")); err != nil || b != 2<<30 {
		t.Errorf("2G: %d, %v", b, err)
	}
}

func TestFromRepo(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not installed")
	}
	ctx := context.Background()
	work := t.TempDir()
	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = work
		cmd.Env = append(os.Environ(), "GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@t", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@t")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v\n%s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	run("init", "-q", "-b", "main")
	if err := os.WriteFile(filepath.Join(work, "main.go"), []byte("package main\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	run("add", "-A")
	run("commit", "-q", "-m", "no manifest")
	without := run("rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(work, FileName), []byte("[health]\npath = \"/healthz\"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	run("add", "-A")
	run("commit", "-q", "-m", "with manifest")
	with := run("rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(work, FileName), []byte("[health]\npath = 3\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	run("add", "-A")
	run("commit", "-q", "-m", "bad manifest")
	bad := run("rev-parse", "HEAD")
	repo := filepath.Join(work, ".git")

	m, found, err := fromRepo(ctx, repo, without, "", defaults)
	if err != nil || found || m != Default(defaults) {
		t.Errorf("commit without the file: %+v, %v, %v", m, found, err)
	}
	m, found, err = fromRepo(ctx, repo, with, "", defaults)
	if err != nil || !found || m.Health.Path != "/healthz" || m.Process.Memory != 256<<20 {
		t.Errorf("commit with the file: %+v, %v, %v", m, found, err)
	}
	if _, _, err := fromRepo(ctx, repo, bad, "", defaults); err == nil || !strings.Contains(err.Error(), FileName) {
		t.Errorf("bad manifest: %v", err)
	}
	if _, _, err := fromRepo(ctx, repo, strings.Repeat("0", 40), "", defaults); err == nil {
		t.Error("unknown commit accepted")
	}
}

// fromRepo is ReadRepo followed by Parse, the way the supervisor uses them.
func fromRepo(ctx context.Context, repo, sha, quarantine string, d Defaults) (Manifest, bool, error) {
	data, found, err := ReadRepo(ctx, repo, sha, quarantine)
	if err != nil {
		return Manifest{}, false, err
	}
	m, err := Parse(data, d)
	return m, found, err
}
