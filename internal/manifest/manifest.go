// Package manifest reads ashore.toml, the optional file at the root of a
// pushed repository that says how the app is checked and run: the health
// check (TCP connect by default, or an HTTP GET path), how long to wait for
// it, and the process limits. A repository without the file gets the
// daemon's defaults. The effective values are stored with each release as
// JSON, so a release means the same thing after the defaults change.
package manifest

import (
	"bytes"
	"context"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// FileName is where the manifest lives in the repository.
const FileName = "ashore.toml"

// Manifest is the effective configuration of one release's instances.
type Manifest struct {
	Health  Health  `toml:"health" json:"health"`
	Process Process `toml:"process" json:"process"`
}

// Health is the readiness check a new instance must pass before it gets
// traffic. An empty Path means "accepts and holds a TCP connection"; a path
// means GET it and expect 2xx.
type Health struct {
	Path     string   `toml:"path" json:"path,omitempty"`
	Timeout  Duration `toml:"timeout" json:"timeout"`
	Interval Duration `toml:"interval" json:"interval"`
}

// Process is how an instance is run and stopped.
type Process struct {
	Grace  Duration `toml:"grace" json:"grace"`
	Memory Bytes    `toml:"memory" json:"memory"`
	CPU    CPUs     `toml:"cpu" json:"cpu"`
}

// Defaults are the daemon's values for whatever the file leaves out.
type Defaults struct {
	Timeout  time.Duration
	Interval time.Duration
	Grace    time.Duration
	Memory   int64
	CPU      float64
}

// Default is the manifest of a repository with no ashore.toml.
func Default(d Defaults) Manifest {
	return Manifest{
		Health:  Health{Timeout: Duration(d.Timeout), Interval: Duration(d.Interval)},
		Process: Process{Grace: Duration(d.Grace), Memory: Bytes(d.Memory), CPU: CPUs(d.CPU)},
	}
}

// Parse decodes a file's contents over the defaults and validates the
// result. Unknown keys are an error: a misspelt section would otherwise be
// silently ignored and the app would run with settings its author never
// asked for.
func Parse(data []byte, d Defaults) (Manifest, error) {
	m := Default(d)
	md, err := toml.Decode(string(data), &m)
	if err != nil {
		return Manifest{}, fmt.Errorf("%s: %w", FileName, err)
	}
	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		names := make([]string, len(undecoded))
		for i, k := range undecoded {
			names[i] = k.String()
		}
		return Manifest{}, fmt.Errorf("%s: unknown key %s", FileName, strings.Join(names, ", "))
	}
	if err := m.validate(); err != nil {
		return Manifest{}, fmt.Errorf("%s: %w", FileName, err)
	}
	return m, nil
}

// Limits below which a setting is a mistake rather than a choice.
const (
	minMemory  = 16 << 20
	maxCPU     = 64
	minTimeout = 100 * time.Millisecond
)

func (m Manifest) validate() error {
	h, p := m.Health, m.Process
	switch {
	case h.Path != "" && (!strings.HasPrefix(h.Path, "/") || strings.ContainsAny(h.Path, " \t\r\n")):
		return fmt.Errorf("health.path %q must start with / and contain no whitespace", h.Path)
	case h.Timeout < Duration(minTimeout):
		return fmt.Errorf("health.timeout %s is shorter than %s", h.Timeout, minTimeout)
	case h.Interval <= 0:
		return fmt.Errorf("health.interval %s must be positive", h.Interval)
	case h.Interval > h.Timeout:
		return fmt.Errorf("health.interval %s is longer than health.timeout %s", h.Interval, h.Timeout)
	case p.Grace <= 0:
		return fmt.Errorf("process.grace %s must be positive", p.Grace)
	case p.Memory < minMemory:
		return fmt.Errorf("process.memory %s is less than %s", p.Memory, Bytes(minMemory))
	case p.CPU <= 0 || p.CPU > maxCPU:
		return fmt.Errorf("process.cpu %s must be between 0 and %d", p.CPU, maxCPU)
	}
	return nil
}

// JSON is the form stored with a release.
func (m Manifest) JSON() string {
	b, _ := json.Marshal(m) // every field marshals as text; cannot fail
	return string(b)
}

// FromJSON decodes a stored manifest. Fields the document leaves out, as a
// release from before manifests does, take the defaults.
func FromJSON(doc string, d Defaults) (Manifest, error) {
	m := Default(d)
	if doc == "" || doc == "{}" {
		return m, nil
	}
	if err := json.Unmarshal([]byte(doc), &m); err != nil {
		return Manifest{}, fmt.Errorf("stored manifest: %w", err)
	}
	if err := m.validate(); err != nil {
		return Manifest{}, fmt.Errorf("stored manifest: %w", err)
	}
	return m, nil
}

// String is the one-line summary shown to a pusher.
func (m Manifest) String() string {
	check := "tcp"
	if m.Health.Path != "" {
		check = "GET " + m.Health.Path
	}
	return fmt.Sprintf("health %s (timeout %s), memory %s, cpu %s, grace %s",
		check, m.Health.Timeout, m.Process.Memory, m.Process.CPU, m.Process.Grace)
}

// FromRepo reads ashore.toml from a commit in a bare repository and parses
// it; a commit without the file is the default manifest and found is false.
func FromRepo(ctx context.Context, repo, sha, quarantine string, d Defaults) (m Manifest, found bool, err error) {
	data, found, err := ReadRepo(ctx, repo, sha, quarantine)
	if err != nil {
		return Manifest{}, false, err
	}
	m, err = Parse(data, d)
	return m, found, err
}

// ReadRepo returns the raw ashore.toml of a commit, or found false when the
// commit has none. quarantine is GIT_QUARANTINE_PATH while a pre-receive
// hook runs, when the pushed objects are not in the repository yet (D23).
// The supervisor takes this function as its seam, so its tests supply the
// file without a repository.
func ReadRepo(ctx context.Context, repo, sha, quarantine string) (data []byte, found bool, err error) {
	// ls-tree prints nothing for a path the commit lacks and exits 0, which
	// cat-file does not; this is the only reliable "does it exist".
	out, err := git(ctx, repo, quarantine, "ls-tree", sha, "--", FileName)
	if err != nil {
		return nil, false, err
	}
	if len(bytes.TrimSpace(out)) == 0 {
		return nil, false, nil
	}
	data, err = git(ctx, repo, quarantine, "cat-file", "blob", sha+":"+FileName)
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
}

func git(ctx context.Context, repo, quarantine string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", append([]string{"--git-dir=" + repo}, args...)...)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "LC_ALL=C"}
	if quarantine != "" {
		cmd.Env = append(cmd.Env, "GIT_ALTERNATE_OBJECT_DIRECTORIES="+quarantine)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", args[0], err, bytes.TrimSpace(stderr.Bytes()))
	}
	return out, nil
}

// Duration is a time.Duration written as "30s" in TOML and JSON.
type Duration time.Duration

var _ encoding.TextUnmarshaler = (*Duration)(nil)

func (d Duration) String() string { return time.Duration(d).String() }

func (d Duration) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

func (d *Duration) UnmarshalText(b []byte) error {
	v, err := time.ParseDuration(string(b))
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// Bytes is a byte count written with a unit: "256m", "1g", "512k", or a
// plain number of bytes.
type Bytes int64

var _ encoding.TextUnmarshaler = (*Bytes)(nil)

func (b Bytes) String() string {
	switch {
	case b >= 1<<30 && b%(1<<30) == 0:
		return strconv.FormatInt(int64(b)>>30, 10) + "g"
	case b >= 1<<20 && b%(1<<20) == 0:
		return strconv.FormatInt(int64(b)>>20, 10) + "m"
	case b >= 1<<10 && b%(1<<10) == 0:
		return strconv.FormatInt(int64(b)>>10, 10) + "k"
	}
	return strconv.FormatInt(int64(b), 10)
}

func (b Bytes) MarshalText() ([]byte, error) { return []byte(b.String()), nil }

func (b *Bytes) UnmarshalText(text []byte) error {
	s := strings.ToLower(strings.TrimSpace(string(text)))
	shift := 0
	switch {
	case strings.HasSuffix(s, "g"):
		shift, s = 30, s[:len(s)-1]
	case strings.HasSuffix(s, "m"):
		shift, s = 20, s[:len(s)-1]
	case strings.HasSuffix(s, "k"):
		shift, s = 10, s[:len(s)-1]
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 || n > (1<<62)>>shift {
		return fmt.Errorf("%q is not a size like 256m or 1g", text)
	}
	*b = Bytes(n << shift)
	return nil
}

// CPUs is a CPU quota such as 0.5. TOML may write it as a number or, as the
// documentation does, a string.
type CPUs float64

var (
	_ toml.Unmarshaler         = (*CPUs)(nil)
	_ encoding.TextUnmarshaler = (*CPUs)(nil)
)

func (c CPUs) String() string { return strconv.FormatFloat(float64(c), 'f', -1, 64) }

func (c CPUs) MarshalText() ([]byte, error) { return []byte(c.String()), nil }

func (c *CPUs) UnmarshalText(b []byte) error {
	f, err := strconv.ParseFloat(strings.TrimSpace(string(b)), 64)
	if err != nil {
		return fmt.Errorf("%q is not a number of CPUs", b)
	}
	*c = CPUs(f)
	return nil
}

func (c *CPUs) UnmarshalTOML(v any) error {
	switch x := v.(type) {
	case float64:
		*c = CPUs(x)
	case int64:
		*c = CPUs(x)
	case string:
		return c.UnmarshalText([]byte(x))
	default:
		return errors.New("cpu must be a number")
	}
	return nil
}
