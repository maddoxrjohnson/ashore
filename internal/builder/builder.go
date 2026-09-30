// Package builder turns a commit in an app's repository into a container
// image. It runs git archive for the commit, hands the tar to the Docker
// daemon as the build context, and relays the daemon's build output to the
// pusher and to a log file under the data directory.
package builder

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/moby/moby/api/types/build"
	"github.com/moby/moby/api/types/jsonstream"
	"github.com/moby/moby/client"
)

// Docker is the slice of the Engine API the builder uses. *client.Client
// satisfies it; tests substitute a fake.
type Docker interface {
	ImageBuild(ctx context.Context, buildContext io.Reader, options client.ImageBuildOptions) (client.ImageBuildResult, error)
}

// Job names one build: the commit to build and where its objects are.
type Job struct {
	App  string
	Repo string // path of the bare repository
	SHA  string // full object name of the commit
	// Quarantine is GIT_QUARANTINE_PATH from the push that delivered SHA.
	// While pre-receive runs, the pushed objects live there and not in
	// Repo (D23). Empty for a commit the repository already holds.
	Quarantine string
}

// Builder builds images with a Docker daemon.
type Builder struct {
	docker  Docker
	logDir  string
	timeout time.Duration
	// Labels are added to every image built, after the builder's own
	// ashore.app and ashore.sha. Tests use it to mark images ashore.test=1.
	Labels map[string]string
}

// New returns a Builder that logs under <dataDir>/builds and gives each
// build at most timeout.
func New(d Docker, dataDir string, timeout time.Duration) *Builder {
	return &Builder{docker: d, logDir: filepath.Join(dataDir, "builds"), timeout: timeout}
}

// Tag is the image tag a build of app at sha produces.
func Tag(app, sha string) string {
	if len(sha) > 7 {
		sha = sha[:7]
	}
	return "ashore/" + app + ":" + sha
}

var errTimeout = errors.New("build timed out")

// Build produces the image Tag(job.App, job.SHA) and returns that tag.
// Everything the daemon prints goes to out and to
// <dataDir>/builds/<app>/<sha>.log; on failure the log ends with the error.
func (b *Builder) Build(ctx context.Context, job Job, out io.Writer) (string, error) {
	ctx, cancel := context.WithTimeoutCause(ctx, b.timeout, errTimeout)
	defer cancel()

	logf, err := b.openLog(job)
	if err != nil {
		return "", err
	}
	defer func() { _ = logf.Close() }()

	tag := Tag(job.App, job.SHA)
	start := time.Now()
	err = b.build(ctx, job, tag, io.MultiWriter(out, logf))
	if err != nil {
		if errors.Is(context.Cause(ctx), errTimeout) {
			err = fmt.Errorf("%w after %s", errTimeout, b.timeout)
		}
		_, _ = fmt.Fprintf(logf, "error: %v\n", err)
		slog.Warn("build failed", "app", job.App, "sha", job.SHA, "err", err, "duration", time.Since(start))
		return "", err
	}
	slog.Info("build finished", "app", job.App, "sha", job.SHA, "tag", tag, "duration", time.Since(start))
	return tag, nil
}

func (b *Builder) openLog(job Job) (*os.File, error) {
	dir := filepath.Join(b.logDir, job.App)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	return os.OpenFile(filepath.Join(dir, job.SHA+".log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
}

func (b *Builder) build(ctx context.Context, job Job, tag string, w io.Writer) error {
	tar, err := archive(ctx, job)
	if err != nil {
		return err
	}
	defer func() {
		_ = tar.Close()
		_ = os.Remove(tar.Name())
	}()

	labels := map[string]string{"ashore.app": job.App, "ashore.sha": job.SHA}
	for k, v := range b.Labels {
		labels[k] = v
	}
	res, err := b.docker.ImageBuild(ctx, tar, client.ImageBuildOptions{
		Tags:        []string{tag},
		Dockerfile:  "Dockerfile",
		Labels:      labels,
		Remove:      true, // intermediate containers go, even when a step fails
		ForceRemove: true,
		Version:     build.BuilderV1,
	})
	if err != nil {
		return fmt.Errorf("docker build: %w", err)
	}
	defer func() { _ = res.Body.Close() }()
	id, err := relay(res.Body, w)
	if err != nil {
		return err
	}
	slog.Info("image built", "tag", tag, "id", id)
	return nil
}

// archive writes `git archive <sha>` to a temp file and returns it, rewound.
// A commit's tree as a tar is exactly the build context the Engine API
// wants. A file rather than a pipe so that a failing git is reported as
// such, not as a truncated upload, and the caller removes it.
func archive(ctx context.Context, job Job) (*os.File, error) {
	f, err := os.CreateTemp("", "ashore-context-*.tar")
	if err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "git", "--git-dir="+job.Repo, "archive", "--format=tar", job.SHA)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "LC_ALL=C"}
	if job.Quarantine != "" {
		cmd.Env = append(cmd.Env, "GIT_ALTERNATE_OBJECT_DIRECTORIES="+job.Quarantine)
	}
	var stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = f, &stderr
	err = cmd.Run()
	if err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		_ = f.Close()
		_ = os.Remove(f.Name())
		return nil, fmt.Errorf("git archive: %w: %s", err, bytes.TrimSpace(stderr.Bytes()))
	}
	return f, nil
}

// relay decodes the daemon's JSON message stream and writes what a person
// wants to read: build steps and their output as they are, image pull
// status lines without the progress-bar updates. A message carrying an
// error ends the build; the image id from the final aux message is
// returned.
func relay(r io.Reader, w io.Writer) (id string, err error) {
	dec := json.NewDecoder(r)
	for {
		var m jsonstream.Message
		if err := dec.Decode(&m); err != nil {
			if errors.Is(err, io.EOF) {
				return id, nil
			}
			return "", fmt.Errorf("reading build output: %w", err)
		}
		switch {
		case m.Error != nil:
			return "", fmt.Errorf("build failed: %s", m.Error.Message)
		case m.Stream != "":
			if _, err := io.WriteString(w, m.Stream); err != nil {
				return "", err
			}
		case m.Status != "":
			if m.Progress != nil && (m.Progress.Current > 0 || m.Progress.Total > 0) {
				continue // one tick of a progress bar; the next status line says when it is done
			}
			line := m.Status
			if m.ID != "" {
				line = m.ID + ": " + m.Status
			}
			if _, err := fmt.Fprintln(w, line); err != nil {
				return "", err
			}
		case m.Aux != nil:
			var aux struct{ ID string }
			if json.Unmarshal(*m.Aux, &aux) == nil && aux.ID != "" {
				id = aux.ID
			}
		}
	}
}
