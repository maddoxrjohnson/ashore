// Package docker implements runtime.Runtime on the Docker Engine API. Each
// instance is one container, published on 127.0.0.1 only, with memory, CPU,
// and process limits and no-new-privileges, and labelled so the daemon can
// find its containers again after a restart.
package docker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sort"
	"strconv"
	"time"

	cerrdefs "github.com/containerd/errdefs"
	"github.com/moby/moby/api/pkg/stdcopy"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"

	"github.com/maddoxrjohnson/ashore/internal/runtime"
)

// Docker is the slice of the Engine API the runtime uses. *client.Client
// satisfies it; tests substitute a fake.
type Docker interface {
	ContainerCreate(ctx context.Context, options client.ContainerCreateOptions) (client.ContainerCreateResult, error)
	ContainerStart(ctx context.Context, id string, options client.ContainerStartOptions) (client.ContainerStartResult, error)
	ContainerStop(ctx context.Context, id string, options client.ContainerStopOptions) (client.ContainerStopResult, error)
	ContainerRemove(ctx context.Context, id string, options client.ContainerRemoveOptions) (client.ContainerRemoveResult, error)
	ContainerWait(ctx context.Context, id string, options client.ContainerWaitOptions) client.ContainerWaitResult
	ContainerLogs(ctx context.Context, id string, options client.ContainerLogsOptions) (client.ContainerLogsResult, error)
	ContainerList(ctx context.Context, options client.ContainerListOptions) (client.ContainerListResult, error)
}

// Runtime runs instances as Docker containers.
type Runtime struct {
	docker Docker
}

var _ runtime.Runtime = (*Runtime)(nil)

// New returns a Runtime that talks to d.
func New(d Docker) *Runtime {
	return &Runtime{docker: d}
}

var loopback = netip.AddrFrom4([4]byte{127, 0, 0, 1})

// Start creates the container, starts it, and returns once Docker reports
// it running. A container that was created but would not start is removed
// so a failed deploy leaves nothing behind.
func (r *Runtime) Start(ctx context.Context, s runtime.Spec) (runtime.Instance, error) {
	if s.HostPort < 1 || s.HostPort > 65535 {
		return runtime.Instance{}, fmt.Errorf("start %s %s: host port %d out of range", s.App, s.Release, s.HostPort)
	}
	if s.ContainerPort < 1 || s.ContainerPort > 65535 {
		return runtime.Instance{}, fmt.Errorf("start %s %s: container port %d out of range", s.App, s.Release, s.ContainerPort)
	}
	port, _ := network.PortFrom(uint16(s.ContainerPort), network.TCP) // in range, so valid
	name, err := containerName(s)
	if err != nil {
		return runtime.Instance{}, err
	}

	pids := s.Pids
	if pids == 0 {
		pids = runtime.DefaultPids
	}
	labels := make(map[string]string, len(s.Labels)+2)
	for k, v := range s.Labels {
		labels[k] = v
	}
	// Ours last, so a caller's label cannot hide an instance from List.
	labels[runtime.LabelApp] = s.App
	labels[runtime.LabelRelease] = s.Release

	res, err := r.docker.ContainerCreate(ctx, client.ContainerCreateOptions{
		Name: name,
		Config: &container.Config{
			Image:        s.Image,
			Env:          environ(s.Env, s.ContainerPort),
			Labels:       labels,
			ExposedPorts: network.PortSet{port: {}},
		},
		HostConfig: &container.HostConfig{
			PortBindings: network.PortMap{port: {{HostIP: loopback, HostPort: strconv.Itoa(s.HostPort)}}},
			SecurityOpt:  []string{"no-new-privileges"},
			Resources: container.Resources{
				Memory: s.Memory,
				// Without this Docker allows as much swap again, and a
				// memory limit of 256m behaves like 512m.
				MemorySwap: s.Memory,
				NanoCPUs:   int64(s.CPU * 1e9),
				PidsLimit:  &pids,
			},
		},
	})
	if err != nil {
		return runtime.Instance{}, fmt.Errorf("create container for %s %s: %w", s.App, s.Release, err)
	}
	if _, err := r.docker.ContainerStart(ctx, res.ID, client.ContainerStartOptions{}); err != nil {
		// ctx may be what failed; the cleanup still has to happen.
		_, _ = r.docker.ContainerRemove(context.WithoutCancel(ctx), res.ID, client.ContainerRemoveOptions{Force: true})
		return runtime.Instance{}, fmt.Errorf("start container for %s %s: %w", s.App, s.Release, err)
	}
	return runtime.Instance{
		ID:        res.ID,
		App:       s.App,
		Release:   s.Release,
		Addr:      net.JoinHostPort(loopback.String(), strconv.Itoa(s.HostPort)),
		StartedAt: time.Now(),
		Running:   true,
	}, nil
}

// containerName is ashore-<app>-<release>-<8 hex>, readable in docker ps.
// The random part keeps a restart of the same release from colliding with
// a container that has not been removed yet.
func containerName(s runtime.Spec) (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return "ashore-" + s.App + "-" + s.Release + "-" + hex.EncodeToString(b), nil
}

// environ turns env into KEY=value lines in a stable order and sets PORT.
func environ(env map[string]string, port int) []string {
	out := make([]string, 0, len(env)+1)
	for k, v := range env {
		if k != "PORT" {
			out = append(out, k+"="+v)
		}
	}
	sort.Strings(out)
	return append(out, "PORT="+strconv.Itoa(port))
}

// Stop stops the container, giving it grace to exit after SIGTERM, then
// removes it. Docker rounds the grace up to whole seconds.
func (r *Runtime) Stop(ctx context.Context, id string, grace time.Duration) error {
	secs := int((grace + time.Second - 1) / time.Second)
	_, err := r.docker.ContainerStop(ctx, id, client.ContainerStopOptions{Timeout: &secs})
	if err != nil && !cerrdefs.IsNotFound(err) {
		return fmt.Errorf("stop %s: %w", short(id), err)
	}
	_, err = r.docker.ContainerRemove(ctx, id, client.ContainerRemoveOptions{Force: true})
	if err != nil && !cerrdefs.IsNotFound(err) {
		return fmt.Errorf("remove %s: %w", short(id), err)
	}
	return nil
}

// Wait waits for the "not-running" condition, which is met at once by a
// container that has already exited, so a watcher that starts late still
// learns the exit code instead of blocking forever.
func (r *Runtime) Wait(ctx context.Context, id string) (int, error) {
	res := r.docker.ContainerWait(ctx, id, client.ContainerWaitOptions{Condition: container.WaitConditionNotRunning})
	select {
	case w := <-res.Result:
		if w.Error != nil && w.Error.Message != "" {
			return -1, fmt.Errorf("wait %s: %s", short(id), w.Error.Message)
		}
		return int(w.StatusCode), nil
	case err := <-res.Error:
		return -1, fmt.Errorf("wait %s: %w", short(id), notFound(err))
	case <-ctx.Done():
		return -1, ctx.Err()
	}
}

// Logs follows the container's output. Without a TTY, Docker multiplexes
// stdout and stderr into one stream of framed chunks; stdcopy splits them
// back apart. It returns nil when the container exits.
func (r *Runtime) Logs(ctx context.Context, id string, since time.Time, stdout, stderr io.Writer) error {
	opts := client.ContainerLogsOptions{ShowStdout: true, ShowStderr: true, Follow: true}
	if !since.IsZero() {
		opts.Since = fmt.Sprintf("%d.%09d", since.Unix(), since.Nanosecond())
	}
	rc, err := r.docker.ContainerLogs(ctx, id, opts)
	if err != nil {
		return fmt.Errorf("logs %s: %w", short(id), notFound(err))
	}
	defer func() { _ = rc.Close() }()
	if _, err := stdcopy.StdCopy(stdout, stderr, rc); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("logs %s: %w", short(id), err)
	}
	return nil
}

// List finds ashore's containers by label, including exited ones, which
// reconcile has to remove. StartedAt is the container's creation time:
// Start creates and starts in one call, so the two are milliseconds apart,
// and List avoids one inspect request per container.
func (r *Runtime) List(ctx context.Context) ([]runtime.Instance, error) {
	res, err := r.docker.ContainerList(ctx, client.ContainerListOptions{
		All:     true,
		Filters: make(client.Filters).Add("label", runtime.LabelApp),
	})
	if err != nil {
		return nil, fmt.Errorf("list containers: %w", err)
	}
	out := make([]runtime.Instance, 0, len(res.Items))
	for _, c := range res.Items {
		inst := runtime.Instance{
			ID:        c.ID,
			App:       c.Labels[runtime.LabelApp],
			Release:   c.Labels[runtime.LabelRelease],
			StartedAt: time.Unix(c.Created, 0),
			Running:   c.State == container.StateRunning,
		}
		for _, p := range c.Ports {
			if p.PublicPort != 0 && p.IP == loopback {
				inst.Addr = net.JoinHostPort(p.IP.String(), strconv.Itoa(int(p.PublicPort)))
			}
		}
		out = append(out, inst)
	}
	return out, nil
}

// notFound turns Docker's "no such container" into runtime.ErrNotFound so
// callers need not know about errdefs.
func notFound(err error) error {
	if cerrdefs.IsNotFound(err) {
		return runtime.ErrNotFound
	}
	return err
}

func short(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}
