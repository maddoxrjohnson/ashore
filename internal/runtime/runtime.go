// Package runtime defines what ashore needs from whatever runs an app's
// containers: start one from an image, stop it, wait for it to exit, read
// its output, and find the ones already running after a daemon restart.
// internal/runtime/docker implements it on the Docker Engine API.
//
// The package shares its name with the standard library's runtime. Nothing
// in ashore needs both in one file; if that changes, alias one of them.
package runtime

import (
	"context"
	"errors"
	"io"
	"time"
)

// Spec describes one instance of a release.
type Spec struct {
	App     string
	Release string
	Image   string
	// Env is the app's environment. The runtime sets PORT to ContainerPort
	// itself, overriding any PORT here, so the two can never disagree.
	Env map[string]string
	// HostPort is where the router reaches the instance, on 127.0.0.1
	// only. The daemon picks it with FreePort.
	HostPort int
	// ContainerPort is the port the app listens on: 8080 inside a Docker
	// container; equal to HostPort under the native runtime, which shares
	// the host's network (D7).
	ContainerPort int
	Memory        int64   // bytes; 0 means no limit
	CPU           float64 // CPUs, e.g. 0.5; 0 means no limit
	Pids          int64   // process limit; 0 means DefaultPids
	// Labels are added to the instance after the runtime's own. Tests use
	// them to mark what they create with ashore.test=1.
	Labels map[string]string
}

// DefaultPids caps the processes in one instance when Spec.Pids is 0. It
// is far above what a web app needs and far below what a fork bomb wants.
const DefaultPids = 256

// Labels every runtime puts on the instances it creates. List finds
// ashore's instances by LabelApp; the rest of the host's containers are
// none of its business.
const (
	LabelApp     = "ashore.app"
	LabelRelease = "ashore.release"
)

// Instance is one started container.
type Instance struct {
	ID      string
	App     string
	Release string
	// Addr is the host:port the router proxies to. List leaves it empty
	// for an instance that is not running.
	Addr      string
	StartedAt time.Time
	// Running is false for an instance whose process has exited but which
	// has not been stopped (removed) yet; reconcile cleans those up.
	Running bool
}

// Runtime runs instances. Implementations are safe for concurrent use.
type Runtime interface {
	// Start creates and starts an instance and returns once its process
	// is running. It does not wait for the app to listen; that is the
	// supervisor's health check.
	Start(ctx context.Context, s Spec) (Instance, error)
	// Stop sends SIGTERM, then SIGKILL after grace, and removes the
	// instance. Stopping an instance that is already gone is not an error.
	Stop(ctx context.Context, id string, grace time.Duration) error
	// Wait blocks until the instance's process exits and returns its exit
	// code. It returns at once for an instance that has already exited.
	Wait(ctx context.Context, id string) (exitCode int, err error)
	// Logs copies the instance's output written since since (the zero
	// time means from the start) to stdout and stderr, and keeps copying
	// until the instance exits or ctx ends.
	Logs(ctx context.Context, id string, since time.Time, stdout, stderr io.Writer) error
	// List returns every instance this runtime created, running or not.
	List(ctx context.Context) ([]Instance, error)
}

// ErrNotFound reports an instance id the runtime does not know.
var ErrNotFound = errors.New("no such instance")
