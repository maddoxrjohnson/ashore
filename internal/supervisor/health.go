// Starting an instance and deciding when it is healthy: a TCP
// connect-and-hold by default, or an HTTP GET when the manifest names a
// path.
package supervisor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/manifest"
	"github.com/maddoxrjohnson/ashore/internal/runtime"
	"github.com/maddoxrjohnson/ashore/internal/store"
)

// Instance defaults for what ashore.toml leaves out.
const (
	defaultMemory = 256 << 20
	defaultCPU    = 1.0
	healthEvery   = 250 * time.Millisecond // default probe interval
	healthHold    = 300 * time.Millisecond // how long a TCP probe connection must stay open
	probeTimeout  = 2 * time.Second        // one HTTP probe, connect to response
	logTail       = 20                     // lines shown when an instance dies during its health check
)

// start runs the release with env as its environment and m's limits, and
// waits for it to pass m's health check. host is the app's hostname, sent
// as the Host header of an HTTP probe. On any failure the instance is
// stopped and removed, so a rejected push leaves nothing running.
func (a *actor) start(ctx context.Context, rel store.Release, env map[string]string, m manifest.Manifest, host string, out io.Writer) (runtime.Instance, error) {
	s := a.sup
	port, err := runtime.FreePort()
	if err != nil {
		return runtime.Instance{}, err
	}
	appPort := s.cfg.ContainerPort
	if appPort == 0 {
		appPort = port
	}
	startAt := time.Now()
	inst, err := s.runtime.Start(ctx, runtime.Spec{
		App: a.name, Release: rel.Name(), Image: rel.Image, Env: env,
		HostPort: port, ContainerPort: appPort,
		Memory: int64(m.Process.Memory), CPU: float64(m.Process.CPU),
	})
	if err != nil {
		return runtime.Instance{}, err
	}
	s.obs.Stage(StageStart, time.Since(startAt))
	s.log.Info("instance started", "app", a.name, "release", rel.Name(), "id", inst.ID, "addr", inst.Addr)
	a.collect(inst, time.Time{})
	if m.Health.Path == "" {
		_, _ = fmt.Fprintf(out, "-----> waiting for %s to listen on port %d\n", rel.Name(), appPort)
	} else {
		_, _ = fmt.Fprintf(out, "-----> waiting for %s to answer GET %s on port %d\n", rel.Name(), m.Health.Path, appPort)
	}

	healthAt := time.Now()
	err = a.waitHealthy(ctx, inst, m.Health, host, appPort)
	s.obs.Stage(StageHealth, time.Since(healthAt))
	if err != nil {
		bg := context.WithoutCancel(ctx)
		if a.exited(bg, inst.ID) {
			a.tail(bg, inst.ID, out)
		}
		if serr := s.runtime.Stop(bg, inst.ID, time.Duration(m.Process.Grace)); serr != nil {
			s.log.Warn("stop failed instance", "app", a.name, "id", inst.ID, "err", serr)
		}
		return runtime.Instance{}, err
	}
	return inst, nil
}

// waitHealthy probes the instance every h.Interval until a probe passes,
// the instance exits, h.Timeout passes, or ctx ends. The probe is a TCP
// connect-and-hold, or GET h.Path expecting 2xx when the manifest names one.
func (a *actor) waitHealthy(ctx context.Context, inst runtime.Instance, h manifest.Health, host string, appPort int) error {
	s := a.sup
	timeout, interval := time.Duration(h.Timeout), time.Duration(h.Interval)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	exited := make(chan int, 1)
	go func() {
		if code, err := s.runtime.Wait(ctx, inst.ID); err == nil {
			exited <- code
		}
	}()

	tick := time.NewTicker(interval)
	defer tick.Stop()
	start := time.Now()
	var last string // what the latest HTTP probe saw, for the timeout message
	for {
		var ok bool
		if h.Path == "" {
			ok = listening(ctx, inst.Addr, interval)
		} else {
			var detail string
			ok, detail = answers(ctx, inst.Addr, host, h.Path)
			if ctx.Err() == nil {
				last = detail // a probe cut off by the deadline says nothing about the app
			}
		}
		if ok {
			s.log.Info("instance healthy", "app", a.name, "id", inst.ID, "after", time.Since(start))
			return nil
		}
		select {
		case code := <-exited:
			return fmt.Errorf("%s exited with code %d before passing its health check", inst.Release, code)
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				if h.Path == "" {
					return fmt.Errorf("%s did not accept connections on port %d within %s", inst.Release, appPort, timeout)
				}
				return fmt.Errorf("%s did not answer 2xx on GET %s within %s (last: %s)", inst.Release, h.Path, timeout, last)
			}
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// listening reports whether something at addr accepts a connection and
// keeps it. A connect alone is not enough: Docker's userland proxy accepts
// on the host port as soon as the container starts and closes the
// connection at once when nothing listens inside yet. So the probe reads
// with a short deadline; the deadline passing (or the app speaking first)
// means a server holds the connection, EOF or a reset means nothing does.
func listening(ctx context.Context, addr string, dialTimeout time.Duration) bool {
	c, err := (&net.Dialer{Timeout: dialTimeout}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	defer func() { _ = c.Close() }()
	_ = c.SetReadDeadline(time.Now().Add(healthHold))
	_, err = c.Read(make([]byte, 1))
	return err == nil || errors.Is(err, os.ErrDeadlineExceeded)
}

// probeClient makes HTTP probes. No keep-alive: each probe is one
// connection, so a probe never holds a socket to an instance that is about
// to be stopped, and no connection pool is shared between apps.
var probeClient = &http.Client{
	Timeout:   probeTimeout,
	Transport: &http.Transport{DisableKeepAlives: true},
	// A health endpoint that redirects is not healthy; report the 3xx.
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

// answers reports whether GET path on the instance returns 2xx, and what it
// saw when it did not. The Host header is the app's own hostname, so an app
// that routes by host answers the same way it will for real requests.
func answers(ctx context.Context, addr, host, path string) (ok bool, detail string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, nil)
	if err != nil {
		return false, err.Error()
	}
	req.Host = host
	resp, err := probeClient.Do(req)
	if err != nil {
		return false, err.Error()
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	return resp.StatusCode/100 == 2, resp.Status
}

// exited reports whether the instance's process has already ended.
func (a *actor) exited(ctx context.Context, id string) bool {
	list, err := a.sup.runtime.List(ctx)
	if err != nil {
		return false
	}
	for _, inst := range list {
		if inst.ID == id {
			return !inst.Running
		}
	}
	return false
}

// tail shows the pusher the last lines an instance wrote before it died.
func (a *actor) tail(ctx context.Context, id string, out io.Writer) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var buf bytes.Buffer
	_ = a.sup.runtime.Logs(ctx, id, time.Time{}, &buf, &buf)
	lines := bytes.Split(bytes.TrimRight(buf.Bytes(), "\n"), []byte{'\n'})
	if len(lines) > logTail {
		lines = lines[len(lines)-logTail:]
	}
	for _, l := range lines {
		if len(l) > 0 {
			_, _ = fmt.Fprintf(out, "       %s\n", l)
		}
	}
}
