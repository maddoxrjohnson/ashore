// Package api is the daemon's HTTP API, which the ashore CLI and the
// dashboard use. It listens on the internal address only (never the
// router's public port), every request carries the admin token as a bearer
// token, and bodies and results are JSON. Operations that take a while, a
// redeploy after a config change, stream their progress as server-sent
// events so a terminal sees the same lines a pusher would.
package api

import (
	"context"
	"crypto/subtle"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/logs"
	"github.com/maddoxrjohnson/ashore/internal/serve"
	"github.com/maddoxrjohnson/ashore/internal/site"
	"github.com/maddoxrjohnson/ashore/internal/store"
	"github.com/maddoxrjohnson/ashore/internal/supervisor"
)

// Store is the slice of the database the API reads and writes.
type Store interface {
	GetApp(ctx context.Context, name string) (store.App, error)
	CreateApp(ctx context.Context, name, host string) (store.App, error)
	DeleteApp(ctx context.Context, name string) error
	ListApps(ctx context.Context) ([]store.App, error)
	GetConfig(ctx context.Context, appID int64) (map[string]string, error)
	UpdateConfig(ctx context.Context, appID int64, set map[string]string, unset []string) (bool, error)
	LiveRelease(ctx context.Context, appID int64) (store.Release, error)
	ListReleases(ctx context.Context, appID int64) ([]store.Release, error)
	ReleaseByVersion(ctx context.Context, appID int64, version int) (store.Release, error)
	AddKey(ctx context.Context, name, fingerprint, pubkey string) (store.Key, error)
}

// Logs is where the API reads an app's output; *logs.Hub satisfies it.
type Logs interface {
	Tail(app string, n int) []logs.Line
	Subscribe(app string, tail int) (<-chan logs.Line, func())
}

// Config is what the API needs to know about the daemon it fronts.
type Config struct {
	DataDir string    // where app repositories are created and removed
	Site    site.Site // how apps are named and reached
}

// Supervisor is what the API asks the deploy machinery to do and to report.
type Supervisor interface {
	Redeploy(ctx context.Context, app, note string, out io.Writer) error
	Rollback(ctx context.Context, app string, version int, out io.Writer) error
	Restart(ctx context.Context, app string, out io.Writer) error
	// Destroy stops the app's instance and removes its route.
	Destroy(ctx context.Context, app string) error
	// Status is the app's instance as the supervisor sees it; ok is false
	// when it has never started one since the daemon came up.
	Status(app string) (st supervisor.Status, ok bool)
}

const maxBody = 1 << 20 // config and keys are small

// Server serves the API. Create it with New.
type Server struct {
	store Store
	sup   Supervisor
	logs  Logs
	token []byte
	cfg   Config
	log   *slog.Logger
	mux   *http.ServeMux
}

// New wires the routes. token is the admin token every request must carry.
// lg may be nil, in which case the logs endpoint answers 503. A nil logger
// means slog.Default.
func New(st Store, sup Supervisor, lg Logs, token string, cfg Config, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{store: st, sup: sup, logs: lg, token: []byte(token), cfg: cfg, log: log, mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /api/v1/apps", s.listApps)
	s.mux.HandleFunc("POST /api/v1/apps", s.createApp)
	s.mux.HandleFunc("GET /api/v1/apps/{app}", s.getApp)
	s.mux.HandleFunc("DELETE /api/v1/apps/{app}", s.destroyApp)
	s.mux.HandleFunc("GET /api/v1/apps/{app}/releases", s.listReleases)
	s.mux.HandleFunc("GET /api/v1/apps/{app}/releases/{version}", s.getRelease)
	s.mux.HandleFunc("POST /api/v1/keys", s.addKey)
	s.mux.HandleFunc("GET /api/v1/apps/{app}/config", s.getConfig)
	s.mux.HandleFunc("PATCH /api/v1/apps/{app}/config", s.updateConfig)
	s.mux.HandleFunc("GET /api/v1/apps/{app}/ps", s.ps)
	s.mux.HandleFunc("POST /api/v1/apps/{app}/restart", s.restart)
	s.mux.HandleFunc("POST /api/v1/apps/{app}/rollback", s.rollback)
	s.mux.HandleFunc("GET /api/v1/apps/{app}/logs", s.appLogs)
	return s
}

// ServeHTTP checks the token, then routes. Everything under /api/ needs the
// token, including unknown paths, so a scan learns nothing about the routes.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &serve.Recorder{ResponseWriter: w}
	if !s.authorized(r) {
		rw.Header().Set("WWW-Authenticate", `Bearer realm="ashore"`)
		writeError(rw, http.StatusUnauthorized, "missing or wrong token")
	} else {
		r.Body = http.MaxBytesReader(rw, r.Body, maxBody)
		s.mux.ServeHTTP(rw, r)
	}
	s.log.Info("api", "method", r.Method, "path", r.URL.Path, "status", rw.Status, "duration", time.Since(start), "remote", r.RemoteAddr)
}

// authorized compares the bearer token in constant time, so the response
// time does not say how much of a guess was right.
func (s *Server) authorized(r *http.Request) bool {
	presented, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok || len(s.token) == 0 {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(presented), s.token) == 1
}

// Serve runs h on ln until ctx is done, then shuts down with a deadline,
// the same shape as the router's Serve. h is the whole internal listener's
// handler, of which this server is the /api/ part; nil means the API alone.
func (s *Server) Serve(ctx context.Context, ln net.Listener, h http.Handler) error {
	if h == nil {
		h = s
	}
	return serve.HTTP(ctx, ln, h, "api", s.log)
}
