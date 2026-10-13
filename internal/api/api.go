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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"

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

// Config is what the API needs to know about the daemon it fronts.
type Config struct {
	DataDir string // where app repositories are created and removed
	Domain  string // apps are <name>.<Domain>
	SSHAddr string // the git listener, for the remote URL apps:create prints
}

// RemoteURL is the git remote a pusher adds for app: the SSH listener's
// host and port, or the domain when it listens on every interface.
func (c Config) RemoteURL(app string) string {
	host, port, err := net.SplitHostPort(c.SSHAddr)
	if err != nil {
		return "ssh://ashore@" + c.Domain + "/" + app
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = c.Domain
	}
	return "ssh://ashore@" + net.JoinHostPort(host, port) + "/" + app
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

const (
	readHeaderTimeout = 10 * time.Second
	idleTimeout       = 2 * time.Minute
	shutdownTimeout   = 10 * time.Second
	maxBody           = 1 << 20 // config and keys are small
)

// Server serves the API. Create it with New.
type Server struct {
	store Store
	sup   Supervisor
	token []byte
	cfg   Config
	log   *slog.Logger
	mux   *http.ServeMux
}

// New wires the routes. token is the admin token every request must carry.
// A nil logger means slog.Default.
func New(st Store, sup Supervisor, token string, cfg Config, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{store: st, sup: sup, token: []byte(token), cfg: cfg, log: log, mux: http.NewServeMux()}
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
	return s
}

// ServeHTTP checks the token, then routes. Everything under /api/ needs the
// token, including unknown paths, so a scan learns nothing about the routes.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rw := &recorder{ResponseWriter: w}
	if !s.authorized(r) {
		rw.Header().Set("WWW-Authenticate", `Bearer realm="ashore"`)
		writeError(rw, http.StatusUnauthorized, "missing or wrong token")
	} else {
		r.Body = http.MaxBytesReader(rw, r.Body, maxBody)
		s.mux.ServeHTTP(rw, r)
	}
	s.log.Info("api", "method", r.Method, "path", r.URL.Path, "status", rw.status, "duration", time.Since(start), "remote", r.RemoteAddr)
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

// Serve runs the API on ln until ctx is done, then shuts down with a
// deadline, the same shape as the router's Serve.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{
		Handler:           s,
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		ErrorLog:          slog.NewLogLogger(s.log.Handler(), slog.LevelWarn),
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case err := <-errc:
		return fmt.Errorf("api serve: %w", err)
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(sctx); err != nil {
		s.log.Warn("api shutdown", "err", err)
		_ = srv.Close()
	}
	<-errc
	return nil
}

// getConfig answers with the app's config vars as one JSON object.
func (s *Server) getConfig(w http.ResponseWriter, r *http.Request) {
	app, err := s.store.GetApp(r.Context(), r.PathValue("app"))
	if err != nil {
		s.fail(w, err)
		return
	}
	vars, err := s.store.GetConfig(r.Context(), app.ID)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, vars)
}

// ConfigChange is the body of PATCH .../config: vars to set and keys to
// unset, applied together.
type ConfigChange struct {
	Set   map[string]string `json:"set,omitempty"`
	Unset []string          `json:"unset,omitempty"`
}

// Note is the release note a change makes: "config:set A B", "config:unset
// C", or both joined with "; ". Keys only; values never leave the store
// except as the app's environment.
func (c ConfigChange) Note() string {
	var parts []string
	if len(c.Set) > 0 {
		keys := make([]string, 0, len(c.Set))
		for k := range c.Set {
			keys = append(keys, k)
		}
		sortStrings(keys)
		parts = append(parts, "config:set "+strings.Join(keys, " "))
	}
	if len(c.Unset) > 0 {
		parts = append(parts, "config:unset "+strings.Join(c.Unset, " "))
	}
	return strings.Join(parts, "; ")
}

// updateConfig writes the change, then redeploys the live release with it
// and streams the deploy as events. The config is saved before the stream
// starts, so a bad key is a plain 400 and a saved change is never lost to
// a failed deploy: the next push picks it up.
func (s *Server) updateConfig(w http.ResponseWriter, r *http.Request) {
	app, err := s.store.GetApp(r.Context(), r.PathValue("app"))
	if err != nil {
		s.fail(w, err)
		return
	}
	var change ConfigChange
	if err := json.NewDecoder(r.Body).Decode(&change); err != nil {
		writeError(w, http.StatusBadRequest, "bad request body: "+err.Error())
		return
	}
	if len(change.Set) == 0 && len(change.Unset) == 0 {
		writeError(w, http.StatusBadRequest, "nothing to set or unset")
		return
	}
	changed, err := s.store.UpdateConfig(r.Context(), app.ID, change.Set, change.Unset)
	if err != nil {
		s.fail(w, err)
		return
	}

	st, err := newStream(w)
	if err != nil {
		s.fail(w, err)
		return
	}
	if !changed {
		st.line("-----> nothing changed")
		st.done()
		return
	}
	if _, err := s.store.LiveRelease(r.Context(), app.ID); errors.Is(err, store.ErrNotFound) {
		st.line("-----> saved; nothing is running yet, the next push uses it")
		st.done()
		return
	} else if err != nil {
		st.fail(err)
		return
	}
	// The deploy outlives a client that hangs up: the config is already
	// written, and an abandoned redeploy would leave the running release
	// disagreeing with it. The supervisor's own lifetime still bounds it.
	if err := s.sup.Redeploy(context.WithoutCancel(r.Context()), app.Name, change.Note(), st); err != nil {
		st.fail(err)
		return
	}
	st.done()
}

// restart streams a restart of the app's live release.
func (s *Server) restart(w http.ResponseWriter, r *http.Request) {
	app, err := s.store.GetApp(r.Context(), r.PathValue("app"))
	if err != nil {
		s.fail(w, err)
		return
	}
	s.stream(w, r, func(ctx context.Context, out io.Writer) error {
		return s.sup.Restart(ctx, app.Name, out)
	})
}

// RollbackRequest is the body of POST .../rollback.
type RollbackRequest struct {
	Version string `json:"version"` // "v3"
}

// rollback streams a rollback to the named release.
func (s *Server) rollback(w http.ResponseWriter, r *http.Request) {
	app, err := s.store.GetApp(r.Context(), r.PathValue("app"))
	if err != nil {
		s.fail(w, err)
		return
	}
	var req RollbackRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad request body: "+err.Error())
		return
	}
	version, err := store.ParseVersion(req.Version)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	s.stream(w, r, func(ctx context.Context, out io.Writer) error {
		return s.sup.Rollback(ctx, app.Name, version, out)
	})
}

// stream runs a long operation with its output as events and its error as
// the final one. The operation gets a context the client cannot cancel: a
// deploy half done is worse than one the client did not watch to the end.
func (s *Server) stream(w http.ResponseWriter, r *http.Request, op func(ctx context.Context, out io.Writer) error) {
	st, err := newStream(w)
	if err != nil {
		s.fail(w, err)
		return
	}
	if err := op(context.WithoutCancel(r.Context()), st); err != nil {
		st.fail(err)
		return
	}
	st.done()
}

// PS is the answer to GET .../ps: the app's instances. One per app today;
// a list so that scaling out does not change the shape.
type PS struct {
	App       string              `json:"app"`
	Instances []supervisor.Status `json:"instances"`
}

// ps reports the app's instance. An app that exists but has nothing
// running since the daemon started answers with an empty list, not a 404:
// "no instances" is a true statement about a real app.
func (s *Server) ps(w http.ResponseWriter, r *http.Request) {
	app, err := s.store.GetApp(r.Context(), r.PathValue("app"))
	if err != nil {
		s.fail(w, err)
		return
	}
	out := PS{App: app.Name, Instances: []supervisor.Status{}}
	if st, ok := s.sup.Status(app.Name); ok {
		out.Instances = append(out.Instances, st)
	}
	writeJSON(w, http.StatusOK, out)
}

// fail maps a store error to a status. Anything unexpected is a 500 with a
// generic body and the detail in the log.
func (s *Server) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, store.ErrExists):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, store.ErrInvalidName), errors.Is(err, store.ErrInvalidKey), errors.Is(err, store.ErrReservedKey):
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		s.log.Error("api internal error", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

// errorBody is every non-2xx JSON response.
type errorBody struct {
	Error string `json:"error"`
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorBody{Error: msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// recorder keeps the status for the request log.
type recorder struct {
	http.ResponseWriter
	status int
}

func (rw *recorder) WriteHeader(code int) {
	if rw.status == 0 {
		rw.status = code
	}
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *recorder) Write(p []byte) (int, error) {
	if rw.status == 0 {
		rw.status = http.StatusOK
	}
	return rw.ResponseWriter.Write(p)
}

func (rw *recorder) Unwrap() http.ResponseWriter { return rw.ResponseWriter }
