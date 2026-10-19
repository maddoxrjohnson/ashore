// Package web is the dashboard: a few server-rendered pages on the internal
// listener that show what the API knows and stream live logs into the
// browser. Login is the admin token; a cookie carries the session after
// that. There is no framework and no build step: html/template, embed, and
// a few dozen lines of JavaScript for the log view and the reveal buttons.
package web

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"errors"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/gitcmd"
	"github.com/maddoxrjohnson/ashore/internal/logs"
	"github.com/maddoxrjohnson/ashore/internal/runtime"
	"github.com/maddoxrjohnson/ashore/internal/site"
	"github.com/maddoxrjohnson/ashore/internal/sse"
	"github.com/maddoxrjohnson/ashore/internal/store"
	"github.com/maddoxrjohnson/ashore/internal/supervisor"
)

// Store is the slice of the database the pages read.
type Store interface {
	GetApp(ctx context.Context, name string) (store.App, error)
	ListApps(ctx context.Context) ([]store.App, error)
	ListReleases(ctx context.Context, appID int64) ([]store.Release, error)
	LiveRelease(ctx context.Context, appID int64) (store.Release, error)
	GetConfig(ctx context.Context, appID int64) (map[string]string, error)
}

// Supervisor reports instances.
type Supervisor interface {
	Status(app string) (supervisor.Status, bool)
}

// Logs is the live log source; *logs.Hub satisfies it.
type Logs interface {
	Subscribe(app string, tail int) (<-chan logs.Line, func())
}

// Config is what the pages need to know about the daemon.
type Config struct {
	DataDir string    // where the builder keeps build logs
	Site    site.Site // how apps are named and linked
	Version string
}

const (
	cookieName = "ashore_session"
	sessionTTL = 24 * time.Hour
	logTail    = 100
)

//go:embed templates/*.html static/*
var content embed.FS

// Server serves the dashboard. Create it with New.
type Server struct {
	store Store
	sup   Supervisor
	logs  Logs
	token []byte
	cfg   Config
	log   *slog.Logger
	mux   *http.ServeMux
	pages map[string]*template.Template

	mu       sync.Mutex
	sessions map[string]time.Time // id -> expiry
}

// New wires the pages. token is the admin token the login form accepts.
func New(st Store, sup Supervisor, lg Logs, token string, cfg Config, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{store: st, sup: sup, logs: lg, token: []byte(token), cfg: cfg, log: log,
		mux: http.NewServeMux(), pages: make(map[string]*template.Template), sessions: make(map[string]time.Time)}
	for _, page := range []string{"login", "apps", "app"} {
		s.pages[page] = template.Must(template.New("").Funcs(template.FuncMap{
			"local":  func(t time.Time) string { return t.Local().Format(time.DateTime) },
			"short":  runtime.Short,
			"commit": gitcmd.ShortSHA,
			"since":  since,
		}).ParseFS(content, "templates/layout.html", "templates/"+page+".html"))
	}
	static, _ := fs.Sub(content, "static")
	s.mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServerFS(static)))
	s.mux.HandleFunc("GET /login", s.loginPage)
	s.mux.HandleFunc("POST /login", s.login)
	s.mux.HandleFunc("POST /logout", s.logout)
	s.mux.HandleFunc("GET /{$}", s.withSession(s.apps))
	s.mux.HandleFunc("GET /apps/{app}", s.withSession(s.app))
	s.mux.HandleFunc("GET /apps/{app}/logs", s.withSession(s.appLogs))
	s.mux.HandleFunc("GET /apps/{app}/builds/{sha}", s.withSession(s.buildLog))
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// withSession sends a visitor without a valid session to the login page.
func (s *Server) withSession(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.loggedIn(r) {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		h(w, r)
	}
}

func (s *Server) loggedIn(r *http.Request) bool {
	c, err := r.Cookie(cookieName)
	if err != nil {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	exp, ok := s.sessions[c.Value]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(s.sessions, c.Value)
		return false
	}
	return true
}

// login checks the token in constant time and starts a session. Expired
// sessions are swept here, the one place the map grows.
func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	presented := strings.TrimSpace(r.FormValue("token"))
	if len(s.token) == 0 || subtle.ConstantTimeCompare([]byte(presented), s.token) != 1 {
		s.log.Warn("dashboard login refused", "remote", r.RemoteAddr)
		s.render(w, http.StatusUnauthorized, "login", map[string]any{"Error": "wrong token"})
		return
	}
	id := rand.Text()
	now := time.Now()
	s.mu.Lock()
	for k, exp := range s.sessions {
		if now.After(exp) {
			delete(s.sessions, k)
		}
	}
	s.sessions[id] = now.Add(sessionTTL)
	s.mu.Unlock()
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: id, Path: "/", HttpOnly: true,
		SameSite: http.SameSiteLaxMode, Secure: r.TLS != nil, MaxAge: int(sessionTTL / time.Second)})
	s.log.Info("dashboard login", "remote", r.RemoteAddr)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(cookieName); err == nil {
		s.mu.Lock()
		delete(s.sessions, c.Value)
		s.mu.Unlock()
	}
	http.SetCookie(w, &http.Cookie{Name: cookieName, Value: "", Path: "/", HttpOnly: true, MaxAge: -1})
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (s *Server) loginPage(w http.ResponseWriter, r *http.Request) {
	if s.loggedIn(r) {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	s.render(w, http.StatusOK, "login", nil)
}

// appRow is one line of the apps page.
type appRow struct {
	Name, Host, URL, Live string
	CreatedAt             time.Time
}

func (s *Server) apps(w http.ResponseWriter, r *http.Request) {
	apps, err := s.store.ListApps(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	rows := make([]appRow, 0, len(apps))
	for _, a := range apps {
		row := appRow{Name: a.Name, Host: a.Host, URL: s.cfg.Site.AppURL(a.Host), CreatedAt: a.CreatedAt}
		if a.LiveReleaseID != 0 {
			if rel, err := s.store.LiveRelease(r.Context(), a.ID); err == nil {
				row.Live = rel.Name()
			}
		}
		rows = append(rows, row)
	}
	s.render(w, http.StatusOK, "apps", map[string]any{"Apps": rows})
}

// configVar is one config var; the value is in the page but hidden until
// its reveal button is pressed.
type configVar struct{ Key, Value string }

// releaseRow is one release on the app page.
type releaseRow struct {
	store.Release
	Name     string
	HasBuild bool // a build log exists for its commit
}

func (s *Server) app(w http.ResponseWriter, r *http.Request) {
	a, err := s.store.GetApp(r.Context(), r.PathValue("app"))
	if err != nil {
		s.fail(w, err)
		return
	}
	rels, err := s.store.ListReleases(r.Context(), a.ID)
	if err != nil {
		s.fail(w, err)
		return
	}
	rows := make([]releaseRow, 0, len(rels))
	for i := len(rels) - 1; i >= 0; i-- { // newest first
		rel := rels[i]
		row := releaseRow{Release: rel, Name: rel.Name()}
		if rel.GitSHA != "" {
			_, err := os.Stat(buildLogPath(s.cfg.DataDir, a.Name, rel.GitSHA))
			row.HasBuild = err == nil
		}
		rows = append(rows, row)
	}
	vars, err := s.store.GetConfig(r.Context(), a.ID)
	if err != nil {
		s.fail(w, err)
		return
	}
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	cfgVars := make([]configVar, 0, len(keys))
	for _, k := range keys {
		cfgVars = append(cfgVars, configVar{Key: k, Value: vars[k]})
	}
	data := map[string]any{
		"App":      a,
		"URL":      s.cfg.Site.AppURL(a.Host),
		"Remote":   s.cfg.Site.RemoteURL(a.Name),
		"Releases": rows,
		"Config":   cfgVars,
	}
	if st, ok := s.sup.Status(a.Name); ok {
		data["Status"] = st
	}
	s.render(w, http.StatusOK, "app", data)
}

// appLogs streams the app's lines as server-sent events, the same frames
// the API sends, on a path the session cookie authorizes: EventSource
// cannot send a bearer token.
func (s *Server) appLogs(w http.ResponseWriter, r *http.Request) {
	a, err := s.store.GetApp(r.Context(), r.PathValue("app"))
	if err != nil {
		s.fail(w, err)
		return
	}
	st, err := sse.New(w)
	if err != nil {
		s.fail(w, err)
		return
	}
	ch, cancel := s.logs.Subscribe(a.Name, logTail)
	defer cancel()
	sse.Follow(r.Context(), st, ch)
}

// buildLog serves one build's log as text. The commit is checked to be
// hex before it becomes a path component.
func (s *Server) buildLog(w http.ResponseWriter, r *http.Request) {
	app, sha := r.PathValue("app"), r.PathValue("sha")
	if store.ValidateName(app) != nil || !gitcmd.IsObjectName(sha) {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	data, err := os.ReadFile(buildLogPath(s.cfg.DataDir, app, sha))
	if errors.Is(err, fs.ErrNotExist) {
		http.Error(w, "no build log for "+sha, http.StatusNotFound)
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write(data)
}

// buildLogPath is where the builder writes a commit's log.
func buildLogPath(dataDir, app, sha string) string {
	return filepath.Join(dataDir, "builds", app, sha+".log")
}

// render executes a page inside the layout.
func (s *Server) render(w http.ResponseWriter, status int, page string, data map[string]any) {
	if data == nil {
		data = map[string]any{}
	}
	data["Version"] = s.cfg.Version
	data["Page"] = page
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	if err := s.pages[page].ExecuteTemplate(w, "layout", data); err != nil {
		s.log.Error("render", "page", page, "err", err)
	}
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrNotFound) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	s.log.Error("dashboard error", "err", err)
	http.Error(w, "internal error", http.StatusInternalServerError)
}

// since says how long ago t was, for an instance's uptime.
func since(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return time.Since(t).Truncate(time.Second).String()
}
