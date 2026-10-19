// Package web is the dashboard: a few server-rendered pages on the internal
// listener that show what the API knows and stream live logs into the
// browser. Login is the admin token; a cookie carries the session after
// that. There is no framework and no build step: html/template, embed, and
// a few dozen lines of JavaScript for the log view and the reveal buttons.
package web

import (
	"context"
	"embed"
	"errors"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/gitcmd"
	"github.com/maddoxrjohnson/ashore/internal/logs"
	"github.com/maddoxrjohnson/ashore/internal/runtime"
	"github.com/maddoxrjohnson/ashore/internal/site"
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

// ServeHTTP routes to the pages; the session check is per route, since
// the login page and the static files have none.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
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
