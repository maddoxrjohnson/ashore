// The pages: the apps list, one app with its releases, instance, config,
// build logs, and the live log stream behind it.
package web

import (
	"errors"
	"io/fs"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/gitcmd"
	"github.com/maddoxrjohnson/ashore/internal/sse"
	"github.com/maddoxrjohnson/ashore/internal/store"
)

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
	cfgVars := make([]configVar, 0, len(vars))
	for _, k := range slices.Sorted(maps.Keys(vars)) {
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
