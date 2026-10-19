package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/maddoxrjohnson/ashore/internal/gitserver"
	"github.com/maddoxrjohnson/ashore/internal/store"
)

// App is an app as the API shows it.
type App struct {
	Name      string    `json:"name"`
	Host      string    `json:"host"`
	Remote    string    `json:"remote"`         // the git remote to push to
	Live      string    `json:"live,omitempty"` // the live release, vN
	CreatedAt time.Time `json:"created_at"`
}

func (s *Server) appInfo(a store.App, live string) App {
	return App{Name: a.Name, Host: a.Host, Remote: s.cfg.Site.RemoteURL(a.Name), Live: live, CreatedAt: a.CreatedAt}
}

// liveName is the app's live release name, or "" when nothing is live.
func (s *Server) liveName(r *http.Request, a store.App) (string, error) {
	if a.LiveReleaseID == 0 {
		return "", nil
	}
	rel, err := s.store.LiveRelease(r.Context(), a.ID)
	if errors.Is(err, store.ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return rel.Name(), nil
}

func (s *Server) listApps(w http.ResponseWriter, r *http.Request) {
	apps, err := s.store.ListApps(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	out := make([]App, 0, len(apps))
	for _, a := range apps {
		live, err := s.liveName(r, a)
		if err != nil {
			s.fail(w, err)
			return
		}
		out = append(out, s.appInfo(a, live))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) getApp(w http.ResponseWriter, r *http.Request) {
	a, err := s.store.GetApp(r.Context(), r.PathValue("app"))
	if err != nil {
		s.fail(w, err)
		return
	}
	live, err := s.liveName(r, a)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, s.appInfo(a, live))
}

// CreateAppRequest is the body of POST /api/v1/apps.
type CreateAppRequest struct {
	Name string `json:"name"`
}

// createApp inserts the row, then creates the bare repository. A repository
// that cannot be created takes the row with it, so the two never disagree.
func (s *Server) createApp(w http.ResponseWriter, r *http.Request) {
	var req CreateAppRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad request body: "+err.Error())
		return
	}
	a, err := s.store.CreateApp(r.Context(), req.Name, s.cfg.Site.AppHost(req.Name))
	if err != nil {
		s.fail(w, err)
		return
	}
	if _, err := gitserver.InitRepo(r.Context(), s.cfg.DataDir, a.Name); err != nil {
		_ = s.store.DeleteApp(r.Context(), a.Name)
		s.fail(w, err)
		return
	}
	s.log.Info("app created", "app", a.Name, "host", a.Host)
	writeJSON(w, http.StatusCreated, s.appInfo(a, ""))
}

// destroyApp stops the app, then removes its rows (releases and config go
// with them) and its repository. Images stay in the local cache; pruning
// them is an operator's job.
func (s *Server) destroyApp(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("app")
	if _, err := s.store.GetApp(r.Context(), name); err != nil {
		s.fail(w, err)
		return
	}
	if err := s.sup.Destroy(r.Context(), name); err != nil {
		s.fail(w, err)
		return
	}
	if err := s.store.DeleteApp(r.Context(), name); err != nil {
		s.fail(w, err)
		return
	}
	if err := os.RemoveAll(gitserver.RepoPath(s.cfg.DataDir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		s.log.Warn("remove repository", "app", name, "err", err)
	}
	s.log.Info("app destroyed", "app", name)
	w.WriteHeader(http.StatusNoContent)
}

// Release is a release as the API shows it.
type Release struct {
	Version   string            `json:"version"` // vN
	Status    string            `json:"status"`
	Image     string            `json:"image,omitempty"`
	Commit    string            `json:"commit,omitempty"`
	Note      string            `json:"note"`
	Config    map[string]string `json:"config"`
	Manifest  json.RawMessage   `json:"manifest"`
	CreatedAt time.Time         `json:"created_at"`
}

func releaseInfo(r store.Release) (Release, error) {
	cfg, err := store.UnmarshalConfig(r.ConfigJSON)
	if err != nil {
		return Release{}, fmt.Errorf("release %s: %w", r.Name(), err)
	}
	m := r.ManifestJSON
	if m == "" {
		m = "{}"
	}
	return Release{Version: r.Name(), Status: r.Status, Image: r.Image, Commit: r.GitSHA, Note: r.Note,
		Config: cfg, Manifest: json.RawMessage(m), CreatedAt: r.CreatedAt}, nil
}

// listReleases answers oldest first; clients order as they like.
func (s *Server) listReleases(w http.ResponseWriter, r *http.Request) {
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
	out := make([]Release, 0, len(rels))
	for _, rel := range rels {
		info, err := releaseInfo(rel)
		if err != nil {
			s.fail(w, err)
			return
		}
		out = append(out, info)
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) getRelease(w http.ResponseWriter, r *http.Request) {
	a, err := s.store.GetApp(r.Context(), r.PathValue("app"))
	if err != nil {
		s.fail(w, err)
		return
	}
	v, err := store.ParseVersion(r.PathValue("version"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	rel, err := s.store.ReleaseByVersion(r.Context(), a.ID, v)
	if err != nil {
		s.fail(w, fmt.Errorf("%s: %w", a.Name, err))
		return
	}
	info, err := releaseInfo(rel)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

// AddKeyRequest is the body of POST /api/v1/keys: a name and one line in
// authorized_keys format, comment optional.
type AddKeyRequest struct {
	Name   string `json:"name"`
	PubKey string `json:"pubkey"`
}

// Key is a registered key as the API shows it.
type Key struct {
	Name        string `json:"name"`
	Fingerprint string `json:"fingerprint"`
}

// addKey parses the key, stores it as <type> <base64> with the comment
// stripped, one canonical line the SSH server can compare byte for byte.
func (s *Server) addKey(w http.ResponseWriter, r *http.Request) {
	var req AddKeyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "bad request body: "+err.Error())
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		writeError(w, http.StatusBadRequest, "key name is required")
		return
	}
	pub, _, _, _, err := ssh.ParseAuthorizedKey([]byte(req.PubKey))
	if err != nil {
		writeError(w, http.StatusBadRequest, "public key: "+err.Error())
		return
	}
	fp := ssh.FingerprintSHA256(pub)
	line := pub.Type() + " " + strings.TrimSpace(strings.TrimPrefix(string(ssh.MarshalAuthorizedKey(pub)), pub.Type()))
	if _, err := s.store.AddKey(r.Context(), req.Name, fp, line); err != nil {
		s.fail(w, err)
		return
	}
	s.log.Info("key added", "name", req.Name, "fingerprint", fp)
	writeJSON(w, http.StatusCreated, Key{Name: req.Name, Fingerprint: fp})
}
