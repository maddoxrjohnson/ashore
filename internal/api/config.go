// Config vars: read them, change them, and redeploy with the change.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/http"
	"slices"
	"strings"

	"github.com/maddoxrjohnson/ashore/internal/sse"
	"github.com/maddoxrjohnson/ashore/internal/store"
)

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
		parts = append(parts, "config:set "+strings.Join(slices.Sorted(maps.Keys(c.Set)), " "))
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

	st, err := sse.New(w)
	if err != nil {
		s.fail(w, err)
		return
	}
	if !changed {
		st.Line("-----> nothing changed")
		st.Done()
		return
	}
	if _, err := s.store.LiveRelease(r.Context(), app.ID); errors.Is(err, store.ErrNotFound) {
		st.Line("-----> saved; nothing is running yet, the next push uses it")
		st.Done()
		return
	} else if err != nil {
		st.Fail(err)
		return
	}
	// The deploy outlives a client that hangs up: the config is already
	// written, and an abandoned redeploy would leave the running release
	// disagreeing with it. The supervisor's own lifetime still bounds it.
	if err := s.sup.Redeploy(context.WithoutCancel(r.Context()), app.Name, change.Note(), st); err != nil {
		st.Fail(err)
		return
	}
	st.Done()
}
