// The operations that run a deploy and stream it: restart, rollback, and
// the instance report behind ps.
package api

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/maddoxrjohnson/ashore/internal/sse"
	"github.com/maddoxrjohnson/ashore/internal/store"
	"github.com/maddoxrjohnson/ashore/internal/supervisor"
)

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
	st, err := sse.New(w)
	if err != nil {
		s.fail(w, err)
		return
	}
	if err := op(context.WithoutCancel(r.Context()), st); err != nil {
		st.Fail(err)
		return
	}
	st.Done()
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
