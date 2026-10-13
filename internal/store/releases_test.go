package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func newApp(t *testing.T, s *Store, name string) App {
	t.Helper()
	a, err := s.CreateApp(context.Background(), name, name+".localhost")
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func mustRelease(t *testing.T, s *Store, appID int64, n NewRelease) Release {
	t.Helper()
	r, err := s.CreateRelease(context.Background(), appID, n)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCreateReleaseNumbersPerApp(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	a, b := newApp(t, s, "a"), newApp(t, s, "b")

	r1 := mustRelease(t, s, a.ID, NewRelease{GitSHA: "1111111111111111111111111111111111111111", Note: "push 1111111"})
	r2 := mustRelease(t, s, a.ID, NewRelease{Image: "ashore/a:2222222"})
	rb := mustRelease(t, s, b.ID, NewRelease{Image: "ashore/b:1111111"})
	if r1.Version != 1 || r2.Version != 2 || rb.Version != 1 {
		t.Errorf("versions a=%d,%d b=%d, want 1,2 and 1", r1.Version, r2.Version, rb.Version)
	}
	// No image yet means the build has not run: building. With one: starting.
	if r1.Status != StatusBuilding || r1.Image != "" || r1.ConfigJSON != "{}" || r1.ManifestJSON != "{}" || r1.Name() != "v1" {
		t.Errorf("release without image = %+v", r1)
	}
	if r2.Status != StatusStarting || r2.GitSHA != "" {
		t.Errorf("release with image = %+v", r2)
	}

	got, err := s.GetRelease(ctx, r1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got != r1 {
		t.Errorf("GetRelease = %+v, want %+v", got, r1)
	}
	if got, err := s.ReleaseByVersion(ctx, a.ID, 2); err != nil || got != r2 {
		t.Errorf("ReleaseByVersion(a, 2) = %+v, %v", got, err)
	}
	if _, err := s.ReleaseByVersion(ctx, b.ID, 2); !errors.Is(err, ErrNotFound) {
		t.Errorf("ReleaseByVersion(b, 2): %v, want ErrNotFound", err)
	}
	list, err := s.ListReleases(ctx, a.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].ID != r1.ID || list[1].ID != r2.ID {
		t.Errorf("ListReleases = %+v", list)
	}
}

func TestCreateReleaseRejectsBadInput(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	if _, err := s.CreateRelease(ctx, 999, NewRelease{Image: "img"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown app: err = %v, want ErrNotFound", err)
	}
	a := newApp(t, s, "a")
	for _, bad := range []string{"nope", "[1]", `{"A": 1}`, `{"A": null}`, "null"} {
		if _, err := s.CreateRelease(ctx, a.ID, NewRelease{ConfigJSON: bad}); err == nil {
			t.Errorf("config %q accepted", bad)
		}
	}
	if _, err := s.CreateRelease(ctx, a.ID, NewRelease{ManifestJSON: "not json"}); err == nil {
		t.Error("bad manifest accepted")
	}
	r := mustRelease(t, s, a.ID, NewRelease{ConfigJSON: `{"GREETING": "hi"}`, ManifestJSON: `{"health":{"path":"/healthz"}}`})
	if got, _ := s.GetRelease(ctx, r.ID); got.ConfigJSON != `{"GREETING": "hi"}` || got.ManifestJSON != `{"health":{"path":"/healthz"}}` {
		t.Errorf("stored as %+v", got)
	}
	if r.Version != 1 {
		t.Errorf("rejected releases consumed version numbers: got v%d", r.Version)
	}
}

func TestParseVersion(t *testing.T) {
	for in, want := range map[string]int{"v1": 1, "v42": 42, "1": 0, "v": 0, "v0": 0, "v-1": 0, "v+1": 0, "vx": 0, "": 0, "V2": 0} {
		got, err := ParseVersion(in)
		if (err == nil) != (want != 0) || got != want {
			t.Errorf("ParseVersion(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
}

// releaseIn makes a release of app in the given status by legal moves only.
func releaseIn(t *testing.T, s *Store, appID int64, status string) Release {
	t.Helper()
	ctx := context.Background()
	var r Release
	switch status {
	case StatusBuilding:
		r = mustRelease(t, s, appID, NewRelease{})
	case StatusStarting:
		r = mustRelease(t, s, appID, NewRelease{Image: "img"})
	case StatusLive:
		r = mustRelease(t, s, appID, NewRelease{Image: "img"})
		if err := s.SetLive(ctx, appID, r.ID); err != nil {
			t.Fatal(err)
		}
	case StatusFailed:
		r = mustRelease(t, s, appID, NewRelease{})
		if err := s.MarkFailed(ctx, r.ID); err != nil {
			t.Fatal(err)
		}
	case StatusSuperseded:
		r = releaseIn(t, s, appID, StatusLive)
		next := releaseIn(t, s, appID, StatusLive)
		if next.ID == r.ID {
			t.Fatal("no second release")
		}
	}
	got, err := s.GetRelease(ctx, r.ID)
	if err != nil || got.Status != status {
		t.Fatalf("releaseIn(%s) = %+v, %v", status, got, err)
	}
	return got
}

var allStatuses = []string{StatusBuilding, StatusStarting, StatusLive, StatusFailed, StatusSuperseded}

// TestReleaseTransitions drives every move from every status. Only the
// moves in the diagram at the top of releases.go succeed; the rest are
// ErrInvalidTransition and change nothing.
func TestReleaseTransitions(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	moves := []struct {
		name string
		move func(appID, id int64) error
		to   string
		from []string // the statuses the move is allowed from
	}{
		{"MarkBuilt", func(_, id int64) error { return s.MarkBuilt(ctx, id, "built") }, StatusStarting, []string{StatusBuilding}},
		{"MarkFailed", func(_, id int64) error { return s.MarkFailed(ctx, id) }, StatusFailed, []string{StatusBuilding, StatusStarting}},
		{"SetLive", func(appID, id int64) error { return s.SetLive(ctx, appID, id) }, StatusLive, []string{StatusStarting}},
	}
	n := 0
	for _, m := range moves {
		for _, from := range allStatuses {
			t.Run(m.name+" from "+from, func(t *testing.T) {
				n++
				app := newApp(t, s, fmt.Sprintf("app%d", n)) // a fresh app isolates the live row
				r := releaseIn(t, s, app.ID, from)
				err := m.move(app.ID, r.ID)
				allowed := false
				for _, f := range m.from {
					allowed = allowed || f == from
				}
				after, _ := s.GetRelease(ctx, r.ID)
				switch {
				case allowed && err != nil:
					t.Fatalf("legal move failed: %v", err)
				case allowed && after.Status != m.to:
					t.Errorf("status after move = %s, want %s", after.Status, m.to)
				case !allowed && !errors.Is(err, ErrInvalidTransition):
					t.Errorf("err = %v, want ErrInvalidTransition", err)
				case !allowed && after.Status != from:
					t.Errorf("a refused move changed the status to %s", after.Status)
				}
				if m.name == "MarkBuilt" && allowed && after.Image != "built" {
					t.Errorf("MarkBuilt did not record the image: %+v", after)
				}
				if !allowed && err != nil && !errors.Is(err, ErrInvalidTransition) {
					t.Errorf("err = %v", err)
				}
			})
		}
	}
	a := newApp(t, s, "a")
	if err := s.MarkBuilt(ctx, 999, "img"); !errors.Is(err, ErrNotFound) {
		t.Errorf("MarkBuilt unknown id: %v", err)
	}
	if err := s.MarkFailed(ctx, 999); !errors.Is(err, ErrNotFound) {
		t.Errorf("MarkFailed unknown id: %v", err)
	}
	if r := releaseIn(t, s, a.ID, StatusBuilding); s.MarkBuilt(ctx, r.ID, "") == nil {
		t.Error("MarkBuilt accepted an empty image")
	}
}

func TestSetLiveSupersedes(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	a := newApp(t, s, "a")
	if _, err := s.LiveRelease(ctx, a.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("LiveRelease before any deploy: %v, want ErrNotFound", err)
	}
	r1 := mustRelease(t, s, a.ID, NewRelease{Image: "img1"})
	r2 := mustRelease(t, s, a.ID, NewRelease{Image: "img2"})
	failed := releaseIn(t, s, a.ID, StatusFailed)

	if err := s.SetLive(ctx, a.ID, r1.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.SetLive(ctx, a.ID, r2.ID); err != nil {
		t.Fatal(err)
	}
	status := func(id int64) string {
		r, err := s.GetRelease(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return r.Status
	}
	if status(r1.ID) != StatusSuperseded || status(r2.ID) != StatusLive || status(failed.ID) != StatusFailed {
		t.Errorf("after SetLive(v2): v1 %s, v2 %s, failed %s", status(r1.ID), status(r2.ID), status(failed.ID))
	}
	app, err := s.GetApp(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	if app.LiveReleaseID != r2.ID {
		t.Errorf("app.LiveReleaseID = %d, want %d", app.LiveReleaseID, r2.ID)
	}
	live, err := s.LiveRelease(ctx, a.ID)
	if err != nil || live.ID != r2.ID {
		t.Errorf("LiveRelease = %+v, %v", live, err)
	}

	// An unknown release, or one of another app, is not found and changes nothing.
	b := newApp(t, s, "b")
	rb := mustRelease(t, s, b.ID, NewRelease{Image: "imgb"})
	if err := s.SetLive(ctx, a.ID, rb.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("SetLive with another app's release: %v", err)
	}
	if err := s.SetLive(ctx, a.ID, 999); !errors.Is(err, ErrNotFound) {
		t.Errorf("SetLive with an unknown release: %v", err)
	}
	if status(r2.ID) != StatusLive || status(rb.ID) != StatusStarting {
		t.Errorf("failed SetLive changed statuses: v2 %s, b/v1 %s", status(r2.ID), status(rb.ID))
	}
}

func TestDeleteAppRemovesReleases(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	a := newApp(t, s, "a")
	r := mustRelease(t, s, a.ID, NewRelease{Image: "img"})
	if err := s.SetLive(ctx, a.ID, r.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteApp(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetRelease(ctx, r.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("release survived its app: %v", err)
	}
}
