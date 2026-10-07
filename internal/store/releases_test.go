package store

import (
	"context"
	"errors"
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

func TestCreateReleaseNumbersPerApp(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	a, b := newApp(t, s, "a"), newApp(t, s, "b")

	r1, err := s.CreateRelease(ctx, a.ID, "ashore/a:1111111", "1111111111111111111111111111111111111111", "push 1111111")
	if err != nil {
		t.Fatal(err)
	}
	r2, err := s.CreateRelease(ctx, a.ID, "ashore/a:2222222", "", "")
	if err != nil {
		t.Fatal(err)
	}
	rb, err := s.CreateRelease(ctx, b.ID, "ashore/b:1111111", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if r1.Version != 1 || r2.Version != 2 || rb.Version != 1 {
		t.Errorf("versions a=%d,%d b=%d, want 1,2 and 1", r1.Version, r2.Version, rb.Version)
	}
	if r2.Name() != "v2" || r1.Status != StatusStarting || r1.ConfigJSON != "{}" {
		t.Errorf("release = %+v", r1)
	}

	got, err := s.GetRelease(ctx, r1.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got != r1 {
		t.Errorf("GetRelease = %+v, want %+v", got, r1)
	}
	if got, err := s.GetRelease(ctx, r2.ID); err != nil || got.GitSHA != "" {
		t.Errorf("GetRelease without sha = %+v, %v", got, err)
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
	if _, err := s.CreateRelease(ctx, 999, "img", "", ""); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown app: err = %v, want ErrNotFound", err)
	}
	a := newApp(t, s, "a")
	if _, err := s.CreateRelease(ctx, a.ID, "", "", ""); err == nil {
		t.Error("empty image accepted")
	}
}

func TestSetLiveSupersedes(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	a := newApp(t, s, "a")
	if _, err := s.LiveRelease(ctx, a.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("LiveRelease before any deploy: %v, want ErrNotFound", err)
	}
	r1, _ := s.CreateRelease(ctx, a.ID, "img1", "", "")
	r2, _ := s.CreateRelease(ctx, a.ID, "img2", "", "")

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
	if status(r1.ID) != StatusSuperseded || status(r2.ID) != StatusLive {
		t.Errorf("after SetLive(v2): v1 %s, v2 %s", status(r1.ID), status(r2.ID))
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

	// An unknown release, or one of another app, changes nothing.
	b := newApp(t, s, "b")
	rb, _ := s.CreateRelease(ctx, b.ID, "imgb", "", "")
	if err := s.SetLive(ctx, a.ID, rb.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("SetLive with another app's release: %v", err)
	}
	if status(r2.ID) != StatusLive || status(rb.ID) != StatusStarting {
		t.Errorf("failed SetLive changed statuses: v2 %s, b/v1 %s", status(r2.ID), status(rb.ID))
	}
}

func TestSetReleaseStatus(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	a := newApp(t, s, "a")
	r, _ := s.CreateRelease(ctx, a.ID, "img", "", "")
	if err := s.SetReleaseStatus(ctx, r.ID, StatusFailed); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GetRelease(ctx, r.ID); got.Status != StatusFailed {
		t.Errorf("status = %s", got.Status)
	}
	if err := s.SetReleaseStatus(ctx, r.ID, "bogus"); err == nil {
		t.Error("schema accepted an unknown status")
	}
	if err := s.SetReleaseStatus(ctx, 999, StatusFailed); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown id: %v", err)
	}
}

func TestDeleteAppRemovesReleases(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	a := newApp(t, s, "a")
	r, _ := s.CreateRelease(ctx, a.ID, "img", "", "")
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
