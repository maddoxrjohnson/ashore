package store

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestValidateName(t *testing.T) {
	tests := []struct {
		name string
		ok   bool
	}{
		{"a", true},
		{"hello", true},
		{"my-app-2", true},
		{"0", true},
		{strings.Repeat("a", 32), true},
		{"", false},
		{"Hello", false},
		{"my_app", false},
		{"a.b", false},
		{"..", false},
		{"a/b", false},
		{"hello world", false},
		{strings.Repeat("a", 33), false},
	}
	for _, tt := range tests {
		err := ValidateName(tt.name)
		if tt.ok && err != nil {
			t.Errorf("%q: unexpected error %v", tt.name, err)
		}
		if !tt.ok && !errors.Is(err, ErrInvalidName) {
			t.Errorf("%q: got %v, want ErrInvalidName", tt.name, err)
		}
	}
}

func sameApp(a, b App) bool {
	return a.ID == b.ID && a.Name == b.Name && a.Host == b.Host && a.CreatedAt.Equal(b.CreatedAt)
}

func TestAppRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	created, err := s.CreateApp(ctx, "hello", "hello.localhost")
	if err != nil {
		t.Fatal(err)
	}
	if created.ID == 0 || created.Name != "hello" || created.Host != "hello.localhost" || created.CreatedAt.IsZero() {
		t.Fatalf("CreateApp returned %+v", created)
	}

	got, err := s.GetApp(ctx, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if !sameApp(got, created) {
		t.Errorf("GetApp = %+v, want %+v", got, created)
	}

	if _, err := s.CreateApp(ctx, "alpha", "alpha.localhost"); err != nil {
		t.Fatal(err)
	}
	apps, err := s.ListApps(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(apps) != 2 || apps[0].Name != "alpha" || apps[1].Name != "hello" {
		t.Errorf("ListApps = %+v, want alpha then hello", apps)
	}

	if err := s.DeleteApp(ctx, "hello"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetApp(ctx, "hello"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetApp after delete = %v, want ErrNotFound", err)
	}
	if err := s.DeleteApp(ctx, "hello"); !errors.Is(err, ErrNotFound) {
		t.Errorf("second DeleteApp = %v, want ErrNotFound", err)
	}
	apps, err = s.ListApps(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(apps) != 1 || apps[0].Name != "alpha" {
		t.Errorf("ListApps after delete = %+v, want only alpha", apps)
	}
}

func TestCreateAppDuplicate(t *testing.T) {
	ctx := context.Background()
	s := open(t)
	if _, err := s.CreateApp(ctx, "hello", "hello.localhost"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateApp(ctx, "hello", "other.localhost"); !errors.Is(err, ErrExists) {
		t.Errorf("duplicate name: got %v, want ErrExists", err)
	}
	if _, err := s.CreateApp(ctx, "other", "hello.localhost"); !errors.Is(err, ErrExists) {
		t.Errorf("duplicate host: got %v, want ErrExists", err)
	}
	apps, err := s.ListApps(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(apps) != 1 {
		t.Errorf("ListApps = %+v, want exactly one app", apps)
	}
}

func TestCreateAppInvalidName(t *testing.T) {
	ctx := context.Background()
	s := open(t)
	if _, err := s.CreateApp(ctx, "Hello", "hello.localhost"); !errors.Is(err, ErrInvalidName) {
		t.Errorf("got %v, want ErrInvalidName", err)
	}
	apps, err := s.ListApps(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(apps) != 0 {
		t.Errorf("ListApps = %+v, want none", apps)
	}
}

func TestCancelledContext(t *testing.T) {
	s := open(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.ListApps(ctx); err == nil {
		t.Error("ListApps ignored a cancelled context")
	}
}
