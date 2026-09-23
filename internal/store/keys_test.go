package store

import (
	"context"
	"errors"
	"testing"
)

func TestKeyRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := open(t)

	added, err := s.AddKey(ctx, "laptop", "SHA256:abc", "ssh-ed25519 AAAA laptop")
	if err != nil {
		t.Fatal(err)
	}
	if added.ID == 0 || added.CreatedAt.IsZero() {
		t.Fatalf("AddKey returned %+v", added)
	}

	got, err := s.KeyByFingerprint(ctx, "SHA256:abc")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != added.ID || got.Name != "laptop" || got.PubKey != "ssh-ed25519 AAAA laptop" || !got.CreatedAt.Equal(added.CreatedAt) {
		t.Errorf("KeyByFingerprint = %+v, want %+v", got, added)
	}

	if _, err := s.AddKey(ctx, "desktop", "SHA256:def", "ssh-ed25519 BBBB desktop"); err != nil {
		t.Fatal(err)
	}
	keys, err := s.ListKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0].Fingerprint != "SHA256:abc" || keys[1].Fingerprint != "SHA256:def" {
		t.Errorf("ListKeys = %+v, want abc then def", keys)
	}

	if _, err := s.KeyByFingerprint(ctx, "SHA256:nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown fingerprint: got %v, want ErrNotFound", err)
	}
}

func TestAddKeyDuplicateFingerprint(t *testing.T) {
	ctx := context.Background()
	s := open(t)
	if _, err := s.AddKey(ctx, "laptop", "SHA256:abc", "ssh-ed25519 AAAA laptop"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AddKey(ctx, "again", "SHA256:abc", "ssh-ed25519 AAAA again"); !errors.Is(err, ErrExists) {
		t.Errorf("got %v, want ErrExists", err)
	}
	keys, err := s.ListKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 1 {
		t.Errorf("ListKeys = %+v, want exactly one key", keys)
	}
}

func TestAddKeyRequiresFields(t *testing.T) {
	ctx := context.Background()
	s := open(t)
	if _, err := s.AddKey(ctx, "laptop", "", "ssh-ed25519 AAAA"); err == nil {
		t.Error("empty fingerprint accepted")
	}
	if _, err := s.AddKey(ctx, "laptop", "SHA256:abc", ""); err == nil {
		t.Error("empty public key accepted")
	}
	keys, err := s.ListKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(keys) != 0 {
		t.Errorf("ListKeys = %+v, want none", keys)
	}
}
