package store

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"
)

func TestValidateKey(t *testing.T) {
	cases := map[string]error{
		"GREETING": nil, "a": nil, "_X1": nil, "lower_case": nil,
		"":                               ErrInvalidKey,
		"1ABC":                           ErrInvalidKey,
		"WITH-DASH":                      ErrInvalidKey,
		"WITH SPACE":                     ErrInvalidKey,
		"A=B":                            ErrInvalidKey,
		strings.Repeat("K", maxKeyLen+1): ErrInvalidKey,
		"PORT":                           ErrReservedKey,
	}
	for key, want := range cases {
		if err := ValidateKey(key); !errors.Is(err, want) {
			t.Errorf("ValidateKey(%q) = %v, want %v", key, err, want)
		}
	}
}

func TestConfigRoundTrip(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	a := newApp(t, s, "a")

	vars, err := s.GetConfig(ctx, a.ID)
	if err != nil || len(vars) != 0 {
		t.Fatalf("fresh app config = %v, %v", vars, err)
	}
	changed, err := s.UpdateConfig(ctx, a.ID, map[string]string{"GREETING": "hi", "MODE": "dev"}, nil)
	if err != nil || !changed {
		t.Fatalf("set: changed=%v err=%v", changed, err)
	}
	vars, _ = s.GetConfig(ctx, a.ID)
	if !maps.Equal(vars, map[string]string{"GREETING": "hi", "MODE": "dev"}) {
		t.Errorf("after set: %v", vars)
	}

	// The same values again are not a change; a new value is.
	if changed, err = s.UpdateConfig(ctx, a.ID, map[string]string{"GREETING": "hi"}, nil); err != nil || changed {
		t.Errorf("same value: changed=%v err=%v", changed, err)
	}
	if changed, err = s.UpdateConfig(ctx, a.ID, map[string]string{"GREETING": "hello"}, nil); err != nil || !changed {
		t.Errorf("new value: changed=%v err=%v", changed, err)
	}

	// Unset removes; unsetting what is not there is not a change.
	if changed, err = s.UpdateConfig(ctx, a.ID, nil, []string{"MODE"}); err != nil || !changed {
		t.Errorf("unset: changed=%v err=%v", changed, err)
	}
	if changed, err = s.UpdateConfig(ctx, a.ID, nil, []string{"MODE"}); err != nil || changed {
		t.Errorf("unset again: changed=%v err=%v", changed, err)
	}
	vars, _ = s.GetConfig(ctx, a.ID)
	if !maps.Equal(vars, map[string]string{"GREETING": "hello"}) {
		t.Errorf("after unset: %v", vars)
	}

	// Set and unset in one call, and an empty value is a value.
	if _, err = s.UpdateConfig(ctx, a.ID, map[string]string{"EMPTY": ""}, []string{"GREETING"}); err != nil {
		t.Fatal(err)
	}
	vars, _ = s.GetConfig(ctx, a.ID)
	if !maps.Equal(vars, map[string]string{"EMPTY": ""}) {
		t.Errorf("after set+unset: %v", vars)
	}

	// Another app's vars are separate, and go with the app.
	b := newApp(t, s, "b")
	if _, err := s.UpdateConfig(ctx, b.ID, map[string]string{"ONLY_B": "1"}, nil); err != nil {
		t.Fatal(err)
	}
	if vars, _ = s.GetConfig(ctx, a.ID); len(vars) != 1 {
		t.Errorf("a sees b's vars: %v", vars)
	}
	if err := s.DeleteApp(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM config WHERE app_id = ?`, b.ID).Scan(&n); err != nil || n != 0 {
		t.Errorf("config rows left after deleting the app: %d, %v", n, err)
	}
}

func TestUpdateConfigRejects(t *testing.T) {
	s := open(t)
	ctx := context.Background()
	a := newApp(t, s, "a")
	if _, err := s.UpdateConfig(ctx, 999, map[string]string{"A": "1"}, nil); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown app: %v", err)
	}
	// A bad key anywhere in the call means nothing is written.
	_, err := s.UpdateConfig(ctx, a.ID, map[string]string{"GOOD": "1", "1BAD": "2"}, nil)
	if !errors.Is(err, ErrInvalidKey) {
		t.Errorf("bad key: %v", err)
	}
	if _, err := s.UpdateConfig(ctx, a.ID, nil, []string{"PORT"}); !errors.Is(err, ErrReservedKey) {
		t.Errorf("unset PORT: %v", err)
	}
	if vars, _ := s.GetConfig(ctx, a.ID); len(vars) != 0 {
		t.Errorf("a rejected call wrote %v", vars)
	}
}

func TestConfigSnapshot(t *testing.T) {
	if got := MarshalConfig(nil); got != "{}" {
		t.Errorf("MarshalConfig(nil) = %q", got)
	}
	got := MarshalConfig(map[string]string{"B": "2", "A": "1", "Q": `say "hi"`})
	if got != `{"A":"1","B":"2","Q":"say \"hi\""}` {
		t.Errorf("MarshalConfig = %s", got)
	}
	back, err := UnmarshalConfig(got)
	if err != nil || !maps.Equal(back, map[string]string{"A": "1", "B": "2", "Q": `say "hi"`}) {
		t.Errorf("UnmarshalConfig = %v, %v", back, err)
	}
	if back, err := UnmarshalConfig(""); err != nil || len(back) != 0 {
		t.Errorf("UnmarshalConfig(\"\") = %v, %v", back, err)
	}
	for _, bad := range []string{"nope", `{"A": 1}`, `[]`, `null`} {
		if _, err := UnmarshalConfig(bad); err == nil {
			t.Errorf("UnmarshalConfig(%q) accepted", bad)
		}
	}
}
