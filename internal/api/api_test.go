package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/store"
	"github.com/maddoxrjohnson/ashore/internal/supervisor"
)

const token = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// fakeSup records redeploys and answers with a few lines, or an error.
type fakeSup struct {
	err    error
	status map[string]supervisor.Status

	mu    sync.Mutex
	calls []string // "app: note"
}

func (f *fakeSup) Status(app string) (supervisor.Status, bool) {
	st, ok := f.status[app]
	return st, ok
}

func (f *fakeSup) Redeploy(_ context.Context, app, note string, out io.Writer) error {
	f.mu.Lock()
	f.calls = append(f.calls, app+": "+note)
	f.mu.Unlock()
	_, _ = fmt.Fprintf(out, "-----> release v2 (%s)\n", note)
	_, _ = io.WriteString(out, "-----> waiting for v2 to listen on port 8080\n-----> live at ")
	_, _ = io.WriteString(out, "http://hello.localhost:8080\n")
	return f.err
}

func (f *fakeSup) redeploys() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

type fixture struct {
	store  *store.Store
	sup    *fakeSup
	server *httptest.Server
	client *Client
	app    store.App
}

// newFixture serves the API over a real store with one app, hello.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(ctx, filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	app, err := st.CreateApp(ctx, "hello", "hello.localhost")
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{store: st, sup: &fakeSup{}, app: app}
	f.server = httptest.NewServer(New(st, f.sup, token, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(f.server.Close)
	f.client = &Client{URL: f.server.URL, Token: token, HTTP: f.server.Client()}
	return f
}

// goLive gives hello a live release so a config change redeploys.
func (f *fixture) goLive(t *testing.T) {
	t.Helper()
	r, err := f.store.CreateRelease(context.Background(), f.app.ID, store.NewRelease{Image: "ashore/hello:abc1234"})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.SetLive(context.Background(), f.app.ID, r.ID); err != nil {
		t.Fatal(err)
	}
}

func (f *fixture) raw(t *testing.T, method, path, auth, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, f.server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := f.server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestAuth(t *testing.T) {
	f := newFixture(t)
	cases := []struct {
		name, auth string
		want       int
	}{
		{"no header", "", http.StatusUnauthorized},
		{"wrong scheme", "Basic " + token, http.StatusUnauthorized},
		{"wrong token", "Bearer " + strings.Repeat("0", len(token)), http.StatusUnauthorized},
		{"short token", "Bearer 0123", http.StatusUnauthorized},
		{"right token", "Bearer " + token, http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := f.raw(t, http.MethodGet, "/api/v1/apps/hello/config", tc.auth, "")
			if resp.StatusCode != tc.want {
				t.Errorf("status %d, want %d", resp.StatusCode, tc.want)
			}
			if tc.want == http.StatusUnauthorized && resp.Header.Get("WWW-Authenticate") == "" {
				t.Error("401 without WWW-Authenticate")
			}
		})
	}
	// Unknown paths need the token too, and are 404 with it.
	if resp := f.raw(t, http.MethodGet, "/api/v1/nope", "", ""); resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("unknown path without token: %d", resp.StatusCode)
	}
	if resp := f.raw(t, http.MethodGet, "/api/v1/nope", "Bearer "+token, ""); resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown path with token: %d", resp.StatusCode)
	}
}

func TestConfigRoundTrip(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	vars, err := f.client.Config(ctx, "hello")
	if err != nil || len(vars) != 0 {
		t.Fatalf("fresh config = %v, %v", vars, err)
	}

	// Before anything runs, a change is saved and nothing is deployed.
	var out bytes.Buffer
	if err := f.client.UpdateConfig(ctx, "hello", ConfigChange{Set: map[string]string{"GREETING": "hi"}}, &out); err != nil {
		t.Fatalf("set: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "nothing is running yet") || len(f.sup.redeploys()) != 0 {
		t.Errorf("set before a release: %q, redeploys %v", out.String(), f.sup.redeploys())
	}
	vars, _ = f.client.Config(ctx, "hello")
	if !maps.Equal(vars, map[string]string{"GREETING": "hi"}) {
		t.Errorf("config after set = %v", vars)
	}

	// With a live release, a change redeploys and the lines stream back.
	f.goLive(t)
	out.Reset()
	change := ConfigChange{Set: map[string]string{"MODE": "dev", "GREETING": "hello"}, Unset: []string{"NOPE"}}
	if err := f.client.UpdateConfig(ctx, "hello", change, &out); err != nil {
		t.Fatalf("set: %v\n%s", err, out.String())
	}
	want := "-----> release v2 (config:set GREETING MODE; config:unset NOPE)\n-----> waiting for v2 to listen on port 8080\n-----> live at http://hello.localhost:8080\n"
	if out.String() != want {
		t.Errorf("stream:\n%s\nwant:\n%s", out.String(), want)
	}
	if got := f.sup.redeploys(); len(got) != 1 || got[0] != "hello: config:set GREETING MODE; config:unset NOPE" {
		t.Errorf("redeploys = %v", got)
	}

	// The same values again change nothing and deploy nothing.
	out.Reset()
	if err := f.client.UpdateConfig(ctx, "hello", ConfigChange{Set: map[string]string{"MODE": "dev"}}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "nothing changed") || len(f.sup.redeploys()) != 1 {
		t.Errorf("unchanged set: %q, redeploys %v", out.String(), f.sup.redeploys())
	}

	// Unset streams too.
	out.Reset()
	if err := f.client.UpdateConfig(ctx, "hello", ConfigChange{Unset: []string{"MODE"}}, &out); err != nil {
		t.Fatal(err)
	}
	if got := f.sup.redeploys(); len(got) != 2 || got[1] != "hello: config:unset MODE" {
		t.Errorf("redeploys = %v", got)
	}
	vars, _ = f.client.Config(ctx, "hello")
	if !maps.Equal(vars, map[string]string{"GREETING": "hello"}) {
		t.Errorf("config at the end = %v", vars)
	}
}

func TestConfigErrors(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	var out bytes.Buffer
	cases := []struct {
		name   string
		call   func() error
		status int
		msg    string
	}{
		{"get unknown app", func() error { _, err := f.client.Config(ctx, "nope"); return err }, http.StatusNotFound, "not found"},
		{"set unknown app", func() error {
			return f.client.UpdateConfig(ctx, "nope", ConfigChange{Set: map[string]string{"A": "1"}}, &out)
		}, http.StatusNotFound, "not found"},
		{"bad key", func() error {
			return f.client.UpdateConfig(ctx, "hello", ConfigChange{Set: map[string]string{"1BAD": "1"}}, &out)
		}, http.StatusBadRequest, "invalid config key"},
		{"reserved key", func() error {
			return f.client.UpdateConfig(ctx, "hello", ConfigChange{Unset: []string{"PORT"}}, &out)
		}, http.StatusBadRequest, "reserved"},
		{"empty change", func() error { return f.client.UpdateConfig(ctx, "hello", ConfigChange{}, &out) }, http.StatusBadRequest, "nothing to set"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			var ae *Error
			if !errors.As(err, &ae) || ae.Status != tc.status || !strings.Contains(ae.Message, tc.msg) {
				t.Errorf("err = %v, want %d %q", err, tc.status, tc.msg)
			}
		})
	}
	if resp := f.raw(t, http.MethodPatch, "/api/v1/apps/hello/config", "Bearer "+token, "{not json"); resp.StatusCode != http.StatusBadRequest {
		t.Errorf("bad json: %d", resp.StatusCode)
	}
	if vars, _ := f.client.Config(ctx, "hello"); len(vars) != 0 {
		t.Errorf("a rejected change wrote %v", vars)
	}

	// A deploy that fails is an error event after the lines, and the
	// config stays saved: the next push runs with it.
	f.goLive(t)
	f.sup.err = errors.New("v2 exited with code 1 before accepting connections")
	out.Reset()
	err := f.client.UpdateConfig(ctx, "hello", ConfigChange{Set: map[string]string{"A": "1"}}, &out)
	if err == nil || err.Error() != "v2 exited with code 1 before accepting connections" {
		t.Errorf("failed deploy: err = %v", err)
	}
	if !strings.Contains(out.String(), "-----> release v2") {
		t.Errorf("lines before the error were lost:\n%s", out.String())
	}
	if vars, _ := f.client.Config(ctx, "hello"); vars["A"] != "1" {
		t.Errorf("config after a failed redeploy = %v", vars)
	}
}

func TestPS(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	ps, err := f.client.PS(ctx, "hello")
	if err != nil || ps.App != "hello" || len(ps.Instances) != 0 {
		t.Errorf("ps with nothing running = %+v, %v", ps, err)
	}
	if _, err := f.client.PS(ctx, "nope"); !errors.Is(err, &Error{Status: http.StatusNotFound}) {
		t.Errorf("ps unknown app: %v", err)
	}
	started := time.Date(2026, 10, 12, 23, 0, 0, 0, time.UTC)
	f.sup.status = map[string]supervisor.Status{"hello": {
		App: "hello", Release: "v3", Instance: "abc123", Addr: "127.0.0.1:41000",
		State: supervisor.StateRestarting, Restarts: 2, StartedAt: started, Detail: "exited with code 1; restart 3 in 4s",
	}}
	ps, err = f.client.PS(ctx, "hello")
	if err != nil || len(ps.Instances) != 1 || ps.Instances[0] != f.sup.status["hello"] {
		t.Errorf("ps = %+v, %v", ps, err)
	}
	// The JSON shape the dashboard will read.
	resp := f.raw(t, http.MethodGet, "/api/v1/apps/hello/ps", "Bearer "+token, "")
	body, _ := io.ReadAll(resp.Body)
	for _, want := range []string{`"state":"restarting"`, `"restarts":2`, `"started_at":"2026-10-12T23:00:00Z"`, `"instances":[`} {
		if !strings.Contains(string(body), want) {
			t.Errorf("ps body lacks %s:\n%s", want, body)
		}
	}
}

func TestReadStream(t *testing.T) {
	cases := []struct {
		name, in, out string
		err           string
	}{
		{"lines then done", "data: one\n\ndata: two\n\nevent: done\ndata: \n\n", "one\ntwo\n", ""},
		{"error", "data: one\n\nevent: error\ndata: boom\n\n", "one\n", "boom"},
		{"multi-line data", "data: a\ndata: b\n\nevent: done\ndata: \n\n", "a\nb\n", ""},
		{"no verdict", "data: one\n\n", "one\n", ErrStream.Error()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			err := readStream(strings.NewReader(tc.in), &out)
			if out.String() != tc.out {
				t.Errorf("out = %q, want %q", out.String(), tc.out)
			}
			if (err == nil) != (tc.err == "") || (err != nil && err.Error() != tc.err) {
				t.Errorf("err = %v, want %q", err, tc.err)
			}
		})
	}
}

func TestToken(t *testing.T) {
	dir := t.TempDir()
	if _, err := ReadToken(dir); err == nil {
		t.Error("ReadToken before the daemon ran succeeded")
	}
	tok, err := LoadToken(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(tok) != 64 {
		t.Errorf("token %q is not 32 bytes of hex", tok)
	}
	info, err := os.Stat(filepath.Join(dir, TokenFile))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Errorf("token file mode %v, err %v", info.Mode(), err)
	}
	again, err := LoadToken(dir)
	if err != nil || again != tok {
		t.Errorf("second LoadToken = %q, %v; want the same token", again, err)
	}
	read, err := ReadToken(dir)
	if err != nil || read != tok {
		t.Errorf("ReadToken = %q, %v", read, err)
	}
	if err := os.WriteFile(filepath.Join(dir, TokenFile), []byte("\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadToken(dir); err == nil {
		t.Error("an empty token file was accepted")
	}
}
