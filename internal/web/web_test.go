package web

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/logs"
	"github.com/maddoxrjohnson/ashore/internal/site"
	"github.com/maddoxrjohnson/ashore/internal/store"
	"github.com/maddoxrjohnson/ashore/internal/supervisor"
)

const token = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

type fakeSup struct{ status map[string]supervisor.Status }

func (f *fakeSup) Status(app string) (supervisor.Status, bool) {
	st, ok := f.status[app]
	return st, ok
}

type fixture struct {
	store   *store.Store
	hub     *logs.Hub
	sup     *fakeSup
	server  *httptest.Server
	client  *http.Client // with a cookie jar, not following redirects
	dataDir string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	dataDir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dataDir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	hub := logs.New(dataDir, nil, nil)
	t.Cleanup(func() { _ = hub.Close() })
	f := &fixture{store: st, hub: hub, sup: &fakeSup{status: map[string]supervisor.Status{}}, dataDir: dataDir}
	cfg := Config{DataDir: dataDir, Site: site.Site{Domain: "localhost", SSHAddr: "127.0.0.1:2222", HTTPPort: 8080}, Version: "test"}
	f.server = httptest.NewServer(New(st, f.sup, hub, token, cfg, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(f.server.Close)
	jar, _ := cookiejar.New(nil)
	f.client = &http.Client{Jar: jar, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return f
}

func (f *fixture) get(t *testing.T, path string) (int, string, *http.Response) {
	t.Helper()
	resp, err := f.client.Get(f.server.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), resp
}

func (f *fixture) login(t *testing.T, tok string) *http.Response {
	t.Helper()
	resp, err := f.client.PostForm(f.server.URL+"/login", map[string][]string{"token": {tok}})
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp
}

func TestLoginFlow(t *testing.T) {
	f := newFixture(t)
	if code, _, resp := f.get(t, "/"); code != http.StatusSeeOther || resp.Header.Get("Location") != "/login" {
		t.Fatalf("anonymous /: %d %q", code, resp.Header.Get("Location"))
	}
	if code, body, _ := f.get(t, "/login"); code != http.StatusOK || !strings.Contains(body, `name="token"`) {
		t.Fatalf("login page: %d\n%s", code, body)
	}
	if resp := f.login(t, "wrong"); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d", resp.StatusCode)
	}
	if code, _, _ := f.get(t, "/"); code != http.StatusSeeOther {
		t.Fatal("a refused login must not start a session")
	}
	resp := f.login(t, token)
	if resp.StatusCode != http.StatusSeeOther || resp.Header.Get("Location") != "/" {
		t.Fatalf("login: %d %q", resp.StatusCode, resp.Header.Get("Location"))
	}
	var cookie *http.Cookie
	for _, c := range resp.Cookies() {
		if c.Name == cookieName {
			cookie = c
		}
	}
	if cookie == nil || !cookie.HttpOnly || cookie.SameSite != http.SameSiteLaxMode || cookie.Value == "" {
		t.Fatalf("session cookie: %+v", cookie)
	}
	if code, body, _ := f.get(t, "/"); code != http.StatusOK || !strings.Contains(body, "No apps yet") || !strings.Contains(body, "ashored test") {
		t.Fatalf("apps page: %d\n%s", code, body)
	}
	if code, _, resp := f.get(t, "/login"); code != http.StatusSeeOther || resp.Header.Get("Location") != "/" {
		t.Fatalf("login page while logged in: %d", code)
	}
	// Static files need no session; everything else does after logout.
	resp, err := f.client.Post(f.server.URL+"/logout", "", nil)
	if err != nil || resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("logout: %v %v", resp, err)
	}
	if code, _, _ := f.get(t, "/"); code != http.StatusSeeOther {
		t.Fatal("still logged in after logout")
	}
	if code, body, _ := f.get(t, "/static/app.js"); code != http.StatusOK || !strings.Contains(body, "EventSource") {
		t.Fatalf("static: %d", code)
	}
}

func TestAppPages(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	app, err := f.store.CreateApp(ctx, "hello", "hello.localhost")
	if err != nil {
		t.Fatal(err)
	}
	const sha = "abc1234567890abc1234567890abc1234567890a"
	v1, _ := f.store.CreateRelease(ctx, app.ID, store.NewRelease{Image: "ashore/hello:abc1234", GitSHA: sha, Note: "push abc1234"})
	if err := f.store.SetLive(ctx, app.ID, v1.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.CreateRelease(ctx, app.ID, store.NewRelease{Note: "push def5678"}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.UpdateConfig(ctx, app.ID, map[string]string{"SECRET": "s3cr3t", "A": "1"}, nil); err != nil {
		t.Fatal(err)
	}
	logDir := filepath.Join(f.dataDir, "builds", "hello")
	_ = os.MkdirAll(logDir, 0o700)
	_ = os.WriteFile(filepath.Join(logDir, sha+".log"), []byte("Step 1/2 : FROM scratch\n"), 0o600)
	f.sup.status["hello"] = supervisor.Status{App: "hello", Release: "v1", Instance: "0123456789abcdef", Addr: "127.0.0.1:40000",
		State: supervisor.StateRunning, Restarts: 2, StartedAt: time.Now().Add(-90 * time.Second)}
	f.login(t, token)

	code, body, _ := f.get(t, "/")
	for _, want := range []string{`<a href="/apps/hello">hello</a>`, `href="http://hello.localhost:8080"`, "<td>v1</td>"} {
		if code != http.StatusOK || !strings.Contains(body, want) {
			t.Errorf("apps page lacks %q (status %d):\n%s", want, code, body)
		}
	}

	code, body, _ = f.get(t, "/apps/hello")
	for _, want := range []string{
		"<h1>hello</h1>", "git remote add ashore ssh://ashore@127.0.0.1:2222/hello",
		"<td>0123456789ab</td>", `class="state-running"`, "<td>2</td>", "<td>1m30s</td>",
		`class="status-live"`, `class="status-building"`, "push def5678",
		`<a href="/apps/hello/builds/` + sha + `">abc1234</a>`, "ashore/hello:abc1234",
		`<code>A</code>`, `data-value="s3cr3t"`, `class="reveal"`, `<pre id="logs" data-app="hello">`,
	} {
		if code != http.StatusOK || !strings.Contains(body, want) {
			t.Errorf("app page lacks %q (status %d):\n%s", want, code, body)
		}
	}
	if strings.Contains(body, ">s3cr3t<") {
		t.Error("config value shown unmasked")
	}
	if code, _, _ := f.get(t, "/apps/nope"); code != http.StatusNotFound {
		t.Errorf("unknown app: %d", code)
	}

	// Build logs: served as text, refused for anything that is not a commit.
	if code, body, resp := f.get(t, "/apps/hello/builds/"+sha); code != http.StatusOK || body != "Step 1/2 : FROM scratch\n" || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/plain") {
		t.Errorf("build log: %d %q", code, body)
	}
	if code, _, _ := f.get(t, "/apps/hello/builds/abc1234567890abc1234567890abc1234567890b"); code != http.StatusNotFound {
		t.Errorf("missing build log: %d", code)
	}
	for _, bad := range []string{"ABC", "abc1234", "zz", "abc1234567890abc1234567890abc1234567890g"} {
		if code, _, _ := f.get(t, "/apps/hello/builds/"+bad); code != http.StatusBadRequest {
			t.Errorf("bad sha %q: %d", bad, code)
		}
	}
}

func TestLogStream(t *testing.T) {
	f := newFixture(t)
	if _, err := f.store.CreateApp(context.Background(), "hello", "hello.localhost"); err != nil {
		t.Fatal(err)
	}
	if code, _, _ := f.get(t, "/apps/hello/logs"); code != http.StatusSeeOther {
		t.Fatalf("anonymous log stream: %d", code)
	}
	f.login(t, token)
	f.hub.Sink("hello").Append(logs.Line{App: "hello", Release: "v1", Stream: logs.StreamStdout, Text: "early"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, f.server.URL+"/apps/hello/logs", nil)
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	rd := bufio.NewReader(resp.Body)
	frame := func() string {
		var b strings.Builder
		for {
			line, err := rd.ReadString('\n')
			if err != nil {
				t.Fatalf("stream ended: %v (got %q)", err, b.String())
			}
			if line == "\n" {
				return b.String()
			}
			b.WriteString(line)
		}
	}
	if fr := frame(); !strings.HasPrefix(fr, "event: line\n") || !strings.Contains(fr, `"text":"early"`) {
		t.Fatalf("first frame %q", fr)
	}
	f.hub.Sink("hello").Append(logs.Line{App: "hello", Stream: logs.StreamStderr, Text: "live"})
	if fr := frame(); !strings.Contains(fr, `"text":"live"`) {
		t.Fatalf("live frame %q", fr)
	}
	if err := f.hub.Forget("hello"); err != nil {
		t.Fatal(err)
	}
	if fr := frame(); fr != "event: done\ndata: \n" {
		t.Fatalf("after forget %q", fr)
	}
}
