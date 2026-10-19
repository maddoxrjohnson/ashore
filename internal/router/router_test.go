package router

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// newBackend serves body and echoes the headers a proxied request carried,
// so a test can see what the app would have seen.
func newBackend(t *testing.T, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Echo-Host", r.Host)
		h.Set("Echo-For", r.Header.Get("X-Forwarded-For"))
		h.Set("Echo-Forwarded-Host", r.Header.Get("X-Forwarded-Host"))
		h.Set("Echo-Proto", r.Header.Get("X-Forwarded-Proto"))
		h.Set("Echo-Id", r.Header.Get("X-Request-Id"))
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func addrOf(srv *httptest.Server) string { return strings.TrimPrefix(srv.URL, "http://") }

// logBuffer collects log lines. The access log line is written after the
// response has left, so a test may read it while the handler still writes.
type logBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

// String waits briefly for the access log line of the last request.
func (l *logBuffer) String() string {
	time.Sleep(20 * time.Millisecond)
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// newRouter returns a router serving on a test server, and its log output.
func newRouter(t *testing.T) (*Router, *httptest.Server, *logBuffer) {
	t.Helper()
	return newRouterWith(t, Options{})
}

// newRouterWith is newRouter with options; the log always goes to the
// returned buffer.
func newRouterWith(t *testing.T, o Options) (*Router, *httptest.Server, *logBuffer) {
	t.Helper()
	logs := &logBuffer{}
	o.Log = slog.New(slog.NewTextHandler(logs, nil))
	r := New(o)
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)
	return r, srv, logs
}

// get sends a request with the given Host header through the router.
func get(t *testing.T, srv *httptest.Server, host string, hdr http.Header) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/path?q=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = host
	for k, v := range hdr {
		req.Header[k] = v
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func body(t *testing.T, resp *http.Response) string {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestRoutesByHost(t *testing.T) {
	r, srv, logs := newRouter(t)
	a, b := newBackend(t, "a"), newBackend(t, "b")
	r.Set("hello.localhost", "hello", addrOf(a))
	r.Set("Other.localhost", "other", addrOf(b))

	resp := get(t, srv, "hello.localhost", http.Header{"X-Forwarded-For": {"1.2.3.4"}, "X-Forwarded-Proto": {"https"}})
	if resp.StatusCode != http.StatusOK || body(t, resp) != "a" {
		t.Fatalf("hello: status %d body %q, want 200 a", resp.StatusCode, body(t, resp))
	}
	h := resp.Header
	if got := h.Get("Echo-Host"); got != "hello.localhost" {
		t.Errorf("app saw Host %q, want the public name", got)
	}
	if got := h.Get("Echo-For"); got != "127.0.0.1" {
		t.Errorf("X-Forwarded-For = %q, want the real client, not what the client sent", got)
	}
	if got := h.Get("Echo-Forwarded-Host"); got != "hello.localhost" {
		t.Errorf("X-Forwarded-Host = %q", got)
	}
	if got := h.Get("Echo-Proto"); got != "http" {
		t.Errorf("X-Forwarded-Proto = %q, want http regardless of what the client sent", got)
	}
	id := h.Get("X-Request-Id")
	if len(id) != 16 {
		t.Errorf("X-Request-Id = %q, want 16 hex chars", id)
	}
	if got := h.Get("Echo-Id"); got != id {
		t.Errorf("app saw request id %q, client got %q", got, id)
	}
	if !strings.Contains(logs.String(), "id="+id) || !strings.Contains(logs.String(), "status=200") {
		t.Errorf("access log missing the request:\n%s", logs.String())
	}

	// Mixed case and a port in the Host header still match.
	resp = get(t, srv, "OTHER.localhost:8080", nil)
	if body(t, resp) != "b" {
		t.Errorf("other.localhost:8080 served %q, want b", body(t, resp))
	}
	if addr, ok := r.Get("other.localhost"); !ok || addr != addrOf(b) {
		t.Errorf("Get = %q, %v", addr, ok)
	}
}

func TestUnknownHostIs404(t *testing.T) {
	_, srv, logs := newRouter(t)
	resp := get(t, srv, "nope.localhost", nil)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d, want 404", resp.StatusCode)
	}
	id := resp.Header.Get("X-Request-Id")
	if id == "" || !strings.Contains(body(t, resp), id) {
		t.Errorf("404 body %q should name request %q", body(t, resp), id)
	}
	if !strings.Contains(logs.String(), "status=404") {
		t.Errorf("access log:\n%s", logs.String())
	}
}

func TestBackendDownIs502(t *testing.T) {
	r, srv, logs := newRouter(t)
	// A port that was free a moment ago: nothing answers there.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := ln.Addr().String()
	_ = ln.Close()
	r.Set("hello.localhost", "hello", dead)

	resp := get(t, srv, "hello.localhost", nil)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status %d, want 502", resp.StatusCode)
	}
	id := resp.Header.Get("X-Request-Id")
	if !strings.Contains(body(t, resp), id) {
		t.Errorf("502 body %q should name request %q", body(t, resp), id)
	}
	if !strings.Contains(logs.String(), "upstream error") || !strings.Contains(logs.String(), "status=502") {
		t.Errorf("log should record the failure:\n%s", logs.String())
	}
}

func TestRemove(t *testing.T) {
	r, srv, _ := newRouter(t)
	a := newBackend(t, "a")
	r.Set("hello.localhost", "hello", addrOf(a))
	r.Remove("hello.localhost")
	r.Remove("hello.localhost") // twice is fine
	if _, ok := r.Get("hello.localhost"); ok {
		t.Error("route still present after Remove")
	}
	if resp := get(t, srv, "hello.localhost", nil); resp.StatusCode != http.StatusNotFound {
		t.Errorf("status %d after Remove, want 404", resp.StatusCode)
	}
}

func TestBodyLimit(t *testing.T) {
	r, srv, _ := newRouterWith(t, Options{MaxBody: 1024})
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = io.Copy(io.Discard, req.Body)
	}))
	t.Cleanup(sink.Close)
	r.Set("hello.localhost", "hello", addrOf(sink))

	for _, tc := range []struct {
		size int
		want int
	}{
		{1024, http.StatusOK},
		{1025, http.StatusRequestEntityTooLarge},
	} {
		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/", bytes.NewReader(make([]byte, tc.size)))
		req.Host = "hello.localhost"
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("%d-byte body: status %d, want %d", tc.size, resp.StatusCode, tc.want)
		}
	}
}

// TestSwapUnderLoad hammers one host while another goroutine flips its
// backend. Under -race this proves the copy-on-write table needs no lock on
// the read side; every request must still reach one backend or the other.
func TestSwapUnderLoad(t *testing.T) {
	r, srv, _ := newRouter(t)
	a, b := newBackend(t, "a"), newBackend(t, "b")
	r.Set("hello.localhost", "hello", addrOf(a))

	const readers, perReader, swaps = 8, 100, 200
	var wg sync.WaitGroup
	errs := make(chan error, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perReader; j++ {
				req, _ := http.NewRequest(http.MethodGet, srv.URL+"/", nil)
				req.Host = "hello.localhost"
				resp, err := srv.Client().Do(req)
				if err != nil {
					errs <- err
					return
				}
				got, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusOK || (string(got) != "a" && string(got) != "b") {
					errs <- fmt.Errorf("status %d body %q", resp.StatusCode, got)
					return
				}
			}
		}()
	}
	for i := 0; i < swaps; i++ {
		if i%2 == 0 {
			r.Set("hello.localhost", "hello", addrOf(b))
		} else {
			r.Set("hello.localhost", "hello", addrOf(a))
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if resp := get(t, srv, "hello.localhost", nil); body(t, resp) != "a" {
		t.Errorf("after the last swap got %q, want a", body(t, resp))
	}
}

// TestStreamingFlushes checks that a chunk reaches the client before the
// backend has finished the response, which is what server-sent events need.
func TestStreamingFlushes(t *testing.T) {
	r, srv, _ := newRouter(t)
	release := make(chan struct{})
	stream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		_, _ = io.WriteString(w, "one\n")
		http.NewResponseController(w).Flush() //nolint:errcheck // test backend
		<-release
		_, _ = io.WriteString(w, "two\n")
	}))
	t.Cleanup(stream.Close)
	t.Cleanup(func() { close(release) })
	r.Set("hello.localhost", "hello", addrOf(stream))

	resp := get(t, srv, "hello.localhost", nil)
	br := bufio.NewReader(resp.Body)
	first := make(chan string, 1)
	go func() {
		line, _ := br.ReadString('\n')
		first <- line
	}()
	select {
	case line := <-first:
		if line != "one\n" {
			t.Fatalf("first line %q", line)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first chunk never arrived: response is being buffered")
	}
}

func TestServeStopsWithContext(t *testing.T) {
	r, _, _ := newRouter(t)
	a := newBackend(t, "a")
	r.Set("hello.localhost", "hello", addrOf(a))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Serve(ctx, ln) }()

	req, _ := http.NewRequest(http.MethodGet, "http://"+ln.Addr().String()+"/", nil)
	req.Host = "hello.localhost"
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if got := body(t, resp); got != "a" {
		t.Fatalf("served %q", got)
	}
	_ = resp.Body.Close()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after cancel")
	}
	if _, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second); err == nil {
		t.Error("listener still open after Serve returned")
	}
}

func TestHostOf(t *testing.T) {
	for in, want := range map[string]string{
		"hello.localhost":      "hello.localhost",
		"Hello.Localhost:8080": "hello.localhost",
		"hello.localhost.":     "hello.localhost",
		"[::1]:8080":           "::1",
		"":                     "",
	} {
		if got := hostOf(in); got != want {
			t.Errorf("hostOf(%q) = %q, want %q", in, got, want)
		}
	}
}

// fakeObserver records what the router reports.
type fakeObserver struct {
	mu       sync.Mutex
	requests []string // "app status"
	errors   []string
}

func (o *fakeObserver) Request(app string, status int, d time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if d <= 0 {
		panic("non-positive duration")
	}
	o.requests = append(o.requests, fmt.Sprintf("%s %d", app, status))
}

func (o *fakeObserver) UpstreamError(app string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.errors = append(o.errors, app)
}

func TestObserverSeesEveryRequest(t *testing.T) {
	obs := &fakeObserver{}
	r, srv, _ := newRouterWith(t, Options{Observer: obs})
	a := newBackend(t, "a")
	r.Set("hello.localhost", "hello", addrOf(a))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := ln.Addr().String()
	_ = ln.Close()
	r.Set("down.localhost", "down", dead)
	for _, host := range []string{"hello.localhost", "nope.localhost", "down.localhost"} {
		resp := get(t, srv, host, nil)
		_ = resp.Body.Close()
	}
	obs.mu.Lock()
	defer obs.mu.Unlock()
	if got := strings.Join(obs.requests, ","); got != "hello 200, 404,down 502" {
		t.Errorf("requests %q", got)
	}
	if len(obs.errors) != 1 || obs.errors[0] != "down" {
		t.Errorf("errors %v", obs.errors)
	}
}

// TestWebSocketPassthrough drives a WebSocket-style upgrade through the
// router by hand: the backend hijacks the connection after a 101 and echoes
// bytes; the client is a raw TCP connection speaking HTTP/1.1. No WebSocket
// library is involved, since what matters here is that the proxy forwards
// the Upgrade, returns the 101, and then copies bytes both ways.
func TestWebSocketPassthrough(t *testing.T) {
	r, srv, _ := newRouter(t)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Upgrade") != "websocket" || req.Header.Get("Connection") != "Upgrade" {
			http.Error(w, "not an upgrade", http.StatusBadRequest)
			return
		}
		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: x\r\n\r\n")
		_ = rw.Flush()
		// Echo until the client hangs up.
		buf := make([]byte, 64)
		for {
			n, err := rw.Read(buf)
			if err != nil {
				return
			}
			_, _ = rw.Write(buf[:n])
			_ = rw.Flush()
		}
	}))
	t.Cleanup(backend.Close)
	r.Set("ws.localhost", "ws", addrOf(backend))

	conn, err := net.Dial("tcp", addrOf(srv))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, _ = io.WriteString(conn, "GET /chat HTTP/1.1\r\nHost: ws.localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n")
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols || resp.Header.Get("Upgrade") != "websocket" {
		t.Fatalf("status %d, upgrade %q", resp.StatusCode, resp.Header.Get("Upgrade"))
	}
	for _, msg := range []string{"ping", "second frame"} {
		if _, err := io.WriteString(conn, msg); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(msg))
		if _, err := io.ReadFull(br, got); err != nil {
			t.Fatalf("reading echo of %q: %v", msg, err)
		}
		if string(got) != msg {
			t.Fatalf("echo %q, want %q", got, msg)
		}
	}
}
