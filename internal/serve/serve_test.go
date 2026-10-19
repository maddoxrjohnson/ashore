package serve

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestServeStopsWithContext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- HTTP(ctx, ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, "hi")
		}), "test", nil)
	}()
	resp, err := http.Get("http://" + ln.Addr().String() + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "hi" {
		t.Fatalf("body %q", body)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after cancel")
	}
	if _, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second); err == nil {
		t.Fatal("listener still open after Serve returned")
	}
}

func TestServeReportsListenerErrors(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = ln.Close()
	if err := HTTP(context.Background(), ln, http.NotFoundHandler(), "closed", nil); err == nil {
		t.Fatal("Serve on a closed listener should fail")
	}
}

func TestRecorder(t *testing.T) {
	rec := httptest.NewRecorder()
	r := &Recorder{ResponseWriter: rec}
	_, _ = r.Write([]byte("abc"))
	r.WriteHeader(http.StatusTeapot) // too late: the first write fixed 200
	_, _ = r.Write([]byte("de"))
	if r.Status != http.StatusOK || r.Bytes != 5 {
		t.Fatalf("status %d bytes %d", r.Status, r.Bytes)
	}
	r = &Recorder{ResponseWriter: httptest.NewRecorder()}
	r.WriteHeader(http.StatusNotFound)
	r.WriteHeader(http.StatusOK)
	if r.Status != http.StatusNotFound || r.Unwrap() == nil {
		t.Fatalf("status %d", r.Status)
	}
}

func TestAcceptHandsOffAndStops(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	handled := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- Accept(ctx, ln, "test", func(ctx context.Context, c net.Conn) {
			_, _ = io.WriteString(c, "hello")
			_ = c.Close()
			<-ctx.Done() // a handler that outlives the accept loop is waited for
			close(handled)
		})
	}()
	c, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(c)
	_ = c.Close()
	if string(b) != "hello" {
		t.Fatalf("read %q", b)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Accept did not return")
	}
	select {
	case <-handled:
	default:
		t.Fatal("Accept returned before its handler finished")
	}
	_ = ln.Close()
	if err := Accept(context.Background(), ln, "closed", func(context.Context, net.Conn) {}); err == nil {
		t.Fatal("Accept on a closed listener should fail")
	}
}
