package sse

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/maddoxrjohnson/ashore/internal/logs"
)

func TestFrames(t *testing.T) {
	rec := httptest.NewRecorder()
	st, err := New(rec)
	if err != nil {
		t.Fatal(err)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}
	_, _ = st.Write([]byte("one\r\ntw"))
	_, _ = st.Write([]byte("o\npart"))
	st.Line("line")
	st.Event("named", "a\nb")
	st.JSON("obj", map[string]int{"n": 1})
	st.Comment("keep")
	st.Done()
	want := "data: one\n\ndata: two\n\ndata: line\n\nevent: named\ndata: a\ndata: b\n\nevent: obj\ndata: {\"n\":1}\n\n: keep\n\ndata: part\n\nevent: done\ndata: \n\n"
	if got := rec.Body.String(); got != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
	if !rec.Flushed {
		t.Fatal("not flushed")
	}
}

func TestReadSkipsComments(t *testing.T) {
	var got []string
	err := Read(strings.NewReader(": keepalive\n\nevent: line\ndata: {\"a\":1}\n\n: again\n\nevent: done\ndata: \n\n"), func(event, data string) error {
		got = append(got, event+"="+data)
		return nil
	})
	if err != nil || len(got) != 1 || got[0] != `line={"a":1}` {
		t.Fatalf("%v %q", err, got)
	}
	if err := Read(strings.NewReader("data: x\n\n"), func(string, string) error { return nil }); !errors.Is(err, ErrNoVerdict) {
		t.Fatalf("no verdict: %v", err)
	}
	if err := Read(strings.NewReader("event: error\ndata: boom\n\n"), func(string, string) error { return nil }); err == nil || err.Error() != "boom" {
		t.Fatalf("error event: %v", err)
	}
}

func TestFollow(t *testing.T) {
	rec := httptest.NewRecorder()
	st, _ := New(rec)
	ch := make(chan logs.Line, 2)
	ch <- logs.Line{App: "a", Text: "one"}
	close(ch)
	Follow(context.Background(), st, ch)
	if got, want := rec.Body.String(), "event: line\ndata: {\"app\":\"a\",\"stream\":\"\",\"time\":\"0001-01-01T00:00:00Z\",\"text\":\"one\"}\n\nevent: done\ndata: \n\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	// A cancelled context ends a follow without a verdict.
	rec = httptest.NewRecorder()
	st, _ = New(rec)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	Follow(ctx, st, make(chan logs.Line))
	if rec.Body.Len() != 0 {
		t.Fatalf("wrote %q after cancel", rec.Body.String())
	}
}

func TestFail(t *testing.T) {
	rec := httptest.NewRecorder()
	st, _ := New(rec)
	_, _ = st.Write([]byte("half"))
	st.Fail(errors.New("boom"))
	if got, want := rec.Body.String(), "data: half\n\nevent: error\ndata: boom\n\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
