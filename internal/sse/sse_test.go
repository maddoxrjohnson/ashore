package sse

import (
	"errors"
	"net/http/httptest"
	"testing"
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

func TestFail(t *testing.T) {
	rec := httptest.NewRecorder()
	st, _ := New(rec)
	_, _ = st.Write([]byte("half"))
	st.Fail(errors.New("boom"))
	if got, want := rec.Body.String(), "data: half\n\nevent: error\ndata: boom\n\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
