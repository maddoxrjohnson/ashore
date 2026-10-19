// Package sse writes server-sent events: a long-lived HTTP response whose
// body is a stream of small frames, each flushed as soon as it is written.
// The daemon uses it for deploy progress and for live logs; the CLI and the
// dashboard read it.
package sse

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
)

// Event names every stream ends with: exactly one Done or Error frame.
// Lines of progress are unnamed ("message") events.
const (
	EventDone  = "done"
	EventError = "error"
)

// Writer frames events onto a response. It is an io.Writer so a deploy can
// write to it directly: complete lines become events, a partial line waits
// for the rest. Safe for concurrent use.
type Writer struct {
	w  http.ResponseWriter
	rc *http.ResponseController

	mu  sync.Mutex
	buf []byte
}

// New sends the headers and the first flush, so the client knows the
// request was accepted before the operation behind the stream starts.
func New(w http.ResponseWriter) (*Writer, error) {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	st := &Writer{w: w, rc: http.NewResponseController(w)}
	if err := st.rc.Flush(); err != nil {
		return nil, fmt.Errorf("event stream: %w", err)
	}
	return st, nil
}

// Write buffers p and sends every complete line as an unnamed event.
func (st *Writer) Write(p []byte) (int, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.buf = append(st.buf, p...)
	for {
		i := bytes.IndexByte(st.buf, '\n')
		if i < 0 {
			return len(p), nil
		}
		st.event("", string(bytes.TrimRight(st.buf[:i], "\r")))
		st.buf = st.buf[i+1:]
	}
}

// Line sends one line of output as an unnamed event.
func (st *Writer) Line(text string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.event("", text)
}

// Event sends one named event with data.
func (st *Writer) Event(name, data string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.event(name, data)
}

// JSON sends one named event whose data is v encoded as JSON.
func (st *Writer) JSON(name string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	st.Event(name, string(b))
}

// Comment sends a line clients ignore, to keep a quiet connection warm.
func (st *Writer) Comment(text string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if _, err := st.w.Write([]byte(": " + text + "\n\n")); err != nil {
		return
	}
	_ = st.rc.Flush()
}

// Done ends the stream after a success; whatever is buffered goes first.
func (st *Writer) Done() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.flushPartial()
	st.event(EventDone, "")
}

// Fail ends the stream with the operation's error.
func (st *Writer) Fail(err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.flushPartial()
	st.event(EventError, err.Error())
}

func (st *Writer) flushPartial() {
	if len(st.buf) > 0 {
		st.event("", string(bytes.TrimRight(st.buf, "\r")))
		st.buf = nil
	}
}

// event writes one frame and flushes it. Multi-line data is sent as one
// data: line per line, as the format requires. Write errors are ignored:
// the client is gone and the operation finishes without it.
func (st *Writer) event(name, data string) {
	var b strings.Builder
	if name != "" {
		fmt.Fprintf(&b, "event: %s\n", name)
	}
	for _, l := range strings.Split(data, "\n") {
		fmt.Fprintf(&b, "data: %s\n", l)
	}
	b.WriteByte('\n')
	if _, err := st.w.Write([]byte(b.String())); err != nil {
		return
	}
	_ = st.rc.Flush()
}
