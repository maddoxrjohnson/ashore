package api

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// Event names on a progress stream. Lines of output are unnamed "message"
// events; exactly one done or error event ends the stream.
const (
	eventDone  = "done"
	eventError = "error"
)

// stream writes server-sent events for a long operation. It is an
// io.Writer so a deploy can write to it directly: complete lines become
// events, a partial line waits for the rest. Safe for concurrent writers.
type stream struct {
	w  http.ResponseWriter
	rc *http.ResponseController

	mu  sync.Mutex
	buf []byte
}

// newStream sends the headers and the first flush, so the client knows the
// request was accepted before the operation starts.
func newStream(w http.ResponseWriter) (*stream, error) {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	st := &stream{w: w, rc: http.NewResponseController(w)}
	if err := st.rc.Flush(); err != nil {
		return nil, fmt.Errorf("stream: %w", err)
	}
	return st, nil
}

func (st *stream) Write(p []byte) (int, error) {
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

// line sends one line of output.
func (st *stream) line(text string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.event("", text)
}

// done ends the stream after a success; whatever is buffered goes first.
func (st *stream) done() {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.flushPartial()
	st.event(eventDone, "")
}

// fail ends the stream with the operation's error.
func (st *stream) fail(err error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	st.flushPartial()
	st.event(eventError, err.Error())
}

func (st *stream) flushPartial() {
	if len(st.buf) > 0 {
		st.event("", string(bytes.TrimRight(st.buf, "\r")))
		st.buf = nil
	}
}

// event writes one event and flushes it. Multi-line data is sent as one
// data: line per line, as the format requires. Write errors are ignored:
// the client is gone and the operation finishes without it.
func (st *stream) event(name, data string) {
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

// sortStrings is sort.Strings, kept here so api.go reads without the import.
func sortStrings(s []string) { sort.Strings(s) }

// ErrStream reports a stream that ended without a verdict: the connection
// dropped, or the daemon went away mid-operation.
var ErrStream = errors.New("stream ended without a result")
