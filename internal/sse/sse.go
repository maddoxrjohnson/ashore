// Package sse writes server-sent events: a long-lived HTTP response whose
// body is a stream of small frames, each flushed as soon as it is written.
// The daemon uses it for deploy progress and for live logs; the CLI and the
// dashboard read it.
package sse

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/lines"
	"github.com/maddoxrjohnson/ashore/internal/logs"
)

// Event names. Every stream ends with exactly one Done or Error frame;
// lines of deploy progress are unnamed ("message") events; a log stream
// carries one EventLine per logs.Line, as JSON.
const (
	EventDone  = "done"
	EventError = "error"
	EventLine  = "line"
)

// keepalive is how often a quiet follow stream sends a comment, so a
// proxy or a client with an idle timeout keeps the connection.
const keepalive = 30 * time.Second

// ErrNoVerdict reports a stream that ended without a done or error event:
// the connection dropped, or the daemon went away mid-operation.
var ErrNoVerdict = errors.New("stream ended without a result")

// Writer frames events onto a response. It is an io.Writer so a deploy can
// write to it directly: complete lines become events, a partial line waits
// for the rest. Safe for concurrent use.
type Writer struct {
	w  http.ResponseWriter
	rc *http.ResponseController

	mu    sync.Mutex
	lines lines.Writer
}

// New sends the headers and the first flush, so the client knows the
// request was accepted before the operation behind the stream starts.
func New(w http.ResponseWriter) (*Writer, error) {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	st := &Writer{w: w, rc: http.NewResponseController(w)}
	st.lines.Emit = func(line string) error { st.event("", line); return nil }
	if err := st.rc.Flush(); err != nil {
		return nil, fmt.Errorf("event stream: %w", err)
	}
	return st, nil
}

// Write buffers p and sends every complete line as an unnamed event.
func (st *Writer) Write(p []byte) (int, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.lines.Write(p)
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

func (st *Writer) flushPartial() { _ = st.lines.Flush() }

// Follow sends every line from ch as an EventLine until the channel
// closes (the app was destroyed; the stream ends with Done) or ctx ends
// (the client went away). A quiet stream gets a keepalive comment.
func Follow(ctx context.Context, st *Writer, ch <-chan logs.Line) {
	tick := time.NewTicker(keepalive)
	defer tick.Stop()
	for {
		select {
		case l, ok := <-ch:
			if !ok {
				st.Done()
				return
			}
			st.JSON(EventLine, l)
		case <-tick.C:
			st.Comment("keepalive")
		case <-ctx.Done():
			return
		}
	}
}

// Read parses a stream and calls fn for each event that is neither Done
// nor Error; those end the read. An Error event is returned as an error;
// a stream that ends without either is ErrNoVerdict. Comments are skipped.
func Read(r io.Reader, fn func(event, data string) error) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	var event string
	var data []string
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if len(data) == 0 && event == "" {
				continue // a comment's blank line
			}
			text := strings.Join(data, "\n")
			switch event {
			case EventDone:
				return nil
			case EventError:
				return errors.New(text)
			default:
				if len(data) > 0 {
					if err := fn(event, text); err != nil {
						return err
					}
				}
			}
			event, data = "", nil
		case strings.HasPrefix(line, ":"):
			// a comment, such as a keepalive
		case strings.HasPrefix(line, "event: "):
			event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			data = append(data, strings.TrimPrefix(line, "data: "))
		case strings.HasPrefix(line, "data:"):
			data = append(data, "")
		}
	}
	if err := sc.Err(); err != nil {
		return err
	}
	return ErrNoVerdict
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
