// Package logs collects what an app's instances print and hands it on: to a
// file per app on disk (the record), to a ring of recent lines in memory
// (what a new reader sees first), and to live subscribers, each behind a
// bounded channel. A subscriber that cannot keep up loses lines and is told
// how many; it never slows the collector, the file, or the other readers.
package logs

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/lines"
	"github.com/maddoxrjohnson/ashore/internal/runtime"
)

// Line is one line an instance printed, stamped when the collector read it.
type Line struct {
	App      string    `json:"app"`
	Release  string    `json:"release,omitempty"`
	Instance string    `json:"instance,omitempty"`
	Stream   string    `json:"stream"`
	Time     time.Time `json:"time"`
	Text     string    `json:"text"`
}

// Streams a Line comes from.
const (
	StreamStdout = "stdout"
	StreamStderr = "stderr"
	// StreamSystem marks lines the daemon itself adds to a reader's stream,
	// such as the notice that lines were dropped.
	StreamSystem = "ashore"
)

const (
	// RingSize is how many recent lines each app keeps in memory.
	RingSize = 1000
	// SubscriberBuffer is how far a live reader may fall behind before it
	// starts losing lines.
	SubscriberBuffer = 64
	// MaxLine is where a line without a newline is cut, so one runaway app
	// cannot grow a buffer without bound.
	MaxLine = 64 << 10

	defaultRotateAt = 50 << 20
	keepFiles       = 5
)

// Observer counts what the hub does; the metrics package implements it.
type Observer interface {
	Line(app string)           // one line collected
	Dropped(app string, n int) // n lines a subscriber did not get
}

type noObserver struct{}

func (noObserver) Line(string)         {}
func (noObserver) Dropped(string, int) {}

// Hub owns one Sink per app and runs the collectors. Create it with New.
type Hub struct {
	dir string // <dataDir>/logs
	rt  runtime.Runtime
	log *slog.Logger

	// RotateAt is the file size that triggers rotation. Set before use.
	RotateAt int64
	// Observer is told about lines and drops. Set before the first sink
	// is made; the default discards.
	Observer Observer

	mu     sync.Mutex
	apps   map[string]*Sink
	closed bool
}

// New returns a hub that writes under <dataDir>/logs and reads instance
// output from rt. A nil logger means slog.Default.
func New(dataDir string, rt runtime.Runtime, log *slog.Logger) *Hub {
	if log == nil {
		log = slog.Default()
	}
	return &Hub{dir: filepath.Join(dataDir, "logs"), rt: rt, log: log, RotateAt: defaultRotateAt, Observer: noObserver{}, apps: make(map[string]*Sink)}
}

// Sink returns the app's sink, creating it on first use. A sink whose file
// cannot be opened still serves the ring and subscribers; the error is
// logged once.
func (h *Hub) Sink(app string) *Sink {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.apps[app]; ok {
		return s
	}
	s := &Sink{app: app, dir: filepath.Join(h.dir, app), obs: h.Observer, log: h.log, ring: newRing(RingSize), subs: make(map[*subscriber]struct{})}
	if !h.closed {
		f, err := openRotating(filepath.Join(s.dir, "current.log"), h.RotateAt)
		if err != nil {
			h.log.Error("open log file", "app", app, "err", err)
		}
		s.file = f // nil on error
		h.apps[app] = s
	} else {
		s.closed = true
	}
	return s
}

// Tail returns an app's last n lines, oldest first.
func (h *Hub) Tail(app string, n int) []Line { return h.Sink(app).Tail(n) }

// Subscribe is Sink.Subscribe for app.
func (h *Hub) Subscribe(app string, tail int) (<-chan Line, func()) {
	return h.Sink(app).Subscribe(tail)
}

// Collect reads the instance's output until it exits or ctx ends, splitting
// it into lines and appending them to the app's sink. It is the one reader
// of the runtime's stream for that instance; whoever starts an instance
// runs this in a goroutine of its own. since is passed to the runtime: the
// zero time replays everything the instance printed, time.Now() takes up
// from here (an instance adopted after a daemon restart).
func (h *Hub) Collect(ctx context.Context, inst runtime.Instance, since time.Time) error {
	sink := h.Sink(inst.App)
	stamp := func(stream string) *lines.Writer {
		return &lines.Writer{Max: MaxLine, Emit: func(text string) error {
			sink.Append(Line{App: inst.App, Release: inst.Release, Instance: inst.ID, Stream: stream, Time: time.Now(), Text: text})
			return nil
		}}
	}
	out, errs := stamp(StreamStdout), stamp(StreamStderr)
	err := h.rt.Logs(ctx, inst.ID, since, out, errs)
	_ = out.Flush()
	_ = errs.Flush()
	if err != nil && ctx.Err() == nil {
		h.log.Warn("log collector ended", "app", inst.App, "instance", inst.ID, "err", err)
	}
	return err
}

// Forget ends an app's sink: subscribers see their channel close, the file
// is closed, and the app's log directory is removed. A collector still
// draining the old instance appends into nothing.
func (h *Hub) Forget(app string) error {
	h.mu.Lock()
	s, ok := h.apps[app]
	delete(h.apps, app)
	h.mu.Unlock()
	if !ok {
		return nil
	}
	err := s.close()
	if rerr := os.RemoveAll(s.dir); rerr != nil && err == nil {
		err = rerr
	}
	return err
}

// Close closes every sink; the files are flushed and subscribers end. The
// hub serves rings and subscribers but writes no files after this.
func (h *Hub) Close() error {
	h.mu.Lock()
	h.closed = true
	sinks := make([]*Sink, 0, len(h.apps))
	for _, s := range h.apps {
		sinks = append(sinks, s)
	}
	h.mu.Unlock()
	var first error
	for _, s := range sinks {
		if err := s.close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Sink is one app's log: its file, its ring of recent lines, and its live
// readers. Safe for concurrent use.
type Sink struct {
	app string
	dir string
	obs Observer
	log *slog.Logger

	mu     sync.Mutex
	file   *rotatingFile // nil when the file could not be opened
	ring   *ring
	subs   map[*subscriber]struct{}
	closed bool
}

// Append records one line: file, ring, then every subscriber, without
// blocking on any of them.
func (s *Sink) Append(l Line) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if s.file != nil {
		if err := s.file.WriteLine(l); err != nil {
			s.log.Warn("write log file", "app", s.app, "err", err)
		}
	}
	s.ring.push(l)
	s.obs.Line(s.app)
	for sub := range s.subs {
		if !sub.send(l) {
			s.obs.Dropped(s.app, 1)
		}
	}
}

// Tail returns the last n lines, oldest first.
func (s *Sink) Tail(n int) []Line {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ring.last(n)
}

// Subscribe returns a channel that first delivers the last tail lines and
// then every line appended from now on, and a function that ends the
// subscription and closes the channel. The channel's buffer holds the tail
// plus SubscriberBuffer live lines; a reader that lets it fill loses lines
// and then receives one StreamSystem line saying how many. Only the sink
// closes the channel, so a reader may range over it.
func (s *Sink) Subscribe(tail int) (<-chan Line, func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	preload := s.ring.last(tail)
	sub := &subscriber{ch: make(chan Line, len(preload)+SubscriberBuffer)}
	for _, l := range preload {
		sub.ch <- l
	}
	if s.closed {
		close(sub.ch)
		return sub.ch, func() {}
	}
	s.subs[sub] = struct{}{}
	cancel := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, ok := s.subs[sub]; ok {
			delete(s.subs, sub)
			close(sub.ch)
		}
	}
	return sub.ch, cancel
}

// close ends the sink: every subscriber's channel is closed and the file
// is closed. Appends after this are discarded.
func (s *Sink) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for sub := range s.subs {
		close(sub.ch)
		delete(s.subs, sub)
	}
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

// subscriber is one live reader. dropped counts lines it missed since the
// last one it got; the next successful send is preceded by a marker.
type subscriber struct {
	ch      chan Line
	dropped int
}

// send delivers l without blocking and reports whether it got through. A
// full channel means the reader is behind: the line is counted as dropped
// and the reader hears about it with the first line that fits again.
func (sub *subscriber) send(l Line) bool {
	if sub.dropped > 0 {
		marker := Line{App: l.App, Stream: StreamSystem, Time: l.Time, Text: fmt.Sprintf("dropped %d lines", sub.dropped)}
		select {
		case sub.ch <- marker:
			sub.dropped = 0
		default:
			sub.dropped++
			return false
		}
	}
	select {
	case sub.ch <- l:
		return true
	default:
		sub.dropped++
		return false
	}
}

// ring keeps the last len(buf) lines pushed.
type ring struct {
	buf  []Line
	next int // where the next push goes
	n    int // lines held, up to len(buf)
}

func newRing(size int) *ring { return &ring{buf: make([]Line, size)} }

func (r *ring) push(l Line) {
	r.buf[r.next] = l
	r.next = (r.next + 1) % len(r.buf)
	if r.n < len(r.buf) {
		r.n++
	}
}

// last returns up to k lines, oldest first.
func (r *ring) last(k int) []Line {
	if k > r.n {
		k = r.n
	}
	if k <= 0 {
		return nil
	}
	out := make([]Line, 0, k)
	start := (r.next - k + len(r.buf)) % len(r.buf)
	for i := range k {
		out = append(out, r.buf[(start+i)%len(r.buf)])
	}
	return out
}
