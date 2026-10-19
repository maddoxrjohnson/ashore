package logs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/maddoxrjohnson/ashore/internal/runtime"
)

func TestSplitterCarriesPartialLinesAcrossChunks(t *testing.T) {
	cases := []struct {
		name   string
		chunks []string
		want   []string
	}{
		{"one chunk", []string{"a\nb\n"}, []string{"a", "b"}},
		{"split mid-line", []string{"hel", "lo\nwor", "ld\n"}, []string{"hello", "world"}},
		{"unterminated final line", []string{"ab", "c\nde"}, []string{"abc", "de"}},
		{"crlf", []string{"a\r\nb\r\n"}, []string{"a", "b"}},
		{"empty lines kept", []string{"\n\nx\n"}, []string{"", "", "x"}},
		{"newline alone in a chunk", []string{"ab", "\n"}, []string{"ab"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			s := newSplitter(func(text string) { got = append(got, text) })
			for _, c := range tc.chunks {
				if _, err := io.WriteString(s, c); err != nil {
					t.Fatal(err)
				}
			}
			s.Flush()
			if strings.Join(got, "|") != strings.Join(tc.want, "|") {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSplitterCutsRunawayLines(t *testing.T) {
	var got []string
	s := newSplitter(func(text string) { got = append(got, text) })
	big := strings.Repeat("x", MaxLine+10)
	if _, err := io.WriteString(s, big); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[0]) != MaxLine {
		t.Fatalf("got %d lines, first of length %d; want 1 line of %d", len(got), len(got[0]), MaxLine)
	}
	if len(s.buf) != 10 {
		t.Fatalf("%d bytes buffered, want 10", len(s.buf))
	}
}

func TestRingWraps(t *testing.T) {
	r := newRing(3)
	if r.last(2) != nil {
		t.Fatal("empty ring should return nil")
	}
	for i := 1; i <= 5; i++ {
		r.push(Line{Text: fmt.Sprint(i)})
	}
	got := texts(r.last(10))
	if got != "3 4 5" {
		t.Fatalf("last(10) = %q, want 3 4 5", got)
	}
	if got := texts(r.last(2)); got != "4 5" {
		t.Fatalf("last(2) = %q, want 4 5", got)
	}
	if got := r.last(0); got != nil {
		t.Fatalf("last(0) = %v, want nil", got)
	}
}

func texts(lines []Line) string {
	parts := make([]string, len(lines))
	for i, l := range lines {
		parts[i] = l.Text
	}
	return strings.Join(parts, " ")
}

// fakeRuntime's Logs writes scripted chunks and then returns.
type fakeRuntime struct {
	stdout, stderr []string
	err            error
	block          bool // keep the stream open until ctx ends
}

func (f *fakeRuntime) Start(context.Context, runtime.Spec) (runtime.Instance, error) {
	return runtime.Instance{}, errors.New("not used")
}
func (f *fakeRuntime) Stop(context.Context, string, time.Duration) error { return nil }
func (f *fakeRuntime) Wait(context.Context, string) (int, error)         { return 0, nil }
func (f *fakeRuntime) List(context.Context) ([]runtime.Instance, error)  { return nil, nil }

func (f *fakeRuntime) Logs(ctx context.Context, _ string, _ time.Time, stdout, stderr io.Writer) error {
	for i := 0; i < len(f.stdout) || i < len(f.stderr); i++ {
		if i < len(f.stdout) {
			_, _ = io.WriteString(stdout, f.stdout[i])
		}
		if i < len(f.stderr) {
			_, _ = io.WriteString(stderr, f.stderr[i])
		}
	}
	if f.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.err
}

func newHub(t *testing.T, rt runtime.Runtime) *Hub {
	t.Helper()
	h := New(t.TempDir(), rt, nil)
	t.Cleanup(func() { _ = h.Close() })
	return h
}

func TestCollectStampsStreamsAndSplits(t *testing.T) {
	rt := &fakeRuntime{stdout: []string{"out", " one\nout two\n"}, stderr: []string{"err one\nerr ", "two"}}
	h := newHub(t, rt)
	inst := runtime.Instance{ID: "inst1", App: "hello", Release: "v2"}
	before := time.Now()
	if err := h.Collect(context.Background(), inst, time.Time{}); err != nil {
		t.Fatal(err)
	}
	lines := h.Sink("hello").Tail(10)
	if len(lines) != 4 {
		t.Fatalf("got %d lines: %v", len(lines), lines)
	}
	byStream := map[string][]string{}
	for _, l := range lines {
		if l.App != "hello" || l.Release != "v2" || l.Instance != "inst1" {
			t.Fatalf("bad stamp: %+v", l)
		}
		if l.Time.Before(before) {
			t.Fatalf("line not stamped at receipt: %v", l.Time)
		}
		byStream[l.Stream] = append(byStream[l.Stream], l.Text)
	}
	if got := strings.Join(byStream[StreamStdout], "|"); got != "out one|out two" {
		t.Fatalf("stdout %q", got)
	}
	if got := strings.Join(byStream[StreamStderr], "|"); got != "err one|err two" {
		t.Fatalf("stderr %q", got)
	}

	data, err := os.ReadFile(filepath.Join(h.dir, "hello", "current.log"))
	if err != nil {
		t.Fatal(err)
	}
	file := string(data)
	if strings.Count(file, "\n") != 4 || !strings.Contains(file, " v2 inst1 stdout out one\n") || !strings.Contains(file, " v2 inst1 stderr err two\n") {
		t.Fatalf("file:\n%s", file)
	}
}

func TestCollectReportsTheRuntimeError(t *testing.T) {
	want := errors.New("gone")
	h := newHub(t, &fakeRuntime{err: want})
	if err := h.Collect(context.Background(), runtime.Instance{ID: "x", App: "a"}, time.Time{}); !errors.Is(err, want) {
		t.Fatalf("err = %v, want %v", err, want)
	}
}

func TestSubscribeGetsTailThenLive(t *testing.T) {
	h := newHub(t, &fakeRuntime{})
	s := h.Sink("hello")
	for i := 1; i <= 5; i++ {
		s.Append(Line{App: "hello", Text: fmt.Sprint(i)})
	}
	ch, cancel := s.Subscribe(2)
	defer cancel()
	s.Append(Line{App: "hello", Text: "6"})
	var got []string
	for range 3 {
		got = append(got, (<-ch).Text)
	}
	if strings.Join(got, " ") != "4 5 6" {
		t.Fatalf("got %q, want 4 5 6", got)
	}
	cancel()
	if _, ok := <-ch; ok {
		t.Fatal("channel should be closed after cancel")
	}
	cancel()                                // a second cancel is harmless
	s.Append(Line{App: "hello", Text: "7"}) // and nothing panics
}

func TestSlowSubscriberDropsAndIsTold(t *testing.T) {
	h := newHub(t, &fakeRuntime{})
	s := h.Sink("hello")
	ch, cancel := s.Subscribe(0)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 1; i <= 200; i++ {
			s.Append(Line{App: "hello", Text: fmt.Sprint(i)})
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Append blocked on a subscriber that never reads")
	}

	// The reader resumes: it gets the first SubscriberBuffer lines, then
	// the marker for the rest, then whatever comes next.
	for i := 1; i <= SubscriberBuffer; i++ {
		if l := <-ch; l.Text != fmt.Sprint(i) {
			t.Fatalf("line %d: got %q", i, l.Text)
		}
	}
	select {
	case l := <-ch:
		t.Fatalf("unexpected line %q after the buffer drained", l.Text)
	default:
	}
	s.Append(Line{App: "hello", Text: "201"})
	marker := <-ch
	if marker.Stream != StreamSystem || marker.Text != fmt.Sprintf("dropped %d lines", 200-SubscriberBuffer) {
		t.Fatalf("marker = %+v", marker)
	}
	if l := <-ch; l.Text != "201" {
		t.Fatalf("after marker: %q", l.Text)
	}
	// Once caught up there is no second marker.
	s.Append(Line{App: "hello", Text: "202"})
	if l := <-ch; l.Text != "202" || l.Stream != "" {
		t.Fatalf("after catching up: %+v", l)
	}
}

func TestTwoSubscribersAreIndependent(t *testing.T) {
	h := newHub(t, &fakeRuntime{})
	s := h.Sink("hello")
	slow, cancelSlow := s.Subscribe(0)
	defer cancelSlow()
	fast, cancelFast := s.Subscribe(0)
	defer cancelFast()
	for i := 1; i <= SubscriberBuffer+10; i++ {
		s.Append(Line{Text: fmt.Sprint(i)})
		if l := <-fast; l.Text != fmt.Sprint(i) {
			t.Fatalf("fast reader got %q at %d", l.Text, i)
		}
	}
	if len(slow) != SubscriberBuffer {
		t.Fatalf("slow reader holds %d, want %d", len(slow), SubscriberBuffer)
	}
}

func TestForgetClosesSubscribersAndRemovesFiles(t *testing.T) {
	h := newHub(t, &fakeRuntime{})
	s := h.Sink("hello")
	s.Append(Line{Text: "x"})
	ch, cancel := s.Subscribe(1)
	defer cancel()
	if err := h.Forget("hello"); err != nil {
		t.Fatal(err)
	}
	if l, ok := <-ch; !ok || l.Text != "x" {
		t.Fatalf("preloaded line lost: %v %v", l, ok)
	}
	if _, ok := <-ch; ok {
		t.Fatal("channel should be closed after Forget")
	}
	if _, err := os.Stat(filepath.Join(h.dir, "hello")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("log dir still there: %v", err)
	}
	s.Append(Line{Text: "late"}) // the old sink discards
	if h.Sink("hello") == s {
		t.Fatal("a new sink should be made after Forget")
	}
	if err := h.Forget("never"); err != nil {
		t.Fatal(err)
	}
}

func TestFileRotation(t *testing.T) {
	h := newHub(t, &fakeRuntime{})
	h.RotateAt = 300
	s := h.Sink("hello")
	line := strings.Repeat("y", 50)
	for i := range 60 {
		s.Append(Line{Time: time.Unix(0, 0), Text: fmt.Sprintf("%02d %s", i, line)})
	}
	dir := filepath.Join(h.dir, "hello")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
		info, _ := e.Info()
		if info.Size() > 300 {
			t.Fatalf("%s is %d bytes, over the limit", e.Name(), info.Size())
		}
	}
	want := "current.log current.log.1 current.log.2 current.log.3 current.log.4 current.log.5"
	if got := strings.Join(names, " "); got != want {
		t.Fatalf("files: %s", got)
	}
	// The newest line is in current.log and the oldest surviving file holds
	// older lines than the newer ones.
	cur, _ := os.ReadFile(filepath.Join(dir, "current.log"))
	if !strings.Contains(string(cur), "59 ") {
		t.Fatalf("current.log lacks the last line:\n%s", cur)
	}
	old, _ := os.ReadFile(filepath.Join(dir, "current.log.5"))
	var first, last int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(old)), "1970-01-01T00:00:00.000000Z - -  %d", &first); err != nil {
		t.Fatalf("current.log.5:\n%s", old)
	}
	if _, err := fmt.Sscanf(strings.TrimSpace(string(cur)), "1970-01-01T00:00:00.000000Z - -  %d", &last); err != nil {
		t.Fatalf("current.log:\n%s", cur)
	}
	if first >= last {
		t.Fatalf("current.log.5 starts at %d, current.log at %d; older files must hold older lines", first, last)
	}
}

func TestHubClose(t *testing.T) {
	h := New(t.TempDir(), &fakeRuntime{}, nil)
	s := h.Sink("a")
	ch, cancel := s.Subscribe(0)
	defer cancel()
	if err := h.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := <-ch; ok {
		t.Fatal("subscriber should end on Close")
	}
	// After Close new sinks serve memory only.
	late := h.Sink("b")
	late.Append(Line{Text: "x"})
	if _, err := os.Stat(filepath.Join(h.dir, "b")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("file written after Close: %v", err)
	}
}

func TestCollectEndsWithContext(t *testing.T) {
	h := newHub(t, &fakeRuntime{block: true})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.Collect(ctx, runtime.Instance{ID: "x", App: "a"}, time.Now()) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Collect did not return after cancel")
	}
}

type countingObserver struct {
	mu           sync.Mutex
	lines, drops int
}

func (o *countingObserver) Line(string) { o.mu.Lock(); o.lines++; o.mu.Unlock() }
func (o *countingObserver) Dropped(_ string, n int) {
	o.mu.Lock()
	o.drops += n
	o.mu.Unlock()
}

func TestObserverCountsLinesAndDrops(t *testing.T) {
	h := newHub(t, &fakeRuntime{})
	obs := &countingObserver{}
	h.Observer = obs
	s := h.Sink("hello")
	_, cancel := s.Subscribe(0) // never read
	defer cancel()
	for i := range SubscriberBuffer + 10 {
		s.Append(Line{Text: fmt.Sprint(i)})
	}
	obs.mu.Lock()
	defer obs.mu.Unlock()
	if obs.lines != SubscriberBuffer+10 || obs.drops != 10 {
		t.Fatalf("lines %d drops %d", obs.lines, obs.drops)
	}
}
