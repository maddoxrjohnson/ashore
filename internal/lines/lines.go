// Package lines turns a stream of writes into lines. Output from a build,
// a deploy, or an app arrives in chunks that end wherever a read happened
// to end, so a line is complete only at '\n'; the bytes after the last
// newline wait for the next write, and Flush hands over whatever is left
// when the stream ends.
package lines

import "bytes"

// Writer is an io.Writer that calls Emit once per complete line, without
// the newline and without a trailing '\r'. With Max set, a line that long
// with no newline in sight is cut there and emitted, so a writer that
// never ends a line cannot grow the buffer without bound. Not safe for
// concurrent use; callers that need that hold their own lock.
type Writer struct {
	Emit func(line string) error
	Max  int

	buf []byte
}

func (w *Writer) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		n := i + 1 // consumed: the line and its newline
		if i < 0 {
			if w.Max <= 0 || len(w.buf) < w.Max {
				return len(p), nil
			}
			i, n = w.Max, w.Max
		}
		if err := w.Emit(string(bytes.TrimSuffix(w.buf[:i], []byte{'\r'}))); err != nil {
			return 0, err
		}
		w.buf = w.buf[n:]
	}
}

// Flush emits what is buffered as a final line. Nothing buffered emits
// nothing.
func (w *Writer) Flush() error {
	if len(w.buf) == 0 {
		return nil
	}
	line := string(bytes.TrimSuffix(w.buf, []byte{'\r'}))
	w.buf = nil
	return w.Emit(line)
}

// Pending reports whether an unfinished line is buffered.
func (w *Writer) Pending() bool { return len(w.buf) > 0 }
