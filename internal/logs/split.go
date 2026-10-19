package logs

import "bytes"

// splitter turns a stream of chunks into lines. Chunks end wherever the
// runtime's read happened to end, so a line is complete only at '\n'; the
// bytes after the last newline wait for the next chunk, and Flush emits
// them when the stream ends. A line longer than MaxLine is cut there and
// emitted as it is, so a runaway writer cannot grow the buffer forever.
type splitter struct {
	emit func(text string)
	buf  []byte
}

func newSplitter(emit func(string)) *splitter { return &splitter{emit: emit} }

func (s *splitter) Write(p []byte) (int, error) {
	s.buf = append(s.buf, p...)
	for {
		i := bytes.IndexByte(s.buf, '\n')
		n := i + 1 // consumed: the line and its newline
		if i < 0 {
			if len(s.buf) < MaxLine {
				return len(p), nil
			}
			i, n = MaxLine, MaxLine // no newline in sight: cut here
		}
		s.emit(string(bytes.TrimSuffix(s.buf[:i], []byte{'\r'})))
		s.buf = s.buf[n:]
	}
}

// Flush emits whatever is buffered as a final line.
func (s *splitter) Flush() {
	if len(s.buf) == 0 {
		return
	}
	s.emit(string(bytes.TrimSuffix(s.buf, []byte{'\r'})))
	s.buf = nil
}
