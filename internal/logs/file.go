package logs

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"

	"github.com/maddoxrjohnson/ashore/internal/runtime"
)

// rotatingFile appends lines to path and, when the file reaches rotateAt
// bytes, renames it to path.1 (path.1 to path.2, and so on up to keepFiles)
// and starts a new one. Nothing reads the old files back yet; they exist so
// an operator can.
type rotatingFile struct {
	path     string
	rotateAt int64
	f        *os.File
	size     int64
}

func openRotating(path string, rotateAt int64) (*rotatingFile, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	r := &rotatingFile{path: path, rotateAt: rotateAt}
	if err := r.open(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *rotatingFile) open() error {
	f, err := os.OpenFile(r.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return err
	}
	r.f, r.size = f, st.Size()
	return nil
}

// WriteLine appends one line in the on-disk format: time, release,
// instance, stream, text. The time is UTC with microseconds, so lines from
// two instances during a deploy sort by the collector's clock.
func (r *rotatingFile) WriteLine(l Line) error {
	rec := l.Time.UTC().Format("2006-01-02T15:04:05.000000Z") + " " + orDash(l.Release) + " " + orDash(runtime.Short(l.Instance)) + " " + l.Stream + " " + l.Text + "\n"
	if r.size > 0 && r.size+int64(len(rec)) > r.rotateAt {
		if err := r.rotate(); err != nil {
			return fmt.Errorf("rotate %s: %w", r.path, err)
		}
	}
	n, err := r.f.WriteString(rec)
	r.size += int64(n)
	return err
}

// rotate closes the current file and shifts the numbered ones up by one;
// the oldest is removed.
func (r *rotatingFile) rotate() error {
	if err := r.f.Close(); err != nil {
		return err
	}
	for i := keepFiles - 1; i >= 1; i-- {
		err := os.Rename(r.numbered(i), r.numbered(i+1))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	if err := os.Rename(r.path, r.numbered(1)); err != nil {
		return err
	}
	return r.open()
}

func (r *rotatingFile) numbered(i int) string { return r.path + "." + strconv.Itoa(i) }

func (r *rotatingFile) Close() error { return r.f.Close() }

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
