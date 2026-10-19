package lines

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func collect(max int) (*Writer, *[]string) {
	var got []string
	w := &Writer{Max: max, Emit: func(l string) error { got = append(got, l); return nil }}
	return w, &got
}

func TestSplitsAcrossWrites(t *testing.T) {
	cases := []struct {
		name   string
		chunks []string
		want   []string
	}{
		{"one write", []string{"a\nb\n"}, []string{"a", "b"}},
		{"split mid-line", []string{"hel", "lo\nwor", "ld\n"}, []string{"hello", "world"}},
		{"unterminated final line", []string{"ab", "c\nde"}, []string{"abc", "de"}},
		{"crlf", []string{"a\r\nb\r\n"}, []string{"a", "b"}},
		{"empty lines kept", []string{"\n\nx\n"}, []string{"", "", "x"}},
		{"newline alone", []string{"ab", "\n"}, []string{"ab"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, got := collect(0)
			for _, c := range tc.chunks {
				if _, err := io.WriteString(w, c); err != nil {
					t.Fatal(err)
				}
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
			if strings.Join(*got, "|") != strings.Join(tc.want, "|") {
				t.Fatalf("got %q, want %q", *got, tc.want)
			}
		})
	}
}

func TestCutsRunawayLines(t *testing.T) {
	w, got := collect(8)
	if _, err := io.WriteString(w, strings.Repeat("x", 19)); err != nil {
		t.Fatal(err)
	}
	if len(*got) != 2 || (*got)[0] != "xxxxxxxx" || !w.Pending() {
		t.Fatalf("got %q pending %v", *got, w.Pending())
	}
	_ = w.Flush()
	if len(*got) != 3 || (*got)[2] != "xxx" || w.Pending() {
		t.Fatalf("after flush %q", *got)
	}
	if err := w.Flush(); err != nil || len(*got) != 3 {
		t.Fatal("a second flush emits nothing")
	}
}

func TestEmitErrorStopsTheWrite(t *testing.T) {
	boom := errors.New("boom")
	w := &Writer{Emit: func(string) error { return boom }}
	if _, err := io.WriteString(w, "a\nb\n"); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}
