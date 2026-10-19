package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/maddoxrjohnson/ashore/internal/config"
)

func TestNewLogger(t *testing.T) {
	cases := []struct {
		name   string
		format string
		level  slog.Level
		debug  bool   // log at debug instead of info
		want   string // substring of the output; empty means nothing is written
	}{
		{"json by default", config.LogFormatJSON, slog.LevelInfo, false, `"msg":"hello"`},
		{"text on request", config.LogFormatText, slog.LevelInfo, false, "msg=hello"},
		{"debug hidden at info", config.LogFormatJSON, slog.LevelInfo, true, ""},
		{"debug shown at debug", config.LogFormatJSON, slog.LevelDebug, true, `"msg":"hello"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.Default()
			cfg.LogFormat, cfg.LogLevel = tc.format, tc.level
			var buf bytes.Buffer
			l := newLogger(&buf, cfg)
			if tc.debug {
				l.Debug("hello", "k", "v")
			} else {
				l.Info("hello", "k", "v")
			}
			got := buf.String()
			if tc.want == "" {
				if got != "" {
					t.Fatalf("expected no output, got %q", got)
				}
				return
			}
			if !strings.Contains(got, tc.want) {
				t.Errorf("output %q does not contain %q", got, tc.want)
			}
			if tc.format == config.LogFormatJSON && !json.Valid(buf.Bytes()) {
				t.Errorf("output is not valid JSON: %q", got)
			}
		})
	}
}
