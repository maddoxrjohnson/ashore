package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestDefaults(t *testing.T) {
	got, err := load(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if got != Default() {
		t.Errorf("load(empty) = %+v, want %+v", got, Default())
	}
}

func TestHostKeyFollowsDataDir(t *testing.T) {
	env := map[string]string{"ASHORE_DATA_DIR": "/var/lib/ashore"}
	got, err := load(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if want := "/var/lib/ashore/host_ed25519"; got.SSHHostKey != want {
		t.Errorf("SSHHostKey = %q, want %q", got.SSHHostKey, want)
	}
}

func TestOverrides(t *testing.T) {
	env := map[string]string{
		"ASHORE_DATA_DIR":          "/srv/ashore",
		"ASHORE_DOMAIN":            "example.test",
		"ASHORE_HTTP_ADDR":         ":80",
		"ASHORE_HTTPS_ADDR":        ":443",
		"ASHORE_ACME_EMAIL":        "ops@example.test",
		"ASHORE_SSH_ADDR":          ":22",
		"ASHORE_SSH_HOST_KEY":      "/etc/ashore/key",
		"ASHORE_INTERNAL_ADDR":     "127.0.0.1:9001",
		"ASHORE_RUNTIME":           "native",
		"ASHORE_DOCKER_HOST":       "unix:///run/docker.sock",
		"ASHORE_BUILD_CONCURRENCY": "4",
		"ASHORE_BUILD_TIMEOUT":     "20m",
		"ASHORE_HEALTH_TIMEOUT":    "1m",
		"ASHORE_DRAIN_GRACE":       "2s",
		"ASHORE_STOP_GRACE":        "15s",
		"ASHORE_RATE_LIMIT_RPS":    "0",
		"ASHORE_LOG_LEVEL":         "debug",
		"ASHORE_LOG_FORMAT":        "text",
	}
	want := Config{
		DataDir:          "/srv/ashore",
		Domain:           "example.test",
		HTTPAddr:         ":80",
		HTTPSAddr:        ":443",
		ACMEEmail:        "ops@example.test",
		SSHAddr:          ":22",
		SSHHostKey:       "/etc/ashore/key",
		InternalAddr:     "127.0.0.1:9001",
		Runtime:          RuntimeNative,
		DockerHost:       "unix:///run/docker.sock",
		BuildConcurrency: 4,
		BuildTimeout:     20 * time.Minute,
		HealthTimeout:    time.Minute,
		DrainGrace:       2 * time.Second,
		StopGrace:        15 * time.Second,
		RateLimitRPS:     0,
		LogLevel:         slog.LevelDebug,
		LogFormat:        LogFormatText,
	}
	got, err := load(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("load = %+v\nwant %+v", got, want)
	}
}

func TestInvalid(t *testing.T) {
	cases := []struct {
		name    string
		env     map[string]string
		wantVar string
	}{
		{"duration", map[string]string{"ASHORE_BUILD_TIMEOUT": "10 minutes"}, "ASHORE_BUILD_TIMEOUT"},
		{"int", map[string]string{"ASHORE_BUILD_CONCURRENCY": "two"}, "ASHORE_BUILD_CONCURRENCY"},
		{"addr without port", map[string]string{"ASHORE_HTTP_ADDR": "8080"}, "ASHORE_HTTP_ADDR"},
		{"addr with empty port", map[string]string{"ASHORE_SSH_ADDR": "localhost:"}, "ASHORE_SSH_ADDR"},
		{"concurrency zero", map[string]string{"ASHORE_BUILD_CONCURRENCY": "0"}, "ASHORE_BUILD_CONCURRENCY"},
		{"negative rps", map[string]string{"ASHORE_RATE_LIMIT_RPS": "-1"}, "ASHORE_RATE_LIMIT_RPS"},
		{"negative duration", map[string]string{"ASHORE_STOP_GRACE": "-5s"}, "ASHORE_STOP_GRACE"},
		{"runtime", map[string]string{"ASHORE_RUNTIME": "podman"}, "ASHORE_RUNTIME"},
		{"log level", map[string]string{"ASHORE_LOG_LEVEL": "loud"}, "ASHORE_LOG_LEVEL"},
		{"log format", map[string]string{"ASHORE_LOG_FORMAT": "yaml"}, "ASHORE_LOG_FORMAT"},
		{"https without email", map[string]string{"ASHORE_HTTPS_ADDR": ":443"}, "ASHORE_ACME_EMAIL"},
		{"https addr", map[string]string{"ASHORE_HTTPS_ADDR": "443", "ASHORE_ACME_EMAIL": "a@b.c"}, "ASHORE_HTTPS_ADDR"},
		{"internal addr", map[string]string{"ASHORE_INTERNAL_ADDR": "9000"}, "ASHORE_INTERNAL_ADDR"},
		{"rps not int", map[string]string{"ASHORE_RATE_LIMIT_RPS": "many"}, "ASHORE_RATE_LIMIT_RPS"},
		{"health not duration", map[string]string{"ASHORE_HEALTH_TIMEOUT": "1 hour"}, "ASHORE_HEALTH_TIMEOUT"},
		{"drain not duration", map[string]string{"ASHORE_DRAIN_GRACE": "soon"}, "ASHORE_DRAIN_GRACE"},
		{"stop not duration", map[string]string{"ASHORE_STOP_GRACE": "later"}, "ASHORE_STOP_GRACE"},
		{"build timeout zero", map[string]string{"ASHORE_BUILD_TIMEOUT": "0"}, "ASHORE_BUILD_TIMEOUT"},
		{"health negative", map[string]string{"ASHORE_HEALTH_TIMEOUT": "-30s"}, "ASHORE_HEALTH_TIMEOUT"},
		{"drain zero", map[string]string{"ASHORE_DRAIN_GRACE": "0s"}, "ASHORE_DRAIN_GRACE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := load(func(k string) string { return tc.env[k] })
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantVar) {
				t.Errorf("err = %q, want it to name %s", err, tc.wantVar)
			}
		})
	}
}

// Load reads the real process environment; t.Setenv restores the variable
// after the test and forbids t.Parallel for that reason.
func TestLoadReadsProcessEnv(t *testing.T) {
	t.Setenv("ASHORE_DOMAIN", "example.test")
	got, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Domain != "example.test" {
		t.Errorf("Domain = %q, want %q", got.Domain, "example.test")
	}
}

// Only a hand-built Config can have empty strings where load would have
// substituted a default, so validate is called directly here.
func TestValidateHandBuilt(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantVar string
	}{
		{"empty data dir", func(c *Config) { c.DataDir = "" }, "ASHORE_DATA_DIR"},
		{"empty domain", func(c *Config) { c.Domain = "" }, "ASHORE_DOMAIN"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := Default()
			tc.mutate(&c)
			err := c.validate()
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantVar) {
				t.Errorf("err = %q, want it to name %s", err, tc.wantVar)
			}
		})
	}
}
