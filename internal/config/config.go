package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// hostKeyFile is the SSH host key's file name inside DataDir.
const hostKeyFile = "host_ed25519"

// Accepted values for Config.Runtime and Config.LogFormat.
const (
	RuntimeDocker = "docker"
	RuntimeNative = "native"
	LogFormatJSON = "json"
	LogFormatText = "text"
)

// Config holds every ASHORE_* setting. Loaded once at startup, read-only after.
type Config struct {
	DataDir          string
	Domain           string
	HTTPAddr         string
	HTTPSAddr        string // empty disables TLS
	ACMEEmail        string
	SSHAddr          string
	SSHHostKey       string
	InternalAddr     string
	Runtime          string // RuntimeDocker or RuntimeNative
	DockerHost       string
	BuildConcurrency int
	BuildTimeout     time.Duration
	HealthTimeout    time.Duration
	DrainGrace       time.Duration
	StopGrace        time.Duration
	RateLimitRPS     int // 0 disables
	LogLevel         slog.Level
	LogFormat        string // LogFormatJSON or LogFormatText
}

// Default returns the settings used when no ASHORE_* variable is set.
func Default() Config {
	return Config{
		DataDir:          "./data",
		Domain:           "localhost",
		HTTPAddr:         ":8080",
		SSHAddr:          ":2222",
		SSHHostKey:       filepath.Join("./data", hostKeyFile),
		InternalAddr:     "127.0.0.1:9000",
		Runtime:          RuntimeDocker,
		BuildConcurrency: 2,
		BuildTimeout:     10 * time.Minute,
		HealthTimeout:    30 * time.Second,
		DrainGrace:       5 * time.Second,
		StopGrace:        10 * time.Second,
		RateLimitRPS:     100,
		LogLevel:         slog.LevelInfo,
		LogFormat:        LogFormatJSON,
	}
}

// Load reads ASHORE_* from the process environment.
func Load() (Config, error) {
	return load(os.Getenv)
}

// load is Load with the environment lookup injected so tests never touch the
// process environment. An empty value is treated as unset.
func load(getenv func(string) string) (Config, error) {
	c := Default()
	if v := getenv("ASHORE_DATA_DIR"); v != "" {
		c.DataDir = v
	}
	if v := getenv("ASHORE_DOMAIN"); v != "" {
		c.Domain = v
	}
	if v := getenv("ASHORE_HTTP_ADDR"); v != "" {
		c.HTTPAddr = v
	}
	if v := getenv("ASHORE_HTTPS_ADDR"); v != "" {
		c.HTTPSAddr = v
	}
	if v := getenv("ASHORE_ACME_EMAIL"); v != "" {
		c.ACMEEmail = v
	}
	if v := getenv("ASHORE_SSH_ADDR"); v != "" {
		c.SSHAddr = v
	}
	// The host key follows DataDir unless set explicitly, so DataDir must be read first.
	if v := getenv("ASHORE_SSH_HOST_KEY"); v != "" {
		c.SSHHostKey = v
	} else {
		c.SSHHostKey = filepath.Join(c.DataDir, hostKeyFile)
	}
	if v := getenv("ASHORE_INTERNAL_ADDR"); v != "" {
		c.InternalAddr = v
	}
	if v := getenv("ASHORE_RUNTIME"); v != "" {
		c.Runtime = v
	}
	if v := getenv("ASHORE_DOCKER_HOST"); v != "" {
		c.DockerHost = v
	}
	if v := getenv("ASHORE_LOG_FORMAT"); v != "" {
		c.LogFormat = v
	}

	var err error
	c.BuildConcurrency, err = lookupInt(getenv, "ASHORE_BUILD_CONCURRENCY", c.BuildConcurrency)
	if err != nil {
		return Config{}, err
	}
	c.RateLimitRPS, err = lookupInt(getenv, "ASHORE_RATE_LIMIT_RPS", c.RateLimitRPS)
	if err != nil {
		return Config{}, err
	}
	c.BuildTimeout, err = lookupDuration(getenv, "ASHORE_BUILD_TIMEOUT", c.BuildTimeout)
	if err != nil {
		return Config{}, err
	}
	c.HealthTimeout, err = lookupDuration(getenv, "ASHORE_HEALTH_TIMEOUT", c.HealthTimeout)
	if err != nil {
		return Config{}, err
	}
	c.DrainGrace, err = lookupDuration(getenv, "ASHORE_DRAIN_GRACE", c.DrainGrace)
	if err != nil {
		return Config{}, err
	}
	c.StopGrace, err = lookupDuration(getenv, "ASHORE_STOP_GRACE", c.StopGrace)
	if err != nil {
		return Config{}, err
	}
	if v := getenv("ASHORE_LOG_LEVEL"); v != "" {
		if err := c.LogLevel.UnmarshalText([]byte(v)); err != nil {
			return Config{}, fmt.Errorf("ASHORE_LOG_LEVEL: %w", err)
		}
	}
	if err := c.validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// validate checks ranges, enums, listener addresses, and the cross-field
// rules. Every error starts with the variable name so the operator can fix it
// without reading this file.
func (c Config) validate() error {
	if c.DataDir == "" {
		return errors.New("ASHORE_DATA_DIR: must not be empty")
	}
	if c.Domain == "" {
		return errors.New("ASHORE_DOMAIN: must not be empty")
	}
	if err := checkAddr("ASHORE_HTTP_ADDR", c.HTTPAddr); err != nil {
		return err
	}
	if err := checkAddr("ASHORE_SSH_ADDR", c.SSHAddr); err != nil {
		return err
	}
	if err := checkAddr("ASHORE_INTERNAL_ADDR", c.InternalAddr); err != nil {
		return err
	}
	if c.HTTPSAddr != "" {
		if err := checkAddr("ASHORE_HTTPS_ADDR", c.HTTPSAddr); err != nil {
			return err
		}
		if c.ACMEEmail == "" {
			return errors.New("ASHORE_ACME_EMAIL: required when ASHORE_HTTPS_ADDR is set")
		}
	}
	switch c.Runtime {
	case RuntimeDocker, RuntimeNative:
	default:
		return fmt.Errorf("ASHORE_RUNTIME: %q is not %s or %s", c.Runtime, RuntimeDocker, RuntimeNative)
	}
	if c.BuildConcurrency < 1 {
		return fmt.Errorf("ASHORE_BUILD_CONCURRENCY: %d is less than 1", c.BuildConcurrency)
	}
	if c.BuildTimeout <= 0 {
		return fmt.Errorf("ASHORE_BUILD_TIMEOUT: %v is not positive", c.BuildTimeout)
	}
	if c.HealthTimeout <= 0 {
		return fmt.Errorf("ASHORE_HEALTH_TIMEOUT: %v is not positive", c.HealthTimeout)
	}
	if c.DrainGrace <= 0 {
		return fmt.Errorf("ASHORE_DRAIN_GRACE: %v is not positive", c.DrainGrace)
	}
	if c.StopGrace <= 0 {
		return fmt.Errorf("ASHORE_STOP_GRACE: %v is not positive", c.StopGrace)
	}
	if c.RateLimitRPS < 0 {
		return fmt.Errorf("ASHORE_RATE_LIMIT_RPS: %d is negative", c.RateLimitRPS)
	}
	switch c.LogFormat {
	case LogFormatJSON, LogFormatText:
	default:
		return fmt.Errorf("ASHORE_LOG_FORMAT: %q is not %s or %s", c.LogFormat, LogFormatJSON, LogFormatText)
	}
	return nil
}

// checkAddr accepts anything net.Listen would take as a TCP address, such as
// ":8080", "127.0.0.1:9000", or "[::1]:80". SplitHostPort allows an empty port,
// so that is rejected here.
func checkAddr(key, val string) error {
	_, port, err := net.SplitHostPort(val)
	if err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	if port == "" {
		return fmt.Errorf("%s: %q has no port", key, val)
	}
	return nil
}

// lookupInt returns def when key is unset, otherwise the parsed value.
// Parse errors are prefixed with the variable name.
func lookupInt(getenv func(string) string, key string, def int) (int, error) {
	v := getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func lookupDuration(getenv func(string) string, key string, def time.Duration) (time.Duration, error) {
	v := getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}
