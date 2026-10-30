// Package config reads process configuration at the application boundary.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/netip"
	"path/filepath"
	"strconv"
	"time"
)

// Config contains startup settings. It is immutable after Parse returns.
type Config struct {
	// Listen is the loopback-only HTTP endpoint for node health probes.
	Listen string
	// RuntimeSocket points to the existing KumaBox daemon, not its database.
	RuntimeSocket string
	// StateDir owns KumaBoxd metadata independently of the runtime data root.
	StateDir string
	// OperationTimeout bounds runtime requests and template registration.
	OperationTimeout time.Duration
	// ProbeTimeout bounds one runtime readiness request.
	ProbeTimeout time.Duration
	// ShutdownTimeout bounds draining the node HTTP server.
	ShutdownTimeout time.Duration
	// ShowVersion prints the build version without starting a server.
	ShowVersion bool
}

// Parse applies explicit flags over environment variables over defaults.
// getenv and output are supplied by the entry point so tests need no globals.
func Parse(args []string, getenv func(string) string, output io.Writer) (Config, error) {
	cfg := Config{
		Listen:           envOr(getenv, "KUMABOXD_LISTEN", "127.0.0.1:8088"),
		RuntimeSocket:    envOr(getenv, "KUMABOXD_RUNTIME_SOCKET", "/run/kumabox/kumabox.sock"),
		StateDir:         envOr(getenv, "KUMABOXD_STATE_DIR", "/var/lib/kumaboxd"),
		OperationTimeout: 2 * time.Minute,
		ProbeTimeout:     2 * time.Second,
		ShutdownTimeout:  5 * time.Second,
	}
	flags := flag.NewFlagSet("kumaboxd", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&cfg.Listen, "listen", cfg.Listen, "loopback HTTP address (KUMABOXD_LISTEN)")
	flags.StringVar(&cfg.RuntimeSocket, "runtime-socket", cfg.RuntimeSocket, "KumaBox Unix socket (KUMABOXD_RUNTIME_SOCKET)")
	flags.StringVar(&cfg.StateDir, "state-dir", cfg.StateDir, "node metadata directory (KUMABOXD_STATE_DIR)")
	flags.DurationVar(&cfg.OperationTimeout, "operation-timeout", cfg.OperationTimeout, "runtime and template operation timeout")
	flags.DurationVar(&cfg.ProbeTimeout, "probe-timeout", cfg.ProbeTimeout, "runtime readiness timeout")
	flags.DurationVar(&cfg.ShutdownTimeout, "shutdown-timeout", cfg.ShutdownTimeout, "HTTP shutdown timeout")
	flags.BoolVar(&cfg.ShowVersion, "version", false, "print build version and exit")
	flags.Usage = func() {
		_, _ = fmt.Fprintln(output, "Usage: kumaboxd [flags]\n\nKumaBox node service with persistent template registration. Warm-pool APIs are not implemented yet.\n\nFlags:")
		flags.SetOutput(output)
		flags.PrintDefaults()
		flags.SetOutput(io.Discard)
	}
	if err := flags.Parse(args); err != nil {
		return Config{}, err
	}
	if flags.NArg() != 0 {
		return Config{}, errors.New("unexpected positional arguments; use --help")
	}
	if !cfg.ShowVersion {
		if err := cfg.Validate(); err != nil {
			return Config{}, err
		}
	}
	return cfg, nil
}

// Validate rejects ambiguous sockets and non-local HTTP exposure in this bootstrap.
func (c Config) Validate() error {
	host, port, err := net.SplitHostPort(c.Listen)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	address, err := netip.ParseAddr(host)
	if err != nil || !address.IsLoopback() {
		return errors.New("listen must use a loopback IP address, such as 127.0.0.1:8088")
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 0 || number > 65535 {
		return errors.New("listen port must be between 0 and 65535")
	}
	if !filepath.IsAbs(c.RuntimeSocket) {
		return errors.New("runtime-socket must be an absolute Unix socket path")
	}
	if !filepath.IsAbs(c.StateDir) {
		return errors.New("state-dir must be an absolute path")
	}
	if c.ProbeTimeout <= 0 || c.ShutdownTimeout <= 0 || c.OperationTimeout <= 0 ||
		c.OperationTimeout > 24*time.Hour || c.ProbeTimeout > 24*time.Hour || c.ShutdownTimeout > 24*time.Hour {
		return errors.New("timeouts must be positive and must not exceed 24h")
	}
	return nil
}

func envOr(getenv func(string) string, key, fallback string) string {
	if value := getenv(key); value != "" {
		return value
	}
	return fallback
}
