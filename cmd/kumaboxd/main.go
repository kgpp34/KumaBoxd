// Command kumaboxd starts the local node service backed by KumaBox runtime RPC.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/kgpp34/kumaboxd/internal/config"
	"github.com/kgpp34/kumaboxd/internal/node"
	"github.com/kgpp34/kumaboxd/internal/runtimeclient"
)

// version is injected by the release build; local go run builds use dev.
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args []string, getenv func(string) string, output, diagnostics io.Writer) int {
	cfg, err := config.Parse(args, getenv, output)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		_, _ = fmt.Fprintln(diagnostics, "kumaboxd:", err)
		return 2
	}
	if cfg.ShowVersion {
		_, _ = fmt.Fprintln(output, "kumaboxd", version)
		return 0
	}
	logger := slog.New(slog.NewJSONHandler(diagnostics, nil))
	client, err := runtimeclient.New(cfg.RuntimeSocket)
	if err != nil {
		logger.Error("configure runtime client", "error", err)
		return 1
	}
	defer func() { _ = client.Close() }()
	server, err := node.New(client, node.Options{
		ProbeTimeout: cfg.ProbeTimeout, ShutdownTimeout: cfg.ShutdownTimeout,
	}, logger)
	if err != nil {
		logger.Error("configure node service", "error", err)
		return 1
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		logger.Error("listen", "error", err)
		return 1
	}
	logger.Info("starting node service", "listen", listener.Addr().String(),
		"runtime_socket", cfg.RuntimeSocket, "version", version)
	if err := server.Run(ctx, listener); err != nil {
		logger.Error("node service stopped", "error", err)
		return 1
	}
	return 0
}
