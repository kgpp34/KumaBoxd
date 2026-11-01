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
	"github.com/kgpp34/kumaboxd/internal/metadata"
	"github.com/kgpp34/kumaboxd/internal/node"
	"github.com/kgpp34/kumaboxd/internal/pool"
	"github.com/kgpp34/kumaboxd/internal/runtimeclient"
	"github.com/kgpp34/kumaboxd/internal/template"
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
	client, err := runtimeclient.New(cfg.RuntimeSocket, cfg.OperationTimeout)
	if err != nil {
		logger.Error("configure runtime client", "error", err)
		return 1
	}
	defer func() { _ = client.Close() }()
	store, err := metadata.Open(cfg.StateDir)
	if err != nil {
		logger.Error("open node metadata", "error", err)
		return 1
	}
	defer func() {
		if err := store.Close(); err != nil {
			logger.Error("close node metadata", "error", err)
		}
	}()
	templates, err := template.New(client, store)
	if err != nil {
		logger.Error("configure templates", "error", err)
		return 1
	}
	poolOptions := cfg.Pool
	poolOptions.OperationTimeout = cfg.OperationTimeout
	inventory, err := pool.New(store, client, templates, pool.FixedPolicy{}, poolOptions, logger)
	if err != nil {
		logger.Error("configure warm pools", "error", err)
		return 1
	}
	server, err := node.New(client, templates, node.Options{
		ProbeTimeout: cfg.ProbeTimeout, ShutdownTimeout: cfg.ShutdownTimeout, OperationTimeout: cfg.OperationTimeout,
	}, logger, node.WithPools(inventory))
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
	serviceCtx, cancelService := context.WithCancel(ctx)
	defer cancelService()
	poolResult := make(chan error, 1)
	go func() { err := inventory.Run(serviceCtx); poolResult <- err; cancelService() }()
	serveErr := server.Run(serviceCtx, listener)
	cancelService()
	if err := errors.Join(serveErr, <-poolResult); err != nil {
		logger.Error("node service stopped", "error", err)
		return 1
	}
	return 0
}
