// Package node serves the local node API independently of VM lifecycle storage.
//
//	HTTP health probes -> node -> runtime RPC -> KumaBox daemon -> VMM / guest
//
// Future template, pool and lease services will use the same runtime boundary.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"
)

// RuntimeChecker is the read-only dependency needed by node readiness.
// More runtime methods belong to the services that actually consume them.
type RuntimeChecker interface {
	Check(context.Context) error
}

// Options bounds dependency probes and graceful server shutdown.
type Options struct {
	// OperationTimeout bounds template operations independently of health probes.
	OperationTimeout time.Duration
	// ProbeTimeout caps the runtime call made by each /readyz request.
	ProbeTimeout time.Duration
	// ShutdownTimeout caps draining requests after the process is signaled.
	ShutdownTimeout time.Duration
}

// Server owns the HTTP API, but never owns runtime processes or databases.
type Server struct {
	runtime   RuntimeChecker
	templates TemplateCatalog
	pools     PoolManager
	options   Options
	logger    *slog.Logger
	handler   http.Handler
}

// New validates required dependencies once and installs the node routes.
func New(runtime RuntimeChecker, templates TemplateCatalog, options Options, logger *slog.Logger, extensions ...Option) (*Server, error) {
	if runtime == nil || templates == nil || logger == nil {
		return nil, errors.New("runtime checker, template catalog and logger are required")
	}
	if options.ProbeTimeout <= 0 || options.ShutdownTimeout <= 0 || options.OperationTimeout <= 0 {
		return nil, errors.New("probe, operation and shutdown timeouts must be positive")
	}
	server := &Server{runtime: runtime, templates: templates, options: options, logger: logger}
	for _, extend := range extensions {
		if extend == nil {
			return nil, errors.New("nil node option")
		}
		if err := extend(server); err != nil {
			return nil, err
		}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		server.respond(w, http.StatusOK, "ok")
	})
	mux.HandleFunc("GET /readyz", server.ready)
	mux.HandleFunc("POST /v1/templates", server.registerTemplate)
	mux.HandleFunc("GET /v1/templates", server.listTemplates)
	mux.HandleFunc("GET /v1/templates/{name}", server.getTemplate)
	if server.pools != nil {
		mux.HandleFunc("POST /v1/pools", server.configurePool)
		mux.HandleFunc("GET /v1/pools", server.listPools)
		mux.HandleFunc("GET /v1/pools/{name}", server.poolStatus)
		mux.HandleFunc("POST /v1/pools/{name}/claims", server.claim)
		mux.HandleFunc("GET /v1/leases/{id}", server.getLease)
		mux.HandleFunc("DELETE /v1/leases/{id}", server.release)
		mux.HandleFunc("PUT /v1/leases/{id}/expiry", server.renew)
	}
	server.handler = mux
	return server, nil
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	if s.pools != nil && !s.pools.Ready() {
		s.respond(w, http.StatusServiceUnavailable, "not_ready")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.options.ProbeTimeout)
	defer cancel()
	if err := s.runtime.Check(ctx); err != nil {
		s.logger.DebugContext(ctx, "runtime not ready", "error", err)
		s.respond(w, http.StatusServiceUnavailable, "not_ready")
		return
	}
	s.respond(w, http.StatusOK, "ready")
}

func (s *Server) respond(w http.ResponseWriter, status int, state string) {
	s.writeJSON(w, status, struct {
		Status string `json:"status"`
	}{Status: state})
}

// Run takes ownership of listener and drains requests when ctx is canceled.
// Stopping KumaBoxd leaves the KumaBox daemon and running sandboxes untouched.
func (s *Server) Run(ctx context.Context, listener net.Listener) error {
	defer func() { _ = listener.Close() }()
	if ctx.Err() != nil {
		return nil
	}
	server := &http.Server{
		Handler:           s.handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      max(s.options.ProbeTimeout, s.options.OperationTimeout) + 5*time.Second,
		IdleTimeout:       time.Minute,
		MaxHeaderBytes:    16 << 10,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}
	finished := make(chan error, 1)
	go func() { finished <- server.Serve(listener) }()
	select {
	case err := <-finished:
		return fmt.Errorf("serve node API: %w", err)
	case <-ctx.Done():
	}
	// The caller is canceled; draining gets a separate, bounded lifetime.
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.options.ShutdownTimeout)
	defer cancel()
	err := server.Shutdown(shutdownCtx)
	if err != nil {
		err = errors.Join(err, server.Close())
	}
	if serveErr := <-finished; !errors.Is(serveErr, http.ErrServerClosed) {
		err = errors.Join(err, serveErr)
	}
	return err
}
