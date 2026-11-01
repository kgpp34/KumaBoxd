package node

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/kgpp34/kumaboxd/internal/pool"
	"github.com/kgpp34/kumaboxd/internal/template"
)

// Inventory manages configured pools; allocation is a separate small contract.
type Inventory interface {
	Configure(context.Context, pool.Spec) (pool.Record, bool, error)
	Status(context.Context, string) (pool.Status, error)
	List(context.Context) ([]pool.Status, error)
}

// Allocator returns allocation receipts with explicit durability without exposing storage handles.
type Allocator interface {
	Claim(context.Context, string, pool.ClaimRequest) (pool.Lease, bool, error)
	GetLease(context.Context, string) (pool.Lease, error)
	Release(context.Context, string) (pool.Lease, error)
	Renew(context.Context, string, time.Time) (pool.Lease, error)
}
type PoolManager interface {
	Ready() bool
	Inventory
	Allocator
}

// Option installs an explicitly supplied node capability.
type Option func(*Server) error

// WithPools exposes local pool/lease APIs. Authentication and remote exposure are
// separate concerns; these routes retain the server's loopback-only boundary.
func WithPools(manager PoolManager) Option {
	return func(s *Server) error {
		if manager == nil {
			return errors.New("pool manager is required")
		}
		s.pools = manager
		return nil
	}
}

func (s *Server) configurePool(w http.ResponseWriter, r *http.Request) {
	var request pool.Spec
	if !s.decodeRequest(w, r, &request) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.options.OperationTimeout)
	defer cancel()
	result, created, err := s.pools.Configure(ctx, request)
	if err != nil {
		s.poolError(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	s.writeJSON(w, status, result)
}
func (s *Server) poolStatus(w http.ResponseWriter, r *http.Request) {
	result, err := s.pools.Status(r.Context(), r.PathValue("name"))
	if err != nil {
		s.poolError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, result)
}
func (s *Server) listPools(w http.ResponseWriter, r *http.Request) {
	result, err := s.pools.List(r.Context())
	if err != nil {
		s.poolError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, struct {
		Items []pool.Status `json:"items"`
	}{result})
}
func (s *Server) claim(w http.ResponseWriter, r *http.Request) {
	var request pool.ClaimRequest
	if !s.decodeRequest(w, r, &request) {
		return
	}
	if len(r.Header.Values("Idempotency-Key")) != 1 {
		s.poolError(w, r, pool.ErrInvalid)
		return
	}
	request.Key = r.Header.Get("Idempotency-Key")
	ctx, cancel := context.WithTimeout(r.Context(), s.options.OperationTimeout)
	defer cancel()
	result, created, err := s.pools.Claim(ctx, r.PathValue("name"), request)
	if err != nil {
		s.poolError(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	s.writeJSON(w, status, result)
}
func (s *Server) getLease(w http.ResponseWriter, r *http.Request) {
	result, err := s.pools.GetLease(r.Context(), r.PathValue("id"))
	if err != nil {
		s.poolError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, result)
}
func (s *Server) release(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.options.OperationTimeout)
	defer cancel()
	result, err := s.pools.Release(ctx, r.PathValue("id"))
	if err != nil {
		s.poolError(w, r, err)
		return
	}
	status := http.StatusAccepted
	if result.State == pool.Released || result.State == pool.Lost {
		status = http.StatusOK
	}
	s.writeJSON(w, status, result)
}
func (s *Server) renew(w http.ResponseWriter, r *http.Request) {
	var request struct {
		ExpiresAt time.Time `json:"expires_at"`
	}
	if !s.decodeRequest(w, r, &request) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.options.OperationTimeout)
	defer cancel()
	result, err := s.pools.Renew(ctx, r.PathValue("id"), request.ExpiresAt)
	if err != nil {
		s.poolError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, result)
}
func (s *Server) decodeRequest(w http.ResponseWriter, r *http.Request, value any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		s.badRequest(w, err)
		return false
	}
	if err := decoder.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("request must contain exactly one JSON object")
		}
		s.badRequest(w, err)
		return false
	}
	return true
}
func (s *Server) poolError(w http.ResponseWriter, r *http.Request, err error) {
	status, code, message := http.StatusInternalServerError, "INTERNAL", "pool operation failed"
	switch {
	case errors.Is(err, pool.ErrInvalid), errors.Is(err, template.ErrInvalid):
		status, code, message = http.StatusBadRequest, "INVALID_ARGUMENT", err.Error()
	case errors.Is(err, pool.ErrNotFound), errors.Is(err, template.ErrNotFound):
		status, code, message = http.StatusNotFound, "NOT_FOUND", "pool resource not found"
	case errors.Is(err, pool.ErrConflict):
		status, code, message = http.StatusConflict, "CONFLICT", "idempotency key or pool binding conflicts with existing state"
	case errors.Is(err, pool.ErrExpired):
		status, code, message = http.StatusGone, "LEASE_EXPIRED", "lease is no longer active"
	case errors.Is(err, pool.ErrEmpty):
		status, code, message = http.StatusServiceUnavailable, "POOL_EMPTY", "no ready sandbox available"
		w.Header().Set("Retry-After", "1")
	case errors.Is(err, pool.ErrUnavailable):
		status, code, message = http.StatusServiceUnavailable, "NOT_READY", "pool controller is recovering"
	case errors.Is(err, context.Canceled):
		status, code, message = http.StatusRequestTimeout, "CANCELED", "pool operation canceled; retry mutations with the same identity"
	case errors.Is(err, context.DeadlineExceeded):
		status, code, message = http.StatusGatewayTimeout, "TIMEOUT", "pool operation timed out; retry mutations with the same identity"
	}
	if status >= 500 && !errors.Is(err, pool.ErrEmpty) && !errors.Is(err, pool.ErrUnavailable) {
		s.logger.ErrorContext(r.Context(), "pool operation failed", "error", err)
	}
	s.writeJSON(w, status, apiError{Code: code, Message: message})
}
