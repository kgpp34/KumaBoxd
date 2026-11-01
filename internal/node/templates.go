package node

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"github.com/kgpp34/kumaboxd/internal/runtimeclient"
	"github.com/kgpp34/kumaboxd/internal/template"
)

// TemplateCatalog is the application contract consumed by the node HTTP API.
type TemplateCatalog interface {
	Register(context.Context, template.RegisterRequest) (template.Template, bool, error)
	Get(context.Context, string) (template.Template, error)
	List(context.Context, string, int) (template.Page, error)
}

func (s *Server) registerTemplate(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.options.OperationTimeout)
	defer cancel()
	var request template.RegisterRequest
	if !s.decodeRequest(w, r, &request) {
		return
	}
	record, created, err := s.templates.Register(ctx, request)
	if err != nil {
		s.templateError(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	w.Header().Set("Location", "/v1/templates/"+url.PathEscape(record.Name))
	s.writeJSON(w, status, record)
}

func (s *Server) getTemplate(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.options.OperationTimeout)
	defer cancel()
	record, err := s.templates.Get(ctx, r.PathValue("name"))
	if err != nil {
		s.templateError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, record)
}

func (s *Server) listTemplates(w http.ResponseWriter, r *http.Request) {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		s.badRequest(w, errors.New("malformed query"))
		return
	}
	limit := 50
	if raw := query.Get("limit"); raw != "" {
		limit, err = strconv.Atoi(raw)
		if err != nil {
			s.badRequest(w, errors.New("limit must be an integer between 1 and 200"))
			return
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.options.OperationTimeout)
	defer cancel()
	page, err := s.templates.List(ctx, query.Get("after"), limit)
	if err != nil {
		s.templateError(w, r, err)
		return
	}
	s.writeJSON(w, http.StatusOK, page)
}

func (s *Server) templateError(w http.ResponseWriter, r *http.Request, err error) {
	status, code, message := http.StatusInternalServerError, "INTERNAL", "template operation failed"
	var remote *runtimeclient.RemoteError
	var transport *runtimeclient.HTTPError
	var requestError *runtimeclient.RequestError
	switch {
	case errors.Is(err, template.ErrInvalid):
		status, code, message = http.StatusBadRequest, "INVALID_ARGUMENT", err.Error()
	case errors.Is(err, template.ErrNotFound):
		status, code, message = http.StatusNotFound, "NOT_FOUND", "template not found"
	case errors.Is(err, template.ErrConflict):
		status, code, message = http.StatusConflict, "NAME_TAKEN", "template name already identifies another snapshot or configuration"
	case errors.Is(err, context.DeadlineExceeded):
		status, code, message = http.StatusGatewayTimeout, "TIMEOUT", "template operation timed out"
	case errors.Is(err, context.Canceled):
		status, code, message = http.StatusRequestTimeout, "CANCELED", "template operation canceled"
	case errors.As(err, &remote):
		status, code, message = http.StatusBadGateway, remote.Code, "runtime rejected template resolution"
		if remote.Code == "NOT_FOUND" {
			status, message = http.StatusNotFound, "snapshot or image not found in runtime"
		}
	case errors.As(err, &transport):
		status, code, message = http.StatusServiceUnavailable, "RUNTIME_UNAVAILABLE", "runtime is unavailable"
	case errors.As(err, &requestError):
		status, code, message = http.StatusServiceUnavailable, "RUNTIME_UNAVAILABLE", "runtime is unavailable"
	}
	s.logger.ErrorContext(r.Context(), "template operation failed", "error", err)
	s.writeJSON(w, status, apiError{Code: code, Message: message})
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (s *Server) badRequest(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	var limit *http.MaxBytesError
	if errors.As(err, &limit) {
		status = http.StatusRequestEntityTooLarge
	}
	s.writeJSON(w, status, apiError{Code: "INVALID_ARGUMENT", Message: err.Error()})
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		s.logger.Debug("write JSON response", "error", err)
	}
}
