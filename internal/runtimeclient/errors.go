package runtimeclient

import (
	"context"
	"fmt"
)

// RemoteError preserves the runtime's classified failure and commit boundary.
type RemoteError struct {
	Message   string `json:"message"`
	Class     uint8  `json:"class"`
	Code      string `json:"code"`
	Committed bool   `json:"committed"`
	Canceled  bool   `json:"canceled"`
	Deadline  bool   `json:"deadline"`
}

func (e *RemoteError) Error() string { return e.Message }

// Unwrap preserves cancellation semantics across the RPC boundary.
func (e *RemoteError) Unwrap() error {
	if e.Canceled {
		return context.Canceled
	}
	if e.Deadline {
		return context.DeadlineExceeded
	}
	return nil
}

// HTTPError represents rejection before a terminal RPC result was received.
type HTTPError struct {
	Status    int
	Operation string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("runtime %s: HTTP %d", e.Operation, e.Status)
}

// OutcomeUnknownError means a mutation has no trustworthy terminal result.
// Callers must inspect resource state before deciding whether to retry it.
type OutcomeUnknownError struct {
	Operation string
	Cause     error
}

func (e *OutcomeUnknownError) Error() string {
	return fmt.Sprintf("runtime %s outcome unknown; inspect state before retrying: %v", e.Operation, e.Cause)
}

func (e *OutcomeUnknownError) Unwrap() error { return e.Cause }

// RequestError identifies a transport or response failure on a read-only request.
type RequestError struct {
	Operation string
	Cause     error
}

func (e *RequestError) Error() string { return fmt.Sprintf("runtime %s: %v", e.Operation, e.Cause) }
func (e *RequestError) Unwrap() error { return e.Cause }

func interrupted(method string, err error, mutation bool) error {
	if mutation {
		return &OutcomeUnknownError{Operation: method, Cause: err}
	}
	return &RequestError{Operation: method, Cause: err}
}
