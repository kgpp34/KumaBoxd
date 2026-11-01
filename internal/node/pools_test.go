package node

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kgpp34/kumaboxd/internal/pool"
)

type poolStub struct{ claimError error }

func (p poolStub) Configure(context.Context, pool.Spec) (pool.Record, bool, error) {
	return pool.Record{}, true, nil
}
func (p poolStub) Status(context.Context, string) (pool.Status, error) { return pool.Status{}, nil }
func (p poolStub) List(context.Context) ([]pool.Status, error)         { return []pool.Status{}, nil }
func (p poolStub) Claim(context.Context, string, pool.ClaimRequest) (pool.Lease, bool, error) {
	return pool.Lease{}, true, p.claimError
}
func (p poolStub) GetLease(context.Context, string) (pool.Lease, error) { return pool.Lease{}, nil }
func (p poolStub) Release(context.Context, string) (pool.Lease, error) {
	return pool.Lease{State: pool.Releasing}, nil
}
func (p poolStub) Renew(context.Context, string, time.Time) (pool.Lease, error) {
	return pool.Lease{}, nil
}

func TestPoolAPIBoundsInputAndMapsAllocationErrors(t *testing.T) {
	for _, test := range []struct {
		name, method, path, body, key string
		failure                       error
		status                        int
	}{
		{name: "missing key", method: "POST", path: "/v1/pools/python/claims", body: `{"owner":"one","ttl_seconds":60}`, status: 400},
		{name: "unknown field", method: "POST", path: "/v1/pools/python/claims", body: `{"owner":"one","unexpected":true}`, key: "one", status: 400},
		{name: "multiple values", method: "POST", path: "/v1/pools", body: `{} {}`, status: 400},
		{name: "oversized", method: "POST", path: "/v1/pools", body: `{"name":"` + strings.Repeat("a", 65536) + `"}`, status: 413},
		{name: "empty", method: "POST", path: "/v1/pools/python/claims", body: `{}`, key: "one", failure: pool.ErrEmpty, status: 503},
		{name: "conflict", method: "POST", path: "/v1/pools/python/claims", body: `{}`, key: "one", failure: pool.ErrConflict, status: 409},
		{name: "expired", method: "POST", path: "/v1/pools/python/claims", body: `{}`, key: "one", failure: pool.ErrExpired, status: 410},
		{name: "recovering", method: "POST", path: "/v1/pools/python/claims", body: `{}`, key: "one", failure: pool.ErrUnavailable, status: 503},
		{name: "private failure", method: "POST", path: "/v1/pools/python/claims", body: `{}`, key: "one", failure: errors.New("private diagnostic"), status: 500},
	} {
		t.Run(test.name, func(t *testing.T) {
			base := newServer(t, func(context.Context) error { return nil })
			server, err := New(base.runtime, base.templates, base.options, base.logger, WithPools(poolStub{claimError: test.failure}))
			if err != nil {
				t.Fatal(err)
			}
			response := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, test.path, strings.NewReader(test.body))
			if test.key != "" {
				request.Header.Set("Idempotency-Key", test.key)
			}
			server.handler.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("HTTP %d want %d: %s", response.Code, test.status, response.Body.String())
			}
			if strings.Contains(response.Body.String(), "private diagnostic") {
				t.Fatal("private error leaked")
			}
			if errors.Is(test.failure, pool.ErrEmpty) && response.Header().Get("Retry-After") == "" {
				t.Fatal("exhaustion did not describe retry behavior")
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("allocation response can be cached")
			}
		})
	}
}

func (poolStub) Ready() bool { return true }
