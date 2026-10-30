package node

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type checkerFunc func(context.Context) error

func (f checkerFunc) Check(ctx context.Context) error { return f(ctx) }

func TestHealthAndReadinessHaveDifferentSemantics(t *testing.T) {
	for _, available := range []bool{false, true} {
		t.Run(map[bool]string{false: "unavailable", true: "available"}[available], func(t *testing.T) {
			calls := 0
			server := newServer(t, func(context.Context) error {
				calls++
				if !available {
					return errors.New("private runtime diagnostic")
				}
				return nil
			})
			live := httptest.NewRecorder()
			server.handler.ServeHTTP(live, httptest.NewRequest(http.MethodGet, "/healthz", nil))
			if live.Code != http.StatusOK || calls != 0 {
				t.Fatalf("liveness depends on runtime: HTTP %d, calls %d", live.Code, calls)
			}
			ready := httptest.NewRecorder()
			server.handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
			want := http.StatusServiceUnavailable
			body := "{\"status\":\"not_ready\"}\n"
			if available {
				want, body = http.StatusOK, "{\"status\":\"ready\"}\n"
			}
			if ready.Code != want || ready.Body.String() != body || calls != 1 {
				t.Fatalf("readiness: HTTP %d body %q calls %d", ready.Code, ready.Body.String(), calls)
			}
		})
	}
}

func TestReadinessHasDeadline(t *testing.T) {
	server := newServer(t, func(ctx context.Context) error {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("runtime probe has no deadline")
		}
		<-ctx.Done()
		return ctx.Err()
	})
	response := httptest.NewRecorder()
	server.handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("HTTP %d, want 503", response.Code)
	}
}

func TestRunCancelsInflightProbeAndClosesListener(t *testing.T) {
	entered := make(chan struct{})
	server := newServer(t, func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- server.Run(ctx, listener) }()
	requestFinished := make(chan error, 1)
	go func() {
		client := &http.Client{Timeout: 5 * time.Second}
		defer client.CloseIdleConnections()
		response, err := client.Get("http://" + listener.Addr().String() + "/readyz")
		if err == nil {
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
		requestFinished <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("readiness probe did not arrive")
	}
	cancel()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not drain")
	}
	if err := <-requestFinished; err != nil {
		t.Fatal(err)
	}
	conn, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if err == nil {
		_ = conn.Close()
		t.Fatal("listener remains open after shutdown")
	}
}

func newServer(t *testing.T, check checkerFunc) *Server {
	t.Helper()
	server, err := New(check, Options{ProbeTimeout: 100 * time.Millisecond, ShutdownTimeout: time.Second},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return server
}
